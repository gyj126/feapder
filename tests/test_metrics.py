# -*- coding: utf-8 -*-
"""
metrics 单元测试

通过 monkeypatch 替换 InfluxDBClient，把所有写入的 Point 收集到内存中并断言，
不依赖真实的 InfluxDB 服务。真连 InfluxDB 的 smoke 用 @pytest.mark.integration 标记，
默认不跑。
"""

import time
from typing import List

import pytest
from influxdb_client import Point

from feapder import setting
import feapder.utils.metrics as metrics_mod
from feapder.utils import metrics


class FakeWriteAPI:
    def __init__(self):
        self.records: List[Point] = []

    def write(self, bucket=None, org=None, record=None, **_):
        if record is None:
            return
        if isinstance(record, list):
            self.records.extend(record)
        else:
            self.records.append(record)

    def flush(self):
        pass

    def close(self):
        pass


class FakeClient:
    last_instance: "FakeClient" = None

    def __init__(self, *args, **kwargs):
        self.write_api_ = FakeWriteAPI()
        FakeClient.last_instance = self

    def write_api(self, *args, **kwargs):
        return self.write_api_

    def close(self):
        pass


@pytest.fixture(autouse=True)
def fake_influx(request, monkeypatch):
    """单元测试用 FakeClient 收集写入；integration 用例不 mock，连真实服务"""
    if request.node.get_closest_marker("integration"):
        monkeypatch.setattr(setting, "INFLUXDB_ENABLE", True)
        yield None
        return
    monkeypatch.setattr(setting, "INFLUXDB_ENABLE", True)
    monkeypatch.setattr(metrics_mod, "InfluxDBClient", FakeClient)
    metrics.close()
    metrics.init(
        url="http://localhost:8086",
        token="fake-token",
        org="feapder",
        bucket="feapder",
        spider="ut",
        runtime_interval=0,
    )
    yield FakeClient.last_instance.write_api_
    metrics.close()


def _records(api: FakeWriteAPI):
    """把收集到的 Point 转成可断言的 dict 列表"""
    out = []
    for p in api.records:
        # influxdb_client.Point 没有公开属性，只能拿 line protocol 来断言
        out.append(p.to_line_protocol())
    return out


def test_emit_download_counter_aggregates_within_second(fake_influx):
    for _ in range(50):
        metrics.emit_download(parser="demo", status="download_total", count=2)
    metrics.flush()
    lines = _records(fake_influx)
    assert any("feapder_download" in l and "count=100i" in l for l in lines), lines
    # 同秒同 tag 应聚合为一个点
    download_lines = [l for l in lines if l.startswith("feapder_download")]
    assert len(download_lines) == 1, download_lines
    assert "spider=ut" in download_lines[0]
    assert "parser=demo" in download_lines[0]
    assert "status=download_total" in download_lines[0]


def test_emit_item(fake_influx):
    metrics.emit_item(table="t1", count=10)
    metrics.emit_item(table="t1", count=5)
    metrics.emit_item(table="t2", count=7)
    metrics.flush()
    lines = _records(fake_influx)
    item_lines = [l for l in lines if l.startswith("feapder_item")]
    assert len(item_lines) == 2, item_lines
    by_table = {l.split("table=")[1].split(",")[0].split(" ")[0]: l for l in item_lines}
    assert "count=15i" in by_table["t1"]
    assert "count=7i" in by_table["t2"]
    assert all("field=" not in l for l in item_lines), "feapder_item 不应再有 field tag"


def test_emit_user_only_status(fake_influx):
    metrics.emit_user(status="success", count=3)
    metrics.emit_user(status="overdue", count=1)
    metrics.flush()
    lines = _records(fake_influx)
    user_lines = [l for l in lines if l.startswith("feapder_user")]
    assert len(user_lines) == 2
    assert all("user_id" not in l for l in user_lines), "emit_user 不应再有 user_id tag"


def test_emit_proxy(fake_influx):
    metrics.emit_proxy(event="pull", count=10)
    metrics.emit_proxy(event="use", count=100)
    metrics.emit_proxy(event="invalid", count=2)
    metrics.flush()
    lines = _records(fake_influx)
    proxy_lines = [l for l in lines if l.startswith("feapder_proxy")]
    assert len(proxy_lines) == 3


def test_emit_custom_counter(fake_influx):
    metrics.emit_custom("api_call", count=10, tags={"endpoint": "/login"})
    metrics.emit_custom("api_call", count=5, tags={"endpoint": "/login"})
    metrics.flush()
    lines = _records(fake_influx)
    custom_lines = [l for l in lines if l.startswith("feapder_custom")]
    assert len(custom_lines) == 1
    assert "count=15i" in custom_lines[0]
    assert "metric=api_call" in custom_lines[0]
    assert "endpoint=/login" in custom_lines[0]


def test_emit_custom_value_and_duration(fake_influx):
    metrics.emit_custom("queue_size", value=128)
    metrics.emit_custom("render_time", duration=1.23)
    metrics.flush()
    lines = _records(fake_influx)
    assert any("metric=queue_size" in l and "value=128i" in l for l in lines), lines
    assert any("metric=render_time" in l and "duration=1.23" in l for l in lines), lines


def test_emit_custom_spider_user_overrides_default(fake_influx):
    """显式传入的 spider 优先于 init 时的 _default_spider"""
    metrics.emit_custom("biz", count=1, spider="other")
    metrics.flush()
    lines = _records(fake_influx)
    biz_line = next(l for l in lines if "metric=biz" in l)
    assert "spider=other" in biz_line, biz_line


def test_emit_custom_default_spider_fallback(fake_influx):
    """未显式传入 spider 时回落到 init 设置的默认值"""
    metrics.emit_custom("biz2", count=1)
    metrics.flush()
    lines = _records(fake_influx)
    biz_line = next(l for l in lines if "metric=biz2" in l)
    assert "spider=ut" in biz_line, biz_line


def test_emit_does_not_raise_on_internal_error(fake_influx, monkeypatch):
    """底层写入抛异常时 emit_* 必须吞掉，不影响业务"""

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(metrics_mod._emitter, "emit_counter", boom)
    metrics.emit_download(parser="x", status="download_total")
    metrics.emit_item(table="t", count=1)
    metrics.emit_proxy(event="use")
    metrics.emit_user(status="ok")
    metrics.emit_custom("x", count=1)


def test_timer_timestamp_is_monotonic_ns(fake_influx):
    """timer 类型每个点都应有独立的纳秒时间戳，避免覆盖"""
    for _ in range(50):
        metrics.emit_custom("step", duration=0.01)
    metrics.flush()
    lines = _records(fake_influx)
    step_lines = [l for l in lines if "metric=step" in l]
    assert len(step_lines) == 50
    timestamps = [int(l.rsplit(" ", 1)[-1]) for l in step_lines]
    assert len(set(timestamps)) == len(timestamps), "timer 时间戳必须互不相同"


def test_close_resets_state(fake_influx):
    metrics.emit_download(parser="demo", status="download_total")
    metrics.close()
    assert metrics_mod._emitter is None
    assert metrics_mod._inited_pid is None
    metrics.emit_download(parser="demo", status="download_total")


@pytest.mark.integration
def test_real_influxdb_smoke():
    """需要本地起 InfluxDB 才会跑：pytest -m integration"""
    metrics.close()
    metrics.init(
        url="http://localhost:8086",
        token="my-token",
        org="feapder",
        bucket="feapder",
        spider="test_metrics_smoke",
        debug=True,
        runtime_interval=0,
    )
    metrics.emit_download(parser="demo", status="download_total", count=10)
    metrics.emit_item(table="t1", count=5)
    metrics.emit_proxy(event="pull", count=1)
    metrics.emit_user(status="success", count=1)
    metrics.emit_custom("smoke", count=1, tags={"case": "ci"})
    metrics.flush()
    time.sleep(1)
    metrics.close()
