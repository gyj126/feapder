# -*- coding: utf-8 -*-
"""
基于 InfluxDB 2.x 的打点监控模块

设计要点：
- 使用 influxdb-client 官方 2.x 客户端（url / token / org / bucket）
- 按业务场景拆分 measurement：feapder_download / feapder_item / feapder_proxy / feapder_user / feapder_custom
- counter 类型同 key 自动累加，timer 类型用纳秒级时间戳错位避免覆盖
- 进程内单例 emitter，多进程下按 pid 重新初始化
"""

import concurrent.futures
import os
import queue
import random
import socket
import string
import threading
import time
from collections import Counter
from typing import Any, Optional

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from feapder import setting
from feapder.utils.log import log
from feapder.utils.tools import aio_wrap, ensure_float, ensure_int

# measurement 常量
MEASUREMENT_DOWNLOAD = "feapder_download"
MEASUREMENT_ITEM = "feapder_item"
MEASUREMENT_PROXY = "feapder_proxy"
MEASUREMENT_USER = "feapder_user"
MEASUREMENT_CUSTOM = "feapder_custom"

# 内部 tag 标识，仅用于 emitter 内部聚合，不会写入 InfluxDB
_INTERNAL_TYPE_KEY = "__type__"
_TYPE_COUNTER = "counter"
_TYPE_STORE = "store"
_TYPE_TIMER = "timer"

_inited_pid: Optional[int] = None
# 协程打点专用线程池，fork 后的子进程不应继承
_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="metrics"
)


class MetricsEmitter:
    """InfluxDB 2.x 打点缓冲与批量写入"""

    def __init__(
        self,
        client: InfluxDBClient,
        bucket: str,
        org: str,
        *,
        batch_size: int = 500,
        emit_interval: int = 10,
        max_points: int = 10240,
        max_timer_seq: int = 0,
        ratio: float = 1.0,
        debug: bool = False,
        add_hostname: bool = False,
        default_tags: Optional[dict] = None,
    ):
        """
        Args:
            client: InfluxDB 2.x 客户端
            bucket: 写入的 bucket
            org: 所属 org
            batch_size: 每次 flush 写入的最大点数
            emit_interval: 最长打点间隔（秒），到点强制 flush
            max_points: 本地缓冲区最大累计点数
            max_timer_seq: 单位时间内同 key 的 timer 最大数量，0 表示不限制
            ratio: store / timer 类型采样率
            debug: 是否打印调试日志
            add_hostname: 是否将 hostname 作为 tag
            default_tags: 公共 tag，会附加到每一个数据点上
        """
        self._client = client
        self._bucket = bucket
        self._org = org
        self._write_api = client.write_api(write_options=SYNCHRONOUS)

        self._pending_points: queue.Queue = queue.Queue()
        self._batch_size = batch_size
        self._emit_interval = emit_interval
        self._max_points = max_points
        self._max_timer_seq = max_timer_seq
        self._ratio = ratio
        self._debug = debug
        self._add_hostname = add_hostname
        self._default_tags = default_tags or {}
        self._hostname = socket.gethostname()

        self._lock = threading.Lock()
        self._last_emit_ts = time.time()

    def make_point(
        self,
        measurement: str,
        tags: Optional[dict] = None,
        fields: Optional[dict] = None,
        timestamp: Optional[int] = None,
        point_type: str = _TYPE_COUNTER,
    ) -> dict:
        """
        构造内部点结构，时间戳默认秒级
        """
        assert measurement, "measurement 不能为空"
        merged_tags = dict(self._default_tags)
        if tags:
            for k, v in tags.items():
                if v is None or v == "":
                    continue
                merged_tags[k] = str(v)
        if self._add_hostname and "host" not in merged_tags:
            merged_tags["host"] = self._hostname

        merged_tags[_INTERNAL_TYPE_KEY] = point_type

        return dict(
            measurement=measurement,
            tags=merged_tags,
            fields=fields or {},
            time=timestamp if timestamp is not None else int(time.time()),
        )

    def emit(self, point: Optional[dict] = None, force: bool = False):
        """
        加入待发送队列，按需触发 flush
        """
        if point is not None:
            self._pending_points.put(point)

        if not (
            force
            or self._pending_points.qsize() >= self._max_points
            or time.time() - self._last_emit_ts > self._emit_interval
        ):
            return

        with self._lock:
            points = self._drain_and_aggregate(force=force)
            if not points:
                return
            try:
                self._write_api.write(
                    bucket=self._bucket,
                    org=self._org,
                    record=points,
                    write_precision=WritePrecision.NS,
                )
                if self._debug:
                    log.info(f"打点写入 {len(points)} 条")
            except Exception as e:
                log.exception(f"打点写入失败: {e}")
            self._last_emit_ts = time.time()

    def flush(self):
        if self._debug:
            log.info(f"flush 打点队列，剩余 {self._pending_points.qsize()} 条")
        self.emit(force=True)

    def close(self):
        try:
            self.flush()
        except Exception as e:
            log.exception(f"flush 异常: {e}")
        try:
            self._write_api.close()
        except Exception as e:
            log.exception(f"关闭 write_api 异常: {e}")
        try:
            self._client.close()
        except Exception as e:
            log.exception(f"关闭 InfluxDB 客户端异常: {e}")

    def _drain_and_aggregate(self, force: bool = False) -> list:
        """
        从 pending 队列取出点并按类型聚合，返回 influxdb-client Point 列表
        """
        raw_points = []
        while len(raw_points) < self._max_points or force:
            try:
                raw_points.append(self._pending_points.get_nowait())
            except queue.Empty:
                break

        if not raw_points:
            return []

        # 同 key 累加 counter，timer 通过纳秒错位避免覆盖
        counters: dict = {}
        timer_seqs: Counter = Counter()
        result_points: list = []

        for point in raw_points:
            point_type = point["tags"].pop(_INTERNAL_TYPE_KEY, _TYPE_COUNTER)
            tagset = self._tagset(point)

            if point_type == _TYPE_COUNTER:
                if tagset in counters:
                    for fk, fv in point["fields"].items():
                        counters[tagset]["fields"][fk] = (
                            counters[tagset]["fields"].get(fk, 0) + fv
                        )
                else:
                    counters[tagset] = point
            elif point_type == _TYPE_TIMER:
                if self._max_timer_seq and timer_seqs[tagset] > self._max_timer_seq:
                    continue
                if self._ratio < 1.0 and random.random() > self._ratio:
                    continue
                point["time"] = self._to_ns(point["time"], offset=timer_seqs[tagset])
                timer_seqs[tagset] += 1
                result_points.append(point)
            else:
                if self._ratio < 1.0 and random.random() > self._ratio:
                    continue
                point["time"] = self._to_ns(point["time"])
                result_points.append(point)

        for point in counters.values():
            point["time"] = self._to_ns(point["time"])
            result_points.append(point)

        return [self._to_influx_point(p) for p in result_points]

    @staticmethod
    def _tagset(point: dict) -> str:
        return f"{point['measurement']}-{sorted(point['tags'].items())}-{point['time']}"

    @staticmethod
    def _to_ns(timestamp: int, offset: int = 0) -> int:
        """
        将秒级时间戳补足到 19 位纳秒级，offset 用于 timer 错位
        """
        if timestamp >= 10**18:
            return timestamp + offset
        ts_str = str(timestamp)
        pad_len = 19 - len(ts_str)
        if pad_len <= 0:
            return timestamp + offset
        random_str = "".join(random.sample(string.digits, pad_len))
        return int(ts_str + random_str) + offset

    @staticmethod
    def _to_influx_point(raw: dict) -> Point:
        point = Point(raw["measurement"]).time(raw["time"], WritePrecision.NS)
        for tk, tv in raw["tags"].items():
            point = point.tag(tk, tv)
        for fk, fv in raw["fields"].items():
            point = point.field(fk, fv)
        return point


_emitter: Optional[MetricsEmitter] = None
_default_spider: Optional[str] = None


def init(
    *,
    url: Optional[str] = None,
    token: Optional[str] = None,
    org: Optional[str] = None,
    bucket: Optional[str] = None,
    spider: Optional[str] = None,
    default_tags: Optional[dict] = None,
    batch_size: Optional[int] = None,
    emit_interval: Optional[int] = None,
    debug: Optional[bool] = None,
    add_hostname: Optional[bool] = None,
    timeout: int = 10000,
    **kwargs,
):
    """
    初始化打点监控

    Args:
        url: InfluxDB 2.x 地址，默认读取 setting.INFLUXDB_URL
        token: 访问 token，默认读取 setting.INFLUXDB_TOKEN
        org: 组织名，默认读取 setting.INFLUXDB_ORG
        bucket: 写入的 bucket，默认读取 setting.INFLUXDB_BUCKET
        spider: 爬虫名，会作为 default tag 写入每个点
        default_tags: 公共 tag
        batch_size: 单次写入最大点数
        emit_interval: 最长打点间隔（秒）
        debug: 是否输出调试日志
        add_hostname: 是否将 hostname 作为 tag
        timeout: 客户端超时时间（毫秒）
        **kwargs: 透传给 MetricsEmitter 的其他参数
    """
    global _inited_pid, _emitter, _default_spider

    if _inited_pid == os.getpid():
        return

    url = url or setting.INFLUXDB_URL
    token = token or setting.INFLUXDB_TOKEN
    org = org or setting.INFLUXDB_ORG
    bucket = bucket or setting.INFLUXDB_BUCKET

    if not (url and token and org and bucket):
        return

    merged_tags = dict(setting.METRICS_DEFAULT_TAGS)
    if default_tags:
        merged_tags.update(default_tags)
    if spider:
        merged_tags.setdefault("spider", spider)
        _default_spider = spider

    try:
        client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout)
    except Exception as e:
        log.error(f"初始化 InfluxDB 客户端失败: {e}")
        return

    _emitter = MetricsEmitter(
        client=client,
        bucket=bucket,
        org=org,
        batch_size=batch_size if batch_size is not None else setting.METRICS_BATCH_SIZE,
        emit_interval=(
            emit_interval
            if emit_interval is not None
            else setting.METRICS_EMIT_INTERVAL
        ),
        debug=debug if debug is not None else setting.METRICS_DEBUG,
        add_hostname=(
            add_hostname if add_hostname is not None else setting.METRICS_ADD_HOSTNAME
        ),
        default_tags=merged_tags,
        **kwargs,
    )
    _inited_pid = os.getpid()
    log.info(f"打点监控初始化成功 url={url} bucket={bucket}")


def _resolve_spider(spider: Optional[str]) -> Optional[str]:
    return spider or _default_spider


def emit_download(
    spider: Optional[str] = None,
    parser: Optional[str] = None,
    status: str = "",
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """
    下载/解析打点

    Args:
        spider: 爬虫名
        parser: parser 名
        status: download_total / download_success / download_exception / parser_exception
        count: 计数
        timestamp: 时间戳，默认当前时间
    """
    if _emitter is None:
        return
    tags = {
        "spider": _resolve_spider(spider),
        "parser": parser,
        "status": status,
    }
    fields = {"count": ensure_int(count)}
    point = _emitter.make_point(
        MEASUREMENT_DOWNLOAD, tags, fields, timestamp, _TYPE_COUNTER
    )
    _emitter.emit(point)


def emit_item(
    spider: Optional[str] = None,
    table: str = "",
    field: Optional[str] = None,
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """
    数据入库打点

    Args:
        spider: 爬虫名
        table: 表名
        field: 字段名；不传或传空时用 __total__ 表示总条数
        count: 计数
        timestamp: 时间戳
    """
    if _emitter is None:
        return
    tags = {
        "spider": _resolve_spider(spider),
        "table": table,
        "field": field if field else "__total__",
    }
    fields = {"count": ensure_int(count)}
    point = _emitter.make_point(
        MEASUREMENT_ITEM, tags, fields, timestamp, _TYPE_COUNTER
    )
    _emitter.emit(point)


def emit_proxy(
    spider: Optional[str] = None,
    event: str = "",
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """
    代理池打点

    Args:
        spider: 爬虫名
        event: pull / use / invalid
        count: 计数
        timestamp: 时间戳
    """
    if _emitter is None:
        return
    tags = {
        "spider": _resolve_spider(spider),
        "event": event,
    }
    fields = {"count": ensure_int(count)}
    point = _emitter.make_point(
        MEASUREMENT_PROXY, tags, fields, timestamp, _TYPE_COUNTER
    )
    _emitter.emit(point)


def emit_user(
    spider: Optional[str] = None,
    user_id: str = "",
    status: str = "",
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """
    账号池打点

    Args:
        spider: 爬虫名
        user_id: 账号 id
        status: 账号状态（如 GoldUserStatus.value）
        count: 计数
        timestamp: 时间戳
    """
    if _emitter is None:
        return
    tags = {
        "spider": _resolve_spider(spider),
        "user_id": user_id,
        "status": status,
    }
    fields = {"count": ensure_int(count)}
    point = _emitter.make_point(MEASUREMENT_USER, tags, fields, timestamp, _TYPE_COUNTER)
    _emitter.emit(point)


def emit_custom(
    metric: str,
    *,
    count: Optional[int] = None,
    value: Any = None,
    duration: Optional[float] = None,
    spider: Optional[str] = None,
    tags: Optional[dict] = None,
    measurement: str = MEASUREMENT_CUSTOM,
    timestamp: Optional[int] = None,
):
    """
    用户自定义打点，count / value / duration 三选一

    Args:
        metric: 指标名，作为 tag
        count: 计数（counter 语义，可累加）
        value: 任意数值（store 语义，会被覆盖）
        duration: 时长（timer 语义，每个点独立保留）
        spider: 爬虫名
        tags: 额外的 tag
        measurement: 表名，默认 feapder_custom
        timestamp: 时间戳
    """
    if _emitter is None:
        return

    merged_tags = dict(tags or {})
    merged_tags["spider"] = _resolve_spider(spider) or merged_tags.get("spider")
    merged_tags["metric"] = metric

    if count is not None:
        fields = {"count": ensure_int(count)}
        point_type = _TYPE_COUNTER
    elif duration is not None:
        fields = {"duration": ensure_float(duration)}
        point_type = _TYPE_TIMER
    elif value is not None:
        fields = {"value": value}
        point_type = _TYPE_STORE
    else:
        fields = {"count": 1}
        point_type = _TYPE_COUNTER

    point = _emitter.make_point(measurement, merged_tags, fields, timestamp, point_type)
    _emitter.emit(point)


def flush():
    """
    强制 flush 本地缓冲到 InfluxDB
    """
    if _emitter is None:
        return
    _emitter.flush()


def close():
    """
    关闭打点系统
    """
    global _emitter, _inited_pid
    if _emitter is None:
        return
    _emitter.close()
    _emitter = None
    _inited_pid = None


# 协程异步打点
aemit_download = aio_wrap(executor=_executor)(emit_download)
aemit_item = aio_wrap(executor=_executor)(emit_item)
aemit_proxy = aio_wrap(executor=_executor)(emit_proxy)
aemit_user = aio_wrap(executor=_executor)(emit_user)
aemit_custom = aio_wrap(executor=_executor)(emit_custom)
