# -*- coding: utf-8 -*-
"""
基于 InfluxDB 2.x 的打点监控模块

设计要点：
- 直接复用 influxdb-client 官方 2.x 客户端 + 非同步批量 WriteOptions
- 按业务场景拆分 measurement: feapder_download / feapder_item / feapder_proxy
  / feapder_user / feapder_runtime / feapder_custom
- counter 类型按"秒"在内存中聚合，同 measurement+tags+秒 自动累加为单点，
  其余类型直接交给 write_api 异步入队
- 全部对外 emit_* 函数被 _safe 包裹，打点异常不影响业务流程
- 进程内单例 emitter；fork 后子进程自动重置；init() 末尾注册 atexit 兜底关闭
"""

import asyncio
import atexit
import functools
import itertools
import os
import random
import socket
import threading
import time
from typing import Any, Optional

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

from feapder import setting
from feapder.utils.log import log
from feapder.utils.tools import ensure_float, ensure_int

MEASUREMENT_DOWNLOAD = "feapder_download"
MEASUREMENT_ITEM = "feapder_item"
MEASUREMENT_PROXY = "feapder_proxy"
MEASUREMENT_USER = "feapder_user"
MEASUREMENT_RUNTIME = "feapder_runtime"
MEASUREMENT_CUSTOM = "feapder_custom"

_TYPE_COUNTER = "counter"
_TYPE_STORE = "store"
_TYPE_TIMER = "timer"

# 进程内单调自增序号，用作 ns 时间戳后缀，避免同 series 同纳秒被覆盖
_seq = itertools.count()


def _next_ns_offset() -> int:
    return next(_seq) % 1_000_000_000


_inited_pid: Optional[int] = None
_emitter: Optional["MetricsEmitter"] = None
_default_spider: Optional[str] = None


def _safe(fn):
    """打点装饰器，吞没异常仅记日志，避免影响业务流程"""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log.warning(f"打点失败 {fn.__name__}: {e}")

    return wrapper


class _CounterAggregator:
    """秒级 counter 聚合器：同 measurement+tags+秒 在内存中累加，定时 flush"""

    def __init__(self, flush_callback, interval: int = 1):
        self._buckets: dict = {}
        self._lock = threading.Lock()
        self._flush_callback = flush_callback
        self._interval = max(1, int(interval))
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="metrics-counter-flush", daemon=True
        )
        self._thread.start()

    def add(self, measurement: str, tags: dict, fields: dict, ts_seconds: int):
        frozen = tuple(sorted(tags.items()))
        key = (measurement, frozen, ts_seconds)
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                self._buckets[key] = {"tags": dict(tags), "fields": dict(fields)}
            else:
                for fk, fv in fields.items():
                    bucket["fields"][fk] = bucket["fields"].get(fk, 0) + fv

    def _loop(self):
        while not self._stop.wait(self._interval):
            try:
                self.flush()
            except Exception as e:
                log.warning(f"counter 聚合 flush 异常: {e}")

    def flush(self, force: bool = False):
        now_seconds = int(time.time())
        ready = []
        with self._lock:
            for key in list(self._buckets.keys()):
                _, _, ts_seconds = key
                if force or ts_seconds < now_seconds:
                    ready.append((key, self._buckets.pop(key)))
        if ready:
            self._flush_callback(ready)

    def stop(self):
        self._stop.set()
        try:
            self._thread.join(timeout=2)
        except Exception:
            pass
        self.flush(force=True)


class _RuntimeSampler:
    """后台进程指标采样：cpu_percent / memory_rss_mb / num_threads"""

    def __init__(self, emit_callback, interval: int):
        self._emit_callback = emit_callback
        self._interval = max(1, int(interval))
        self._stop = threading.Event()
        self._proc = None
        try:
            import psutil

            self._proc = psutil.Process()
            self._proc.cpu_percent(None)
        except Exception as e:
            log.warning(f"runtime 采样初始化失败，将跳过: {e}")
            return
        self._thread = threading.Thread(
            target=self._loop, name="metrics-runtime", daemon=True
        )
        self._thread.start()

    def _loop(self):
        while not self._stop.wait(self._interval):
            try:
                self._sample_once()
            except Exception as e:
                log.warning(f"runtime 采样异常: {e}")

    def _sample_once(self):
        proc = self._proc
        if proc is None:
            return
        cpu = proc.cpu_percent(None)
        mem_mb = proc.memory_info().rss / 1024 / 1024
        threads = proc.num_threads()
        self._emit_callback(
            cpu_percent=float(cpu),
            memory_rss_mb=float(mem_mb),
            num_threads=int(threads),
        )

    def stop(self):
        self._stop.set()
        if self._proc is None:
            return
        try:
            self._thread.join(timeout=2)
        except Exception:
            pass


class MetricsEmitter:
    """InfluxDB 2.x 异步批量打点客户端"""

    def __init__(
        self,
        client: InfluxDBClient,
        bucket: str,
        org: str,
        *,
        batch_size: int = 500,
        emit_interval: int = 10,
        ratio: float = 1.0,
        debug: bool = False,
        add_hostname: bool = False,
        default_tags: Optional[dict] = None,
        runtime_interval: int = 5,
        **_legacy,
    ):
        """
        Args:
            client: InfluxDB 2.x 客户端
            bucket: 写入的 bucket
            org: 所属 org
            batch_size: 单批最大写入点数
            emit_interval: 后台 flush 间隔（秒）
            ratio: store / timer 类型采样率
            debug: 是否打印调试日志
            add_hostname: 是否将 hostname 作为 tag
            default_tags: 公共 tag，会附加到每一个数据点上
            runtime_interval: 进程级 runtime 指标采样间隔（秒），<=0 关闭
        """
        if _legacy:
            log.warning(f"MetricsEmitter 收到未识别参数将被忽略: {list(_legacy.keys())}")
        self._client = client
        self._bucket = bucket
        self._org = org
        self._ratio = ratio
        self._debug = debug
        self._add_hostname = add_hostname
        self._default_tags = default_tags or {}
        self._hostname = socket.gethostname()

        self._write_api = client.write_api(
            write_options=WriteOptions(
                batch_size=batch_size,
                flush_interval=max(1, emit_interval) * 1000,
                jitter_interval=2000,
                retry_interval=1000,
                max_retries=3,
                exponential_base=2,
            ),
            success_callback=self._on_write_success,
            error_callback=self._on_write_error,
            retry_callback=self._on_write_retry,
        )

        self._counter_agg = _CounterAggregator(
            flush_callback=self._flush_counters, interval=1
        )

        if runtime_interval and runtime_interval > 0:
            self._runtime_sampler = _RuntimeSampler(
                emit_callback=self._emit_runtime, interval=runtime_interval
            )
        else:
            self._runtime_sampler = None

    def _on_write_success(self, conf, data):
        if self._debug:
            log.info(f"打点写入成功 size={len(data)}")

    def _on_write_error(self, conf, data, exception):
        log.warning(f"打点写入失败: {exception}")

    def _on_write_retry(self, conf, data, exception):
        if self._debug:
            log.info(f"打点写入重试: {exception}")

    def _merge_tags(self, tags: Optional[dict]) -> dict:
        merged = dict(self._default_tags)
        if tags:
            for k, v in tags.items():
                if v is None or v == "":
                    continue
                merged[k] = str(v)
        if self._add_hostname and "host" not in merged:
            merged["host"] = self._hostname
        return merged

    def emit_counter(
        self,
        measurement: str,
        tags: Optional[dict],
        fields: dict,
        timestamp: Optional[int] = None,
    ):
        merged_tags = self._merge_tags(tags)
        ts_seconds = int(timestamp) if timestamp is not None else int(time.time())
        self._counter_agg.add(measurement, merged_tags, fields, ts_seconds)

    def emit_timer(
        self,
        measurement: str,
        tags: Optional[dict],
        fields: dict,
        timestamp: Optional[int] = None,
    ):
        if self._ratio < 1.0 and random.random() > self._ratio:
            return
        merged_tags = self._merge_tags(tags)
        self._write_point(measurement, merged_tags, fields, self._timestamp_ns(timestamp))

    def emit_store(
        self,
        measurement: str,
        tags: Optional[dict],
        fields: dict,
        timestamp: Optional[int] = None,
    ):
        if self._ratio < 1.0 and random.random() > self._ratio:
            return
        merged_tags = self._merge_tags(tags)
        self._write_point(measurement, merged_tags, fields, self._timestamp_ns(timestamp))

    @staticmethod
    def _timestamp_ns(timestamp: Optional[int]) -> int:
        if timestamp is None:
            return time.time_ns() + _next_ns_offset()
        if timestamp < 10**12:
            return int(timestamp) * 1_000_000_000 + _next_ns_offset()
        if timestamp < 10**15:
            return int(timestamp) * 1_000_000 + _next_ns_offset()
        if timestamp < 10**18:
            return int(timestamp) * 1_000 + _next_ns_offset()
        return int(timestamp) + _next_ns_offset()

    def _write_point(self, measurement: str, tags: dict, fields: dict, ts_ns: int):
        point = Point(measurement).time(ts_ns, WritePrecision.NS)
        for tk, tv in tags.items():
            point = point.tag(tk, tv)
        for fk, fv in fields.items():
            point = point.field(fk, fv)
        try:
            self._write_api.write(bucket=self._bucket, org=self._org, record=point)
        except Exception as e:
            log.warning(f"打点入队失败: {e}")

    def _flush_counters(self, ready: list):
        points = []
        for (measurement, _frozen, ts_seconds), bucket in ready:
            ts_ns = ts_seconds * 1_000_000_000 + _next_ns_offset()
            point = Point(measurement).time(ts_ns, WritePrecision.NS)
            for tk, tv in bucket["tags"].items():
                point = point.tag(tk, tv)
            for fk, fv in bucket["fields"].items():
                point = point.field(fk, fv)
            points.append(point)
        if not points:
            return
        try:
            self._write_api.write(bucket=self._bucket, org=self._org, record=points)
        except Exception as e:
            log.warning(f"counter 批量入队失败: {e}")

    def _emit_runtime(self, **fields):
        self.emit_store(MEASUREMENT_RUNTIME, tags=None, fields=fields)

    def flush(self):
        try:
            self._counter_agg.flush(force=True)
        except Exception as e:
            log.warning(f"counter flush 异常: {e}")
        try:
            self._write_api.flush()
        except Exception as e:
            log.warning(f"write_api flush 异常: {e}")

    def close(self):
        try:
            self._counter_agg.stop()
        except Exception as e:
            log.warning(f"counter aggregator 关闭异常: {e}")
        if self._runtime_sampler is not None:
            try:
                self._runtime_sampler.stop()
            except Exception as e:
                log.warning(f"runtime sampler 关闭异常: {e}")
        try:
            self._write_api.close()
        except Exception as e:
            log.warning(f"write_api 关闭异常: {e}")
        try:
            self._client.close()
        except Exception as e:
            log.warning(f"InfluxDB 客户端关闭异常: {e}")


def _is_enabled() -> bool:
    """打点开关：默认关闭，仅当用户在 setting.py 中显式 INFLUXDB_ENABLE=True 才启用"""
    return getattr(setting, "INFLUXDB_ENABLE", False) is True


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
    runtime_interval: Optional[int] = None,
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
        batch_size: 单批写入最大点数
        emit_interval: 后台 flush 间隔（秒）
        debug: 是否输出调试日志
        add_hostname: 是否将 hostname 作为 tag
        runtime_interval: 进程级 runtime 指标采样间隔（秒），<=0 关闭
        timeout: 客户端超时时间（毫秒）
        **kwargs: 透传给 MetricsEmitter 的其他参数
    """
    global _inited_pid, _emitter, _default_spider

    if not _is_enabled():
        return
    if _inited_pid == os.getpid():
        return

    url = url or setting.INFLUXDB_URL
    token = token or setting.INFLUXDB_TOKEN
    org = org or setting.INFLUXDB_ORG
    bucket = bucket or setting.INFLUXDB_BUCKET
    if not (url and token and org and bucket):
        return

    if _emitter is not None:
        try:
            _emitter.close()
        except Exception as e:
            log.warning(f"旧 emitter 关闭异常: {e}")
        _emitter = None

    merged_tags = dict(getattr(setting, "METRICS_DEFAULT_TAGS", {}))
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

    try:
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
            runtime_interval=(
                runtime_interval
                if runtime_interval is not None
                else getattr(setting, "METRICS_RUNTIME_INTERVAL", 5)
            ),
            **kwargs,
        )
    except Exception as e:
        log.error(f"初始化 MetricsEmitter 失败: {e}")
        try:
            client.close()
        except Exception:
            pass
        return

    _inited_pid = os.getpid()
    atexit.register(close)
    log.info(f"打点监控初始化成功 url={url} bucket={bucket}")


def _resolve_spider(spider: Optional[str]) -> Optional[str]:
    return spider or _default_spider


def _reset_after_fork():
    """fork 后子进程重置全局状态，避免继承父进程的连接 / 线程"""
    global _inited_pid, _emitter
    _inited_pid = None
    _emitter = None


try:
    os.register_at_fork(after_in_child=_reset_after_fork)
except AttributeError:
    pass


@_safe
def emit_download(
    spider: Optional[str] = None,
    parser: Optional[str] = None,
    status: str = "",
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """下载/解析事件计数"""
    if _emitter is None:
        return
    tags = {"spider": _resolve_spider(spider), "parser": parser, "status": status}
    fields = {"count": ensure_int(count)}
    _emitter.emit_counter(MEASUREMENT_DOWNLOAD, tags, fields, timestamp)


@_safe
def emit_item(
    spider: Optional[str] = None,
    table: str = "",
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """入库总条数事件计数"""
    if _emitter is None:
        return
    tags = {"spider": _resolve_spider(spider), "table": table}
    fields = {"count": ensure_int(count)}
    _emitter.emit_counter(MEASUREMENT_ITEM, tags, fields, timestamp)


@_safe
def emit_proxy(
    spider: Optional[str] = None,
    event: str = "",
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """代理池事件计数"""
    if _emitter is None:
        return
    tags = {"spider": _resolve_spider(spider), "event": event}
    fields = {"count": ensure_int(count)}
    _emitter.emit_counter(MEASUREMENT_PROXY, tags, fields, timestamp)


@_safe
def emit_user(
    spider: Optional[str] = None,
    status: str = "",
    count: int = 1,
    timestamp: Optional[int] = None,
):
    """账号池事件计数（仅按 status 维度聚合）"""
    if _emitter is None:
        return
    tags = {"spider": _resolve_spider(spider), "status": status}
    fields = {"count": ensure_int(count)}
    _emitter.emit_counter(MEASUREMENT_USER, tags, fields, timestamp)


@_safe
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
        count: 计数（counter 语义，按秒累加）
        value: 任意数值（store 语义）
        duration: 时长（timer 语义，每个点独立保留）
        spider: 爬虫名
        tags: 额外的 tag
        measurement: 表名，默认 feapder_custom
        timestamp: 时间戳
    """
    if _emitter is None:
        return

    merged_tags = dict(tags or {})
    if spider:
        merged_tags["spider"] = spider
    elif "spider" not in merged_tags and _default_spider:
        merged_tags["spider"] = _default_spider
    merged_tags["metric"] = metric

    if count is not None:
        _emitter.emit_counter(
            measurement, merged_tags, {"count": ensure_int(count)}, timestamp
        )
    elif duration is not None:
        _emitter.emit_timer(
            measurement, merged_tags, {"duration": ensure_float(duration)}, timestamp
        )
    elif value is not None:
        _emitter.emit_store(measurement, merged_tags, {"value": value}, timestamp)
    else:
        _emitter.emit_counter(measurement, merged_tags, {"count": 1}, timestamp)


def flush():
    """强制 flush 本地缓冲到 InfluxDB"""
    if _emitter is None:
        return
    _emitter.flush()


def close():
    """关闭打点系统"""
    global _emitter, _inited_pid
    if _emitter is None:
        return
    try:
        _emitter.close()
    except Exception as e:
        log.warning(f"emitter 关闭异常: {e}")
    _emitter = None
    _inited_pid = None


async def aemit_download(*args, **kwargs):
    await asyncio.to_thread(emit_download, *args, **kwargs)


async def aemit_item(*args, **kwargs):
    await asyncio.to_thread(emit_item, *args, **kwargs)


async def aemit_proxy(*args, **kwargs):
    await asyncio.to_thread(emit_proxy, *args, **kwargs)


async def aemit_user(*args, **kwargs):
    await asyncio.to_thread(emit_user, *args, **kwargs)


async def aemit_custom(*args, **kwargs):
    await asyncio.to_thread(emit_custom, *args, **kwargs)
