import concurrent.futures
import json
import os
import queue
import random
import socket
import string
import threading
import time
from collections import Counter
from typing import Any

import psutil
from influxdb_client import BucketRetentionRules, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

from feapder import setting
from feapder.utils.log import log
from feapder.utils.tools import aio_wrap, ensure_float, ensure_int, get_localhost_ip

_inited_pid = None
# this thread should stop running in the forked process
_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="metrics"
)


class MetricsEmitter:
    def __init__(
        self,
        influxdb,
        *,
        bucket=None,
        org=None,
        batch_size=10,
        max_timer_seq=0,
        emit_interval=10,
        ratio=1.0,
        debug=False,
        add_hostname=False,
        max_points=10240,
        default_tags=None,
    ):
        """
        Args:
            influxdb: influxdb 2.x client 实例
            bucket: 写入的 bucket
            org: 所属 org
            batch_size: 打点的批次大小
            max_timer_seq: 每个时间间隔内最多收集多少个 timer 类型点, 0 表示不限制
            emit_interval: 最多等待多长时间必须打点
            ratio: store 和 timer 类型采样率，比如 0.1 表示只有 10% 的点会留下
            debug: 是否打印调试日志
            add_hostname: 是否添加 hostname 作为 tag
            max_points: 本地 buffer 最多累计多少个点
        """
        self.pending_points = queue.Queue()
        self.batch_size = batch_size
        self.influxdb: InfluxDBClient = influxdb
        self.write_api = influxdb.write_api(write_options=SYNCHRONOUS)
        self.bucket = bucket
        self.org = org
        self.tagkv = {}
        self.max_timer_seq = max_timer_seq
        self.lock = threading.Lock()
        self.hostname = get_localhost_ip() or socket.gethostname()
        self.last_emit_ts = time.time()  # 上次提交时间
        self.emit_interval = emit_interval  # 提交间隔
        self.max_points = max_points
        self.debug = debug
        self.add_hostname = add_hostname
        self.ratio = ratio
        self.default_tags = default_tags or {}
        self.process_tags = {
            "hostname": self.hostname,
            "pid": str(os.getpid()),
        }

    def define_tagkv(self, tagk, tagvs):
        self.tagkv[tagk] = set(tagvs)

    def _point_tagset(self, p):
        return f"{p['measurement']}-{sorted(p['tags'].items())}-{p['time']}"

    def _make_time_to_ns(self, _time):
        """
        将时间转换为 ns 级别的时间戳，补足长度 19 位
        Args:
            _time:

        Returns:

        """
        time_len = len(str(_time))
        random_str = "".join(random.sample(string.digits, 19 - time_len))
        return int(str(_time) + random_str)

    def _accumulate_points(self, points):
        """
        对于处于同一个 key 的点做聚合

          - 对于 counter 类型，同一个 key 的值(_count)可以累加
          - 对于 store 类型，不做任何操作，influxdb 会自行覆盖
          - 对于 timer 类型，通过添加一个 _seq 值来区分每个不同的点
        """
        counters = {}  # 临时保留 counter 类型的值
        timer_seqs = Counter()  # 记录不同 key 的 timer 序列号
        new_points = []

        for point in points:
            point_type = point["tags"].get("_type", None)
            tagset = self._point_tagset(point)

            # counter 类型全部聚合，不做丢弃
            if point_type == "counter":
                if tagset not in counters:
                    counters[tagset] = point
                else:
                    counters[tagset]["fields"]["_count"] += point["fields"]["_count"]
            elif point_type == "timer":
                if self.max_timer_seq and timer_seqs[tagset] >= self.max_timer_seq:
                    continue
                # 掷一把骰子，如果足够幸运才打点
                if self.ratio < 1.0 and random.random() > self.ratio:
                    continue
                # 增加 _seq tag，以便区分不同的点
                point["tags"]["_seq"] = timer_seqs[tagset]
                point["time"] = self._make_time_to_ns(point["time"])
                timer_seqs[tagset] += 1
                new_points.append(point)
            else:
                if self.ratio < 1.0 and random.random() > self.ratio:
                    continue
                point["time"] = self._make_time_to_ns(point["time"])
                new_points.append(point)

        for point in counters.values():
            # 修改下counter类型的点的时间戳，补足19位, 伪装成纳秒级时间戳，防止influxdb对同一秒内的数据进行覆盖
            point["time"] = self._make_time_to_ns(point["time"])
            new_points.append(point)
        return new_points

    def _get_ready_emit(self, force=False):
        """
        把当前 pending 的值做聚合并返回
        """
        if self.debug:
            log.info("got %s raw points", self.pending_points.qsize())

        # 从 pending 中读取点, 设定一个最大值，避免一直打点，一直获取
        points = []
        while len(points) < self.max_points or force:
            try:
                points.append(self.pending_points.get_nowait())
            except queue.Empty:
                break

        # 聚合点
        points = self._accumulate_points(points)

        if self.debug:
            log.info("got %s point", len(points))
            log.info(json.dumps(points, indent=4))

        return points

    def emit(self, point=None, force=False):
        """
        1. 添加新点到 pending
        2. 如果符合条件，尝试聚合并打点
        3. 更新打点时间

        :param point:
        :param force: 强制提交所有点 默认False
        :return:
        """
        if point:
            self.pending_points.put(point)

        # 判断是否需要提交点 1、数量 2、间隔 3、强力打点
        if not (
            force
            or self.pending_points.qsize() >= self.max_points  # noqa: W503
            or time.time() - self.last_emit_ts > self.emit_interval  # noqa: W503
        ):
            return

        # 需要打点，读取可以打点的值, 确保只有一个线程在做点的压缩
        with self.lock:
            points = self._get_ready_emit(force=force)

            if not points:
                return
            try:
                self.write_api.write(
                    bucket=self.bucket,
                    org=self.org,
                    record=points,
                    write_precision=WritePrecision.NS,
                )
            except Exception:
                log.exception("error writing points")

            self.last_emit_ts = time.time()

    def flush(self):
        if self.debug:
            log.info("start draining points %s", self.pending_points.qsize())
        self.emit(force=True)

    def close(self):
        self.flush()
        try:
            self.write_api.close()
        except Exception as e:
            log.exception(e)
        try:
            self.influxdb.close()
        except Exception as e:
            log.exception(e)

    def make_point(self, measurement, tags: dict, fields: dict, timestamp=None):
        """
        默认的时间戳是"秒"级别的
        """
        assert measurement, "measurement can't be null"
        tags = tags.copy() if tags else {}
        for tagk, tagv in self.default_tags.items():
            tags.setdefault(tagk, tagv)
        for tagk, tagv in self.process_tags.items():
            tags.setdefault(tagk, tagv)
        fields = fields.copy() if fields else {}
        if timestamp is None:
            timestamp = int(time.time())
        # 兼容旧参数，默认已为所有指标添加 hostname。
        if self.add_hostname and "hostname" not in tags:
            tags["hostname"] = self.hostname
        point = dict(measurement=measurement, tags=tags, fields=fields, time=timestamp)
        if self.tagkv:
            for tagk, tagv in tags.items():
                if tagv not in self.tagkv[tagk]:
                    raise ValueError("tag value = %s not in %s", tagv, self.tagkv[tagk])
        return point

    def get_counter_point(
        self,
        measurement: str,
        key: str = None,
        count: int = 1,
        tags: dict = None,
        timestamp: int = None,
    ):
        """
        counter 不能被覆盖
        """
        tags = tags.copy() if tags else {}
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "counter"
        count = ensure_int(count)
        fields = dict(_count=count)
        point = self.make_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def get_store_point(
        self,
        measurement: str,
        key: str = None,
        value: Any = 0,
        tags: dict = None,
        timestamp=None,
    ):
        tags = tags.copy() if tags else {}
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "store"
        fields = dict(_value=value)
        point = self.make_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def get_timer_point(
        self,
        measurement: str,
        key: str = None,
        duration: float = 0,
        tags: dict = None,
        timestamp=None,
    ):
        tags = tags.copy() if tags else {}
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "timer"
        fields = dict(_duration=ensure_float(duration))
        point = self.make_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def emit_any(self, *args, **kwargs):
        point = self.make_point(*args, **kwargs)
        self.emit(point)

    def emit_counter(self, *args, **kwargs):
        point = self.get_counter_point(*args, **kwargs)
        self.emit(point)

    def emit_store(self, *args, **kwargs):
        point = self.get_store_point(*args, **kwargs)
        self.emit(point)

    def emit_timer(self, *args, **kwargs):
        point = self.get_timer_point(*args, **kwargs)
        self.emit(point)


_emitter: MetricsEmitter = None
_measurement: str = None
_init_signature = None


def _duration_to_seconds(duration):
    """将 180d / 24h / 30m / 60s / 2w 形式的保留时长转换为秒, 0 表示永久保留"""
    if not duration:
        return 0
    if str(duration) == "0":
        return 0
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    unit = str(duration)[-1].lower()
    if unit not in units:
        raise ValueError(f"invalid retention duration: {duration}")
    return int(str(duration)[:-1]) * units[unit]


def init(
    *,
    influxdb_url=None,
    influxdb_token=None,
    influxdb_org=None,
    influxdb_bucket=None,
    influxdb_measurement=None,
    bucket_retention_duration="180d",
    emit_interval=10,
    batch_size=100,
    debug=False,
    timeout=22000,
    **kwargs,
):
    """
    打点监控初始化
    Args:
        influxdb_url: influxdb 2.x 地址, 如 http://localhost:8086
        influxdb_token: 访问令牌
        influxdb_org: 所属组织
        influxdb_bucket: 写入的 bucket
        influxdb_measurement: 存储的表, 也可以在打点的时候指定
        bucket_retention_duration: bucket 保留时长, 如 180d, 0 表示永久
        emit_interval: 打点最大间隔
        batch_size: 打点的批次大小
        debug: 是否开启调试
        timeout: 与 influxdb 建立连接时的超时时间, 单位毫秒
        **kwargs: 可传递 MetricsEmitter 类的参数

    Returns:

    """
    global _inited_pid, _emitter, _measurement, _init_signature

    influxdb_url = influxdb_url or setting.INFLUXDB_URL
    influxdb_token = influxdb_token or setting.INFLUXDB_TOKEN
    influxdb_org = influxdb_org or setting.INFLUXDB_ORG
    influxdb_bucket = influxdb_bucket or setting.INFLUXDB_BUCKET
    measurement = influxdb_measurement or setting.INFLUXDB_MEASUREMENT
    init_signature = (
        influxdb_url,
        influxdb_token,
        influxdb_org,
        influxdb_bucket,
        measurement,
        bucket_retention_duration,
        emit_interval,
        batch_size,
        debug,
        timeout,
        repr(sorted(kwargs.items())),
    )
    if _inited_pid == os.getpid():
        if _init_signature != init_signature:
            log.warning(
                "metrics already initialized in current process, new init args will be ignored"
            )
        return

    # 校验连接必需项, 缺失则跳过初始化
    if not all([influxdb_url, influxdb_token, influxdb_org, influxdb_bucket]):
        return

    influxdb_client = InfluxDBClient(
        url=influxdb_url,
        token=influxdb_token,
        org=influxdb_org,
        timeout=timeout,
    )
    # 自动创建 bucket（含保留策略）, 等价于 1.x 的自动建库 + 保留策略
    # 注: 若 token 无 bucket 管理权限, 创建会失败, 此时假定 bucket 已存在并继续写入
    try:
        buckets_api = influxdb_client.buckets_api()
        if not buckets_api.find_bucket_by_name(influxdb_bucket):
            retention_seconds = _duration_to_seconds(bucket_retention_duration)
            retention_rules = None
            if retention_seconds:
                retention_rules = BucketRetentionRules(
                    type="expire",
                    every_seconds=retention_seconds,
                )
            buckets_api.create_bucket(
                bucket_name=influxdb_bucket,
                retention_rules=retention_rules,
                org=influxdb_org,
            )
    except Exception as e:
        log.warning(f"自动创建 bucket 失败, 将假定 bucket 已存在: {e}")

    _emitter = MetricsEmitter(
        influxdb_client,
        bucket=influxdb_bucket,
        org=influxdb_org,
        debug=debug,
        batch_size=batch_size,
        emit_interval=emit_interval,
        **kwargs,
    )
    _inited_pid = os.getpid()
    _measurement = measurement
    _init_signature = init_signature
    log.info("metrics init successfully")


def emit_any(
    tags: dict,
    fields: dict,
    *,
    classify: str = "",
    measurement: str = None,
    timestamp=None,
):
    """
    原生的打点，不进行额外的处理
    Args:
        tags: influxdb的tag的字段和值
        fields: influxdb的field的字段和值
        classify: 点的类别
        measurement: 存储的表
        timestamp: 点的时间戳，默认为当前时间

    Returns:

    """
    if not _emitter:
        return

    tags = tags or {}
    tags["_classify"] = classify
    measurement = measurement or _measurement
    _emitter.emit_any(measurement, tags, fields, timestamp)


def emit_counter(
    key: str = None,
    count: int = 1,
    *,
    classify: str = "",
    tags: dict = None,
    measurement: str = None,
    timestamp: int = None,
):
    """
    聚合打点，即会将一段时间内的点求和，然后打一个点数和
    Args:
        key: 与点绑定的key值
        count: 点数
        classify: 点的类别
        tags: influxdb的tag的字段和值
        measurement: 存储的表
        timestamp: 点的时间戳，默认为当前时间

    Returns:

    """
    if not _emitter:
        return

    tags = tags or {}
    tags["_classify"] = classify
    measurement = measurement or _measurement
    _emitter.emit_counter(measurement, key, count, tags, timestamp)


def emit_timer(
    key: str = None,
    duration: float = 0,
    *,
    classify: str = "",
    tags: dict = None,
    measurement: str = None,
    timestamp=None,
):
    """
    时间打点，用于监控程序的运行时长等，每个duration一个点，不会被覆盖
    Args:
        key: 与点绑定的key值
        duration: 时长
        classify: 点的类别
        tags: influxdb的tag的字段和值
        measurement: 存储的表
        timestamp: 点的时间戳，默认为当前时间

    Returns:

    """
    if not _emitter:
        return

    tags = tags or {}
    tags["_classify"] = classify
    measurement = measurement or _measurement
    _emitter.emit_timer(measurement, key, duration, tags, timestamp)


def emit_store(
    key: str = None,
    value: Any = 0,
    *,
    classify: str = "",
    tags: dict = None,
    measurement: str = None,
    timestamp=None,
):
    """
    直接打点，不进行额外的处理
    Args:
        key: 与点绑定的key值
        value: 点的值
        classify: 点的类别
        tags: influxdb的tag的字段和值
        measurement: 存储的表
        timestamp: 点的时间戳，默认为当前时间

    Returns:

    """
    if not _emitter:
        return

    tags = tags or {}
    tags["_classify"] = classify
    measurement = measurement or _measurement
    _emitter.emit_store(measurement, key, value, tags, timestamp)


def flush():
    """
    强刷点到influxdb
    Returns:

    """
    if not _emitter:
        return
    _emitter.flush()


def close():
    """
    关闭并清空，允许同一爬虫在同进程内重新初始化
    Returns:

    """
    global _emitter, _inited_pid, _measurement, _init_signature
    if not _emitter:
        return
    _emitter.close()
    _emitter = None
    _inited_pid = None
    _measurement = None
    _init_signature = None


class SpiderStatusReporter(threading.Thread):
    """周期上报爬虫运行状态：心跳 heartbeat、内存 memory(MB)"""

    def __init__(self, interval=10):
        if interval <= 0:
            raise ValueError("interval must be greater than 0")
        super().__init__(daemon=True)
        self._interval = interval
        self._stop_event = threading.Event()
        self._process = psutil.Process(os.getpid())

    def run(self):
        while not self._stop_event.is_set():
            try:
                emit_store(
                    "heartbeat",
                    int(time.time()),
                    classify="runtime",
                )
                memory = int(round(self._process.memory_info().rss / 1024 / 1024))
                emit_store("memory", memory, classify="runtime")
            except Exception as e:
                log.error(f"运行状态打点异常: {e}")
            self._stop_event.wait(self._interval)

    def stop(self):
        self._stop_event.set()


# 协程打点
aemit_counter = aio_wrap(executor=_executor)(emit_counter)
aemit_store = aio_wrap(executor=_executor)(emit_store)
aemit_timer = aio_wrap(executor=_executor)(emit_timer)
