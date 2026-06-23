from unittest import mock

from feapder.core.parser_control import ParserControl
from feapder.utils import metrics


class FakeWriteApi:
    def __init__(self):
        self.written = []
        self.closed = False

    def write(self, *args, **kwargs):
        self.written.append((args, kwargs))

    def close(self):
        self.closed = True


class FakeBucketsApi:
    def __init__(self, existing_buckets):
        self._existing = set(existing_buckets)
        self.created = []
        self.created_retention_rules = []

    def find_bucket_by_name(self, name):
        return name if name in self._existing else None

    def create_bucket(self, bucket_name=None, retention_rules=None, org=None):
        self.created.append(bucket_name)
        self.created_retention_rules.append(retention_rules)
        self._existing.add(bucket_name)


class FakeInfluxDBClient:
    instances = []
    existing_buckets = set()

    def __init__(self, *args, **kwargs):
        self.closed = False
        self._write_api = FakeWriteApi()
        self._buckets_api = FakeBucketsApi(FakeInfluxDBClient.existing_buckets)
        FakeInfluxDBClient.instances.append(self)

    def write_api(self, *args, **kwargs):
        return self._write_api

    def buckets_api(self):
        return self._buckets_api

    def close(self):
        self.closed = True


def reset_metrics():
    metrics._emitter = None
    metrics._inited_pid = None
    metrics._measurement = None
    metrics._init_signature = None
    FakeInfluxDBClient.instances = []
    FakeInfluxDBClient.existing_buckets = set()


def test_init_skips_existing_bucket():
    reset_metrics()
    FakeInfluxDBClient.existing_buckets = {"crawler"}

    with mock.patch.object(metrics, "InfluxDBClient", FakeInfluxDBClient):
        metrics.init(
            influxdb_url="http://localhost:8086",
            influxdb_token="token",
            influxdb_org="org",
            influxdb_bucket="crawler",
            influxdb_measurement="crawler",
        )

    assert len(FakeInfluxDBClient.instances) == 1
    assert FakeInfluxDBClient.instances[0].buckets_api().created == []
    metrics.close()


def test_init_creates_bucket_when_missing():
    reset_metrics()

    with mock.patch.object(metrics, "InfluxDBClient", FakeInfluxDBClient):
        metrics.init(
            influxdb_url="http://localhost:8086",
            influxdb_token="token",
            influxdb_org="org",
            influxdb_bucket="crawler",
            influxdb_measurement="crawler",
        )

    assert len(FakeInfluxDBClient.instances) == 1
    assert FakeInfluxDBClient.instances[0].buckets_api().created == ["crawler"]
    metrics.close()


def test_init_creates_bucket_without_retention_rule_for_permanent_retention():
    reset_metrics()

    with mock.patch.object(metrics, "InfluxDBClient", FakeInfluxDBClient):
        metrics.init(
            influxdb_url="http://localhost:8086",
            influxdb_token="token",
            influxdb_org="org",
            influxdb_bucket="crawler",
            influxdb_measurement="crawler",
            bucket_retention_duration="0",
        )

    buckets_api = FakeInfluxDBClient.instances[0].buckets_api()
    assert buckets_api.created == ["crawler"]
    assert buckets_api.created_retention_rules == [None]
    metrics.close()


def test_init_warns_when_same_process_reinitializes_with_different_args(caplog):
    reset_metrics()
    FakeInfluxDBClient.existing_buckets = {"crawler"}

    with mock.patch.object(metrics, "InfluxDBClient", FakeInfluxDBClient):
        metrics.init(
            influxdb_url="http://localhost:8086",
            influxdb_token="token",
            influxdb_org="org",
            influxdb_bucket="crawler",
            influxdb_measurement="crawler",
            default_tags={"project": "a", "spider": "one"},
        )
        metrics.init(
            influxdb_url="http://localhost:8086",
            influxdb_token="token",
            influxdb_org="org",
            influxdb_bucket="crawler",
            influxdb_measurement="another",
            default_tags={"project": "a", "spider": "two"},
        )

    assert "metrics already initialized in current process" in caplog.text
    assert len(FakeInfluxDBClient.instances) == 1
    assert metrics._measurement == "crawler"
    metrics.close()


def test_parser_control_task_status_count_uses_locked_snapshot():
    ParserControl._failed_task_count = 0
    ParserControl._success_task_count = 0
    ParserControl._total_task_count = 0

    ParserControl.incr_task_status_count(failed=2, success=8, total=10)

    assert ParserControl.get_task_status_count() == (2, 8, 10)


def test_metrics_points_include_process_tags_by_default():
    emitter = metrics.MetricsEmitter(
        FakeInfluxDBClient(),
        default_tags={"project": "demo", "spider": "DemoSpider"},
    )

    point = emitter.get_counter_point("crawler", key="download_total", count=1)

    assert point["tags"]["project"] == "demo"
    assert point["tags"]["spider"] == "DemoSpider"
    assert point["tags"]["hostname"]
    assert point["tags"]["pid"]


def test_metrics_explicit_process_tags_are_not_overridden():
    emitter = metrics.MetricsEmitter(
        FakeInfluxDBClient(),
        default_tags={"project": "demo", "spider": "DemoSpider"},
    )

    point = emitter.get_counter_point(
        "crawler",
        key="download_total",
        count=1,
        tags={"hostname": "custom-host", "pid": "custom-pid"},
    )

    assert point["tags"]["hostname"] == "custom-host"
    assert point["tags"]["pid"] == "custom-pid"


def test_emit_flush_writes_points_to_configured_bucket_and_org():
    reset_metrics()
    FakeInfluxDBClient.existing_buckets = {"crawler"}

    with mock.patch.object(metrics, "InfluxDBClient", FakeInfluxDBClient):
        metrics.init(
            influxdb_url="http://localhost:8086",
            influxdb_token="token",
            influxdb_org="org",
            influxdb_bucket="crawler",
            influxdb_measurement="crawler",
            default_tags={"project": "demo", "spider": "DemoSpider"},
        )

    metrics.emit_counter(
        "download_total",
        count=2,
        classify="document",
        timestamp=1234567890,
    )
    metrics.flush()

    write_api = FakeInfluxDBClient.instances[0].write_api()
    assert len(write_api.written) == 1
    _, kwargs = write_api.written[0]
    assert kwargs["bucket"] == "crawler"
    assert kwargs["org"] == "org"
    assert kwargs["write_precision"] == metrics.WritePrecision.NS
    assert len(kwargs["record"]) == 1
    point = kwargs["record"][0]
    assert point["measurement"] == "crawler"
    assert point["tags"]["project"] == "demo"
    assert point["tags"]["spider"] == "DemoSpider"
    assert point["tags"]["_classify"] == "document"
    assert point["tags"]["_key"] == "download_total"
    assert point["fields"]["_count"] == 2
    metrics.close()
