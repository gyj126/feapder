from unittest import mock

from feapder.core.parser_control import ParserControl
from feapder.utils import metrics


class FakeInfluxDBClient:
    instances = []

    def __init__(self, *args, **kwargs):
        self.created_retention_policies = []
        self.closed = False
        FakeInfluxDBClient.instances.append(self)

    def create_database(self, database):
        self.database = database

    def get_list_retention_policies(self, database):
        return [{"name": "crawler_180d"}]

    def create_retention_policy(self, *args, **kwargs):
        self.created_retention_policies.append((args, kwargs))

    def close(self):
        self.closed = True

    def write_points(self, *args, **kwargs):
        pass


def reset_metrics():
    metrics._emitter = None
    metrics._inited_pid = None
    metrics._measurement = None
    metrics._init_signature = None
    FakeInfluxDBClient.instances = []


def test_init_skips_existing_retention_policy():
    reset_metrics()

    with mock.patch.object(metrics, "InfluxDBClient", FakeInfluxDBClient):
        metrics.init(
            influxdb_host="localhost",
            influxdb_port=8086,
            influxdb_udp_port=8089,
            influxdb_database="crawler",
            influxdb_measurement="crawler",
        )

    assert len(FakeInfluxDBClient.instances) == 1
    assert FakeInfluxDBClient.instances[0].created_retention_policies == []
    metrics.close()


def test_init_warns_when_same_process_reinitializes_with_different_args(caplog):
    reset_metrics()

    with mock.patch.object(metrics, "InfluxDBClient", FakeInfluxDBClient):
        metrics.init(
            influxdb_host="localhost",
            influxdb_port=8086,
            influxdb_udp_port=8089,
            influxdb_database="crawler",
            influxdb_measurement="crawler",
            default_tags={"project": "a", "spider": "one"},
        )
        metrics.init(
            influxdb_host="localhost",
            influxdb_port=8086,
            influxdb_udp_port=8089,
            influxdb_database="crawler",
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
