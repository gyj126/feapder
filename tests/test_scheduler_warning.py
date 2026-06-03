import time
import unittest
from unittest import mock

import feapder.setting as setting
from feapder.core.parser_control import ParserControl
from feapder.core.scheduler import Scheduler


class DummyRedisDB:
    def __init__(self, failed_count=0, pending_count=0):
        self.failed_count = failed_count
        self.pending_count = pending_count

    def zget_count(self, key):
        if key == "failed":
            return self.failed_count
        if key == "requests":
            return self.pending_count
        return 0


class DummyItemBuffer:
    export_falied_times = 0


def build_scheduler():
    scheduler = Scheduler.__new__(Scheduler)
    scheduler._last_check_task_status_time = 0
    scheduler._last_check_task_count_time = time.time()
    scheduler._last_task_count = 0
    scheduler._spider_name = "test_spider"
    scheduler._tab_failed_requests = "failed"
    scheduler._tab_requests = "requests"
    scheduler._redisdb = DummyRedisDB()
    scheduler._item_buffer = DummyItemBuffer()
    scheduler.sent_messages = []

    def send_msg(msg, level="info", message_prefix=""):
        scheduler.sent_messages.append(
            {"msg": msg, "level": level, "message_prefix": message_prefix}
        )

    scheduler.send_msg = send_msg
    return scheduler


class TestSchedulerWarning(unittest.TestCase):
    def setUp(self):
        self.setting_patches = [
            mock.patch.object(setting, "WARNING_FAILED_COUNT", 1000),
            mock.patch.object(setting, "WARNING_SUCCESS_RATE", 0.8),
            mock.patch.object(setting, "WARNING_SUCCESS_RATE_MIN_COUNT", 100),
            mock.patch.object(setting, "WARNING_CHECK_TASK_COUNT_INTERVAL", 1200),
            mock.patch.object(setting, "EXPORT_DATA_MAX_FAILED_TIMES", 10),
        ]
        for patcher in self.setting_patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_success_rate_warning_skips_low_sample_count(self):
        with mock.patch.object(
            ParserControl, "get_task_status_count", return_value=(1, 1, 2)
        ):
            scheduler = build_scheduler()

            scheduler.check_task_status()

        self.assertEqual(scheduler.sent_messages, [])

    def test_success_rate_warning_sends_after_min_count(self):
        with mock.patch.object(
            ParserControl, "get_task_status_count", return_value=(80, 20, 100)
        ):
            scheduler = build_scheduler()

            scheduler.check_task_status()

        self.assertEqual(len(scheduler.sent_messages), 1)
        self.assertEqual(scheduler.sent_messages[0]["level"], "warning")
        self.assertIn("请求成功率报警", scheduler.sent_messages[0]["message_prefix"])


if __name__ == "__main__":
    unittest.main()
