import time
import unittest
from unittest import mock

import feapder.setting as setting
from feapder.buffer.item_buffer import ItemBuffer
from feapder.core.parser_control import ParserControl
from feapder.core.scheduler import Scheduler


class DummyRedisDB:
    def zget_count(self, key):
        return 0


class DummyItemBuffer:
    export_falied_times = 0


def build_scheduler(spider_name="RedditClean", export_falied_times=11):
    scheduler = Scheduler.__new__(Scheduler)
    scheduler._last_check_task_status_time = 0
    scheduler._last_check_task_count_time = time.time()
    scheduler._last_task_count = 0
    scheduler._spider_name = spider_name
    scheduler._tab_failed_requests = "failed"
    scheduler._tab_requests = "requests"
    scheduler._redisdb = DummyRedisDB()
    scheduler._item_buffer = DummyItemBuffer()
    scheduler._item_buffer.export_falied_times = export_falied_times
    scheduler.sent_messages = []

    def send_msg(msg, level="info", message_prefix=""):
        scheduler.sent_messages.append({"msg": msg, "level": level, "message_prefix": message_prefix})

    scheduler.send_msg = send_msg
    return scheduler


def build_item_buffer(export_falied_times=10):
    buffer = ItemBuffer.__new__(ItemBuffer)
    buffer._redis_key = "reddit:clean"
    buffer.export_falied_times = export_falied_times
    buffer.export_retry_times = 0
    buffer._is_adding_to_db = False
    buffer._item_pipelines = {}
    buffer._item_update_keys = {}
    buffer._table_request = "requests"
    return buffer


class TestExportFailedWarning(unittest.TestCase):
    def setUp(self):
        self.setting_patches = [
            mock.patch.object(setting, "WARNING_FAILED_COUNT", 1000),
            mock.patch.object(setting, "WARNING_SUCCESS_RATE", 0.8),
            mock.patch.object(setting, "WARNING_SUCCESS_RATE_MIN_COUNT", 100),
            mock.patch.object(setting, "WARNING_CHECK_TASK_COUNT_INTERVAL", 1200),
            mock.patch.object(setting, "EXPORT_DATA_MAX_FAILED_TIMES", 10),
            mock.patch.object(setting, "EXPORT_DATA_MAX_RETRY_TIMES", 100),
            mock.patch.object(setting, "ITEM_FILTER_ENABLE", False),
        ]
        for patcher in self.setting_patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_scheduler_sends_export_failed_warning_with_class_name(self):
        with mock.patch.object(ParserControl, "get_task_status_count", return_value=(0, 0, 0)):
            scheduler = build_scheduler(spider_name="RedditClean", export_falied_times=11)
            scheduler.check_task_status()

        self.assertEqual(len(scheduler.sent_messages), 1)
        self.assertEqual(scheduler.sent_messages[0]["level"], "warning")
        self.assertEqual(scheduler.sent_messages[0]["message_prefix"], "《RedditClean》爬虫导出数据失败")
        self.assertIn("失败次数：11", scheduler.sent_messages[0]["msg"])

    def test_item_buffer_does_not_send_msg_on_export_failed(self):
        buffer = build_item_buffer(export_falied_times=10)
        with (
            mock.patch.object(ItemBuffer, "_ItemBuffer__pick_items", side_effect=[{"test_table": [{}]}, {}]),
            mock.patch.object(ItemBuffer, "_ItemBuffer__export_to_db", return_value=False),
            mock.patch("feapder.buffer.item_buffer.tools.send_msg") as send_msg,
        ):
            buffer._ItemBuffer__add_item_to_db([], [], [], [], [])

        send_msg.assert_not_called()


if __name__ == "__main__":
    unittest.main()
