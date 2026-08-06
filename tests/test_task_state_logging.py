import io
import logging
import unittest
from unittest import mock

from feapder.core.base_parser import TaskParser
from feapder.core.spiders.batch_spider import DebugBatchSpider
from feapder.core.spiders.file_spider import DebugFileSpider
from feapder.core.spiders.task_spider import DebugTaskSpider
from feapder.db.mysqldb import MysqlDB


class DummyMysqlDB:
    def __init__(self, affect_count):
        self.affect_count = affect_count

    def update(self, sql):
        return self.affect_count


class TestTaskStateLogging(unittest.TestCase):
    parser_cases = (
        (TaskParser, "feapder.core.base_parser.log"),
        (DebugTaskSpider, "feapder.core.spiders.task_spider.log"),
        (DebugBatchSpider, "feapder.core.spiders.batch_spider.log"),
        (DebugFileSpider, "feapder.core.spiders.file_spider.log"),
    )

    @staticmethod
    def make_parser(parser_class, affect_count):
        parser = parser_class.__new__(parser_class)
        parser._mysqldb = DummyMysqlDB(affect_count)
        parser._task_table = "model_files_task"
        parser._task_state = "state"
        parser._update_task = True
        return parser

    def test_update_success_logs_debug(self):
        for parser_class, log_path in self.parser_cases:
            with self.subTest(parser_class=parser_class.__name__), mock.patch(
                log_path
            ) as mocked_log:
                parser = self.make_parser(parser_class, 1)

                parser.update_task_state(172298654, 1)

                mocked_log.debug.assert_called_once()
                mocked_log.warning.assert_not_called()
                mocked_log.error.assert_not_called()

    def test_zero_affected_rows_logs_warning(self):
        for parser_class, log_path in self.parser_cases:
            with self.subTest(parser_class=parser_class.__name__), mock.patch(
                log_path
            ) as mocked_log:
                parser = self.make_parser(parser_class, 0)

                parser.update_task_state(172298654, 1)

                warning = mocked_log.warning.call_args.args[0]
                self.assertIn("任务可能不存在或目标状态已经是1", warning)
                self.assertIn("172298654", warning)
                self.assertIn("model_files_task", warning)
                mocked_log.debug.assert_not_called()
                mocked_log.error.assert_not_called()

    def test_database_error_logs_error(self):
        for parser_class, log_path in self.parser_cases:
            with self.subTest(parser_class=parser_class.__name__), mock.patch(
                log_path
            ) as mocked_log:
                parser = self.make_parser(parser_class, None)

                parser.update_task_state(172298654, 1)

                error = mocked_log.error.call_args.args[0]
                self.assertIn("数据库执行异常", error)
                self.assertIn("172298654", error)
                self.assertIn("model_files_task", error)
                mocked_log.debug.assert_not_called()
                mocked_log.warning.assert_not_called()


class TestMysqlDBUpdateLogging(unittest.TestCase):
    def test_update_exception_logs_full_traceback_and_returns_none(self):
        db = MysqlDB.__new__(MysqlDB)
        connection = mock.Mock()
        cursor = mock.Mock()
        cursor.execute.side_effect = RuntimeError("database unavailable")
        db.get_connection = mock.Mock(return_value=(connection, cursor))
        db.close_connection = mock.Mock()
        sql = "update `model_files_task` set `state`=1 where id = 172298654"
        log_output = io.StringIO()
        logger = logging.getLogger(self.id())
        logger.handlers = [logging.StreamHandler(log_output)]
        logger.setLevel(logging.ERROR)
        logger.propagate = False

        with mock.patch("feapder.db.mysqldb.log", logger):
            result = db.update(sql)

        self.assertIsNone(result)
        exception_log = log_output.getvalue()
        self.assertIn("Traceback (most recent call last)", exception_log)
        self.assertIn("RuntimeError: database unavailable", exception_log)
        self.assertIn(sql, exception_log)
        db.close_connection.assert_called_once_with(connection, cursor)


if __name__ == "__main__":
    unittest.main()
