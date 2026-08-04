import unittest

from feapder.core.spiders.batch_spider import BatchSpider


class DummyMySQLDB:
    def __init__(self):
        self.sqls = []

    def execute(self, sql, params=None):
        self.sqls.append(sql)
        return 0


class TestBatchSpider(unittest.TestCase):
    def test_create_batch_record_table_uses_idempotent_ddl(self):
        spider = BatchSpider.__new__(BatchSpider)
        spider._batch_record_table = "test_batch_record"
        spider._mysqldb = DummyMySQLDB()

        spider.create_batch_record_table()
        spider.create_batch_record_table()

        self.assertEqual(len(spider._mysqldb.sqls), 2)
        for sql in spider._mysqldb.sqls:
            self.assertIn(
                "CREATE TABLE IF NOT EXISTS `test_batch_record`", sql
            )
            self.assertNotIn("information_schema", sql)


if __name__ == "__main__":
    unittest.main()
