# -*- coding: utf-8 -*-
"""
场景一：保存到本地磁盘

最简单的用法，下载文件保存到本地。
任务表结构见 table.sql
"""

import json

import feapder
from feapder.utils.log import log


class LocalFileSpider(feapder.FileSpider):
    __custom_setting__ = dict(
        REDISDB_IP_PORTS="localhost:6379",
        REDISDB_USER_PASS="",
        REDISDB_DB=0,
        MYSQL_IP="localhost",
        MYSQL_PORT=3306,
        MYSQL_DB="feapder",
        MYSQL_USER_NAME="feapder",
        MYSQL_USER_PASS="feapder123",
    )

    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    def on_file_downloaded(self, request):
        log.info(f"任务{request.task_id} 文件保存成功 path={request.file_path}")

    def on_task_all_done(self, task, result, stats):
        if stats.fail == 0 and stats.success > 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)


if __name__ == "__main__":
    spider = LocalFileSpider(
        redis_key="local_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
        save_dir="./downloads",
    )
    spider.start()
