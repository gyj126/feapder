# -*- coding: utf-8 -*-
"""
场景四：启用文件去重

通过 file_dedup 参数启用去重，同一 URL 跨任务不重复下载。

去重行为：
- 框架在派发用户 yield 的下载请求前，先查去重缓存
- 缓存命中：直接复用已有结果，不下发该请求，不重复下载
- 缓存未命中：正常下载，成功后自动写入去重缓存
- 跨任务共享：不同任务中出现的相同 URL 只下载一次
"""

import json

import feapder
from feapder.utils.log import log


class DedupFileSpider(feapder.FileSpider):
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
        log.info(f"任务{request.task_id} 文件就绪 path={request.file_path}")

    def on_task_all_done(self, task, result, stats):
        log.info(f"任务{task.id} 完成 成功={stats.success} 失败={stats.fail}")
        yield self.update_task_batch(task.id, 1 if stats.fail == 0 and stats.success > 0 else -1)


if __name__ == "__main__":
    spider = DedupFileSpider(
        redis_key="dedup_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
        save_dir="./downloads",
        file_dedup="redis",  # "redis" / "mysql" / FileDedup 实例
    )
    spider.start()
