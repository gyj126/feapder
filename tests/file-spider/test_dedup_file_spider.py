# -*- coding: utf-8 -*-
"""
场景四：启用文件去重

通过 file_dedup 参数启用去重，同一 URL 跨任务不重复下载。

去重行为：
- start_requests 中遍历 URL 列表时，先查去重缓存
- 缓存命中：直接复用已有结果，不生成 Request，不重复下载
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

    def get_download_urls(self, task):
        return json.loads(task.file_urls)

    def on_file_downloaded(self, task_id, url, file_path):
        log.info(f"任务{task_id} 文件就绪 path={file_path}")

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        log.info(f"任务{task.id} 完成 成功={success_count} 失败={fail_count}")
        yield self.update_task_batch(task.id, 1 if fail_count == 0 and success_count > 0 else -1)


if __name__ == "__main__":
    spider = DedupFileSpider(
        redis_key="dedup_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
        save_dir="./downloads",
        file_dedup="redis",  # "redis" / "mysql" / FileDedup 实例
    )
    spider.start()
