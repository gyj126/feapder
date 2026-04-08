# -*- coding: utf-8 -*-
"""
场景二：上传云存储（不落盘）

重写 process_file 实现直接上传云存储，文件不保存到本地磁盘。
"""

import json
import os
from urllib.parse import urlparse, unquote

import feapder
from feapder.utils.log import log


class OssFileSpider(feapder.FileSpider):
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 初始化云存储客户端
        # self.oss_client = OSSClient(bucket="my-bucket")

    def get_download_urls(self, task):
        return json.loads(task.file_urls)

    def get_file_path(self, task, url, index):
        """返回 OSS 存储 key（不是本地路径）"""
        filename = os.path.basename(unquote(urlparse(url).path))
        return f"files/{task.id}/{index}_{filename}"

    def process_file(self, task_id, url, file_path, response):
        """上传到 OSS，返回云存储 URL"""
        # self.oss_client.put_object(file_path, response.content)
        cloud_url = f"https://my-bucket.oss.aliyuncs.com/{file_path}"
        log.info(f"任务{task_id} 上传成功 url={cloud_url}")
        return cloud_url

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        log.info(f"任务{task.id} 完成 成功={success_count} 失败={fail_count}")
        if fail_count == 0 and success_count > 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)


if __name__ == "__main__":
    spider = OssFileSpider(
        redis_key="oss_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
    )
    spider.start()
