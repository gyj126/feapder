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

    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    def file_path(self, request):
        """返回 OSS 存储 key，即 result 列表里要存的值"""
        filename = os.path.basename(unquote(urlparse(request.url).path))
        return f"files/{request.task.id}/{request.index}_{filename}"

    def process_file(self, request, response):
        """上传到 OSS。返回 None=成功，返回 False=显式失败，抛异常=重试"""
        # self.oss_client.put_object(request.file_path, response.content)
        log.info(f"任务{request.task_id} 上传成功 key={request.file_path}")
        return None

    def on_task_all_done(self, task, result, stats):
        log.info(f"任务{task.id} 完成 成功={stats.success} 失败={stats.fail}")
        if stats.fail == 0 and stats.success > 0:
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
