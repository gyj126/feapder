# -*- coding: utf-8 -*-
"""
场景三：上传云存储 + 结果入库

下载文件上传到云存储后，将有序的云存储 URL 列表组装成 Item 写入结果表。

使用前先创建结果 Item:
    feapder create -i file_result

然后编辑 items/file_result_item.py 添加 task_id、result_urls 字段。
"""

import json
import os
from urllib.parse import urlparse, unquote

import feapder
from feapder import ArgumentParser
from feapder.network.item import Item
from feapder.utils.log import log


class FileResultItem(Item):
    """
    结果表 Item（实际项目中应通过 feapder create -i 生成）
    对应的 MySQL 表:
        CREATE TABLE `file_result` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `task_id` int(11) DEFAULT NULL,
          `result_urls` text COMMENT '云存储URL列表，JSON数组',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = "file_result"
        self.task_id = None
        self.result_urls = None


class OssResultSpider(feapder.FileSpider):
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
        # self.oss_client = OSSClient(bucket="my-bucket")

    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    OSS_BASE_URL = "https://my-bucket.oss.aliyuncs.com"

    def file_path(self, task, url, index):
        """返回 OSS 存储 key（即 result 列表里要存的值）"""
        filename = os.path.basename(unquote(urlparse(url).path))
        return f"files/{task.id}/{index}_{filename}"

    def process_file(self, task_id, url, file_path, response):
        # self.oss_client.put_object(file_path, response.content)
        return None

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        # result 与 start_requests 中 yield 的下载请求顺序严格位置对应
        # 元素是 file_path() 返回的 OSS key，下载失败/跳过为 None
        log.info(
            f"任务{task.id} 完成 成功={success_count} 失败={fail_count} "
            f"跳过={skipped_count} 去重={dup_count}"
        )

        # 把 OSS key 拼成可访问 URL 后写入结果表
        result_urls = [
            f"{self.OSS_BASE_URL}/{key}" if key else None for key in result
        ]
        item = FileResultItem()
        item.task_id = task.id
        item.result_urls = result_urls
        yield item

        if fail_count == 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)


if __name__ == "__main__":
    spider = OssResultSpider(
        redis_key="oss_result_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
    )

    parser = ArgumentParser(description="OssResultSpider 文件下载爬虫")
    parser.add_argument(
        "--start_master",
        action="store_true",
        help="添加任务",
        function=spider.start_monitor_task,
    )
    parser.add_argument(
        "--start_worker",
        action="store_true",
        help="启动爬虫",
        function=spider.start,
    )
    parser.start()
