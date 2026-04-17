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
        for index, url in enumerate(json.loads(task.file_urls)):
            filename = os.path.basename(unquote(urlparse(url).path))
            oss_key = f"files/{task.id}/{index}_{filename}"
            yield self.download_request(task, url, file_path=oss_key)

    def process_file(self, task_id, url, file_path, response):
        # self.oss_client.put_object(file_path, response.content)
        return f"https://my-bucket.oss.aliyuncs.com/{file_path}"

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        # result 与 start_requests 中 yield 的下载请求顺序严格位置对应
        # 例: ["https://oss.com/a.jpg", "https://oss.com/b.jpg", None, "https://oss.com/d.jpg"]
        log.info(
            f"任务{task.id} 完成 成功={success_count} 失败={fail_count} "
            f"跳过={skipped_count} 去重={dup_count}"
        )

        # 组装结果 Item 写入结果表
        item = FileResultItem()
        item.task_id = task.id
        item.result_urls = result
        yield item

        # 更新任务状态
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
