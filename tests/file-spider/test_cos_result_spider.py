# -*- coding: utf-8 -*-
"""
场景三：接口模式 + 上传 COS + 结果入库

先请求下载接口换取加签直链，流式上传到 COS 后，
将有序的 COS URL 列表组装成 Item 写入结果表。

使用前先创建结果 Item:
    feapder create -i file_result

然后编辑 items/file_result_item.py 添加 task_id、result_urls 字段。
任务表结构见 table.sql
"""

import json

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


class CosResultSpider(feapder.FileSpider):
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

    COS_BASE_URL = "https://my-bucket-1250000000.cos.ap-guangzhou.myqcloud.com"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = "my-bucket-1250000000"
        # from qcloud_cos import CosConfig, CosS3Client
        # self.cos_client = CosS3Client(
        #     CosConfig(Region="ap-guangzhou", SecretId="xxx", SecretKey="xxx")
        # )

    def start_requests(self, task):
        for file_id in json.loads(task.file_ids):
            yield feapder.Request(
                "https://api.example.com/download",
                method="POST",
                json={"file_id": file_id},
                callback=self.parse_download_api,
                dedup_key=file_id,
                file_id=file_id,
            )

    def parse_download_api(self, request, response):
        yield self.download_request(request.task, response.json["data"]["download_url"])

    def file_path(self, request):
        """返回 COS 存储 key（即 result 列表里要存的值）"""
        return f"files/{request.task.id}/{request.file_id}.pdf"

    def process_file(self, request, response):
        """流式上传，整个文件不进内存"""
        # self.cos_client.put_object(
        #     Bucket=self.bucket, Key=request.file_path, Body=self.file_chunks(response)
        # )
        size = sum(len(chunk) for chunk in self.file_chunks(response))
        log.info(f"任务{request.task_id} 上传成功 key={request.file_path} size={size}")
        return None

    def on_task_all_done(self, task, result, stats):
        # result 与 start_requests 中 yield 的槽位顺序严格位置对应
        # 元素是 file_path() 返回的 COS key，失败/跳过为 None
        log.info(
            f"任务{task.id} 完成 成功={stats.success} 失败={stats.fail} "
            f"跳过={stats.skipped} 去重={stats.dup}"
        )

        # 把 COS key 拼成可访问 URL 后写入结果表
        result_urls = [f"{self.COS_BASE_URL}/{key}" if key else None for key in result]
        item = FileResultItem()
        item.task_id = task.id
        item.result_urls = result_urls
        yield item

        if stats.fail == 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)


if __name__ == "__main__":
    spider = CosResultSpider(
        redis_key="cos_result_spider",
        task_table="file_task",
        task_keys=["id", "file_ids"],
        file_dedup="redis",
    )

    parser = ArgumentParser(description="CosResultSpider 文件下载爬虫")
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
