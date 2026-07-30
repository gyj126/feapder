# -*- coding: utf-8 -*-
"""
场景二：接口模式 + 流式压缩上传 COS

任务表存的是文件 ID，需先请求下载接口换取加签直链（有效期只有几分钟），
框架会在解析出直链后紧邻执行下载，下载/压缩/上传全程流式，不落临时盘。

依赖 stream-zip 与 cos-python-sdk-v5:
    uv add stream-zip cos-python-sdk-v5
任务表结构见 table.sql
"""

import json
from datetime import datetime

import feapder
from feapder.utils.log import log


class CosFileSpider(feapder.FileSpider):
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
        self.bucket = "my-bucket-1250000000"
        # from qcloud_cos import CosConfig, CosS3Client
        # self.cos_client = CosS3Client(
        #     CosConfig(Region="ap-guangzhou", SecretId="xxx", SecretKey="xxx")
        # )

    def start_requests(self, task):
        """每个文件一个槽位，槽位请求打的是下载接口"""
        for file_id in json.loads(task.file_ids):
            yield feapder.Request(
                "https://api.example.com/download",
                method="POST",
                json={"file_id": file_id},
                callback=self.parse_download_api,
                validate=self.validate_api,
                dedup_key=file_id,
                file_id=file_id,
            )

    def parse_download_api(self, request, response):
        """解析下载接口，产出唯一的下载请求；框架会立刻在当前线程完成下载"""
        yield self.download_request(request.task, response.json["data"]["download_url"])

    def validate_api(self, request, response):
        """校验下载接口响应：限流触发重试，业务失败直接丢弃"""
        code = response.json.get("code")
        if code == 429:
            raise Exception(f"接口限流 file_id={request.file_id}")
        if code != 0:
            log.warning(f"下载接口业务失败 file_id={request.file_id} code={code}")
            return False

    def validate(self, request, response):
        """兜底校验，这里只会收到文件响应"""
        if "text/html" in response.headers.get("content-type", ""):
            raise Exception(f"响应不是文件 url={request.url}")
        return super().validate(request, response)

    def file_path(self, request):
        """派发期调用，只能用任务字段和槽位请求上的自定义字段拼路径"""
        return f"files/{request.task.id}/{request.file_id}.zip"

    def zip_chunks(self, inner_name, chunks):
        """把单个文件流包成 zip 流"""
        from stream_zip import ZIP_64, stream_zip

        def members():
            yield inner_name, datetime.now(), 0o600, ZIP_64, chunks

        return stream_zip(members())

    def process_file(self, request, response):
        """边下边压边传，整个文件不进内存。put_object 是覆盖语义，天然幂等"""
        # self.cos_client.put_object(
        #     Bucket=self.bucket,
        #     Key=request.file_path,
        #     Body=self.zip_chunks(f"{request.file_id}.pdf", self.file_chunks(response)),
        # )
        size = sum(len(chunk) for chunk in self.file_chunks(response))
        log.info(f"任务{request.task_id} 上传成功 key={request.file_path} size={size}")
        return None

    def on_task_all_done(self, task, result, stats):
        log.info(f"任务{task.id} 完成 成功={stats.success} 失败={stats.fail}")
        if stats.fail == 0 and stats.success > 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)


if __name__ == "__main__":
    spider = CosFileSpider(
        redis_key="cos_file_spider",
        task_table="file_task",
        task_keys=["id", "file_ids"],
        file_dedup="redis",
    )
    spider.start()
