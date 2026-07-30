# -*- coding: utf-8 -*-
"""
FileSpider 接口模式集成测试

用真实的 ParserControl 驱动槽位请求，验证两步链路的重试语义：
下载阶段失败时整个槽位请求重试，也就是重新调下载接口换一条新直链，
而不是就地重试已经过期的旧直链。

不依赖 Redis/MySQL/网络：接口响应与文件响应都由 download_midware 直接给出。
"""

from types import SimpleNamespace

import feapder
import feapder.setting as setting
from feapder.core.parser_control import ParserControl
from feapder.core.spiders.file_spider import FileSpider
from feapder.network.response import Response


class RetrySpySpider(FileSpider):
    """记录接口调用与下载尝试；前两次下载失败，第三次成功"""

    name = "retry_spy"

    def __init__(self):
        self.api_calls = []
        self.downloads = []
        self.processed = []
        self._file_dedup = None
        self._redis_key = "resolve_integration"
        self._save_dir = "/tmp"
        self._dedup_key_overridden = False

    def file_path(self, request):
        return f"/tmp/{request.file_id}.pdf"

    def fake_api(self, request):
        return request, Response.from_text('{"code": 0}', url=request.url)

    def parse_api(self, request, response):
        self.api_calls.append(request.file_id)
        # 每次调接口都签发一条新直链
        url = f"https://cdn.example.com/{request.file_id}.pdf?sig={len(self.api_calls)}"
        yield self.download_request(request.task, url, download_midware=self.fake_download)

    def fake_download(self, request):
        self.downloads.append(request.url)
        if len(self.downloads) < 3:
            raise Exception("直链已过期")
        return request, Response.from_text("BINARY", url=request.url)

    def process_file(self, request, response):
        self.processed.append(request.url)
        return None

    def validate(self, request, response):
        return None

    def on_task_all_done(self, task, result, stats):
        return []

    def record_and_check_done(self, *args):
        return 1, 1, 1, 0, 0, 0

    def _assemble_results(self, task_id, total):
        return ["/tmp/f1.pdf"]

    def _cleanup_task_redis(self, task_id):
        return None


class DummyBuffer:
    def __init__(self):
        self.requests = []

    def put_request(self, request):
        self.requests.append(request)

    def put_failed_request(self, request):
        pass

    def put_del_request(self, request):
        pass

    def put_item(self, item):
        pass

    def get_items_count(self):
        return 0

    def flush(self):
        pass


def build_slot_request(spider, task):
    """构造一个已完成派发期注入的接口模式槽位请求"""
    request = feapder.Request(
        "https://api.example.com/dl",
        json={"id": "f1"},
        callback=spider._on_slot_response,
        download_midware=spider.fake_api,
        file_id="f1",
    )
    request.task = task
    request.task_id = task.id
    request.index = 0
    request.run_id = "rid"
    request.file_path = "/tmp/f1.pdf"
    request.slot_callback = "parse_api"
    request.dedup_key = None
    request.parser_name = spider.name
    return request


def test_download_failure_retries_from_api():
    setting.SAVE_FAILED_REQUEST = False
    setting.SPIDER_MAX_RETRY_TIMES = 5

    spider = RetrySpySpider()
    task = SimpleNamespace(id=1)
    request_buffer = DummyBuffer()
    controller = ParserControl(None, "resolve_integration", request_buffer, DummyBuffer())
    controller.add_parser(spider)

    request = build_slot_request(spider, task)
    for _ in range(3):
        controller.deal_request({"request_obj": request, "request_redis": None})
        if spider.processed:
            break
        # 框架把失败的槽位请求重新入队，取出来模拟下一轮消费
        request = request_buffer.requests.pop()

    assert len(spider.api_calls) == 3, "每次重试都应重新调下载接口"
    assert len(set(spider.downloads)) == 3, "每次重试都应使用新签发的直链"
    assert spider.processed == ["https://cdn.example.com/f1.pdf?sig=3"], "最终应落地最新直链的内容"
