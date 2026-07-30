# -*- coding: utf-8 -*-
"""
FileSpider 接口模式（两步链路）与流式原语单元测试

不依赖 Redis/MySQL/网络，覆盖：
- 派发期把 start_requests 产出的所有 Request 当作文件槽位并注入元数据
- 接口模式槽位的用户回调被收进 slot_callback，由 _on_slot_response 接管
- _on_slot_response 对下载请求数量的约束（恰好 1 个）与普通 Request 的拒绝
- 下载阶段的异常向上抛出，使槽位请求整体重试（回到重新调接口的起点）
- per-request validate 优先于 parser.validate
- 启用 file_dedup 时接口模式必须提供去重键，去重钩子失败则中断整个任务的派发
- 去重缓存命中时完全不派发请求（连接口都不调）
- 下载响应在校验抛异常时仍被关闭、浏览器仍被归还
- download_midware 返回新请求时不替换原下载请求，槽位上下文完整
- 回调副产物在下载成功后才分发，下载失败一个都不分发
- file_chunks 的零字节校验与响应关闭
"""

import unittest
from types import SimpleNamespace

from _stubs import install_test_stubs

install_test_stubs()

import feapder.setting as setting
from feapder.core.spiders.file_spider import FileSpider
from feapder.network.item import Item
from feapder.network.request import Request


class DummyPipe:
    def __init__(self):
        self.operations = []

    def delete(self, key):
        self.operations.append(("delete", key))
        return self

    def hset(self, key, field, value):
        self.operations.append(("hset", key, field, value))
        return self

    def expire(self, key, ttl):
        self.operations.append(("expire", key, ttl))
        return self

    def execute(self):
        return self.operations


class DummyRedis:
    def __init__(self):
        self.pipe = DummyPipe()

    def pipeline(self):
        return self.pipe


class DummyBuffer:
    def __init__(self):
        self.requests = []
        self.items = []

    def put_request(self, request):
        self.requests.append(request)

    def put_item(self, item):
        self.items.append(item)

    def get_items_count(self):
        return len(self.items)

    def flush(self):
        return None


class DummyFileDedup:
    def __init__(self, mapping=None):
        self.mapping = mapping or {}
        self.get_calls = []
        self.set_calls = []

    def get(self, dedup_key):
        self.get_calls.append(dedup_key)
        return self.mapping.get(dedup_key)

    def set(self, dedup_key, result_url):
        self.set_calls.append((dedup_key, result_url))
        self.mapping[dedup_key] = result_url


class DummyResponse:
    """最小响应桩，覆盖 validate / file_chunks / save_file 用到的接口"""

    def __init__(
        self,
        url="https://cdn.example.com/f1.pdf",
        status_code=200,
        chunks=(b"payload",),
        browser=None,
    ):
        self.url = url
        self.status_code = status_code
        self._chunks = chunks
        self.browser = browser
        self.closed = False
        self.close_count = 0

    def iter_content(self, chunk_size=None):
        return iter(self._chunks)

    def close(self):
        self.closed = True
        self.close_count += 1


class DummyRenderDownloader:
    def __init__(self):
        self.put_back_calls = []

    def put_back(self, browser):
        self.put_back_calls.append(browser)


class DummyParser:
    def __init__(self, produced, name="test_parser"):
        self._produced = produced
        self.name = name

    def start_requests(self, task):
        return list(self._produced)


API_URL = "https://api.example.com/download"


class ResolveSpider(FileSpider):
    """接口模式最小实现：槽位请求打下载接口，回调里产出下载请求"""

    def file_path(self, request):
        return f"files/{request.task.id}/{request.file_id}.pdf"

    def parse_api(self, request, response):
        yield self.download_request(request.task, f"https://cdn.example.com/{request.file_id}.pdf")


def build_spider(spider_cls=ResolveSpider, file_dedup=None):
    """绕过 __init__ 构造 spider，只装配被测代码实际依赖的协作对象"""
    spider = spider_cls.__new__(spider_cls)
    spider._file_dedup = file_dedup
    spider._redis_key = "unit_test_resolve"
    spider._save_dir = "/tmp/feapder"
    spider._redisdb = SimpleNamespace(_redis=DummyRedis())
    spider._request_buffer = DummyBuffer()
    spider._item_buffer = DummyBuffer()
    spider._dedup_key_overridden = type(spider).dedup_key is not FileSpider.dedup_key
    spider._assemble_results = lambda task_id, total: []
    spider._cleanup_task_redis = lambda task_id: None
    spider.on_task_all_done = lambda task, result, stats: []
    return spider


def slot_request(spider, task, file_id="f1", **kwargs):
    """构造一个已完成派发期注入的接口模式槽位请求"""
    request = Request(API_URL, data={"file_id": file_id}, file_id=file_id, **kwargs)
    request.task = task
    request.task_id = task.id
    request.index = 0
    request.run_id = "rid"
    request.dedup_key = file_id
    request.file_path = f"files/{task.id}/{file_id}.pdf"
    request.slot_callback = "parse_api"
    return request


class TestSlotDispatch(unittest.TestCase):
    """派发期：所有 Request 都是文件槽位"""

    def test_raw_request_becomes_slot_with_injected_metadata(self):
        spider = build_spider()
        task = SimpleNamespace(id=7)
        request = Request(API_URL, data={"file_id": "f1"}, callback=spider.parse_api, file_id="f1")

        spider._dispatch_one_task(DummyParser([request]), task)

        self.assertEqual(len(spider._request_buffer.requests), 1)
        dispatched = spider._request_buffer.requests[0]
        self.assertEqual(dispatched.index, 0)
        self.assertEqual(dispatched.task_id, 7)
        self.assertEqual(dispatched.file_path, "files/7/f1.pdf")
        self.assertEqual(dispatched.parser_name, "test_parser")
        # 用户回调被收进 slot_callback，callback 交给框架分派器
        self.assertEqual(dispatched.slot_callback, "parse_api")
        self.assertEqual(dispatched.callback, spider._on_slot_response)

    def test_direct_and_resolve_slots_share_index_sequence(self):
        spider = build_spider()
        task = SimpleNamespace(id=8)
        produced = [
            Request(API_URL, data={"file_id": "a"}, callback=spider.parse_api, file_id="a"),
            spider.download_request(task, "https://cdn.example.com/b.pdf", file_id="b"),
        ]

        spider._dispatch_one_task(DummyParser(produced), task)

        dispatched = spider._request_buffer.requests
        self.assertEqual([r.index for r in dispatched], [0, 1])
        self.assertEqual(dispatched[0].slot_callback, "parse_api")
        # 直链槽位直接进 save_file，没有 slot_callback
        self.assertEqual(dispatched[1].callback, spider.save_file)
        self.assertFalse(hasattr(dispatched[1], "slot_callback"))

    def test_total_counts_every_request(self):
        spider = build_spider()
        task = SimpleNamespace(id=9)
        produced = [
            Request(API_URL, data={"file_id": str(i)}, callback=spider.parse_api, file_id=str(i))
            for i in range(3)
        ]

        spider._dispatch_one_task(DummyParser(produced), task)

        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=spider._redis_key, task_id=9
        )
        operations = spider._redisdb._redis.pipe.operations
        self.assertIn(("hset", progress_key, "total", 3), operations)

    def test_identical_api_url_slots_are_not_deduped(self):
        """接口模式各槽位 url 相同，不能因默认去重键退化成 url 而被折叠成一个文件"""
        spider = build_spider()
        task = SimpleNamespace(id=13)
        produced = [
            Request(API_URL, data={"file_id": str(i)}, callback=spider.parse_api, file_id=str(i))
            for i in range(3)
        ]

        spider._dispatch_one_task(DummyParser(produced), task)

        self.assertEqual(len(spider._request_buffer.requests), 3)
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=spider._redis_key, task_id=13
        )
        operations = spider._redisdb._redis.pipe.operations
        self.assertIn(("hset", progress_key, "dup", 0), operations)
        # 未启用去重时显式置空，保证下游继承元数据时属性存在
        self.assertEqual([r.dedup_key for r in spider._request_buffer.requests], [None] * 3)

    def test_direct_mode_still_dedups_by_url(self):
        """直链模式的默认去重键仍是 url，重复直链只下载一次"""
        spider = build_spider()
        task = SimpleNamespace(id=14)
        produced = [
            spider.download_request(task, "https://cdn.example.com/same.pdf", file_id="a"),
            spider.download_request(task, "https://cdn.example.com/same.pdf", file_id="b"),
        ]

        spider._dispatch_one_task(DummyParser(produced), task)

        self.assertEqual(len(spider._request_buffer.requests), 1)
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=spider._redis_key, task_id=14
        )
        operations = spider._redisdb._redis.pipe.operations
        self.assertIn(("hset", progress_key, "dup", 1), operations)


class TestDedupGuard(unittest.TestCase):
    def test_resolve_slot_without_dedup_key_raises(self):
        spider = build_spider(file_dedup=DummyFileDedup())
        task = SimpleNamespace(id=10)
        request = Request(API_URL, data={"file_id": "f1"}, callback=spider.parse_api, file_id="f1")

        with self.assertRaises(ValueError) as ctx:
            spider._dispatch_one_task(DummyParser([request]), task)

        self.assertIn("接口模式槽位未提供去重键", str(ctx.exception))

    def test_dedup_key_hook_satisfies_guard(self):
        class HookSpider(ResolveSpider):
            def dedup_key(self, request):
                return request.file_id

        spider = build_spider(HookSpider, DummyFileDedup())
        task = SimpleNamespace(id=11)
        request = Request(API_URL, data={"file_id": "f1"}, callback=spider.parse_api, file_id="f1")

        spider._dispatch_one_task(DummyParser([request]), task)

        self.assertEqual(spider._file_dedup.get_calls, ["f1"])
        self.assertEqual(len(spider._request_buffer.requests), 1)

    def test_hook_exception_aborts_dispatch(self):
        """去重键错误会影响文件之间的折叠关系，必须中断整个任务的派发而非带病运行"""

        class BadHookSpider(ResolveSpider):
            def dedup_key(self, request):
                raise RuntimeError("钩子实现有误")

        spider = build_spider(BadHookSpider, DummyFileDedup())
        task = SimpleNamespace(id=15)
        produced = [
            Request(API_URL, data={"file_id": str(i)}, callback=spider.parse_api, file_id=str(i))
            for i in range(3)
        ]

        with self.assertRaises(RuntimeError):
            spider._dispatch_one_task(DummyParser(produced), task)

        self.assertEqual(spider._request_buffer.requests, [])

    def test_cache_hit_skips_api_call_entirely(self):
        dedup = DummyFileDedup({"f1": "files/12/cached.pdf"})
        spider = build_spider(file_dedup=dedup)
        task = SimpleNamespace(id=12)
        request = Request(
            API_URL, data={"file_id": "f1"}, callback=spider.parse_api, file_id="f1", dedup_key="f1"
        )

        spider._dispatch_one_task(DummyParser([request]), task)

        # 命中缓存即不下发槽位请求，下载接口一次都不会被调用
        self.assertEqual(len(spider._request_buffer.requests), 0)
        self.assertEqual(request.file_path, "files/12/cached.pdf")


class TestOnSlotResponse(unittest.TestCase):
    """接口响应分派器：约束校验与元数据继承"""

    def setUp(self):
        self.spider = build_spider()
        self.spider.record_and_check_done = lambda *args: (0, 1, 1, 0, 0, 0)
        self.task = SimpleNamespace(id=20)

    def drive(self, request, response=None):
        return list(self.spider._on_slot_response(request, response or DummyResponse()))

    def test_download_request_inherits_slot_metadata(self):
        captured = {}

        def process_file(request, response):
            captured["request"] = request
            return None

        self.spider.process_file = process_file
        request = slot_request(self.spider, self.task)
        download_response = DummyResponse()
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (r, download_response),
                )
            ]
        )

        self.drive(request)

        downloaded = captured["request"]
        self.assertEqual(downloaded.index, request.index)
        self.assertEqual(downloaded.run_id, request.run_id)
        self.assertEqual(downloaded.task_id, request.task_id)
        self.assertEqual(downloaded.file_path, request.file_path)
        self.assertEqual(downloaded.dedup_key, request.dedup_key)
        self.assertEqual(downloaded.url, "https://cdn.example.com/f1.pdf")
        self.assertTrue(download_response.closed)

    def test_zero_download_request_raises(self):
        request = slot_request(self.spider, self.task)
        self.spider.parse_api = lambda req, resp: iter([])

        with self.assertRaises(Exception) as ctx:
            self.drive(request)

        self.assertIn("恰好1个 download_request", str(ctx.exception))

    def test_two_download_requests_raise(self):
        request = slot_request(self.spider, self.task)
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(req.task, "https://cdn.example.com/a.pdf"),
                self.spider.download_request(req.task, "https://cdn.example.com/b.pdf"),
            ]
        )

        with self.assertRaises(Exception) as ctx:
            self.drive(request)

        self.assertIn("恰好1个 download_request", str(ctx.exception))

    def test_plain_request_in_callback_raises(self):
        request = slot_request(self.spider, self.task)
        self.spider.parse_api = lambda req, resp: iter([Request("https://api.example.com/next")])

        with self.assertRaises(Exception) as ctx:
            self.drive(request)

        self.assertIn("不支持 yield 普通 Request", str(ctx.exception))

    def test_non_request_products_are_forwarded(self):
        self.spider.process_file = lambda request, response: None
        request = slot_request(self.spider, self.task)
        item = Item()
        download_response = DummyResponse()
        self.spider.parse_api = lambda req, resp: iter(
            [
                item,
                self.spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (r, download_response),
                ),
            ]
        )

        results = self.drive(request)

        self.assertIn(item, results)

    def test_inherits_null_dedup_key(self):
        """未启用去重时槽位的 dedup_key 为 None，继承时不能因属性缺失而报错"""
        self.spider.process_file = lambda request, response: None
        request = slot_request(self.spider, self.task)
        request.dedup_key = None
        download_response = DummyResponse()
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (r, download_response),
                )
            ]
        )

        self.drive(request)

        self.assertTrue(download_response.closed)

    def test_download_request_cannot_redeclare_dedup_key(self):
        self.spider._file_dedup = DummyFileDedup()
        request = slot_request(self.spider, self.task)
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(
                    req.task, "https://cdn.example.com/f1.pdf", dedup_key="other"
                )
            ]
        )

        with self.assertRaises(ValueError) as ctx:
            self.drive(request)

        self.assertIn("不可再传 dedup_key", str(ctx.exception))


class TestRetryFallsBackToApi(unittest.TestCase):
    """下载阶段失败必须向上抛，由槽位请求整体重试，而不是就地重试旧直链"""

    def setUp(self):
        self.spider = build_spider()
        self.spider.record_and_check_done = lambda *args: (0, 1, 0, 1, 0, 0)
        self.task = SimpleNamespace(id=30)

    def test_download_exception_propagates(self):
        request = slot_request(self.spider, self.task)

        def failing_midware(req):
            raise Exception("直链已过期")

        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(
                    req.task, "https://cdn.example.com/f1.pdf", download_midware=failing_midware
                )
            ]
        )

        with self.assertRaises(Exception) as ctx:
            list(self.spider._on_slot_response(request, DummyResponse()))

        self.assertIn("直链已过期", str(ctx.exception))

    def test_process_file_exception_propagates(self):
        request = slot_request(self.spider, self.task)
        download_response = DummyResponse()

        def process_file(req, resp):
            raise Exception("写盘失败")

        self.spider.process_file = process_file
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (r, download_response),
                )
            ]
        )

        with self.assertRaises(Exception) as ctx:
            list(self.spider._on_slot_response(request, DummyResponse()))

        self.assertIn("写盘失败", str(ctx.exception))
        # 即使 process_file 抛异常，响应仍被关闭
        self.assertTrue(download_response.closed)

    def test_validate_false_counts_fail_without_retry(self):
        self.spider.record_and_check_done = lambda *args: (1, 1, 0, 1, 0, 0)
        failed = []
        self.spider.on_file_failed = lambda request, error: failed.append(request.url)
        request = slot_request(self.spider, self.task)
        download_response = DummyResponse(status_code=404)
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (r, download_response),
                )
            ]
        )

        list(self.spider._on_slot_response(request, DummyResponse()))

        self.assertEqual(failed, ["https://cdn.example.com/f1.pdf"])
        self.assertTrue(download_response.closed)


class TestDownloadResponseRelease(unittest.TestCase):
    """下载响应是 _download_sync 的局部变量，外层 ParserControl 看不到，必须自行释放"""

    def setUp(self):
        self.spider = build_spider()
        self.spider.record_and_check_done = lambda *args: (0, 1, 1, 0, 0, 0)
        self.spider.process_file = lambda request, response: None
        self.task = SimpleNamespace(id=50)

    def yield_download(self, download_response, **kwargs):
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (r, download_response),
                    **kwargs,
                )
            ]
        )

    def test_validate_exception_still_closes_response(self):
        """5xx 之类由 validate 抛异常的场景，响应必须关闭，否则持续泄漏连接池"""
        download_response = DummyResponse(status_code=502)
        self.yield_download(download_response)
        request = slot_request(self.spider, self.task)

        with self.assertRaises(Exception) as ctx:
            list(self.spider._on_slot_response(request, DummyResponse()))

        self.assertIn("HTTP 502", str(ctx.exception))
        self.assertTrue(download_response.closed)

    def test_browser_returned_after_download(self):
        browser = object()
        download_response = DummyResponse(browser=browser)
        downloader = DummyRenderDownloader()
        self.yield_download(download_response)
        request = slot_request(self.spider, self.task)

        with_downloader = Request.render_downloader
        Request.render_downloader = downloader
        try:
            list(self.spider._on_slot_response(request, DummyResponse()))
        finally:
            Request.render_downloader = with_downloader

        self.assertEqual(downloader.put_back_calls, [browser])

    def test_browser_returned_when_validate_raises(self):
        browser = object()
        download_response = DummyResponse(status_code=502, browser=browser)
        downloader = DummyRenderDownloader()
        self.yield_download(download_response)
        request = slot_request(self.spider, self.task)

        with_downloader = Request.render_downloader
        Request.render_downloader = downloader
        try:
            with self.assertRaises(Exception):
                list(self.spider._on_slot_response(request, DummyResponse()))
        finally:
            Request.render_downloader = with_downloader

        self.assertEqual(downloader.put_back_calls, [browser])


class TestMidwareReplacingRequest(unittest.TestCase):
    """download_midware 返回新请求时不得替换原下载请求，否则丢失全部槽位上下文"""

    def test_replacement_request_is_not_adopted(self):
        spider = build_spider()
        spider.record_and_check_done = lambda *args: (0, 1, 1, 0, 0, 0)
        task = SimpleNamespace(id=60)
        captured = {}
        spider.process_file = lambda request, response: captured.setdefault("request", request)

        download_response = DummyResponse()
        replacement = Request("https://cdn.example.com/replaced.pdf")
        spider.parse_api = lambda req, resp: iter(
            [
                spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (replacement, download_response),
                )
            ]
        )
        request = slot_request(spider, task)

        list(spider._on_slot_response(request, DummyResponse()))

        downloaded = captured["request"]
        self.assertIsNot(downloaded, replacement)
        self.assertEqual(downloaded.url, "https://cdn.example.com/f1.pdf")
        self.assertEqual(downloaded.task_id, 60)
        self.assertEqual(downloaded.index, 0)
        self.assertEqual(downloaded.file_path, "files/60/f1.pdf")


class TestSideProductsDeferred(unittest.TestCase):
    """回调副产物必须等下载成功后才分发，否则重试会让同一批 Item 重复入库"""

    def setUp(self):
        self.spider = build_spider()
        self.task = SimpleNamespace(id=70)
        self.item = Item()

    def yield_item_then_download(self, download_response):
        self.spider.parse_api = lambda req, resp: iter(
            [
                self.item,
                self.spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    download_midware=lambda r: (r, download_response),
                ),
            ]
        )

    def test_not_dispatched_when_download_fails(self):
        self.spider.record_and_check_done = lambda *args: (0, 1, 0, 1, 0, 0)
        self.spider.process_file = lambda request, response: (_ for _ in ()).throw(
            Exception("写盘失败")
        )
        self.yield_item_then_download(DummyResponse())
        request = slot_request(self.spider, self.task)

        collected = []
        with self.assertRaises(Exception):
            for produced in self.spider._on_slot_response(request, DummyResponse()):
                collected.append(produced)

        self.assertEqual(collected, [])

    def test_dispatched_before_task_done_products_on_success(self):
        done_item = Item()
        self.spider.record_and_check_done = lambda *args: (1, 1, 1, 0, 0, 0)
        self.spider.on_task_all_done = lambda task, result, stats: [done_item]
        self.spider.process_file = lambda request, response: None
        self.yield_item_then_download(DummyResponse())
        request = slot_request(self.spider, self.task)

        results = list(self.spider._on_slot_response(request, DummyResponse()))

        self.assertIs(results[0], self.item)
        self.assertIn(done_item, results[1:])


class TestPerRequestValidate(unittest.TestCase):
    def test_request_validate_takes_priority(self):
        spider = build_spider()
        spider.record_and_check_done = lambda *args: (0, 1, 1, 0, 0, 0)
        spider.process_file = lambda request, response: None
        task = SimpleNamespace(id=40)
        seen = []
        request = slot_request(spider, task)
        download_response = DummyResponse()

        def validate_file(req, resp):
            seen.append(resp.url)

        spider.parse_api = lambda req, resp: iter(
            [
                spider.download_request(
                    req.task,
                    "https://cdn.example.com/f1.pdf",
                    validate=validate_file,
                    download_midware=lambda r: (r, download_response),
                )
            ]
        )

        list(spider._on_slot_response(request, DummyResponse()))

        self.assertEqual(seen, ["https://cdn.example.com/f1.pdf"])

    def test_validate_serializes_to_method_name(self):
        spider = build_spider()
        request = Request(
            "https://cdn.example.com/f1.pdf", validate=spider.validate, callback=spider.parse_api
        )

        request_dict = request.to_dict

        self.assertEqual(request_dict["validate"], "validate")
        self.assertEqual(request_dict["callback"], "parse_api")

    def test_validate_absent_when_not_set(self):
        request = Request("https://cdn.example.com/f1.pdf")

        self.assertNotIn("validate", request.to_dict)


class TestFileChunks(unittest.TestCase):
    def test_yields_non_empty_chunks(self):
        spider = build_spider()
        response = DummyResponse(chunks=(b"a", b"", b"bc"))

        self.assertEqual(list(spider.file_chunks(response)), [b"a", b"bc"])

    def test_empty_stream_raises(self):
        spider = build_spider()
        response = DummyResponse(chunks=(b"", b""))

        with self.assertRaises(Exception) as ctx:
            list(spider.file_chunks(response))

        self.assertIn("文件内容为空", str(ctx.exception))

    def test_default_process_file_streams_to_disk(self):
        import os
        import tempfile

        spider = build_spider()
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "nested", "f1.pdf")
            request = Request("https://cdn.example.com/f1.pdf")
            request.file_path = file_path
            response = DummyResponse(chunks=(b"hello ", b"world"))

            spider.process_file(request, response)

            with open(file_path, "rb") as f:
                self.assertEqual(f.read(), b"hello world")


if __name__ == "__main__":
    unittest.main()
