# -*- coding: utf-8 -*-
"""
FileSpider dedup_key 接线与 _resolve_dedup_key 单元测试

不依赖 Redis/MySQL，覆盖：
- FileSpider._resolve_dedup_key 的优先级解析（显式参数 > 钩子 > 默认 URL）
- FileSpider 在任务派发/成功回写阶段对 dedup_key 的实际接线

normalize_url 的纯函数测试见 test_normalize_url.py
"""

import sys
import types
import unittest
from types import SimpleNamespace


def install_test_stubs():
    if "redis" not in sys.modules:
        redis_module = types.ModuleType("redis")
        redis_connection = types.ModuleType("redis.connection")
        redis_connection.Encoder = type("Encoder", (), {})

        redis_exceptions = types.ModuleType("redis.exceptions")
        for name in ["ConnectionError", "TimeoutError", "DataError", "NoScriptError"]:
            setattr(redis_exceptions, name, type(name, (Exception,), {}))

        redis_sentinel = types.ModuleType("redis.sentinel")
        redis_sentinel.Sentinel = type("Sentinel", (), {})

        redis_cluster = types.ModuleType("redis.cluster")
        redis_cluster.RedisCluster = type("RedisCluster", (), {})
        redis_cluster.ClusterNode = type("ClusterNode", (), {})

        class DummyStrictRedis:
            def __init__(self, *args, **kwargs):
                pass

            @classmethod
            def from_url(cls, *args, **kwargs):
                return cls()

            def ping(self):
                return True

        redis_module.StrictRedis = DummyStrictRedis
        redis_module.connection = redis_connection
        redis_module.exceptions = redis_exceptions
        redis_module.sentinel = redis_sentinel

        sys.modules["redis"] = redis_module
        sys.modules["redis.connection"] = redis_connection
        sys.modules["redis.exceptions"] = redis_exceptions
        sys.modules["redis.sentinel"] = redis_sentinel
        sys.modules["redis.cluster"] = redis_cluster

    if "pymysql" not in sys.modules:
        pymysql_module = types.ModuleType("pymysql")
        pymysql_cursors = types.ModuleType("pymysql.cursors")
        pymysql_cursors.SSCursor = object
        pymysql_err = types.ModuleType("pymysql.err")
        pymysql_err.InterfaceError = type("InterfaceError", (Exception,), {})
        pymysql_err.OperationalError = type("OperationalError", (Exception,), {})

        pymysql_module.cursors = pymysql_cursors
        pymysql_module.err = pymysql_err

        sys.modules["pymysql"] = pymysql_module
        sys.modules["pymysql.cursors"] = pymysql_cursors
        sys.modules["pymysql.err"] = pymysql_err

    if "dbutils.pooled_db" not in sys.modules:
        dbutils_module = types.ModuleType("dbutils")
        pooled_db_module = types.ModuleType("dbutils.pooled_db")

        class DummyPooledDB:
            def __init__(self, *args, **kwargs):
                pass

        pooled_db_module.PooledDB = DummyPooledDB
        dbutils_module.pooled_db = pooled_db_module

        sys.modules["dbutils"] = dbutils_module
        sys.modules["dbutils.pooled_db"] = pooled_db_module

    if "influxdb" not in sys.modules:
        influxdb_module = types.ModuleType("influxdb")
        influxdb_module.InfluxDBClient = type("InfluxDBClient", (), {})
        sys.modules["influxdb"] = influxdb_module

    if "six" not in sys.modules:
        six_module = types.ModuleType("six")
        six_module.string_types = (str,)
        six_module.text_type = str
        six_module.moves = types.SimpleNamespace(xrange=range)
        sys.modules["six"] = six_module

    # 不桩 w3lib：项目本身依赖 w3lib，让真实库参与 canonicalize_url，
    # 避免桩实现与生产行为发散导致测试失去保护意义

    if "loguru" not in sys.modules:
        loguru_module = types.ModuleType("loguru")

        class DummyLoguruLogger:
            def opt(self, *args, **kwargs):
                return self

            def log(self, *args, **kwargs):
                return None

        loguru_module.logger = DummyLoguruLogger()
        sys.modules["loguru"] = loguru_module

    if "better_exceptions" not in sys.modules:
        better_exceptions = types.ModuleType("better_exceptions")
        better_exceptions.format_exception = lambda *args, **kwargs: ""
        sys.modules["better_exceptions"] = better_exceptions

    if "requests" not in sys.modules:
        requests_module = types.ModuleType("requests")
        requests_module.__path__ = []
        requests_cookies = types.ModuleType("requests.cookies")
        requests_models = types.ModuleType("requests.models")
        requests_adapters = types.ModuleType("requests.adapters")
        requests_packages = types.ModuleType("requests.packages")
        urllib3_module = types.ModuleType("requests.packages.urllib3")
        urllib3_exceptions = types.ModuleType("requests.packages.urllib3.exceptions")
        urllib3_exceptions.InsecureRequestWarning = type("InsecureRequestWarning", (Warning,), {})
        urllib3_module.exceptions = urllib3_exceptions
        urllib3_module.disable_warnings = lambda *args, **kwargs: None
        requests_packages.urllib3 = urllib3_module

        class DummyRequestsCookieJar(dict):
            def get_dict(self):
                return dict(self)

        class DummyResponse:
            def __init__(self, *args, **kwargs):
                self.__dict__.update(kwargs)

        class DummyHTTPAdapter:
            def __init__(self, *args, **kwargs):
                pass

        class DummySession:
            def mount(self, *args, **kwargs):
                return None

            def request(self, *args, **kwargs):
                return DummyResponse()

        requests_cookies.RequestsCookieJar = DummyRequestsCookieJar
        requests_models.Response = DummyResponse
        requests_adapters.HTTPAdapter = DummyHTTPAdapter
        requests_module.cookies = requests_cookies
        requests_module.models = requests_models
        requests_module.adapters = requests_adapters
        requests_module.packages = requests_packages
        requests_module.utils = types.SimpleNamespace(dict_from_cookiejar=lambda jar: dict(jar))
        requests_module.get = lambda *args, **kwargs: DummyResponse()
        requests_module.post = lambda *args, **kwargs: DummyResponse()
        requests_module.request = lambda *args, **kwargs: DummyResponse()
        requests_module.Session = DummySession

        sys.modules["requests"] = requests_module
        sys.modules["requests.cookies"] = requests_cookies
        sys.modules["requests.models"] = requests_models
        sys.modules["requests.adapters"] = requests_adapters
        sys.modules["requests.packages"] = requests_packages
        sys.modules["requests.packages.urllib3"] = urllib3_module
        sys.modules["requests.packages.urllib3.exceptions"] = urllib3_exceptions

    if "bs4" not in sys.modules:
        bs4_module = types.ModuleType("bs4")

        class DummyUnicodeDammit:
            def __init__(self, html, is_html=False):
                self.unicode_markup = html

        class DummyBeautifulSoup:
            def __init__(self, *args, **kwargs):
                pass

        bs4_module.UnicodeDammit = DummyUnicodeDammit
        bs4_module.BeautifulSoup = DummyBeautifulSoup
        sys.modules["bs4"] = bs4_module

    if "lxml" not in sys.modules:
        lxml_module = types.ModuleType("lxml")
        etree_module = types.ModuleType("lxml.etree")
        etree_module.fromstring = lambda *args, **kwargs: object()
        etree_module.HTMLParser = type("HTMLParser", (), {"__init__": lambda self, *args, **kwargs: None})
        etree_module.XMLParser = type("XMLParser", (), {"__init__": lambda self, *args, **kwargs: None})
        lxml_module.etree = etree_module
        sys.modules["lxml"] = lxml_module
        sys.modules["lxml.etree"] = etree_module

    if "packaging" not in sys.modules:
        packaging_module = types.ModuleType("packaging")
        version_module = types.ModuleType("packaging.version")

        class DummyVersion(str):
            def _tuple(self):
                return tuple(int(part) for part in self.split(".") if part.isdigit())

            def __lt__(self, other):
                return self._tuple() < DummyVersion(other)._tuple()

        version_module.parse = lambda value: DummyVersion(str(value))
        packaging_module.version = version_module
        sys.modules["packaging"] = packaging_module
        sys.modules["packaging.version"] = version_module

    if "parsel" not in sys.modules:
        parsel_module = types.ModuleType("parsel")
        parsel_module.__version__ = "1.8.0"
        parsel_module.Selector = type("Selector", (), {})
        parsel_module.SelectorList = type("SelectorList", (list,), {})

        parsel_selector = types.ModuleType("parsel.selector")
        parsel_selector.create_root_node = lambda *args, **kwargs: None
        parsel_module.selector = parsel_selector

        sys.modules["parsel"] = parsel_module
        sys.modules["parsel.selector"] = parsel_selector


install_test_stubs()

from feapder.core.spiders.file_spider import FileSpider
from feapder.network.request import Request
from feapder.utils.tools import normalize_url


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
        self.operations.append(("execute",))
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
        self.flush_count = 0

    def put_request(self, request):
        self.requests.append(request)

    def put_item(self, item):
        self.items.append(item)

    def get_items_count(self):
        return len(self.items)

    def flush(self):
        self.flush_count += 1


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


class DummyParser:
    def __init__(self, produced, name="test_parser"):
        self._produced = produced
        self.name = name

    def start_requests(self, task):
        return list(self._produced)


class NormalizingSpider(FileSpider):
    def dedup_key(self, request):
        return normalize_url(request.url)


def make_request(url, dedup_key=None):
    """构造伪 Request 对象，避免引入 Request 完整依赖"""
    req = SimpleNamespace(url=url)
    if dedup_key is not None:
        req.dedup_key = dedup_key
    return req


def build_spider(spider_cls=FileSpider, file_dedup=None):
    spider = spider_cls.__new__(spider_cls)
    spider._file_dedup = file_dedup
    spider._redis_key = "unit_test_file_spider"
    spider._save_dir = "/tmp/feapder"
    spider._redisdb = SimpleNamespace(_redis=DummyRedis())
    spider._request_buffer = DummyBuffer()
    spider._item_buffer = DummyBuffer()
    spider._assemble_results = lambda task_id, total: []
    spider._cleanup_task_redis = lambda task_id: None
    spider._dispatch_non_download = lambda produced: None
    spider.on_file_downloaded_calls = []
    spider.on_file_downloaded = (
        lambda request: spider.on_file_downloaded_calls.append((request.url, request.file_path))
    )
    spider.on_task_all_done = lambda task, result, stats: []
    return spider


class TestResolveDedupKey(unittest.TestCase):
    """直接测试 _resolve_dedup_key，不实例化 FileSpider（避免依赖 Redis）"""

    def setUp(self):
        self.spider = FileSpider.__new__(FileSpider)

    def test_default_returns_url(self):
        req = make_request("https://example.com/a.jpg")
        key = self.spider._resolve_dedup_key(req)
        self.assertEqual(key, "https://example.com/a.jpg")
        self.assertEqual(req.dedup_key, "https://example.com/a.jpg")

    def test_explicit_param_highest_priority(self):
        req = make_request("https://example.com/a.jpg?Signature=x", dedup_key="oss://bucket/a.jpg")
        key = self.spider._resolve_dedup_key(req)
        self.assertEqual(key, "oss://bucket/a.jpg")

    def test_hook_used_when_no_explicit_param(self):
        spider = NormalizingSpider.__new__(NormalizingSpider)
        req = make_request("https://oss.example.com/a.jpg?Expires=1&Signature=x")
        key = spider._resolve_dedup_key(req)
        self.assertEqual(key, "https://oss.example.com/a.jpg")
        self.assertEqual(req.dedup_key, "https://oss.example.com/a.jpg")

    def test_explicit_overrides_hook(self):
        class MySpider(FileSpider):
            def dedup_key(self, request):
                return "from-hook"

        spider = MySpider.__new__(MySpider)
        req = make_request("https://example.com/a.jpg", dedup_key="from-explicit")
        key = spider._resolve_dedup_key(req)
        self.assertEqual(key, "from-explicit")

    def test_hook_exception_falls_back_to_url(self):
        class BadSpider(FileSpider):
            def dedup_key(self, request):
                raise RuntimeError("boom")

        spider = BadSpider.__new__(BadSpider)
        req = make_request("https://example.com/a.jpg")
        key = spider._resolve_dedup_key(req)
        self.assertEqual(key, "https://example.com/a.jpg")

    def test_cached_dedup_key_not_recomputed(self):
        """已存在 request.dedup_key 时应直接返回，不再调用钩子"""
        call_count = {"n": 0}

        class CountingSpider(FileSpider):
            def dedup_key(self, request):
                call_count["n"] += 1
                return "from-hook"

        spider = CountingSpider.__new__(CountingSpider)
        req = make_request("https://example.com/a.jpg", dedup_key="precomputed")
        spider._resolve_dedup_key(req)
        spider._resolve_dedup_key(req)
        self.assertEqual(call_count["n"], 0)


class TestFileSpiderDedupFlow(unittest.TestCase):
    def test_dispatch_uses_normalized_key_for_in_task_dedup(self):
        dedup = DummyFileDedup()
        spider = build_spider(NormalizingSpider, dedup)
        task = SimpleNamespace(id=1)
        url1 = (
            "https://bucket.s3.amazonaws.com/a.png"
            "?page=2&biz=1&X-Amz-Date=20260423T000000Z&X-Amz-Signature=first"
        )
        url2 = (
            "https://bucket.s3.amazonaws.com/a.png"
            "?biz=1&X-Amz-Date=20260423T000100Z&page=2&x-amz-signature=second"
        )
        parser = DummyParser(
            [
                spider.download_request(task, url1),
                spider.download_request(task, url2),
            ]
        )

        spider._dispatch_one_task(parser, task)

        self.assertEqual(len(spider._request_buffer.requests), 1)
        self.assertEqual(dedup.get_calls, ["https://bucket.s3.amazonaws.com/a.png?biz=1&page=2"])
        self.assertEqual(
            spider._request_buffer.requests[0].dedup_key,
            "https://bucket.s3.amazonaws.com/a.png?biz=1&page=2",
        )

    def test_dispatch_reuses_cached_result_by_dedup_key(self):
        normalized = "https://bucket.s3.amazonaws.com/a.png?biz=1"
        dedup = DummyFileDedup({normalized: "/cached/a.png"})
        spider = build_spider(NormalizingSpider, dedup)
        task = SimpleNamespace(id=2)
        parser = DummyParser(
            [
                spider.download_request(
                    task,
                    "https://bucket.s3.amazonaws.com/a.png?biz=1&X-Amz-Signature=abc",
                )
            ]
        )

        spider._dispatch_one_task(parser, task)

        self.assertEqual(dedup.get_calls, [normalized])
        self.assertEqual(len(spider._request_buffer.requests), 0)
        self.assertEqual(spider.on_file_downloaded_calls, [(parser._produced[0].url, "/cached/a.png")])

    def test_save_file_writes_back_normalized_dedup_key(self):
        dedup = DummyFileDedup()
        spider = build_spider(FileSpider, dedup)
        spider.process_file = lambda request, response: None
        spider.record_and_check_done = lambda *args, **kwargs: (0, 1, 1, 0, 0, 0)

        request = Request("https://bucket.s3.amazonaws.com/a.png?X-Amz-Signature=abc")
        request.task = SimpleNamespace(id=3)
        request.task_id = 3
        request.index = 0
        request.run_id = "rid"
        request.file_path = "/tmp/a.png"
        request.dedup_key = "https://bucket.s3.amazonaws.com/a.png"

        list(spider.save_file(request, SimpleNamespace()))

        self.assertEqual(
            dedup.set_calls,
            [("https://bucket.s3.amazonaws.com/a.png", "/tmp/a.png")],
        )


if __name__ == "__main__":
    unittest.main()
