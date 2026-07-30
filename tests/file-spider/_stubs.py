# -*- coding: utf-8 -*-
"""
FileSpider 单元测试的第三方依赖桩

独立运行单个测试文件时，redis / pymysql / requests 等依赖可能不可用或不该真正连接，
在导入 feapder 之前用轻量桩顶替。若真实依赖已在 sys.modules 中（例如同批次的其他
测试已导入 feapder），则一律不替换，让真实库参与，避免桩与生产行为发散。
"""

import sys
import types


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

    if "influxdb_client" not in sys.modules:
        influxdb_client_module = types.ModuleType("influxdb_client")
        influxdb_client_module.InfluxDBClient = type("InfluxDBClient", (), {})
        influxdb_client_module.BucketRetentionRules = type(
            "BucketRetentionRules", (), {}
        )
        sys.modules["influxdb_client"] = influxdb_client_module

        client_pkg = types.ModuleType("influxdb_client.client")
        sys.modules["influxdb_client.client"] = client_pkg
        domain_pkg = types.ModuleType("influxdb_client.domain")
        sys.modules["influxdb_client.domain"] = domain_pkg

        write_api_module = types.ModuleType("influxdb_client.client.write_api")
        write_api_module.SYNCHRONOUS = object()
        sys.modules["influxdb_client.client.write_api"] = write_api_module

        write_precision_module = types.ModuleType(
            "influxdb_client.domain.write_precision"
        )
        write_precision_module.WritePrecision = type("WritePrecision", (), {"NS": "ns"})
        sys.modules["influxdb_client.domain.write_precision"] = write_precision_module

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

            def close(self):
                return None

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
