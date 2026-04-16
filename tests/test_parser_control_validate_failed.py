import feapder.setting as setting
from feapder.core.base_parser import BaseParser
from feapder.core.parser_control import AirSpiderParserControl, ParserControl
from feapder.network.item import Item
from feapder.network.request import Request
from feapder.network.response import Response


class DummyRequestBuffer:
    def __init__(self):
        self.requests = []
        self.failed_requests = []
        self.del_requests = []

    def put_request(self, request):
        self.requests.append(request)

    def put_failed_request(self, request):
        self.failed_requests.append(request)

    def put_del_request(self, request):
        self.del_requests.append(request)


class DummyItemBuffer:
    def __init__(self):
        self.items = []

    def put_item(self, item):
        self.items.append(item)


class DummyMemoryDB:
    def get(self):
        return None


class ValidateFalseParser(BaseParser):
    def __init__(self, failed_results):
        self.failed_results = failed_results
        self.parse_called = False
        self.failed_called = False
        self.error = None

    def validate(self, request, response):
        return False

    def parse(self, request, response):
        self.parse_called = True
        return []

    def failed_request(self, request, response, e):
        self.failed_called = True
        self.error = e
        return self.failed_results


def build_response(url):
    return Response.from_text("<html><title>ok</title></html>", url=url)


def build_request(url, parser_name, response):
    def provide_response(request):
        return request, response

    return Request(url=url, parser_name=parser_name, download_midware=provide_response)


def reset_counter():
    ParserControl._success_task_count = 0
    ParserControl._failed_task_count = 0
    ParserControl._total_task_count = 0
    AirSpiderParserControl._success_task_count = 0
    AirSpiderParserControl._failed_task_count = 0
    AirSpiderParserControl._total_task_count = 0


def prepare_env():
    setting.SPIDER_SLEEP_TIME = 0
    setting.RESPONSE_CACHED_ENABLE = False
    setting.SAVE_FAILED_REQUEST = True
    reset_counter()


def test_spider_validate_false_dispatch_request():
    prepare_env()
    request_buffer = DummyRequestBuffer()
    item_buffer = DummyItemBuffer()
    next_request = Request("https://example.com/next")
    parser = ValidateFalseParser([next_request])
    controller = ParserControl(None, "test:key", request_buffer, item_buffer)
    controller.add_parser(parser)

    request = build_request("https://example.com/a", parser.name, build_response("https://example.com/a"))
    controller.deal_request({"request_obj": request, "request_redis": None})

    assert parser.failed_called is True
    assert parser.parse_called is False
    assert request.is_abandoned is True
    assert "validate返回False" in request.error_msg
    assert request_buffer.failed_requests == [next_request]
    assert request_buffer.requests == []
    assert item_buffer.items == []
    assert ParserControl._failed_task_count == 1


def test_spider_validate_false_dispatch_item_and_callback():
    prepare_env()
    request_buffer = DummyRequestBuffer()
    item_buffer = DummyItemBuffer()
    callback = lambda: None
    parser = ValidateFalseParser([Item(value=1), callback])
    controller = ParserControl(None, "test:key", request_buffer, item_buffer)
    controller.add_parser(parser)

    request = build_request("https://example.com/b", parser.name, build_response("https://example.com/b"))
    controller.deal_request({"request_obj": request, "request_redis": None})

    assert len(item_buffer.items) == 1
    assert isinstance(item_buffer.items[0], Item)
    assert item_buffer.items[0].to_dict["value"] == 1
    assert request_buffer.requests == [callback]
    assert request_buffer.failed_requests == []


def test_spider_validate_false_none_use_default_failed_request():
    prepare_env()
    request_buffer = DummyRequestBuffer()
    item_buffer = DummyItemBuffer()
    parser = ValidateFalseParser(None)
    controller = ParserControl(None, "test:key", request_buffer, item_buffer)
    controller.add_parser(parser)

    request = build_request("https://example.com/c", parser.name, build_response("https://example.com/c"))
    controller.deal_request({"request_obj": request, "request_redis": None})

    assert request_buffer.failed_requests == [request]
    assert request_buffer.requests == []
    assert item_buffer.items == []


def test_air_spider_validate_false_dispatch_request():
    prepare_env()
    request_buffer = DummyRequestBuffer()
    item_buffer = DummyItemBuffer()
    next_request = Request("https://example.com/next-air")
    parser = ValidateFalseParser([next_request])
    controller = AirSpiderParserControl(
        memory_db=DummyMemoryDB(),
        request_buffer=request_buffer,
        item_buffer=item_buffer,
    )
    controller.add_parser(parser)

    request = build_request("https://example.com/d", parser.name, build_response("https://example.com/d"))
    controller.deal_request(request)

    assert request_buffer.requests == [next_request]
    assert next_request.parser_name == parser.name
    assert item_buffer.items == []
    assert AirSpiderParserControl._failed_task_count == 1


def test_air_spider_validate_false_dispatch_item_and_callback():
    prepare_env()
    request_buffer = DummyRequestBuffer()
    item_buffer = DummyItemBuffer()
    callback = lambda: None
    parser = ValidateFalseParser([Item(value=2), callback])
    controller = AirSpiderParserControl(
        memory_db=DummyMemoryDB(),
        request_buffer=request_buffer,
        item_buffer=item_buffer,
    )
    controller.add_parser(parser)

    request = build_request("https://example.com/e", parser.name, build_response("https://example.com/e"))
    controller.deal_request(request)

    assert len(item_buffer.items) == 2
    assert isinstance(item_buffer.items[0], Item)
    assert item_buffer.items[0].to_dict["value"] == 2
    assert callable(item_buffer.items[1])
    assert request_buffer.requests == []


def test_air_spider_validate_false_none_no_extra_dispatch():
    prepare_env()
    request_buffer = DummyRequestBuffer()
    item_buffer = DummyItemBuffer()
    parser = ValidateFalseParser(None)
    controller = AirSpiderParserControl(
        memory_db=DummyMemoryDB(),
        request_buffer=request_buffer,
        item_buffer=item_buffer,
    )
    controller.add_parser(parser)

    request = build_request("https://example.com/f", parser.name, build_response("https://example.com/f"))
    controller.deal_request(request)

    assert request_buffer.requests == []
    assert request_buffer.failed_requests == []
    assert item_buffer.items == []
