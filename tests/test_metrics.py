import asyncio

from feapder.utils import metrics

metrics.init(
    url="http://localhost:8086",
    token="my-token",
    org="feapder",
    bucket="feapder",
    spider="test_metrics",
    debug=True,
)


async def test_download_async():
    for _ in range(100):
        await metrics.aemit_download(
            parser="demo", status="download_total", count=100
        )
        for _ in range(100):
            await metrics.aemit_download(
                parser="demo", status="download_success", count=1
            )


def test_download():
    for _ in range(100):
        metrics.emit_download(parser="demo", status="download_total", count=100)
        for _ in range(100):
            metrics.emit_download(parser="demo", status="download_success", count=1)


def test_item():
    for _ in range(10):
        metrics.emit_item(table="test_table", count=100)
        for field in ("title", "url", "content"):
            metrics.emit_item(table="test_table", field=field, count=80)


def test_proxy():
    metrics.emit_proxy(event="pull", count=10)
    metrics.emit_proxy(event="use", count=100)
    metrics.emit_proxy(event="invalid", count=2)


def test_user():
    metrics.emit_user(user_id="user_1", status="success")
    metrics.emit_user(user_id="user_1", status="overdue")


def test_custom_counter():
    metrics.emit_custom("api_call", count=10, tags={"endpoint": "/login"})


def test_custom_value():
    metrics.emit_custom("queue_size", value=128)


def test_custom_duration():
    metrics.emit_custom("render_time", duration=1.23)


if __name__ == "__main__":
    asyncio.run(test_download_async())
    test_download()
    test_item()
    test_proxy()
    test_user()
    test_custom_counter()
    test_custom_value()
    test_custom_duration()
    metrics.close()
