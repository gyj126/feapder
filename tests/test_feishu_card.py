"""
飞书卡片消息告警测试
手动填入 webhook_url 后运行，查看各级别卡片样式
"""

from feapder.utils.log import log
from feapder.utils.tools import feishu_warning

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/57f62378-bda9-4f50-9fbb-96832983a936"  # 在此填入飞书机器人 webhook 地址
AT_USER_OPEN_ID = {"open_id": "", "name": "测试用户"}  # 在此填入要@的用户 open_id
AT_USER_USER_ID = {"user_id": "7550124722945261569"}  # 在此填入要@的用户 user_id


def test_error():
    feishu_warning(
        message="《测试爬虫》本批次已超时2小时 批次时间 2026-04-15, 批次进度 8000/10000, 期望时间1天, 任务处理速度约 500条/小时, 预计还需 4小时",
        message_prefix="《测试爬虫》批次超时",
        level="error",
        url=WEBHOOK_URL,
        rate_limit=0,
    )


def test_warning():
    feishu_warning(
        message="《测试爬虫》本批次正在进行, 批次时间 2026-04-15, 批次进度 6000/10000, 该批次可能会超时 1小时30分钟, 请及时处理",
        message_prefix="《测试爬虫》批次可能超时",
        level="warning",
        url=WEBHOOK_URL,
        rate_limit=0,
    )


def test_info():
    feishu_warning(
        message="《测试爬虫》本批次完成 批次时间 2026-04-15 共处理 10000 条任务",
        message_prefix="《测试爬虫》批次完成",
        level="info",
        url=WEBHOOK_URL,
        rate_limit=0,
    )


def test_debug():
    feishu_warning(
        message="《测试爬虫》爬虫开始",
        level="debug",
        url=WEBHOOK_URL,
        rate_limit=0,
    )


def test_at_user_open_id():
    feishu_warning(
        message="《测试爬虫》@指定人语法测试（open_id），请检查是否真的@到人",
        message_prefix="《测试爬虫》@指定人测试-open_id",
        level="warning",
        url=WEBHOOK_URL,
        user=AT_USER_OPEN_ID,
        rate_limit=0,
    )


def test_at_user_user_id():
    feishu_warning(
        message="《测试爬虫》@指定人语法测试（user_id），请检查是否真的@到人",
        message_prefix="《测试爬虫》@指定人测试-user_id",
        level="warning",
        url=WEBHOOK_URL,
        user=AT_USER_USER_ID,
        rate_limit=0,
    )


if __name__ == "__main__":
    if not WEBHOOK_URL:
        log.error("请先在 WEBHOOK_URL 中填入飞书机器人 webhook 地址")
        exit(1)
    if not AT_USER_OPEN_ID.get("open_id") and not AT_USER_USER_ID.get("user_id"):
        log.error("请先在 AT_USER_OPEN_ID 或 AT_USER_USER_ID 中填入可用用户标识")
        exit(1)

    test_error()
    test_warning()
    test_info()
    test_debug()

    send_count = 4
    if AT_USER_OPEN_ID.get("open_id"):
        test_at_user_open_id()
        send_count += 1

    if AT_USER_USER_ID.get("user_id"):
        test_at_user_user_id()
        send_count += 1

    log.info(f"已发送{send_count}条测试消息，请在飞书中查看卡片样式和@指定人效果")
