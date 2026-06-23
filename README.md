# feapder 二次开发版差异说明

本项目基于开源项目 [Boris-code/feapder](https://github.com/Boris-code/feapder) 二次开发，保留原框架的 AirSpider、Spider、TaskSpider、BatchSpider、任务队列、断点续爬、Pipeline、去重、浏览器渲染等核心能力。

当前根目录 README 不再沿用原作者的项目介绍、赞助、社群与推广内容，仅记录本仓库相对原版的功能差异、兼容变化和使用入口。

## 与原版的主要差异

### 1. 新增 FileSpider 文件下载爬虫

新增 `feapder.FileSpider`，面向批量文件/图片下载场景，继承 TaskSpider 的任务调度与分布式能力。

核心能力：

- 一个任务可包含多个待下载文件。
- 使用 `download_request(task, url, **kwargs)` 构造文件下载请求。
- 自动追踪单任务内文件处理进度。
- 下载结果按 `start_requests` 中的 yield 顺序返回。
- 支持本地保存，也可通过重写 `process_file` 上传到 OSS/S3/COS 等对象存储。
- 支持任务内 URL 去重，以及 Redis/MySQL/自定义跨任务文件去重。
- 任务完成后通过 `on_task_all_done(task, result, stats)` 统一处理结果和更新任务状态。

新增公开对象：

```python
from feapder import FileSpider, FileTaskStats
```

文档入口：

- [docs/usage/FileSpider.md](docs/usage/FileSpider.md)

### 2. 新增 FileParser

新增 `FileParser`，用于文件下载类 TaskParser 场景，提供与 FileSpider 一致的基础钩子：

- `start_requests(task)`
- `download_request(task, url, **kwargs)`
- `file_path(request)`
- `process_file(request, response)`
- `on_file_downloaded(request)`
- `on_file_failed(request, error)`
- `on_task_all_done(task, result, stats)`

公开入口：

```python
from feapder import FileParser
```

### 3. 新增文件去重缓存

新增 `feapder.dedup.file_dedup`：

- `FileDedup`：文件去重接口。
- `RedisFileDedup`：基于 Redis Hash 的文件结果缓存。
- `MysqlFileDedup`：基于 MySQL 的持久化文件结果缓存，首次使用自动建表。

文件去重保存的是 `dedup_key -> result_url/file_path` 映射，不只是存在性判断，因此跨任务命中后可直接复用已下载结果。

同时新增 `feapder.utils.tools.normalize_url`，用于剥离 OSS/S3/COS 等签名 URL 中容易变化的 query 参数，提升文件去重命中率。

### 4. 新增 Kafka Pipeline

新增 Kafka 数据导出能力：

- `feapder.pipelines.kafka_pipeline.KafkaPipeline`
- `feapder.network.item.KafkaItem`
- `feapder create -i` 支持创建 KafkaItem 模板。

`KafkaItem` 使用 `table_name` 表示 Kafka topic，通过 `message` 设置消息内容，通过 `key` 设置消息 key。

示例：

```python
import feapder


class DemoKafkaItem(feapder.KafkaItem):
    __table_name__ = "demo_topic"


item = DemoKafkaItem()
item.message = {"id": 1, "name": "demo"}
item.key = "1"
```

配置项：

```python
ITEM_PIPELINES = [
    "feapder.pipelines.kafka_pipeline.KafkaPipeline",
]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_PRODUCER_CONFIG = {}
KAFKA_FLUSH_TIMEOUT = 10
```

> 使用 Kafka Pipeline 需要额外安装 `confluent-kafka`。

### 5. 监控体系重构为 InfluxDB + Grafana 看板

本仓库新增 Grafana 看板与监控说明：

- [monitoring/grafana/README.md](monitoring/grafana/README.md)
- [monitoring/grafana/dashboards/feapder-crawler.json](monitoring/grafana/dashboards/feapder-crawler.json)

监控相关变化：

- 默认 measurement 改为 `crawler`。
- 所有爬虫通过 tag 区分 `project`、`spider`、`hostname`、`pid`。
- 新增运行状态打点：心跳、进程内存。
- 新增 HTTP 状态码分布与响应耗时打点。
- 支持 Grafana 中按项目、爬虫、主机、时间筛选。
- 基于 InfluxDB 2.x（org/bucket/token），Grafana 看板使用 Flux 查询。

主要配置：

```python
PROJECT_NAME = "your_project"
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "crawler"
INFLUXDB_MEASUREMENT = "crawler"
METRICS_RUNTIME_INTERVAL = 10
```

### 6. 告警逻辑调整

告警相关逻辑做了以下调整：

- `WARNING_LEVEL` 改为阈值语义，支持 `DEBUG / INFO / WARNING / ERROR`。
- 失败请求数报警文案从“失败任务数”调整为“失败请求数”。
- 新增请求成功率报警：
  - `WARNING_SUCCESS_RATE`
  - `WARNING_SUCCESS_RATE_MIN_COUNT`
- 导出失败报警由 Scheduler 统一发送，避免 ItemBuffer 内重复报警。
- 导出失败次数只在失败次数增长时再次报警，降低刷屏风险。
- 飞书告警改为交互式卡片，支持按告警等级展示不同颜色。
- `validate` 返回 `False` 时会进入 `failed_request` 回调，并计入失败统计。

### 7. ParserControl 请求处理增强

请求生命周期有以下变化：

- 成功、失败、总数统计增加线程锁保护。
- 在 `validate` 前记录 HTTP 状态码和响应耗时，避免校验失败导致监控数据缺失。
- `validate` 返回 `False` 时设置 `request.is_abandoned = True`，调用 `failed_request`。
- 请求处理结束后主动关闭 response，减少 `stream=True` 下连接池占用。
- AirSpider 的失败回调分发逻辑单独整理，支持返回 `Request`、`Item` 或 callback。

### 8. 命令行模板扩展

`feapder create` 模板增加：

- `FileSpider` 爬虫模板。
- `KafkaItem` Item 模板。

新增模板文件：

- `feapder/templates/file_spider_template.tmpl`
- `feapder/templates/kafka_item_template.tmpl`

### 9. 依赖与兼容性调整

依赖变化：

- `redis` 版本上限从 `<4.0.0` 放宽为 `<6.0.0`。
- 新增 `psutil>=5.9.0`，用于运行状态/内存监控。
- `all` 扩展依赖中移除 `redis-py-cluster`。

环境要求仍保持：

- Python 3.6.0+
- Linux / Windows / macOS

### 10. 文档与平台相关内容调整

本仓库删除了原版中的 `docs/feapder_platform` 平台文档，并从导航中移除对应入口。

根 README 也移除了原版中的以下内容：

- 赞助商展示。
- 微信赞赏。
- 学习交流群。
- 原作者工具推荐和推广信息。

如需完整原版介绍、历史版本说明或作者社群信息，请查看上游仓库。

## 快速安装

本仓库仍可按原 feapder 方式安装使用。

精简版：

```shell
pip install feapder
```

浏览器渲染版：

```shell
pip install "feapder[render]"
```

完整版：

```shell
pip install "feapder[all]"
```

本仓库新增 Kafka Pipeline，如需使用 Kafka 导出，请额外安装：

```shell
pip install confluent-kafka
```

## FileSpider 最小示例

```python
import json
import feapder


class ImageFileSpider(feapder.FileSpider):
    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    def on_task_all_done(self, task, result, stats):
        state = 1 if stats.fail == 0 else -1
        yield self.update_task_batch(task.id, state=state)


if __name__ == "__main__":
    ImageFileSpider(
        redis_key="image_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
        file_dedup="redis",
    ).start()
```

## 主要新增文件

- `feapder/core/spiders/file_spider.py`
- `feapder/dedup/file_dedup.py`
- `feapder/pipelines/kafka_pipeline.py`
- `feapder/commands/create/create_kafka_item.py`
- `feapder/templates/file_spider_template.tmpl`
- `feapder/templates/kafka_item_template.tmpl`
- `docs/usage/FileSpider.md`
- `monitoring/grafana/README.md`
- `monitoring/grafana/dashboards/feapder-crawler.json`

## 测试覆盖补充

本仓库新增了以下方向的测试：

- FileSpider 本地文件下载、OSS 场景、去重键、URL 归一化。
- 监控指标与运行状态打点。
- 飞书卡片告警。
- 导出失败告警。
- `validate=False` 失败回调。
- Scheduler 告警逻辑。

相关测试文件位于：

- `tests/file-spider/`
- `tests/test_monitoring_metrics.py`
- `tests/test_feishu_card.py`
- `tests/test_export_failed_warning.py`
- `tests/test_parser_control_validate_failed.py`
- `tests/test_scheduler_warning.py`

## 上游声明

本项目是基于原 feapder 的二次开发版本。原项目版权、许可证与基础能力归属于上游项目及其贡献者。本仓库 README 仅描述当前分叉版本相对原版的差异，避免用户误以为本项目仍是原作者原始版本。
