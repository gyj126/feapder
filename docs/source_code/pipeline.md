# Pipeline

Pipeline是数据入库时流经的管道，用户可自定义，以便对接其他数据库。

框架已内置mysql、mongo、csv、kafka管道，其他管道作为扩展方式提供，可从[feapder_pipelines](https://github.com/Boris-code/feapder_pipelines)项目中按需安装

项目地址：https://github.com/Boris-code/feapder_pipelines

## 选择内置的pipeline

在配置文件 `setting.py` 中的 `ITEM_PIPELINES` 中启用：

```python
ITEM_PIPELINES = [
    "feapder.pipelines.mysql_pipeline.MysqlPipeline",
    # "feapder.pipelines.mongo_pipeline.MongoPipeline",
    # "feapder.pipelines.csv_pipeline.CsvPipeline",
    # "feapder.pipelines.console_pipeline.ConsolePipeline",
    # "feapder.pipelines.kafka_pipeline.KafkaPipeline",
]
```

然后 爬虫中`yield`的`item`会流经选择的pipeline自动存储

### KafkaPipeline说明

KafkaPipeline基于`confluent-kafka`库，将爬虫数据发送到Kafka topic。需配合`KafkaItem`使用，通过`message`字段设置消息内容，通过`key`字段设置消息key。

**依赖安装：**

```bash
pip install confluent-kafka
```

**配置项（setting.py）：**

```python
# KAFKA
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Kafka broker地址，支持环境变量 KAFKA_BOOTSTRAP_SERVERS
KAFKA_PRODUCER_CONFIG = {}  # confluent-kafka Producer 额外配置，如 {"acks": "all", "retries": 3}
KAFKA_FLUSH_TIMEOUT = 10  # flush超时时间（秒），支持环境变量 KAFKA_FLUSH_TIMEOUT
```

**启用方式：**

```python
ITEM_PIPELINES = [
    "feapder.pipelines.kafka_pipeline.KafkaPipeline",
]
```

#### 创建KafkaItem

使用`feapder create -i`命令创建，无需数据库连接：

```bash
feapder create -i <topic_name>
# 选择 "KafkaItem table_name为topic名称"
```

例如创建一个topic为`spider_data_topic`的KafkaItem：

```bash
feapder create -i spider_data_topic
```

生成的`spider_data_topic_item.py`：

```python
from feapder import KafkaItem


class SpiderDataTopicItem(KafkaItem):
    __table_name__ = "spider_data_topic"

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
```

#### 使用KafkaItem

在爬虫中通过`message`字段设置消息内容，通过`key`字段指定消息key（可选）：

```python
from items.spider_data_topic_item import SpiderDataTopicItem

def parse(self, request, response):
    item = SpiderDataTopicItem()
    item.message = {
        "url": response.url,
        "title": response.xpath("//title/text()").extract_first(),
        "content": response.xpath("//body//text()").extract(),
    }
    item.key = "custom_key"  # 可选，用于Kafka分区路由
    yield item
```

- `message`：消息内容，支持dict（自动JSON序列化）、str、bytes
- `key`：消息key，可选，用于Kafka分区路由
- `__table_name__`：对应Kafka的topic名称

## 自定义pipeline

注：item会被聚合成多条一起流经pipeline，方便批量入库

### 1. 编写pipeline

```python
from feapder.pipelines import BasePipeline
from typing import Dict, List, Tuple


class Pipeline(BasePipeline):
    """
    pipeline 是单线程的，批量保存数据的操作，不建议在这里写网络请求代码，如下载图片等
    """

    def save_items(self, table, items: List[Dict]) -> bool:
        """
        保存数据
        Args:
            table: 表名
            items: 数据，[{},{},...]

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """

        print("自定义pipeline， 保存数据 >>>>", table, items)

        return True

    def update_items(self, table, items: List[Dict], update_keys=Tuple) -> bool:
        """
        更新数据, 与UpdateItem配合使用，若爬虫中没使用UpdateItem，则可不实现此接口
        Args:
            table: 表名
            items: 数据，[{},{},...]
            update_keys: 更新的字段, 如 ("title", "publish_time")

        Returns: 是否更新成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """

        print("自定义pipeline， 更新数据 >>>>", table, items, update_keys)

        return True
```

`Pipeline`需继承`BasePipeline`，类名和存放位置随意，需要实现`save_items`接口。一定要有返回值，返回`False`表示数据没保存成功，会触发重试逻辑

`update_items`接口与`UpdateItem`配合使用，更新数据时使用，若爬虫中没使用UpdateItem，则可不实现此接口

### 2. 编写配置文件

```python
# 数据入库的pipeline，支持多个
ITEM_PIPELINES = [
    "pipeline.Pipeline"
]
``` 

将编写好的pipeline配置进来，值为类的模块路径，需要指定到具体的类名

## 示例

地址：https://github.com/Boris-code/feapder/tree/master/tests/test-pipeline
