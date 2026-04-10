# -*- coding: utf-8 -*-
"""
Created on 2026-04-10
---------
@summary: Kafka 数据导出Pipeline
---------
"""
import json
from typing import Dict, List, Tuple

import feapder.setting as setting
from feapder.pipelines import BasePipeline
from feapder.utils.log import log


class KafkaPipeline(BasePipeline):
    """
    基于 confluent-kafka 的数据导出Pipeline

    将爬虫数据发送到 Kafka topic，table 参数映射为 topic 名称。
    """

    def __init__(self):
        self._producer = None
        self._failed_count = 0

    @property
    def producer(self):
        if not self._producer:
            from confluent_kafka import Producer

            config = {
                "bootstrap.servers": setting.KAFKA_BOOTSTRAP_SERVERS,
                **setting.KAFKA_PRODUCER_CONFIG,
            }
            self._producer = Producer(config)

        return self._producer

    def delivery_callback(self, err, msg):
        if err is not None:
            self._failed_count += 1
            log.error(f"Kafka消息投递失败, topic: {msg.topic()}, error: {err}")

    def save_items(self, table, items: List[Dict]) -> bool:
        """
        保存数据到Kafka

        Args:
            table: topic名称
            items: KafkaItem序列化后的数据，[{"message": ..., "key": ...}, ...]

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库
        """
        if not items:
            return True

        self._failed_count = 0

        try:
            for item in items:
                message = item["message"]
                key = item.get("key")

                if isinstance(message, bytes):
                    value = message
                elif isinstance(message, str):
                    value = message.encode("utf-8")
                else:
                    value = json.dumps(message, ensure_ascii=False).encode("utf-8")

                key_bytes = key.encode("utf-8") if isinstance(key, str) else key

                self.producer.produce(topic=table, value=value, key=key_bytes, callback=self.delivery_callback)
                self.producer.poll(0)

            remain_count = self.producer.flush(timeout=setting.KAFKA_FLUSH_TIMEOUT)

            datas_size = len(items)
            failed_count = self._failed_count + remain_count

            if failed_count == 0:
                log.info(f"共导出 {datas_size} 条数据 到 Kafka topic: {table}")
            else:
                success_count = max(datas_size - failed_count, 0)
                log.error(
                    f"导出 {datas_size} 条数据到 Kafka topic: {table}, 成功 {success_count} 条, 失败 {failed_count} 条(回调失败 {self._failed_count} 条, flush后剩余 {remain_count} 条未投递)"
                )

            return failed_count == 0

        except Exception as e:
            log.exception(e)
            return False

    def update_items(self, table, items: List[Dict], update_keys=Tuple) -> bool:
        """
        更新数据

        Kafka无更新语义，复用save_items逻辑，将数据重新发送到对应topic。

        Args:
            table: topic名称
            items: 数据，[{},{},...]
            update_keys: 更新的字段（Kafka场景下未使用）

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库
        """
        return self.save_items(table, items)

    def close(self):
        if self._producer:
            self._producer.flush(timeout=setting.KAFKA_FLUSH_TIMEOUT)
            self._producer = None
            log.info("Kafka Producer已关闭")
