# -*- coding: utf-8 -*-
"""
Created on 2018-07-29 22:48:30
---------
@summary: 导出数据
---------
@author: Boris
@email:  boris_liu@foxmail.com
"""
from typing import Dict, List, Tuple

import feapder.utils.tools as tools
from feapder.db.mysqldb import MysqlDB
from feapder.pipelines import BasePipeline
from feapder.utils.log import log


class MysqlPipeline(BasePipeline):
    def __init__(self):
        self._to_db = None

    @property
    def to_db(self):
        if not self._to_db:
            self._to_db = MysqlDB()

        return self._to_db

    def save_items(self, table, items: List[Dict]) -> bool:
        """
        保存数据
        Args:
            table: 表名
            items: 数据，[{},{},...]

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """

        sql, datas = tools.make_batch_sql(table, items)
        add_count = self.to_db.add_batch(sql, datas)
        datas_size = len(datas)
        if add_count:
            log.info(
                "共导出 %s 条数据 到 %s, 重复 %s 条" % (datas_size, table, datas_size - add_count)
            )
        else:
            log.debug("没有插入数据，可能全部重复")

        return add_count != None

    def update_items(self, table, items: List[Dict], update_keys=Tuple) -> bool:
        """
        更新数据
        Args:
            table: 表名
            items: 数据，[{},{},...]
            update_keys: 更新的字段, 如 ("title", "publish_time")

        Returns: 是否更新成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """

        sql, datas = tools.make_batch_sql(
            table, items, update_columns=update_keys or list(items[0].keys())
        )
        affected = self.to_db.add_batch(sql, datas)
        if affected is not None:
            batch_size = len(datas)
            msg = f"共导出 {batch_size} 条数据到 {table}，MySQL affected_rows={affected}"
            log.info(msg)

        return affected is not None
