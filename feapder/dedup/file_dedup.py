# -*- coding: utf-8 -*-
"""
文件去重缓存

与现有 Dedup（布隆过滤器等，只判断存在性）不同，
FileDedup 存储 URL -> result_url 的完整映射，用于直接复用下载结果。
"""

from feapder.db.mysqldb import MysqlDB
from feapder.db.redisdb import RedisDB
from feapder.utils.log import log


class FileDedup:
    """文件去重缓存接口

    用于存储和检索文件下载结果的缓存。
    子类需实现 get / set 方法。
    """

    def get(self, url):
        """获取 URL 对应的缓存结果

        Args:
            url: 文件原始 URL

        Returns:
            str or None: 缓存的文件存储位置，无缓存返回 None
        """
        return None

    def set(self, url, result_url):
        """缓存 URL 的处理结果

        Args:
            url: 文件原始 URL
            result_url: 文件最终存储位置（本地路径或云存储 URL）
        """
        pass

    def close(self):
        pass


class RedisFileDedup(FileDedup):
    """基于 Redis Hash 的文件去重

    适合分布式场景，多进程共享。
    """

    def __init__(self, table, expire_time=None):
        """
        Args:
            table: Redis Hash 的 key
            expire_time: 过期时间（秒），None 表示不过期
        """
        self._redisdb = RedisDB()
        self._table = table
        self._expire_time = expire_time

    def get(self, url):
        result = self._redisdb.hget(self._table, url)
        if result is None:
            return None
        if isinstance(result, bytes):
            result = result.decode()
        return result or None

    def set(self, url, result_url):
        self._redisdb.hset(self._table, url, result_url)
        if self._expire_time:
            self._redisdb._redis.expire(self._table, self._expire_time)


class MysqlFileDedup(FileDedup):
    """基于 MySQL 表的文件去重

    持久化可靠，适合长期缓存。
    首次使用时会自动建表。
    """

    _table_ensured = set()

    def __init__(self, table="file_dedup", mysqldb=None):
        """
        Args:
            table: MySQL 表名
            mysqldb: MysqlDB 实例，默认使用全局配置
        """
        self._mysqldb = mysqldb or MysqlDB()
        self._table = table
        self._ensure_table()

    def _ensure_table(self):
        if self._table in self.__class__._table_ensured:
            return
        sql = (
            f"CREATE TABLE IF NOT EXISTS `{self._table}` ("
            f"  `id` int(11) NOT NULL AUTO_INCREMENT,"
            f"  `url` varchar(2048) NOT NULL COMMENT '文件原始URL',"
            f"  `result_url` text COMMENT '文件存储位置',"
            f"  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            f"  PRIMARY KEY (`id`),"
            f"  UNIQUE KEY `uk_url` (`url`) USING BTREE"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )
        self._mysqldb.execute(sql)
        self.__class__._table_ensured.add(self._table)

    def get(self, url):
        sql = f"SELECT result_url FROM `{self._table}` WHERE `url` = %s LIMIT 1"
        result = self._mysqldb.find(sql, (url,))
        return result[0][0] if result else None

    def set(self, url, result_url):
        sql = (
            f"INSERT INTO `{self._table}` (`url`, `result_url`) VALUES (%s, %s) "
            f"ON DUPLICATE KEY UPDATE `result_url` = VALUES(`result_url`)"
        )
        self._mysqldb.execute(sql, (url, result_url))
