# -*- coding: utf-8 -*-
"""
Created on 2026/4/7
---------
@summary: 文件下载爬虫
---------
"""

import os
import warnings
from urllib.parse import urlparse, unquote

import feapder.setting as setting
import feapder.utils.tools as tools
from feapder.core.spiders.task_spider import TaskSpider
from feapder.dedup.file_dedup import FileDedup, RedisFileDedup, MysqlFileDedup
from feapder.network.item import UpdateItem
from feapder.network.request import Request
from feapder.utils.log import log

CONSOLE_PIPELINE_PATH = "feapder.pipelines.console_pipeline.ConsolePipeline"


class FileSpider(TaskSpider):
    """
    文件下载爬虫

    基于 TaskSpider，专用于批量下载文件/图片的场景。
    - 一个任务包含多个待下载文件的 URL 列表（一对多）
    - 框架自动追踪每个任务的下载进度
    - 支持保存到本地磁盘或上传云存储
    - 任务成功/失败由用户在 on_task_all_done 中显式决定
    - 可选文件去重，同一 URL 不重复下载
    """

    def __init__(
        self,
        redis_key,
        task_table,
        task_keys,
        save_dir="./downloads",
        file_dedup=None,
        file_dedup_expire=None,
        task_table_type="mysql",
        task_state="state",
        min_task_count=10000,
        check_task_interval=5,
        task_limit=10000,
        related_redis_key=None,
        related_batch_record=None,
        task_condition="",
        task_order_by="",
        thread_count=None,
        begin_callback=None,
        end_callback=None,
        delete_keys=(),
        keep_alive=None,
        batch_interval=0,
        use_mysql=True,
        **kwargs,
    ):
        """
        @summary: 文件下载爬虫
        ---------
        @param redis_key: 任务等数据存放在 redis 中的 key 前缀
        @param task_table: mysql 中的任务表
        @param task_keys: 需要获取的任务字段 列表
        @param save_dir: 文件保存根目录，默认 ./downloads
        @param file_dedup: 文件去重策略。
            None: 不去重（默认）
            "redis": 使用 Redis Hash 去重
            "mysql": 使用 MySQL 表去重
            FileDedup 实例: 自定义去重实现
        @param file_dedup_expire: Redis 去重缓存过期时间（秒），仅 file_dedup="redis" 时生效
        @param task_table_type: 任务表类型 支持 redis、mysql
        @param task_state: mysql 中任务表的任务状态字段
        @param min_task_count: redis 中最少任务数，少于这个数量会从种子表中取任务
        @param check_task_interval: 检查是否还有任务的时间间隔
        @param task_limit: 每次从数据库中取任务的数量
        @param related_redis_key: 有关联的其他爬虫任务表（redis）
        @param related_batch_record: 有关联的其他爬虫批次表（mysql）
        @param task_condition: 任务条件，用于筛选任务
        @param task_order_by: 取任务时的排序条件
        @param thread_count: 线程数
        @param begin_callback: 爬虫开始回调函数
        @param end_callback: 爬虫结束回调函数
        @param delete_keys: 爬虫启动时删除的 key
        @param keep_alive: 爬虫是否常驻
        @param batch_interval: 抓取时间间隔（天）
        @param use_mysql: 是否使用 mysql 数据库
        ---------
        """

        super(FileSpider, self).__init__(
            redis_key=redis_key,
            task_table=task_table,
            task_table_type=task_table_type,
            task_keys=task_keys,
            task_state=task_state,
            min_task_count=min_task_count,
            check_task_interval=check_task_interval,
            task_limit=task_limit,
            related_redis_key=related_redis_key,
            related_batch_record=related_batch_record,
            task_condition=task_condition,
            task_order_by=task_order_by,
            thread_count=thread_count,
            begin_callback=begin_callback,
            end_callback=end_callback,
            delete_keys=delete_keys,
            keep_alive=keep_alive,
            batch_interval=batch_interval,
            use_mysql=use_mysql,
            **kwargs,
        )

        self._save_dir = save_dir

        if file_dedup == "redis":
            dedup_table = setting.TAB_FILE_DEDUP.format(redis_key=self._redis_key)
            self._file_dedup = RedisFileDedup(dedup_table, file_dedup_expire)
        elif file_dedup == "mysql":
            self._file_dedup = MysqlFileDedup()
        elif isinstance(file_dedup, FileDedup):
            self._file_dedup = file_dedup
        else:
            self._file_dedup = None

    # ===================== 用户需实现/可重写的方法 =====================

    def get_download_urls(self, task):
        """
        从 task 中获取需要下载的文件 URL 列表，用户必须实现
        @param task: 任务信息
        @return: List[str] - URL 列表
        """
        raise NotImplementedError("必须实现 get_download_urls 方法")

    def get_file_path(self, task, url, index):
        """
        返回文件保存路径/标识，用户可重写
        本地场景: 返回本地文件路径
        云存储场景: 返回存储标识/key
        @param task: 任务信息
        @param url: 文件 URL
        @param index: 文件在 URL 列表中的索引，默认实现用于避免同名文件覆盖
        @return: str
        """
        parsed = urlparse(url)
        filename = os.path.basename(unquote(parsed.path)) or "unknown"
        filename = f"{index}_{filename}"
        return os.path.join(self._save_dir, str(task.id), filename)

    def process_file(self, task_id, url, file_path, response):
        """
        处理下载的文件内容，返回文件最终存储位置。用户按需重写
        默认实现: 流式保存到本地磁盘，返回本地路径
        云存储场景: 重写此方法上传到 OSS/S3 等，返回云存储 URL
        @param task_id: 任务 ID
        @param url: 文件原始 URL
        @param file_path: get_file_path 返回的路径/标识
        @param response: 下载响应
        @return: str - 文件最终存储位置
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return file_path

    def on_file_downloaded(self, task_id, url, file_path):
        """
        单个文件下载成功的回调，用户可重写
        @param task_id: 任务 ID
        @param url: 文件原始 URL
        @param file_path: 文件存储位置
        """
        pass

    def on_file_failed(self, task_id, url, error):
        """
        单个文件下载失败的回调，用户可重写
        @param task_id: 任务 ID
        @param url: 文件原始 URL
        @param error: 异常信息
        """
        pass

    def on_task_all_done(self, task_id, success_count, fail_count, total_count, results):
        """
        任务所有文件处理完毕的回调
        用户应在此方法中 yield Item 写入结果表、yield self.update_task_batch() 更新任务状态
        @param task_id: 任务 ID
        @param success_count: 成功数
        @param fail_count: 失败数
        @param total_count: 总数
        @param results: List[str|None] - 每个文件的处理结果，
            顺序与 get_download_urls 返回的列表一致。
            成功为文件存储位置，失败为 None
        """
        pass

    # ===================== 框架内部方法 =====================

    # Lua 脚本: 原子递增计数并判断是否首次达到完成条件
    # 返回值: 0=未完成或已触发过, 1=首次达到完成条件
    _LUA_INCR_AND_CHECK = """
local key = KEYS[1]
local field = ARGV[1]
redis.call('hincrby', key, field, 1)
local total = tonumber(redis.call('hget', key, 'total') or 0)
local success = tonumber(redis.call('hget', key, 'success') or 0)
local fail = tonumber(redis.call('hget', key, 'fail') or 0)
if success + fail >= total and total > 0 then
    local done = redis.call('hsetnx', key, 'done', 1)
    if done == 1 then
        return 1
    end
end
return 0
"""
    _lua_incr_and_check_sha = None

    def _incr_and_check_done(self, progress_key, field):
        """原子递增计数并检查是否首次达到完成条件"""
        redis_client = self._redisdb._redis
        if self.__class__._lua_incr_and_check_sha is None:
            self.__class__._lua_incr_and_check_sha = redis_client.script_load(
                self._LUA_INCR_AND_CHECK
            )
        return redis_client.evalsha(
            self.__class__._lua_incr_and_check_sha, 1, progress_key, field
        )

    def start_requests(self, task):
        """
        遍历 URL 列表生成下载请求。
        去重缓存命中的 URL 直接复用结果，不生成 Request。
        """
        urls = self.get_download_urls(task)
        if not urls:
            log.warning(f"任务{task.id}无下载URL")
            for result in self.on_task_all_done(task.id, 0, 0, 0, []) or []:
                yield result
            return

        total = len(urls)
        task_id = task.id
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )

        self._redisdb.hset(progress_key, "total", total)
        self._redisdb.hset(progress_key, "success", 0)
        self._redisdb.hset(progress_key, "fail", 0)

        cached_count = 0
        skipped_count = 0
        for index, url in enumerate(urls):
            if not url or not isinstance(url, str) or not url.strip():
                self._redisdb.hset(result_key, str(index), "")
                self._redisdb.hincrby(progress_key, "fail", 1)
                skipped_count += 1
                log.warning(f"任务{task_id} 跳过无效URL index={index}")
                continue

            # 去重缓存检查
            if self._file_dedup:
                cached_result = self._file_dedup.get(url)
                if cached_result is not None:
                    self._redisdb.hset(result_key, str(index), cached_result)
                    self._redisdb.hincrby(progress_key, "success", 1)
                    cached_count += 1
                    log.debug(f"任务{task_id} 文件去重命中 url={url}")
                    try:
                        self.on_file_downloaded(task_id, url, cached_result)
                    except Exception as e:
                        log.error(f"任务{task_id} on_file_downloaded回调异常 url={url} error={e}")
                    continue

            try:
                file_path = self.get_file_path(task, url, index)
            except Exception as e:
                self._redisdb.hset(result_key, str(index), "")
                self._redisdb.hincrby(progress_key, "fail", 1)
                skipped_count += 1
                log.error(f"任务{task_id} get_file_path异常 url={url} error={e}")
                continue

            yield Request(
                url,
                task_id=task_id,
                file_index=index,
                file_path=file_path,
                callback=self.save_file,
            )

        if cached_count > 0:
            log.info(f"任务{task_id} 去重命中{cached_count}/{total}个文件")

        # 全部命中缓存或跳过，直接触发 on_task_all_done
        if cached_count + skipped_count >= total:
            results = self._assemble_results(task_id, total)
            for result in self.on_task_all_done(
                task_id, cached_count, skipped_count, total, results
            ) or []:
                yield result
            self._cleanup_task_redis(task_id)

    def save_file(self, request, response):
        """
        框架内部回调，处理文件保存和进度追踪。用户不应重写此方法。
        """
        task_id = request.task_id
        file_index = request.file_index
        url = request.url
        file_path = request.file_path

        result_url = self.process_file(task_id, url, file_path, response)

        # 写入去重缓存
        if self._file_dedup and result_url:
            self._file_dedup.set(url, result_url)

        # 记录结果
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        self._redisdb.hset(result_key, str(file_index), result_url or "")

        # 原子递增成功计数并检查是否首次完成
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        is_first_done = self._incr_and_check_done(progress_key, "success")

        total = int(self._redisdb.hget(progress_key, "total") or 0)
        success = int(self._redisdb.hget(progress_key, "success") or 0)
        fail = int(self._redisdb.hget(progress_key, "fail") or 0)

        log.info(f"任务{task_id} 文件下载成功 [{success + fail}/{total}] url={url}")

        try:
            self.on_file_downloaded(task_id, url, result_url)
        except Exception as e:
            log.error(f"任务{task_id} on_file_downloaded回调异常 url={url} error={e}")

        if is_first_done:
            results = self._assemble_results(task_id, total)
            for result in self.on_task_all_done(
                task_id, success, fail, total, results
            ) or []:
                yield result
            self._cleanup_task_redis(task_id)

    def failed_request(self, request, response, e):
        """
        文件下载失败（重试耗尽）的处理。
        """
        task_id = getattr(request, "task_id", None)
        file_index = getattr(request, "file_index", None)

        if task_id is None or file_index is None:
            yield request
            return

        # 记录失败结果
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        self._redisdb.hset(result_key, str(file_index), "")

        # 原子递增失败计数并检查是否首次完成
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        is_first_done = self._incr_and_check_done(progress_key, "fail")

        total = int(self._redisdb.hget(progress_key, "total") or 0)
        success = int(self._redisdb.hget(progress_key, "success") or 0)
        fail = int(self._redisdb.hget(progress_key, "fail") or 0)

        log.error(f"任务{task_id} 文件下载失败 [{success + fail}/{total}] url={request.url}")

        try:
            self.on_file_failed(task_id, request.url, e)
        except Exception as e_cb:
            log.error(f"任务{task_id} on_file_failed回调异常 url={request.url} error={e_cb}")

        if is_first_done:
            results = self._assemble_results(task_id, total)
            for result in self.on_task_all_done(
                task_id, success, fail, total, results
            ) or []:
                yield result
            self._cleanup_task_redis(task_id)

        yield request

    def _assemble_results(self, task_id, total):
        """
        从 Redis 结果 Hash 中一次性拉取所有文件处理结果，
        按 0~total-1 顺序组装为有序列表返回。
        """
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        all_data = self._redisdb.hgetall(result_key)
        results = []
        for i in range(total):
            value = all_data.get(str(i)) or all_data.get(str(i).encode())
            if value is None or value == b"" or value == "":
                results.append(None)
            else:
                results.append(value.decode() if isinstance(value, bytes) else value)
        return results

    def _cleanup_task_redis(self, task_id):
        """清理任务相关的 Redis 进度和结果 key"""
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        self._redisdb.clear(progress_key)
        self._redisdb.clear(result_key)

    @classmethod
    def to_DebugFileSpider(cls, *args, **kwargs):
        DebugFileSpider.__bases__ = (cls,)
        DebugFileSpider.__name__ = cls.__name__
        return DebugFileSpider(*args, **kwargs)


class DebugFileSpider(FileSpider):
    """
    Debug 文件下载爬虫
    """

    __debug_custom_setting__ = dict(
        COLLECTOR_TASK_COUNT=1,
        SPIDER_THREAD_COUNT=1,
        SPIDER_SLEEP_TIME=0,
        SPIDER_MAX_RETRY_TIMES=10,
        REQUEST_LOST_TIMEOUT=600,
        PROXY_ENABLE=False,
        RETRY_FAILED_REQUESTS=False,
        SAVE_FAILED_REQUEST=False,
        ITEM_FILTER_ENABLE=False,
        REQUEST_FILTER_ENABLE=False,
        OSS_UPLOAD_TABLES=(),
        DELETE_KEYS=True,
    )

    def __init__(
        self,
        task_id=None,
        task=None,
        save_to_db=False,
        update_task=False,
        *args,
        **kwargs,
    ):
        """
        @param task_id: 任务 id
        @param task: 任务，task 与 task_id 二者选一即可。如 task = {"url":""}
        @param save_to_db: 数据是否入库，默认否
        @param update_task: 是否更新任务，默认否
        """
        warnings.warn(
            "您正处于debug模式下，该模式下不会更新任务状态及数据入库，仅用于调试。"
            "正式发布前请更改为正常模式",
            category=Warning,
        )

        if not task and not task_id:
            raise Exception("task_id 与 task 不能同时为空")

        kwargs["redis_key"] = kwargs["redis_key"] + "_debug"
        if not save_to_db:
            self.__class__.__debug_custom_setting__["ITEM_PIPELINES"] = [
                CONSOLE_PIPELINE_PATH
            ]
        self.__class__.__custom_setting__.update(
            self.__class__.__debug_custom_setting__
        )

        super(DebugFileSpider, self).__init__(*args, **kwargs)

        self._task_id = task_id
        self._task = task
        self._update_task = update_task

    def start_monitor_task(self):
        if not self._parsers:
            self._is_more_parsers = False
            self._parsers.append(self)
        elif len(self._parsers) <= 1:
            self._is_more_parsers = False

        if self._task:
            self.distribute_task([self._task])
        else:
            tasks = self.get_todo_task_from_mysql()
            if not tasks:
                raise Exception(
                    f"未获取到任务 请检查 task_id: {self._task_id} 是否存在"
                )
            self.distribute_task(tasks)

        log.debug("下发任务完毕")

    def get_todo_task_from_mysql(self):
        task_keys = ", ".join([f"`{key}`" for key in self._task_keys])
        sql = "select %s from %s where id=%s" % (
            task_keys,
            self._task_table,
            self._task_id,
        )
        tasks = self._mysqldb.find(sql)
        return tasks

    def save_cached(self, request, response, table):
        pass

    def update_task_state(self, task_id, state=1, *args, **kwargs):
        if self._update_task:
            kwargs["id"] = task_id
            kwargs[self._task_state] = state

            sql = tools.make_update_sql(
                self._task_table,
                kwargs,
                condition=f"id = {task_id}",
            )

            if self._mysqldb.update(sql):
                log.debug(f"置任务{task_id}状态成功")
            else:
                log.error(f"置任务{task_id}状态失败 sql={sql}")

    def update_task_batch(self, task_id, state=1, *args, **kwargs):
        if self._update_task:
            kwargs["id"] = task_id
            kwargs[self._task_state] = state

            update_item = UpdateItem(**kwargs)
            update_item.table_name = self._task_table
            update_item.name_underline = self._task_table + "_item"

            return update_item

    def run(self):
        self.start_monitor_task()

        if not self._parsers:
            self._parsers.append(self)

        self._start()

        while True:
            try:
                if self.all_thread_is_done():
                    self._stop_all_thread()
                    break
            except Exception as e:
                log.exception(e)

            tools.delay_time(1)

        self.delete_tables([self._redis_key + "*"])
