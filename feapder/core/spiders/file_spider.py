# -*- coding: utf-8 -*-
"""
Created on 2026/4/7
---------
@summary: 文件下载爬虫
---------
"""

import hashlib
import os
import re
import warnings
from collections import namedtuple
from collections.abc import Iterable
from urllib.parse import urlparse, unquote

from redis.exceptions import NoScriptError

import feapder.setting as setting
import feapder.utils.tools as tools
from feapder.core.spiders.task_spider import TaskSpider
from feapder.dedup.file_dedup import FileDedup, RedisFileDedup, MysqlFileDedup
from feapder.network.item import Item, UpdateItem
from feapder.network.request import Request
from feapder.utils.log import log
from feapder.utils.perfect_dict import PerfectDict

CONSOLE_PIPELINE_PATH = "feapder.pipelines.console_pipeline.ConsolePipeline"


FileTaskStats = namedtuple(
    "FileTaskStats",
    ["success", "fail", "skipped", "dup", "total"],
    defaults=(0, 0, 0, 0, 0),
)
"""文件下载任务统计

字段:
    success: 成功数（含跨任务去重缓存命中）
    fail:    失败数（重试耗尽 + process_file 显式返回 False）
    skipped: 跳过数（无效 URL、file_path 异常等）
    dup:     任务内重复 URL 数（不含首次出现）
    total:   总数

不变式: total == success + fail + skipped + dup

支持元组解包与命名访问:
    success, fail, skipped, dup, total = stats   # 解包
    stats.success, stats.fail, ...               # 命名访问
"""


class FileSpider(TaskSpider):
    """
    文件下载爬虫

    基于 TaskSpider，专用于批量下载文件/图片的场景。
    - 一个任务包含多个待下载文件，由用户在 start_requests 中 yield 多个下载请求
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
        save_dir=None,
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
        @param save_dir: 文件保存根目录；不传时从 setting.FILE_SAVE_DIR 读取（默认 "./downloads"），传入则覆盖配置
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

        self._save_dir = save_dir if save_dir is not None else setting.FILE_SAVE_DIR

        if file_dedup == "redis":
            dedup_table = setting.TAB_FILE_DEDUP.format(redis_key=self._redis_key)
            self._file_dedup = RedisFileDedup(dedup_table, file_dedup_expire)
        elif file_dedup == "mysql":
            if file_dedup_expire is not None:
                log.warning("file_dedup_expire仅在file_dedup='redis'时生效")
            redis_namespace = re.sub(r"[^0-9a-zA-Z_]+", "_", self._redis_key).strip("_")
            dedup_table = f"file_dedup_{redis_namespace}" if redis_namespace else "file_dedup_default"
            self._file_dedup = MysqlFileDedup(table=dedup_table)
        elif isinstance(file_dedup, FileDedup):
            self._file_dedup = file_dedup
        elif file_dedup is not None:
            raise ValueError(
                f"file_dedup参数无效: {file_dedup!r}, "
                f"支持: None, 'redis', 'mysql', 或 FileDedup 实例"
            )
        else:
            self._file_dedup = None

        self._lua_record_and_check_sha = self._redisdb._redis.script_load(
            self._LUA_RECORD_AND_CHECK
        )

    # ===================== 用户需实现/可重写的方法 =====================

    def start_requests(self, task):
        """
        用户必须实现：yield 该任务的所有下载请求

        必须使用 self.download_request(task, url, ...) 构造下载请求，框架据此完成进度追踪。
        允许在同一方法内混合 yield 普通 Item / update_task_batch 等非下载产物。

        约束:
        - 一个任务的全部下载请求必须直接从此方法 yield，不要在中间回调里再产出，
          否则进度统计无法获得 total。

        @param task: PerfectDict - 任务对象，包含 task_keys 指定的字段
        """
        raise NotImplementedError("必须实现 start_requests 方法")

    def download_request(self, task, url, **kwargs):
        """
        构造下载请求的辅助方法。

        @param task: 任务对象（必须传入，框架据此追踪进度）
        @param url: 文件 URL
        @param kwargs: 透传到 Request 的其他参数（headers/method/data/proxies/render/timeout 等）
        @return: Request - 标记为下载请求的 Request 对象

        说明:
        文件保存路径/存储标识统一由 file_path(task, url, index) 决定，
        如需自定义命名规则或上传到云存储，请重写 file_path。
        """
        if "callback" in kwargs and kwargs["callback"] is not self.save_file:
            log.warning("download_request 的 callback 将被强制设为 save_file，用户传入的回调被忽略")
        kwargs["callback"] = self.save_file
        return Request(
            url,
            task=task,
            is_file_download=True,
            **kwargs,
        )

    def file_path(self, request):
        """
        返回文件最终存储位置/标识，用户可重写
        本地场景: 返回本地文件路径
        云存储场景: 返回存储标识/key

        该返回值是文件下载链路上的"权威路径"，会被同步用于：
        - 写入 result 列表（on_task_all_done 收到的 result 元素）
        - 写入 file_dedup 缓存（跨任务去重命中时直接复用）
        - 写回 request.file_path，供 process_file/on_file_downloaded 使用

        @param request: 当前下载请求；可访问 request.task / request.url / request.index /
            request.task_id，以及用户在 download_request 里挂的任何业务字段。
            注意: 该钩子调用时 request.file_path 还不存在（它就是本钩子的返回值）。
        @return: str - 文件路径或存储标识
        """
        url = request.url
        parsed = urlparse(url)
        raw_name = os.path.basename(unquote(parsed.path)) or "unknown"
        _, ext = os.path.splitext(raw_name)
        name_hash = hashlib.md5(raw_name.encode()).hexdigest()
        filename = f"{request.index}_{name_hash}{ext}"
        return os.path.join(self._save_dir, str(request.task.id), filename)

    def process_file(self, request, response):
        """
        将下载内容落地到 request.file_path 指定位置。用户按需重写
        默认实现: 流式保存到本地磁盘
        云存储场景: 重写此方法上传到 OSS/S3 等

        注意:
        - 此方法在下载失败重试时可能被多次调用，实现需保证幂等性
        - 不返回路径，路径以 file_path() 的返回值为准（即 request.file_path）

        @param request: 当前下载请求；可访问 request.url / request.file_path /
            request.task / request.task_id / request.index 等
        @param response: 下载响应
        @return:
            True / None: 处理成功
            False: 显式失败（计入 fail，不再重试）
            抛异常: 触发框架重试
        """
        file_path = request.file_path
        dirname = os.path.dirname(file_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return None

    def validate(self, request, response):
        """文件下载默认校验: 4xx/5xx响应抛异常触发重试，3xx由requests自动跟随。用户可重写"""
        if response and response.status_code >= 400:
            raise Exception(
                f"文件下载HTTP {response.status_code} url={request.url}"
            )

    def on_file_downloaded(self, request):
        """
        单个文件下载成功的回调，用户可重写
        @param request: 当前下载请求；可访问 request.url / request.file_path /
            request.task / request.task_id / request.index 等
        """
        pass

    def on_file_failed(self, request, error):
        """
        单个文件下载失败的回调，用户可重写
        @param request: 当前下载请求
        @param error: 异常对象
        """
        pass

    def on_task_all_done(self, task, result, stats):
        """
        任务所有文件处理完毕的回调
        用户应在此方法中 yield Item 写入结果表、yield self.update_task_batch() 更新任务状态
        @param task: PerfectDict - 任务对象，包含 task_keys 指定的字段
        @param result: List[str|None] - 每个文件的处理结果，
            顺序与 start_requests 中 yield 的下载请求一致。
            成功为 file_path() 的返回值，失败为 None。
            任务内重复URL的结果继承首次出现的结果
        @param stats: FileTaskStats - 任务计数器
            - stats.success: 成功数（含跨任务去重缓存命中）
            - stats.fail:    失败数（重试耗尽 + process_file 显式返回 False）
            - stats.skipped: 跳过数（无效 URL、file_path 异常等）
            - stats.dup:     任务内重复 URL 数（不含首次出现）
            - stats.total:   总数
            - 不变式: total == success + fail + skipped + dup
            - 支持元组解包: success, fail, skipped, dup, total = stats
        """
        pass

    # ===================== 框架内部方法 =====================

    # Lua 脚本: 原子操作 - 轮次校验 + 幂等写入结果 + 递增计数 + 设置TTL + 检查完成
    # KEYS[1]=progress_key  KEYS[2]=result_key
    # ARGV[1]=field("success"/"fail")  ARGV[2]=file_index  ARGV[3]=result_value  ARGV[4]=run_id
    # 返回值: {status, total, success, fail, skipped, dup}
    #   status: -1=key不存在或run_id不匹配(过期回调), 0=未完成, 1=首次完成
    _LUA_RECORD_AND_CHECK = """
if redis.call('exists', KEYS[1]) == 0 then
    return {-1, 0, 0, 0, 0, 0}
end
if redis.call('hget', KEYS[1], 'run_id') ~= ARGV[4] then
    return {-1, 0, 0, 0, 0, 0}
end
local is_new = redis.call('hsetnx', KEYS[2], ARGV[2], ARGV[3])
if is_new == 1 then
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
end
redis.call('expire', KEYS[2], 86400)
redis.call('expire', KEYS[1], 86400)
local total = tonumber(redis.call('hget', KEYS[1], 'total')) or 0
local success = tonumber(redis.call('hget', KEYS[1], 'success')) or 0
local fail = tonumber(redis.call('hget', KEYS[1], 'fail')) or 0
local skipped = tonumber(redis.call('hget', KEYS[1], 'skipped')) or 0
local dup = tonumber(redis.call('hget', KEYS[1], 'dup')) or 0
if success + fail + skipped + dup >= total and total > 0 then
    local done = redis.call('hsetnx', KEYS[1], 'done', 1)
    if done == 1 then
        return {1, total, success, fail, skipped, dup}
    end
end
return {0, total, success, fail, skipped, dup}
"""

    def record_and_check_done(self, progress_key, result_key, field, file_index, result_value, run_id):
        """原子操作: 轮次校验 + 幂等写入结果 + 递增计数 + 检查完成
        run_id 不匹配时视为过期回调直接丢弃，防止跨轮次数据污染。
        同一 file_index 仅首次写入时递增计数器。
        @return: (status, total, success, fail, skipped, dup)
            status: -1=key不存在或run_id不匹配(过期回调), 0=未完成, 1=首次完成
        """
        try:
            result = self._redisdb._redis.evalsha(
                self._lua_record_and_check_sha, 2,
                progress_key, result_key, field, file_index, result_value, run_id,
            )
        except NoScriptError:
            self._lua_record_and_check_sha = self._redisdb._redis.script_load(
                self._LUA_RECORD_AND_CHECK
            )
            result = self._redisdb._redis.evalsha(
                self._lua_record_and_check_sha, 2,
                progress_key, result_key, field, file_index, result_value, run_id,
            )
        return result[0], result[1], result[2], result[3], result[4], result[5]

    def distribute_task(self, tasks):
        """
        重写父类分发逻辑：
        - 调用用户 start_requests 拿到全部产出
        - 识别下载请求（is_file_download=True），补齐 task_id/file_index/file_path/run_id
        - 任务内 URL 去重 + 跨任务 file_dedup 缓存命中处理
        - 写入 Redis 进度状态后再下发请求
        - 非下载产出（Item/callable/普通 Request）按父类规则原样转交
        """
        for task in tasks:
            if self._is_more_parsers:
                parser = self._match_parser(task)
                if parser is None:
                    continue
                task = self._wrap_task(task)
                self._dispatch_one_task(parser, task)
            else:
                task = self._wrap_task(task)
                for parser in self._parsers:
                    self._dispatch_one_task(parser, task)

        self._request_buffer.flush()
        self._item_buffer.flush()

    def _wrap_task(self, task):
        """将 tuple/dict 任务统一包装为 PerfectDict"""
        if isinstance(task, dict):
            return PerfectDict(_dict=task)
        return PerfectDict(
            _dict=dict(zip(self._task_keys, task)),
            _values=list(task),
        )

    def _match_parser(self, task):
        """多模板模式下根据 task 中的 parser_name 匹配对应的 parser"""
        for parser in self._parsers:
            if parser.name in task:
                return parser
        return None

    def _dispatch_one_task(self, parser, task):
        """处理单个任务：物化 start_requests 产出 → 富化下载请求 → 写 Redis → 下发"""
        try:
            produced = parser.start_requests(task)
        except Exception as e:
            log.error(f"任务{task.id} start_requests调用异常 error={e}")
            return

        if produced and not isinstance(produced, Iterable):
            raise Exception(f"{parser.name}.start_requests 返回值必须可迭代")

        download_requests = []
        non_download_items = []
        for produced_item in produced or []:
            if isinstance(produced_item, Request) and getattr(produced_item, "is_file_download", False):
                download_requests.append(produced_item)
            else:
                non_download_items.append(produced_item)

        if not download_requests:
            log.warning(f"任务{task.id} start_requests未产出任何下载请求")
            self._fire_empty_task_done(parser, task, non_download_items)
            return

        task_id = task.id
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        dup_key = setting.TAB_FILE_DUP.format(
            redis_key=self._redis_key, task_id=task_id
        )

        run_id = os.urandom(8).hex()
        total = len(download_requests)
        cached_count = 0
        skipped_count = 0
        dup_count = 0
        result_mapping = {}
        dup_to_source = {}
        seen_urls = {}
        pending_requests = []

        for index, request in enumerate(download_requests):
            url = request.url
            if not url or not isinstance(url, str) or not url.strip():
                result_mapping[str(index)] = ""
                skipped_count += 1
                log.warning(f"任务{task_id} 跳过无效URL index={index}")
                continue

            url = url.strip()
            request.url = url

            if url in seen_urls:
                dup_to_source[index] = seen_urls[url]
                dup_count += 1
                log.debug(f"任务{task_id} URL任务内去重 index={index} -> {seen_urls[url]}")
                continue
            seen_urls[url] = index

            # 提前注入文件维度上下文，供 file_path 钩子及缓存命中回调使用
            request.task = task
            request.task_id = task_id
            request.index = index
            request.run_id = run_id

            if self._file_dedup:
                try:
                    cached_result = self._file_dedup.get(url)
                except Exception as e:
                    log.error(f"任务{task_id} 去重缓存查询异常 url={url} error={e}")
                    cached_result = None
                if cached_result is not None:
                    result_mapping[str(index)] = cached_result
                    cached_count += 1
                    request.file_path = cached_result
                    log.debug(f"任务{task_id} 文件去重命中 url={url}")
                    try:
                        self.on_file_downloaded(request)
                    except Exception as e:
                        log.error(f"任务{task_id} on_file_downloaded回调异常 url={url} error={e}")
                    continue

            try:
                request.file_path = self.file_path(request)
            except Exception as e:
                result_mapping[str(index)] = ""
                skipped_count += 1
                log.error(f"任务{task_id} file_path异常 url={url} error={e}")
                continue

            request.callback = self.save_file
            request.parser_name = request.parser_name or parser.name
            pending_requests.append(request)

        # 清理旧 key 并通过 pipeline 原子写入初始状态
        pipe = self._redisdb._redis.pipeline()
        pipe.delete(progress_key)
        pipe.delete(result_key)
        pipe.delete(dup_key)
        progress_fields = {
            "total": total, "success": cached_count,
            "fail": 0, "skipped": skipped_count, "dup": dup_count,
            "run_id": run_id,
        }
        for field, value in progress_fields.items():
            pipe.hset(progress_key, field, value)
        pipe.expire(progress_key, 86400)
        if result_mapping:
            for field, value in result_mapping.items():
                pipe.hset(result_key, field, value)
        pipe.expire(result_key, 86400)
        if dup_to_source:
            for dup_idx, src_idx in dup_to_source.items():
                pipe.hset(dup_key, str(dup_idx), str(src_idx))
            pipe.expire(dup_key, 86400)
        pipe.execute()

        if dup_count > 0:
            log.info(f"任务{task_id} 任务内URL去重{dup_count}个")
        if cached_count > 0:
            log.info(f"任务{task_id} 去重缓存命中{cached_count}/{total}个文件")

        # 先派发用户在 start_requests 中产出的非下载项（如 Item / update_task_batch / lambda）
        for non_download in non_download_items:
            self._dispatch_non_download(non_download)

        # 全部命中缓存/跳过/去重，直接触发 on_task_all_done
        if cached_count + skipped_count + dup_count >= total:
            try:
                result = self._assemble_results(task_id, total)
                stats = FileTaskStats(
                    success=cached_count, fail=0,
                    skipped=skipped_count, dup=dup_count, total=total,
                )
                done_iter = self.on_task_all_done(task, result, stats)
                for done_item in done_iter or []:
                    self._dispatch_non_download(done_item)
            except Exception as e:
                log.error(f"任务{task_id} on_task_all_done异常 error={e}")
                log.warning(f"任务{task_id} 状态未更新, 请检查on_task_all_done实现")
            finally:
                self._cleanup_task_redis(task_id)
            return

        for request in pending_requests:
            self._request_buffer.put_request(request)

    def _fire_empty_task_done(self, parser, task, non_download_items):
        """start_requests 未产出任何下载请求时，仍尝试触发用户的收尾逻辑"""
        for non_download in non_download_items:
            self._dispatch_non_download(non_download)

        try:
            stats = FileTaskStats()
            done_iter = self.on_task_all_done(task, [], stats)
            for done_item in done_iter or []:
                self._dispatch_non_download(done_item)
        except Exception as e:
            log.error(f"任务{task.id} on_task_all_done异常 error={e}")
            log.warning(f"任务{task.id} 状态未更新, 请检查on_task_all_done实现")

    def _dispatch_non_download(self, produced):
        """将非下载产出按其类型推入对应的 buffer，规则与父类 distribute_task 保持一致"""
        if isinstance(produced, Request):
            self._request_buffer.put_request(produced)
        elif isinstance(produced, Item):
            self._item_buffer.put_item(produced)
            if self._item_buffer.get_items_count() >= setting.ITEM_MAX_CACHED_COUNT:
                self._item_buffer.flush()
        elif callable(produced):
            self._item_buffer.put_item(produced)
            if self._item_buffer.get_items_count() >= setting.ITEM_MAX_CACHED_COUNT:
                self._item_buffer.flush()
        else:
            raise TypeError(
                f"start_requests yield result type error, expect Request、Item、callback func, got: {type(produced)}"
            )

    def save_file(self, request, response):
        """
        框架内部回调，处理文件保存和进度追踪。用户不应重写此方法。

        process_file 返回值语义：
        - True / None: 成功，写入 result_key（值为 file_path）和 file_dedup 缓存
        - False: 显式失败，计入 fail，调 on_file_failed，不再重试
        - 抛异常: 触发框架重试
        """
        task_id = request.task_id
        file_index = request.index
        url = request.url
        file_path = request.file_path
        run_id = getattr(request, "run_id", "")

        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )

        try:
            ok = self.process_file(request, response)
        except Exception as e:
            log.error(f"任务{task_id} process_file异常 url={url} error={e}")
            raise

        if ok is False:
            status, total, success, fail, skipped, dup = self.record_and_check_done(
                progress_key, result_key, "fail", str(file_index), "", run_id,
            )
            if status == -1:
                log.debug(f"任务{task_id} 过期回调已丢弃 url={url}")
                return

            log.error(f"任务{task_id} 文件处理显式失败 [{success + fail + skipped + dup}/{total}] url={url}")

            try:
                self.on_file_failed(request, Exception("process_file返回False"))
            except Exception as e_cb:
                log.error(f"任务{task_id} on_file_failed回调异常 url={url} error={e_cb}")

            if status == 1:
                task = request.task
                try:
                    result = self._assemble_results(task_id, total)
                    stats = FileTaskStats(success=success, fail=fail, skipped=skipped, dup=dup, total=total)
                    for item in self.on_task_all_done(task, result, stats) or []:
                        yield item
                except Exception as e:
                    log.error(f"任务{task_id} on_task_all_done异常 error={e}")
                    log.warning(f"任务{task_id} 状态未更新, 请检查on_task_all_done实现")
                finally:
                    yield lambda: self._cleanup_task_redis(task_id)
            return

        status, total, success, fail, skipped, dup = self.record_and_check_done(
            progress_key, result_key, "success", str(file_index), file_path, run_id,
        )

        if status == -1:
            log.debug(f"任务{task_id} 过期回调已丢弃 url={url}")
            return

        # 仅在 run_id 校验通过、结果被正式接受时写入跨任务去重缓存，
        # 避免过期回调（旧轮次请求晚到）污染 dedup
        if self._file_dedup:
            try:
                self._file_dedup.set(url, file_path)
            except Exception as e:
                log.error(f"任务{task_id} 去重缓存写入异常 url={url} error={e}")

        log.info(f"任务{task_id} 文件下载成功 [{success + fail + skipped + dup}/{total}] url={url}")

        try:
            self.on_file_downloaded(request)
        except Exception as e:
            log.error(f"任务{task_id} on_file_downloaded回调异常 url={url} error={e}")

        if status == 1:
            task = request.task
            try:
                result = self._assemble_results(task_id, total)
                stats = FileTaskStats(success=success, fail=fail, skipped=skipped, dup=dup, total=total)
                for item in self.on_task_all_done(task, result, stats) or []:
                    yield item
            except Exception as e:
                log.error(f"任务{task_id} on_task_all_done异常 error={e}")
                log.warning(f"任务{task_id} 状态未更新, 请检查on_task_all_done实现")
            finally:
                yield lambda: self._cleanup_task_redis(task_id)

    def failed_request(self, request, response, e):
        """
        文件下载失败（重试耗尽）的处理。
        """
        task_id = getattr(request, "task_id", None)
        file_index = getattr(request, "index", None)

        if task_id is None or file_index is None:
            yield request
            return

        run_id = getattr(request, "run_id", "")

        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        status, total, success, fail, skipped, dup = self.record_and_check_done(
            progress_key, result_key, "fail", str(file_index), "", run_id,
        )

        if status == -1:
            log.debug(f"任务{task_id} 过期回调已丢弃 url={request.url}")
            return

        log.error(f"任务{task_id} 文件下载失败 [{success + fail + skipped + dup}/{total}] url={request.url}")

        try:
            self.on_file_failed(request, e)
        except Exception as e_cb:
            log.error(f"任务{task_id} on_file_failed回调异常 url={request.url} error={e_cb}")

        if status == 1:
            task = request.task
            try:
                result = self._assemble_results(task_id, total)
                stats = FileTaskStats(success=success, fail=fail, skipped=skipped, dup=dup, total=total)
                for item in self.on_task_all_done(task, result, stats) or []:
                    yield item
            except Exception as e_done:
                log.error(f"任务{task_id} on_task_all_done异常 error={e_done}")
                log.warning(f"任务{task_id} 状态未更新, 请检查on_task_all_done实现")
            finally:
                yield lambda: self._cleanup_task_redis(task_id)

        yield request

    def _assemble_results(self, task_id, total):
        """
        从 Redis 中拉取文件处理结果和任务内重复映射，
        按 0~total-1 顺序组装为有序列表，重复索引继承首次出现的结果。
        使用 hscan_iter 分批读取，避免超大任务时 hgetall 的内存峰值。
        """
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        all_data = {}
        for k, v in self._redisdb._redis.hscan_iter(result_key, count=1000):
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            all_data[key] = val
        result = [all_data.get(str(i)) or None for i in range(total)]

        dup_key = setting.TAB_FILE_DUP.format(
            redis_key=self._redis_key, task_id=task_id
        )
        for dup_idx_raw, src_idx_raw in self._redisdb._redis.hscan_iter(dup_key, count=1000):
            dup_idx = int(dup_idx_raw.decode() if isinstance(dup_idx_raw, bytes) else dup_idx_raw)
            src_idx = int(src_idx_raw.decode() if isinstance(src_idx_raw, bytes) else src_idx_raw)
            result[dup_idx] = result[src_idx]

        return result

    def _cleanup_task_redis(self, task_id):
        """清理任务相关的 Redis 进度、结果和重复映射 key"""
        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        dup_key = setting.TAB_FILE_DUP.format(
            redis_key=self._redis_key, task_id=task_id
        )
        self._redisdb.clear(progress_key)
        self._redisdb.clear(result_key)
        self._redisdb.clear(dup_key)

    def close(self):
        """释放文件去重缓存资源"""
        if self._file_dedup:
            try:
                self._file_dedup.close()
            except Exception as e:
                log.error(f"文件去重缓存关闭异常 error={e}")

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
