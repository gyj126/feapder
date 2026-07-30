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
from feapder.core.parser_control import fetch, validate_response
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
    - 一个任务包含多个待下载文件，由用户在 start_requests 中 yield 多个请求，
      每个请求占一个"文件槽位"，按 yield 顺序分配 index
    - 槽位支持两种模式：
      直链模式 - yield self.download_request(task, url)，拿到 url 直接下载
      接口模式 - yield feapder.Request(接口地址, callback=自己的方法)，
                 在回调中 yield 一个 download_request 完成下载。
                 适用于直链需先请求下载接口签发、且有效期只有几分钟的场景，
                 框架在同一线程内紧邻执行下载，任何失败都回退到重新调接口
    - 框架自动追踪每个任务的下载进度
    - 支持保存到本地磁盘或流式上传云存储
    - 任务成功/失败由用户在 on_task_all_done 中显式决定
    - 可选文件去重，同一去重键不重复下载
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
        min_task_count=500,
        check_task_interval=5,
        task_limit=500,
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

        # 接口模式槽位的 url 是接口地址，用它做去重键会误命中，
        # 据此判断用户是否已提供了可用的去重键来源
        self._dedup_key_overridden = type(self).dedup_key is not FileSpider.dedup_key

        self._lua_record_and_check_sha = self._redisdb._redis.script_load(
            self._LUA_RECORD_AND_CHECK
        )

    # ===================== 用户需实现/可重写的方法 =====================

    def start_requests(self, task):
        """
        用户必须实现：yield 该任务的所有文件请求

        此方法 yield 的每个 Request 都占一个"文件槽位"，按 yield 顺序分配 index，
        槽位总数即 stats.total，result 列表与之一一对应。两种槽位形态:

        直链模式 - yield self.download_request(task, url, ...)
            url 就是文件直链，框架直接下载。

        接口模式 - yield feapder.Request(接口地址, callback=self.你的方法, ...)
            用于直链需先调接口签发、且有效期很短的场景。框架下载接口响应后交给
            你的 callback，你在其中 yield 恰好一个 self.download_request(...) 即可，
            框架会在同一线程内紧邻执行下载。

        允许在同一方法内混合 yield Item / update_task_batch 等非请求产物，
        它们不占槽位。

        约束:
        - 一个任务的全部槽位必须直接从此方法 yield，进度统计需在派发前获得 total
        - 此方法中不能 yield 与文件无关的普通 Request，它会被当成一个文件槽位

        @param task: PerfectDict - 任务对象，包含 task_keys 指定的字段
        """
        raise NotImplementedError("必须实现 start_requests 方法")

    def download_request(self, task, url, dedup_key=None, **kwargs):
        """
        构造下载请求的辅助方法。

        直链模式下从 start_requests 直接 yield；接口模式下在槽位回调中 yield，
        且必须恰好 yield 一个。

        @param task: 任务对象（必须传入，框架据此追踪进度）
        @param url: 文件直链
        @param dedup_key: 显式去重键，优先级最高。
            URL 带时效签名（OSS/S3/COS 等）时，传入稳定标识可避免去重失效。
            不传则走 dedup_key(request) 钩子，最后 fallback 到 request.url。
            接口模式下去重键须声明在槽位请求上，此处不可再传。
        @param kwargs: 透传到 Request 的其他参数
            （headers/method/data/proxies/render/timeout/validate/download_midware 等）
        @return: Request - 标记为下载请求的 Request 对象

        说明:
        文件保存路径/存储标识统一由 file_path(request) 决定，
        如需自定义命名规则或上传到云存储，请重写 file_path。
        """
        if "callback" in kwargs and kwargs["callback"] is not self.save_file:
            log.warning("download_request 的 callback 将被强制设为 save_file，用户传入的回调被忽略")
        kwargs["callback"] = self.save_file
        kwargs["stream"] = True
        request = Request(
            url,
            task=task,
            is_file_download=True,
            **kwargs,
        )
        if dedup_key is not None:
            request.dedup_key = dedup_key
        return request

    def dedup_key(self, request):
        """
        返回用于跨任务/任务内去重的稳定键，用户可重写。
        默认返回 request.url。

        典型场景：URL 带时效签名（如阿里云 OSS、AWS S3、腾讯云 COS）时，
        签名/时间戳每次都不同，会导致去重失效与缓存膨胀。重写本方法剥离签名相关
        query 参数即可恢复去重命中率，可配合 feapder.utils.tools.normalize_url 使用。

        优先级（由 _resolve_dedup_key 决定）：
            request.dedup_key（显式参数） > self.dedup_key(request)（本钩子） > request.url

        注意默认值只对直链模式有意义。接口模式下 request.url 是接口地址、各文件往往完全
        相同，框架不会用它兜底：未提供去重键时该槽位不参与任何去重，启用 file_dedup
        时则直接抛异常。

        @param request: 当前槽位请求；可访问 request.url / request.task / request.index 等
        @return: str - 去重键
        """
        return request.url

    def file_path(self, request):
        """
        返回文件最终存储位置/标识，用户可重写
        本地场景: 返回本地文件路径
        云存储场景: 返回存储标识/key

        该返回值是文件下载链路上的"权威路径"，会被同步用于：
        - 写入 result 列表（on_task_all_done 收到的 result 元素）
        - 写入 file_dedup 缓存（跨任务去重命中时直接复用）
        - 写回 request.file_path，供 process_file/on_file_downloaded 使用

        该钩子在派发期调用，此时接口模式还没有请求下载接口，因此路径只能由任务表字段
        和 start_requests 中挂在请求上的业务字段推导，拿不到接口响应里的文件名。
        接口模式下 request.url 是接口地址，默认实现解析出的文件名没有意义，需重写本钩子。

        @param request: 当前槽位请求；可访问 request.task / request.url / request.index /
            request.task_id，以及用户在请求上挂的任何业务字段。
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

    def file_chunks(self, response, chunk_size=1024 * 1024):
        """
        返回文件内容的字节流迭代器，用户可重写以插入自定义校验

        整个流读完仍为零字节时抛异常触发重试：文件下载场景下的空响应几乎总是异常
        （鉴权失败返回空体、直链已过期、上游截断），静默落地空文件会污染存储。
        需要接受零字节文件时重写本方法去掉该校验。

        分块大小不做配置项，需要调整时重写本方法即可。

        @param response: 下载响应
        @param chunk_size: 分块大小，默认 1MB
        @return: Iterator[bytes]
        """
        has_content = False
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                has_content = True
                yield chunk
        if not has_content:
            raise Exception(f"文件内容为空 url={response.url}")

    def process_file(self, request, response):
        """
        将下载内容落地到 request.file_path 指定位置。用户按需重写
        默认实现: 流式保存到本地磁盘
        云存储场景: 重写此方法流式上传到 COS/OSS/S3 等

        消费 self.file_chunks(response) 即可获得全程流式的字节流，
        既不落临时盘也不把整个文件读入内存；响应由框架负责关闭，无需自己 finally。

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
            for chunk in self.file_chunks(response):
                f.write(chunk)
        return None

    def validate(self, request, response):
        """
        默认校验: 404 直接判失败不重试，其余 4xx/5xx 仍抛异常触发重试。用户可重写

        本钩子是所有请求的兜底校验。接口模式下接口响应与文件响应性质不同（JSON 业务
        错误码 vs 二进制流），不必在此做分支判断，在各自的请求上传 validate=你的方法
        即可让两套校验逻辑分离。

        注意判空必须用 is not None：requests.Response.__bool__ 返回的是 self.ok，
        4xx/5xx 响应本身为假值，写成 if response 会让整段校验失效。
        """
        if response is None:
            return

        if response.status_code == 404:
            log.warning(f"资源404，丢弃当前请求 url={request.url}")
            return False

        if response.status_code >= 400:
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
        单个文件失败的回调，用户可重写

        @param request: 失败的请求。接口模式下若失败发生在接口阶段（重试耗尽或
            validate 返回 False），这里是槽位请求，其 url 为接口地址；
            失败发生在下载阶段则是下载请求，url 为直链
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

    def _resolve_dedup_key(self, request):
        """
        解析下载请求的去重键并回写到 request.dedup_key，避免重复计算。
        优先级：request.dedup_key（显式参数） > self.dedup_key(request)（钩子） > request.url。

        @return: str - 去重键
        """
        existing = getattr(request, "dedup_key", None)
        if existing:
            return existing
        try:
            key = self.dedup_key(request)
        except Exception as e:
            log.error(f"dedup_key钩子异常 url={request.url} error={e}")
            key = None
        if not key:
            key = request.url
        request.dedup_key = key
        return key

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
        - 每个 Request 视为一个文件槽位，按序补齐 task_id/index/file_path/run_id
        - 任务内去重键去重 + 跨任务 file_dedup 缓存命中处理
        - 写入 Redis 进度状态后再下发请求
        - 非请求产出（Item/callable）按父类规则原样转交
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
        """处理单个任务：物化 start_requests 产出 → 富化槽位请求 → 写 Redis → 下发"""
        try:
            produced = parser.start_requests(task)
        except Exception as e:
            log.error(f"任务{task.id} start_requests调用异常 error={e}")
            return

        if produced and not isinstance(produced, Iterable):
            raise Exception(f"{parser.name}.start_requests 返回值必须可迭代")

        slot_requests = []
        non_slot_items = []
        for produced_item in produced or []:
            if isinstance(produced_item, Request):
                slot_requests.append(produced_item)
            else:
                non_slot_items.append(produced_item)

        if not slot_requests:
            log.warning(f"任务{task.id} start_requests未产出任何文件请求")
            self._fire_empty_task_done(task, non_slot_items)
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
        total = len(slot_requests)
        cached_count = 0
        skipped_count = 0
        dup_count = 0
        result_mapping = {}
        dup_to_source = {}
        seen_keys = {}
        pending_requests = []

        for index, request in enumerate(slot_requests):
            url = request.url
            if not url or not isinstance(url, str) or not url.strip():
                result_mapping[str(index)] = ""
                skipped_count += 1
                log.warning(f"任务{task_id} 跳过无效URL index={index}")
                continue

            url = url.strip()
            request.url = url

            # 提前注入文件维度上下文，供 file_path / dedup_key 钩子及缓存命中回调使用
            request.task = task
            request.task_id = task_id
            request.index = index
            request.run_id = run_id

            # 直链模式下 url 就是文件身份，可作为默认去重键；接口模式下 url 是接口地址，
            # 各文件往往完全相同，必须由用户提供去重键，否则该槽位不参与任何去重
            is_download = getattr(request, "is_file_download", False)
            if is_download or getattr(request, "dedup_key", None) or self._dedup_key_overridden:
                dedup_key = self._resolve_dedup_key(request)
            elif self._file_dedup:
                raise ValueError(
                    f"任务{task_id} 接口模式槽位未提供去重键 index={index} url={url}。"
                    f"接口地址在各文件间往往相同，用它作去重键会造成大面积误命中，"
                    f"请在 Request 中传 dedup_key 或重写 dedup_key 钩子"
                )
            else:
                # 显式置空，保证派发出去的槽位请求上 dedup_key 属性一定存在
                request.dedup_key = dedup_key = None

            if dedup_key is not None:
                if dedup_key in seen_keys:
                    dup_to_source[index] = seen_keys[dedup_key]
                    dup_count += 1
                    log.debug(f"任务{task_id} 任务内去重 index={index} -> {seen_keys[dedup_key]} key={dedup_key}")
                    continue
                seen_keys[dedup_key] = index

            if self._file_dedup:
                try:
                    cached_result = self._file_dedup.get(dedup_key)
                except Exception as e:
                    log.error(f"任务{task_id} 去重缓存查询异常 key={dedup_key} error={e}")
                    cached_result = None
                if cached_result is not None:
                    result_mapping[str(index)] = cached_result
                    cached_count += 1
                    request.file_path = cached_result
                    log.debug(f"任务{task_id} 文件去重命中 key={dedup_key}")
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

            if is_download:
                request.callback = self.save_file
            else:
                # 接口模式：把用户回调收进 slot_callback，由框架分派器接管，
                # 以便在回调产出下载请求后立刻在同一线程完成下载
                request.slot_callback = request.callback_name or "parse"
                request.callback = self._on_slot_response
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
            log.info(f"任务{task_id} 任务内去重{dup_count}个")
        if cached_count > 0:
            log.info(f"任务{task_id} 去重缓存命中{cached_count}/{total}个文件")

        # 先派发用户在 start_requests 中产出的非请求项（如 Item / update_task_batch / lambda）
        for non_slot in non_slot_items:
            self._dispatch_non_download(non_slot)

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

    def _fire_empty_task_done(self, task, non_slot_items):
        """start_requests 未产出任何文件请求时，仍尝试触发用户的收尾逻辑"""
        for non_slot in non_slot_items:
            self._dispatch_non_download(non_slot)

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

    def _on_slot_response(self, request, response):
        """
        框架内部回调，接口模式槽位的响应分派器。用户不应重写此方法。

        调用用户在槽位请求上声明的 callback，取出其唯一的下载请求，在当前线程内
        紧邻执行下载，使加签直链从签发到使用的间隔最小。下载链路上的任何异常都
        向上抛给 ParserControl，由槽位请求整体重试，即回到"重新调下载接口"的起点，
        已过期的旧直链不会被重复使用。
        """
        task_id = request.task_id
        slot_callback = tools.get_method(self, request.slot_callback)
        produced = slot_callback(request, response)

        if produced and not isinstance(produced, Iterable):
            raise Exception(f"{request.slot_callback} 返回值必须可迭代")

        download_requests = []
        non_slot_items = []
        for produced_item in produced or []:
            if isinstance(produced_item, Request):
                if not getattr(produced_item, "is_file_download", False):
                    raise Exception(
                        f"任务{task_id} {request.slot_callback} 不支持 yield 普通 Request，"
                        f"下载请求需用 self.download_request(task, url) 构造 url={produced_item.url}"
                    )
                download_requests.append(produced_item)
            else:
                non_slot_items.append(produced_item)

        if len(download_requests) != 1:
            raise Exception(
                f"任务{task_id} {request.slot_callback} 需 yield 恰好1个 download_request，"
                f"实际{len(download_requests)}个 index={request.index} url={request.url}"
            )

        download_request = download_requests[0]
        if self._file_dedup and getattr(download_request, "dedup_key", None):
            raise ValueError(
                f"任务{task_id} 接口模式下 download_request 不可再传 dedup_key index={request.index}。"
                f"去重键在派发期就要用于查缓存，须声明在 start_requests 的槽位请求上，"
                f"否则缓存的写入键与查询键不一致，永远无法命中"
            )

        # 槽位与下载请求共用同一份文件维度上下文，进度计数才能落到同一个 index 上
        for attr in ("task", "task_id", "index", "run_id", "file_path", "dedup_key"):
            setattr(download_request, attr, getattr(request, attr))

        yield from non_slot_items
        yield from self._download_sync(download_request)

    def _download_sync(self, request):
        """
        在当前线程执行下载请求并交给 save_file 处理

        复用 ParserControl 的 fetch / validate_response，使 download_midware 与
        per-request validate 在接口模式下与直链模式行为一致。
        """
        request_temp, response = fetch(request, self)
        if request_temp:
            request = request_temp

        if response is None:
            raise Exception(f"连接超时 url={request.url}")

        if validate_response(request, self, response) == False:
            response.close()
            log.warning(f"任务{request.task_id} 文件校验未通过，丢弃当前请求 url={request.url}")
            error = Exception(f"validate返回False, 丢弃文件下载 url={request.url}")
            yield from self._record_file_failure(request, error, "文件校验失败")
            return

        yield from self.save_file(request, response)

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
        finally:
            response.close()

        if ok is False:
            yield from self._record_file_failure(
                request, Exception("process_file返回False"), "文件处理显式失败"
            )
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
            # 启用去重时派发期必然已解析出 dedup_key（否则接口模式已抛异常），
            # 这里不做 url 兜底，避免写入与查询键不一致
            dedup_key = request.dedup_key
            try:
                self._file_dedup.set(dedup_key, file_path)
            except Exception as e:
                log.error(f"任务{task_id} 去重缓存写入异常 key={dedup_key} error={e}")

        log.info(f"任务{task_id} 文件下载成功 [{success + fail + skipped + dup}/{total}] url={url}")

        try:
            self.on_file_downloaded(request)
        except Exception as e:
            log.error(f"任务{task_id} on_file_downloaded回调异常 url={url} error={e}")

        if status == 1:
            yield from self._fire_task_all_done(
                request.task, task_id, total, success, fail, skipped, dup
            )

    def _record_file_failure(self, request, error, reason):
        """
        记录单个文件失败：递增 fail 计数、调 on_file_failed，并在任务收尾时触发 on_task_all_done

        @param request: 失败的请求（槽位请求或下载请求）
        @param error: 异常对象，透传给 on_file_failed
        @param reason: 失败原因，用于日志区分下载失败/校验失败/处理显式失败
        @return: 生成器，产出 on_task_all_done 的结果与 Redis 清理回调
        """
        task_id = request.task_id
        run_id = getattr(request, "run_id", "")

        progress_key = setting.TAB_FILE_PROGRESS.format(
            redis_key=self._redis_key, task_id=task_id
        )
        result_key = setting.TAB_FILE_RESULT.format(
            redis_key=self._redis_key, task_id=task_id
        )
        status, total, success, fail, skipped, dup = self.record_and_check_done(
            progress_key, result_key, "fail", str(request.index), "", run_id,
        )

        if status == -1:
            log.debug(f"任务{task_id} 过期回调已丢弃 url={request.url}")
            return

        log.error(f"任务{task_id} {reason} [{success + fail + skipped + dup}/{total}] url={request.url}")

        try:
            self.on_file_failed(request, error)
        except Exception as e_cb:
            log.error(f"任务{task_id} on_file_failed回调异常 url={request.url} error={e_cb}")

        if status == 1:
            yield from self._fire_task_all_done(
                request.task, task_id, total, success, fail, skipped, dup
            )

    def _fire_task_all_done(self, task, task_id, total, success, fail, skipped, dup):
        """
        触发用户的任务收尾回调并清理任务的 Redis 状态

        @return: 生成器，产出 on_task_all_done 的结果与 Redis 清理回调
        """
        try:
            result = self._assemble_results(task_id, total)
            stats = FileTaskStats(
                success=success, fail=fail, skipped=skipped, dup=dup, total=total
            )
            for item in self.on_task_all_done(task, result, stats) or []:
                yield item
        except Exception as e:
            log.error(f"任务{task_id} on_task_all_done异常 error={e}")
            log.warning(f"任务{task_id} 状态未更新, 请检查on_task_all_done实现")
        finally:
            yield lambda: self._cleanup_task_redis(task_id)

    def failed_request(self, request, response, e):
        """
        请求失败（重试耗尽或 validate 返回 False）的处理。

        接口模式下槽位请求在此计入 fail，此时该文件从未拿到直链，
        on_file_failed 收到的是 url 为接口地址的槽位请求。
        """
        task_id = getattr(request, "task_id", None)
        file_index = getattr(request, "index", None)

        if task_id is None or file_index is None:
            yield request
            return

        yield from self._record_file_failure(request, e, "文件下载失败")
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
