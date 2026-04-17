# -*- coding: utf-8 -*-
"""
Created on 2018-07-25 11:41:57
---------
@summary: parser 的基类
---------
@author: Boris
@email:  boris_liu@foxmail.com
"""
import os
from urllib.parse import urlparse, unquote

import feapder.utils.tools as tools
from feapder.db.mysqldb import MysqlDB
from feapder.network.item import UpdateItem
from feapder.utils.log import log
from feapder.network.request import Request
from feapder.network.response import Response
from feapder.utils.perfect_dict import PerfectDict


class BaseParser(object):
    def start_requests(self):
        """
        @summary: 添加初始url
        ---------
        ---------
        @result: yield Request()
        """

        pass

    def download_midware(self, request: Request):
        """
        @summary: 下载中间件 可修改请求的一些参数, 或可自定义下载，然后返回 request, response
        ---------
        @param request:
        ---------
        @result: return request / request, response
        """

        pass

    def validate(self, request: Request, response: Response):
        """
        @summary: 校验函数, 可用于校验response是否正确
        若函数内抛出异常，则重试请求
        若返回True 或 None，则进入解析函数
        若返回False，则丢弃当前请求，并调用 failed_request 回调（request.is_abandoned=True）
        可通过request.callback_name 区分不同的回调函数，编写不同的校验逻辑
        ---------
        @param request:
        @param response:
        ---------
        @result: True / None / False
        """

        pass

    def parse(self, request: Request, response: Response):
        """
        @summary: 默认的解析函数
        ---------
        @param request:
        @param response:
        ---------
        @result:
        """

        pass

    def exception_request(self, request: Request, response: Response, e: Exception):
        """
        @summary: 请求或者parser里解析出异常的request
        ---------
        @param request:
        @param response:
        @param e: 异常
        ---------
        @result: request / callback / None (返回值必须可迭代)
        """

        pass

    def failed_request(self, request: Request, response: Response, e: Exception):
        """
        @summary: 失败请求回调（超过最大重试次数或 validate 返回False）
        可返回修改后的request  若不返回request，则将传进来的request直接人redis的failed表。否则将修改后的request入failed表
        ---------
        @param request:
        @param response:
        @param e: 异常
        ---------
        @result: request / item / callback / None (返回值必须可迭代)
        """

        pass

    def start_callback(self):
        """
        @summary: 程序开始的回调
        ---------
        ---------
        @result: None
        """

        pass

    def end_callback(self):
        """
        @summary: 程序结束的回调
        ---------
        ---------
        @result: None
        """

        pass

    @property
    def name(self):
        return self.__class__.__name__

    def close(self):
        pass


class TaskParser(BaseParser):
    def __init__(self, task_table, task_state, mysqldb=None):
        self._mysqldb = mysqldb or MysqlDB()  # mysqldb

        self._task_state = task_state  # mysql中任务表的state字段名
        self._task_table = task_table  # mysql中的任务表

    def add_task(self):
        """
        @summary: 添加任务, 每次启动start_monitor 都会调用，且在init_task之前调用
        ---------
        ---------
        @result:
        """

    def start_requests(self, task: PerfectDict):
        """
        @summary:
        ---------
        @param task: 任务信息 list
        ---------
        @result:
        """

    def update_task_state(self, task_id, state=1, **kwargs):
        """
        @summary: 更新任务表中任务状态，做完每个任务时代码逻辑中要主动调用。可能会重写
        调用方法为 yield lambda : self.update_task_state(task_id, state)
        ---------
        @param task_id:
        @param state:
        ---------
        @result:
        """

        kwargs["id"] = task_id
        kwargs[self._task_state] = state

        sql = tools.make_update_sql(
            self._task_table, kwargs, condition="id = {task_id}".format(task_id=task_id)
        )

        if self._mysqldb.update(sql):
            log.debug("置任务%s状态成功" % task_id)
        else:
            log.error("置任务%s状态失败  sql=%s" % (task_id, sql))

    update_task = update_task_state

    def update_task_batch(self, task_id, state=1, **kwargs):
        """
        批量更新任务 多处调用，更新的字段必须一致
        注意：需要 写成 yield update_task_batch(...) 否则不会更新
        @param task_id:
        @param state:
        @param kwargs:
        @return:
        """
        kwargs["id"] = task_id
        kwargs[self._task_state] = state

        update_item = UpdateItem(**kwargs)
        update_item.table_name = self._task_table
        update_item.name_underline = self._task_table + "_item"

        return update_item


class FileParser(TaskParser):
    """
    @summary: 文件下载爬虫模版
    ---------
    """

    def __init__(self, task_table, task_state, mysqldb=None, save_dir="./downloads"):
        super(FileParser, self).__init__(
            task_table=task_table, task_state=task_state, mysqldb=mysqldb
        )
        self._save_dir = save_dir

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

    def download_request(self, task, url, file_path=None, **kwargs):
        """
        构造下载请求的辅助方法。

        @param task: 任务对象（必须传入，框架据此追踪进度）
        @param url: 文件 URL
        @param file_path: 可选，文件保存路径/存储标识；不传则在派发时调用 get_file_path 生成
        @param kwargs: 透传到 Request 的其他参数（headers/method/data/proxies/render/timeout 等）
        @return: Request - 标记为下载请求的 Request 对象
        """
        save_file = getattr(self, "save_file", None)
        if "callback" in kwargs and save_file is not None and kwargs["callback"] is not save_file:
            log.warning("download_request 的 callback 将被强制设为 save_file，用户传入的回调被忽略")
        if save_file is not None:
            kwargs["callback"] = save_file
        return Request(
            url,
            task=task,
            file_path=file_path,
            is_file_download=True,
            **kwargs,
        )

    def get_file_path(self, task, url, index):
        """
        返回文件保存路径/标识，用户可重写
        本地场景: 返回本地文件路径，如 ./downloads/123/0_image.jpg
        云存储场景: 返回存储标识/key，如 bucket/prefix/123/0_image.jpg
        @param task: 任务信息
        @param url: 文件 URL
        @param index: 文件在下载请求序列中的索引（按 start_requests yield 顺序），默认实现用于避免同名文件覆盖
        @return: str - 文件路径或存储标识
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
        注意:
        - 此方法在下载失败重试时可能被多次调用，实现需保证幂等性
        - 必须返回非空字符串，返回空值会触发重试直至失败
        @param task_id: 任务 ID
        @param url: 文件原始 URL
        @param file_path: get_file_path 返回的路径/标识
        @param response: 下载响应
        @return: str - 文件最终存储位置（不可为空）
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

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        """
        任务所有文件处理完毕的回调
        用户应在此方法中 yield Item 写入结果表、yield self.update_task_batch() 更新任务状态
        @param task: PerfectDict - 任务对象，包含 task_keys 指定的字段
        @param         result: List[str|None] - 每个文件的处理结果，
            顺序与 start_requests 中 yield 的下载请求一致。
            成功为文件存储位置（本地路径或云存储 URL），失败为 None。
            任务内重复URL的结果继承首次出现的结果
        @param success_count: 成功数（含去重缓存命中）
        @param fail_count: 下载失败数（重试耗尽）
        @param skipped_count: 跳过数（无效URL、get_file_path异常等）
        @param dup_count: 任务内重复URL数
        @param total_count: 总数（success + fail + skipped + dup = total）
        """
        pass


class BatchParser(TaskParser):
    """
    @summary: 批次爬虫模版
    ---------
    """

    def __init__(
        self, task_table, batch_record_table, task_state, date_format, mysqldb=None
    ):
        super(BatchParser, self).__init__(
            task_table=task_table, task_state=task_state, mysqldb=mysqldb
        )
        self._batch_record_table = batch_record_table  # mysql 中的批次记录表
        self._date_format = date_format  # 批次日期格式

    @property
    def batch_date(self):
        """
        @summary: 获取批次时间
        ---------
        ---------
        @result:
        """

        batch_date = os.environ.get("batch_date")
        if not batch_date:
            sql = 'select date_format(batch_date, "{date_format}") from {batch_record_table} order by id desc limit 1'.format(
                date_format=self._date_format.replace(":%M", ":%i"),
                batch_record_table=self._batch_record_table,
            )
            batch_info = MysqlDB().find(sql)  # (('2018-08-19'),)
            if batch_info:
                os.environ["batch_date"] = batch_date = batch_info[0][0]
            else:
                log.error("需先运行 start_monitor_task()")
                os._exit(137)  # 使退出码为35072 方便爬虫管理器重启

        return batch_date
