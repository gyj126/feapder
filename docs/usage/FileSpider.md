# FileSpider

FileSpider 是一款分布式文件下载爬虫，专用于批量下载文件/图片的场景。

核心特征：
- **一对多**: 一个任务包含多个待下载文件，由用户在 `start_requests` 中 yield 多个下载请求
- **请求灵活**: 通过 `download_request` 辅助方法构造请求，可自由设置 headers/method/data/proxies/render 等
- **进度追踪**: 框架自动追踪每个任务的下载进度（成功数/失败数/跳过数/去重数/总数）
- **结果有序**: 下载结果列表与 `start_requests` 中 yield 的下载请求顺序严格对应
- **灵活存储**: 默认保存到本地磁盘，可重写为上传云存储（OSS/S3 等），不落盘
- **文件去重**: 任务内相同 URL 自动去重；可选跨任务去重（Redis / MySQL / 自定义）
- **HTTP 校验**: 默认对 4xx/5xx 响应触发重试，用户可重写 `validate` 自定义校验
- **用户控制**: 任务成功/失败由用户在回调中显式决定

FileSpider 继承自 TaskSpider，复用了全部任务管理能力（MySQL 任务表、Redis 队列、断点续爬、丢失任务回收、分布式支持等）。

## 1. 任务表

### MySQL 任务表（建议结构）

```sql
CREATE TABLE `file_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `file_urls` text COMMENT '待下载文件URL列表，JSON数组格式',
  `state` int(11) DEFAULT 0 COMMENT '任务状态: 0待做 2下载中 1完成 -1失败',
  PRIMARY KEY (`id`),
  KEY `idx_state` (`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

字段说明：
- `id`: 任务主键，必须有
- `file_urls`: 存放待下载文件 URL 的 JSON 数组，字段名可自定义
- `state`: 任务状态字段，字段名可通过 `task_state` 参数配置。0=待做，2=已下发（框架自动设置），1=完成，-1=失败（由用户代码设置）

索引建议：
- `state` 是调度核心字段，框架会按 `check_task_interval`（默认 5 秒）轮询 `where state=0/2`；任务表行数较多时，建议加单列索引 `KEY idx_state (state)`，避免反复全表扫描。
- 如果使用了 `task_condition` 按业务字段筛选任务（例如 `biz_type='image' and priority>=10`），建议改建复合索引 `KEY idx_state_biz (state, biz_type, priority)`，将 `state` 放在最左。
- 如果配置了非主键的 `task_order_by`，可把排序字段放到复合索引尾部以避免 filesort。

## 2. 用户需实现的方法

### 必须实现

| 方法 | 说明 |
|------|------|
| `start_requests(task)` | yield 该任务的所有下载请求，必须使用 `self.download_request(task, url, ...)` 构造 |
| `on_task_all_done(task, result, success_count, fail_count, skipped_count, dup_count, total_count)` | 任务所有文件处理完毕的回调，在此 yield Item 或 update_task_batch 更新状态 |

### 框架提供的辅助方法

| 方法 | 说明 |
|------|------|
| `download_request(task, url, **kwargs)` | 构造下载请求，自动注入框架元数据（task_id、file_index、run_id、callback）。`**kwargs` 透传到 `Request`，可设置 headers/method/data/proxies/render/timeout 等。文件保存路径统一由 `file_path` 决定 |

### 可选重写

| 方法 | 说明 | 默认行为 |
|------|------|----------|
| `file_path(task, url, index)` | 返回文件最终存储位置/标识；该返回值会作为 `result` 列表元素、`file_dedup` 缓存值、`on_file_downloaded` 第三参数 | `{save_dir}/{task_id}/{index}_{md5(filename)}{ext}` |
| `process_file(task_id, url, file_path, response)` | 将下载内容落地到 `file_path`；返回 `True`/`None` 视为成功，返回 `False` 显式失败（不重试），抛异常触发重试 | 流式保存到本地磁盘，返回 `None` |
| `validate(request, response)` | 校验下载响应 | 4xx/5xx抛异常触发重试，3xx自动跟随 |
| `on_file_downloaded(task_id, url, file_path)` | 单个文件下载成功回调（`file_path` 即 `file_path()` 返回值） | 无 |
| `on_file_failed(task_id, url, error)` | 单个文件下载失败回调 | 无 |

### 方法分层

```
start_requests (用户实现)
  └── yield self.download_request(task, url, **kwargs)  # 一个任务的所有下载请求都需在此 yield

distribute_task (框架层，按 yield 顺序分配 file_index、URL 去重、文件缓存命中、写 Redis 进度)
  ├── file_path(task, url, index) (用户层，按需重写) → 决定权威存储位置
  └── 下发请求

save_file (框架层，不应重写)
  ├── process_file(task_id, url, file_path, response) (用户层，按需重写)
  │     ├── return True/None: 成功 → 写 result/dedup → on_file_downloaded
  │     ├── return False: 显式失败 → 计入 fail → on_file_failed
  │     └── raise: 触发重试
  ├── Redis 进度追踪 (自动，幂等计数)
  └── 检查是否所有文件完成
        └── on_task_all_done (用户实现)
              ├── yield Item → 写入结果表
              └── yield update_task_batch → 更新任务状态
```

### 重要约束

- **下载请求必须从 `start_requests(task)` 直接 yield**：进度追踪需要在派发前知道下载请求总数，
  因此不支持在中间回调（如先抓列表页再 yield 下载请求）中产出下载请求。如有此类需求，
  需先用普通 Spider 解析出 URL 列表落入任务表，再交给 FileSpider 下载。
- **必须使用 `self.download_request(task, url, ...)`**：直接 `yield Request(url)` 不会被识别为下载请求，
  框架不会做进度追踪和回调处理。
- 在 `start_requests` 中允许同时 yield `Item` / `update_task_batch` 等非下载产物，框架会按原有规则分发。

### `process_file` 约束

`process_file` 是"落地动作"，**不再负责返回路径**——路径以 `file_path()` 的返回值为准。

**返回值语义**:

| 返回值 | 含义 |
|--------|------|
| `True` 或 `None` | 处理成功；框架写入 `result_key`、`file_dedup` 缓存（值均为 `file_path`），调用 `on_file_downloaded` |
| `False` | 显式失败：**不再重试**，直接计入 `fail_count`，调用 `on_file_failed` |
| 抛异常 | 触发框架重试机制 |

**幂等性要求**: 在下载失败重试时可能被多次调用（同一 URL、同一 `file_path`），实现需保证幂等：
- 默认实现使用 `"wb"` 模式覆盖写入，天然幂等
- 重写时避免使用追加模式（`"ab"`）
- 云存储场景建议使用 `put_object` 等覆盖语义的 API

**何时该用 `False` vs 抛异常**:
- 用 `False`：内容校验失败、业务规则不允许保存、下载到的文件明显是错的 —— 这些重试也无意义。
- 抛异常：临时性错误（网络写盘失败、OSS 偶发 5xx 等）—— 框架会按重试策略再尝试。

### `on_task_all_done` 参数说明

```python
def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
    """
    task: PerfectDict - 任务对象，包含 task_keys 指定的字段，可通过 task.id 获取任务 ID
    result: List[str|None]
    - 与 start_requests 中 yield 的下载请求顺序严格对应
    - 成功: file_path() 的返回值
    - 失败/跳过: None
    - 任务内重复URL: 继承首次出现的结果
    例: ["downloads/1/0_a.jpg", "downloads/1/1_b.jpg", None, "downloads/1/3_d.jpg"]
    success_count: 成功数（含去重缓存命中）
    fail_count: 下载失败数（重试耗尽 + process_file 显式返回 False）
    skipped_count: 跳过数（无效URL、file_path异常等）
    dup_count: 任务内重复URL数
    total_count: 总数（success + fail + skipped + dup = total）
    """
```

#### 重复 URL 与计数器关系

`result` 列表的长度严格等于 `start_requests` yield 的下载请求数（即 `total_count`），**重复 URL 不会被压缩，仍然占一个位置**，其值继承首次出现位置的最终结果。计数器满足不变式：

```
total = success + fail + skipped + dup
```

| 计数器 | 含义 | 是否包含重复位置 |
|--------|------|------|
| `success_count` | 下载成功 + 跨任务去重缓存命中 | 否 |
| `fail_count` | 下载失败（重试耗尽 或 `process_file` 显式返回 `False`） | 否 |
| `skipped_count` | 无效 URL、`file_path` 异常等被跳过 | 否 |
| `dup_count` | **任务内**重复 URL 的"额外位置"数（首次出现那个不计入 dup） | — |

举例：`start_requests` 顺序 yield 4 个下载请求 `[A, B, B, C]`（index=2 是任务内重复）。

| 场景 | result | 计数器 |
|------|--------|--------|
| 全部下载成功 | `["url_A", "url_B", "url_B", "url_C"]` | total=4, success=3, fail=0, skipped=0, dup=1 |
| B 下载失败 | `["url_A", None, None, "url_C"]` | total=4, success=2, fail=1, skipped=0, dup=1 |
| B 命中跨任务去重缓存 | `["url_A", "cached_B", "cached_B", "url_C"]` | total=4, success=3（含1个cached）, fail=0, skipped=0, dup=1 |

注意：跨任务去重缓存命中（`file_dedup`）属于 `success`，**不属于 `dup`**；`dup` 仅用于同一任务内同 URL 重复出现的情况。

### `on_task_all_done` 设计约定与实现建议

`on_task_all_done` 是业务回调，**任务状态由用户代码显式控制**（通常通过 `yield self.update_task_batch(...)`）。

- 若该方法抛异常，框架不会自动改写任务状态；任务可能保持 `doing(2)`
- 后续会由 TaskSpider 的丢失任务恢复机制重新下发任务
- 因此该方法建议按“可重试、可重入”方式实现，保证幂等

推荐实践：
- 先产出结果数据，再更新任务状态，避免状态先行导致结果缺失
- 对外部副作用（通知、回调第三方、写非幂等系统）增加幂等保护
- 异常日志要包含 `task.id`、计数信息和关键上下文，便于快速排障

#### 新手解释：什么是“幂等”

幂等可以理解为：**同一个操作执行 1 次和执行多次，最终结果一致**。

在 `FileSpider` 中，常见重试来源有网络重试、进程重启、丢失任务回收。  
因此 `on_task_all_done` 需要按“可能被重复执行”来设计：

- 幂等写法：`state` 直接设置为目标值（如 1 或 -1）
- 非幂等写法：每次执行都做自增/重复插入/重复通知

#### 推荐写法案例（可重试、可重入）

```python
from feapder.utils.log import log


class MyFileSpider(feapder.FileSpider):
    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        task_id = task.id
        log.info(
            f"任务{task_id}完成 success={success_count} fail={fail_count} "
            f"skipped={skipped_count} dup={dup_count} total={total_count}"
        )

        # 1) 先写业务结果（示例：可按需 yield Item）
        # item = FileResultItem()
        # item.task_id = task_id
        # item.result_urls = result
        # yield item

        # 2) 最后更新任务状态（设置目标值，天然幂等）
        done_state = 1 if fail_count == 0 and success_count > 0 else -1
        yield self.update_task_batch(task_id, done_state)
```

## 3. 构造参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `redis_key` | str | Redis key 前缀（必填） |
| `task_table` | str | MySQL 任务表名（必填） |
| `task_keys` | list | 需要获取的任务字段列表（必填） |
| `save_dir` | str | 文件保存根目录；不传时从配置项 `FILE_SAVE_DIR` 读取（默认 `./downloads`），传入则覆盖配置 |
| `file_dedup` | None/str/FileDedup | 文件去重策略：None 不去重，`"redis"` / `"mysql"` / FileDedup 实例 |
| `file_dedup_expire` | int | Redis 去重缓存过期时间（秒），仅 `file_dedup="redis"` 时生效 |
| `task_state` | str | 任务状态字段名，默认 `state` |
| `min_task_count` | int | Redis 中最少任务数，默认 10000 |
| `check_task_interval` | int | 检查任务间隔（秒），默认 5 |
| `task_limit` | int | 每次取任务数量，默认 10000 |
| `task_condition` | str | 任务筛选条件（WHERE 后的 SQL） |
| `task_order_by` | str | 取任务排序条件 |
| `thread_count` | int | 线程数 |
| `keep_alive` | bool | 是否常驻 |

### 相关配置项（`feapder.setting` / 项目 `setting.py`）

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `FILE_SAVE_DIR` | str | `./downloads` | 文件下载根目录。可通过环境变量 `FILE_SAVE_DIR` 覆盖；构造参数 `save_dir` 优先级高于此项 |

优先级：`FileSpider(save_dir=...)` > 环境变量 `FILE_SAVE_DIR` > 项目 `setting.py` 中的 `FILE_SAVE_DIR` > 框架默认 `./downloads`。

## 4. 使用示例

### 启动方式（单进程 / master-worker 分离）

FileSpider 支持两种启动方式：

1. 单进程：`spider.start()`，适合本地调试
2. 分离运行：master 仅负责派发任务，worker 仅负责下载处理，适合生产部署

```python
from feapder import ArgumentParser

if __name__ == "__main__":
    spider = MyFileSpider(
        redis_key="my_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
    )

    parser = ArgumentParser(description="MyFileSpider 文件下载爬虫")
    parser.add_argument(
        "--start_master",
        action="store_true",
        help="添加任务",
        function=spider.start_monitor_task,
    )
    parser.add_argument(
        "--start_worker",
        action="store_true",
        help="启动爬虫",
        function=spider.start,
    )
    parser.start()
```

命令行启动：

```bash
uv run my_file_spider.py --start_master
uv run my_file_spider.py --start_worker
```

### 场景一：保存到本地磁盘

最简单的用法，下载文件保存到本地：

```python
import json
import feapder


class LocalFileSpider(feapder.FileSpider):
    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        # fail_count == 0 且有实际成功下载则标记完成；全部跳过或无有效URL标记失败
        if fail_count == 0 and success_count > 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)


if __name__ == "__main__":
    spider = LocalFileSpider(
        redis_key="local_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
        # save_dir 不传时使用 setting.FILE_SAVE_DIR；显式传入将覆盖配置
        # save_dir="./my_files",
    )
    spider.start()
```

### 场景二：上传云存储

重写 `file_path` 决定 OSS 存储 key（这个 key 同时也是 result 列表里要存的值），重写 `process_file` 实现上传：

```python
import json
import os
import feapder
from urllib.parse import urlparse, unquote


class OssFileSpider(feapder.FileSpider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.oss_client = OSSClient(bucket="my-bucket")

    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    def file_path(self, task, url, index):
        """返回 OSS 存储 key（即 result 列表里要存的值）"""
        filename = os.path.basename(unquote(urlparse(url).path))
        return f"files/{task.id}/{index}_{filename}"

    def process_file(self, task_id, url, file_path, response):
        """上传 OSS。返回 None=成功，返回 False=显式失败（不重试），抛异常=重试"""
        self.oss_client.put_object(file_path, response.content)
        # return None

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        if success_count > 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)


if __name__ == "__main__":
    spider = OssFileSpider(
        redis_key="oss_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
    )
    spider.start()
```

> 提示：`result` 列表里存的是 OSS key（如 `files/123/0_a.jpg`），而不是公网 URL。如果业务方要拿到 URL，可以在 `on_task_all_done` 里基于固定 base 拼接，或者在消费方按需拼接。这样做的好处是 OSS 域名/CDN 切换时不用回写库。

### 场景三：上传云存储 + 结果入库

先创建结果 Item：

```bash
feapder create -i file_result
```

编辑生成的 `items/file_result_item.py`，添加所需字段，然后在爬虫中引用：

```python
import json
import os
import feapder
from urllib.parse import urlparse, unquote
from items.file_result_item import FileResultItem


class OssResultSpider(feapder.FileSpider):
    OSS_BASE_URL = "https://my-bucket.oss.aliyuncs.com"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.oss_client = OSSClient(bucket="my-bucket")

    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    def file_path(self, task, url, index):
        filename = os.path.basename(unquote(urlparse(url).path))
        return f"files/{task.id}/{index}_{filename}"

    def process_file(self, task_id, url, file_path, response):
        self.oss_client.put_object(file_path, response.content)
        # return None

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        # result 与 start_requests 中 yield 的下载请求顺序严格位置对应
        # 元素是 file_path() 返回的 OSS key，下载失败/跳过为 None
        # 入库时把 OSS key 拼成可访问 URL
        result_urls = [
            f"{self.OSS_BASE_URL}/{key}" if key else None for key in result
        ]
        item = FileResultItem()
        item.task_id = task.id
        item.result_urls = result_urls
        yield item

        if fail_count == 0 and success_count > 0:
            yield self.update_task_batch(task.id, 1)
        else:
            yield self.update_task_batch(task.id, -1)
```

### 场景四：启用文件去重

通过 `file_dedup` 参数启用跨任务去重，同一 URL 跨任务不重复下载：

```python
import json
import feapder


class DedupFileSpider(feapder.FileSpider):
    def start_requests(self, task):
        for url in json.loads(task.file_urls):
            yield self.download_request(task, url)

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        yield self.update_task_batch(task.id, 1 if fail_count == 0 and success_count > 0 else -1)


if __name__ == "__main__":
    spider = DedupFileSpider(
        redis_key="dedup_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
        save_dir="./downloads",
        file_dedup="redis",  # "redis" / "mysql" / FileDedup 实例
    )
    spider.start()
```

### 场景五：自定义请求参数

`download_request` 透传所有 `Request` 参数，可按文件维度自由设置请求行为：

```python
import json
import feapder


class CustomRequestSpider(feapder.FileSpider):
    def start_requests(self, task):
        common_headers = {"Referer": "https://example.com/", "User-Agent": "MyBot/1.0"}
        for url in json.loads(task.file_urls):
            yield self.download_request(
                task,
                url,
                headers=common_headers,
                proxies={"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"},
                timeout=30,
                render=False,
            )

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        yield self.update_task_batch(task.id, 1 if fail_count == 0 and success_count > 0 else -1)
```

也可以根据 URL 不同走不同的下载策略，例如部分文件需要鉴权头：

```python
def start_requests(self, task):
    for url in json.loads(task.file_urls):
        if "private" in url:
            yield self.download_request(task, url, headers={"Authorization": "Bearer xxx"})
        else:
            yield self.download_request(task, url)
```

## 5. 文件去重

### 去重层级

FileSpider 提供两级去重：

1. **任务内去重（自动）**: 同一任务的 URL 列表中出现的重复 URL，只下载一次，重复项继承首次出现的结果
2. **跨任务去重（可选）**: 通过 `file_dedup` 参数启用，不同任务中出现的相同 URL 只下载一次

### 跨任务去重策略

| 策略 | 参数值 | 存储 | 适用场景 |
|------|--------|------|----------|
| 不去重 | `None`（默认） | - | 每次都重新下载 |
| Redis 去重 | `"redis"` | Redis Hash | 分布式共享，多进程安全 |
| MySQL 去重 | `"mysql"` | MySQL 表（按 `redis_key` 自动分表） | 持久化，隔离不同业务 |
| 自定义去重 | `FileDedup` 实例 | 用户自定义 | 特殊需求 |

### 自定义去重

继承 `FileDedup` 接口：

```python
from feapder.dedup.file_dedup import FileDedup

class MyFileDedup(FileDedup):
    def get(self, url):
        """返回缓存结果，无缓存返回 None"""
        ...

    def set(self, url, result_url):
        """缓存处理结果"""
        ...
```

## 6. Debug 模式

支持 Debug 模式，可针对单个任务调试：

```python
if __name__ == "__main__":
    spider = MyFileSpider.to_DebugFileSpider(
        task_id=1,
        redis_key="my_file_spider",
        task_table="file_task",
        task_keys=["id", "file_urls"],
        save_dir="./downloads",
    )
    spider.start()
```

Debug 模式下默认不入库、不更新任务状态。
