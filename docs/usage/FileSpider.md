# FileSpider

FileSpider 是一款分布式文件下载爬虫，专用于批量下载文件/图片的场景。

核心特征：
- **一对多**: 一个任务包含多个待下载文件的 URL 列表，框架自动遍历生成下载请求
- **进度追踪**: 框架自动追踪每个任务的下载进度（成功数/失败数/跳过数/去重数/总数）
- **结果有序**: 下载结果列表与原始 URL 列表严格位置对应
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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

字段说明：
- `id`: 任务主键，必须有
- `file_urls`: 存放待下载文件 URL 的 JSON 数组，字段名可自定义
- `state`: 任务状态字段，字段名可通过 `task_state` 参数配置。0=待做，2=已下发（框架自动设置），1=完成，-1=失败（由用户代码设置）

## 2. 用户需实现的方法

### 必须实现

| 方法 | 说明 |
|------|------|
| `get_download_urls(task)` | 从 task 中提取文件 URL 列表，返回 `List[str]` |
| `on_task_all_done(task, result, success_count, fail_count, skipped_count, dup_count, total_count)` | 任务所有文件处理完毕的回调，在此 yield Item 或 update_task_batch 更新状态 |

### 可选重写

| 方法 | 说明 | 默认行为 |
|------|------|----------|
| `get_file_path(task, url, index)` | 返回文件保存路径/存储标识 | `{save_dir}/{task_id}/{index}_{md5(filename)}{ext}` |
| `process_file(task_id, url, file_path, response)` | 处理文件内容，返回最终存储位置（需保证幂等） | 流式保存到本地磁盘，返回本地路径 |
| `validate(request, response)` | 校验下载响应 | 4xx/5xx抛异常触发重试，3xx自动跟随 |
| `on_file_downloaded(task_id, url, file_path)` | 单个文件下载成功回调 | 无 |
| `on_file_failed(task_id, url, error)` | 单个文件下载失败回调 | 无 |

### 方法分层

```
save_file (框架层，不应重写)
  ├── process_file (用户层，按需重写)
  │     ├── 默认: 保存到本地磁盘，返回本地路径
  │     └── 重写: 上传云存储，返回云存储 URL
  ├── Redis 进度追踪 (自动，幂等计数)
  ├── on_file_downloaded 回调
  └── 检查是否所有文件完成
        └── on_task_all_done (用户实现)
              ├── yield Item → 写入结果表
              └── yield update_task_batch → 更新任务状态
```

### `process_file` 幂等性要求

`process_file` 在下载失败重试时可能被多次调用（同一 URL、同一 `file_path`），实现需保证幂等性：
- 默认实现使用 `"wb"` 模式覆盖写入，天然幂等
- 重写时避免使用追加模式（`"ab"`）
- 云存储场景建议使用 `put_object` 等覆盖语义的 API

### `on_task_all_done` 参数说明

```python
def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
    """
    task: PerfectDict - 任务对象，包含 task_keys 指定的字段，可通过 task.id 获取任务 ID
    result: List[str|None]
    - 与 get_download_urls 返回的列表严格位置对应
    - 成功: 文件存储位置（本地路径或云存储 URL）
    - 失败/跳过: None
    - 任务内重复URL: 继承首次出现的结果
    例: ["https://oss.com/a.jpg", "https://oss.com/b.jpg", None, "https://oss.com/a.jpg"]
    success_count: 成功数（含去重缓存命中）
    fail_count: 下载失败数（重试耗尽）
    skipped_count: 跳过数（无效URL、get_file_path异常等）
    dup_count: 任务内重复URL数
    total_count: 总数（success + fail + skipped + dup = total）
    """
```

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
| `save_dir` | str | 文件保存根目录，默认 `./downloads` |
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

## 4. 使用示例

### 场景一：保存到本地磁盘

最简单的用法，下载文件保存到本地：

```python
import json
import feapder


class LocalFileSpider(feapder.FileSpider):
    def get_download_urls(self, task):
        return json.loads(task.file_urls)

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
        save_dir="./downloads",
    )
    spider.start()
```

### 场景二：上传云存储

重写 `process_file` 实现直接上传云存储：

```python
import json
import os
import feapder
from urllib.parse import urlparse, unquote


class OssFileSpider(feapder.FileSpider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 初始化云存储客户端
        self.oss_client = OSSClient(bucket="my-bucket")

    def get_download_urls(self, task):
        return json.loads(task.file_urls)

    def get_file_path(self, task, url, index):
        """返回 OSS 存储 key（不是本地路径）"""
        filename = os.path.basename(unquote(urlparse(url).path))
        return f"files/{task.id}/{index}_{filename}"

    def process_file(self, task_id, url, file_path, response):
        """上传 OSS，返回云存储 URL"""
        self.oss_client.put_object(file_path, response.content)
        return f"https://my-bucket.oss.aliyuncs.com/{file_path}"

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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.oss_client = OSSClient(bucket="my-bucket")

    def get_download_urls(self, task):
        return json.loads(task.file_urls)

    def get_file_path(self, task, url, index):
        filename = os.path.basename(unquote(urlparse(url).path))
        return f"files/{task.id}/{index}_{filename}"

    def process_file(self, task_id, url, file_path, response):
        self.oss_client.put_object(file_path, response.content)
        return f"https://my-bucket.oss.aliyuncs.com/{file_path}"

    def on_task_all_done(self, task, result, success_count, fail_count, skipped_count, dup_count, total_count):
        # result 与 get_download_urls 返回的列表严格位置对应，下载失败的用 None 占位
        item = FileResultItem()
        item.task_id = task.id
        item.result_urls = result
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
    def get_download_urls(self, task):
        return json.loads(task.file_urls)

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
| MySQL 去重 | `"mysql"` | MySQL 表（自动建表） | 持久化，长期缓存 |
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
