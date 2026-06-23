# feapder 监控（InfluxDB + Grafana）

feapder 内置打点监控，运行时将请求下载、运行状态、响应耗时等指标写入 InfluxDB 2.x，再通过 Grafana 看板可视化。整套监控只需配置数据源地址、导入看板 JSON 即可使用。

## 数据模型

所有打点写入同一个 measurement（默认 `crawler`），通过 tag 区分维度：

| tag | 含义 |
| --- | --- |
| `project` | 站点/项目名，默认取用户 setting.py 所在目录名，可在 `setting.py` 用 `PROJECT_NAME` 覆盖 |
| `spider` | 爬虫名（爬虫类的 `name`） |
| `hostname` | 进程所在主机，取本机内网 IP（探测失败时回退为机器名），所有指标均带 |
| `pid` | 进程号，所有指标均带，用于区分同一主机上的多个进程实例 |
| `_classify` | 指标分组：`document`（下载）、`http_status`（状态码分布）、`latency`（响应耗时）、`runtime`（运行状态）、`<表名>`（入库）、自定义分类 |
| `_key` | 序列名（折线名） |
| `_type` | `counter` / `timer` / `store` |

| field | 含义 |
| --- | --- |
| `_count` | counter 计数 |
| `_duration` | timer 耗时（秒） |
| `_value` | store 快照值（如 `runtime` 的 heartbeat/memory） |

## 一、爬虫侧配置

在项目 `setting.py` 或环境变量中配置 InfluxDB 2.x 连接：

```python
INFLUXDB_URL = "http://106.54.14.129:8086"
INFLUXDB_TOKEN = "my-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "crawler"
# 可选：自定义 measurement 名称，需与 Grafana 看板的 measurement 变量一致
INFLUXDB_MEASUREMENT = "crawler"
# 可选：站点名，默认取用户 setting.py 所在目录名
PROJECT_NAME = "taobao"
```

对应的环境变量为 `INFLUXDB_URL` / `INFLUXDB_TOKEN` / `INFLUXDB_ORG` / `INFLUXDB_BUCKET` / `INFLUXDB_MEASUREMENT` / `PROJECT_NAME`。

爬虫正常运行即会自动打点，bucket（默认保留 180 天）会在首次运行时自动创建；若所用 token 无 bucket 管理权限，需先在 InfluxDB 中手动建好 bucket。

## 二、Grafana 配置数据源

1. Grafana 进入 `Connections` -> `Data sources` -> `Add data source` -> `InfluxDB`。
2. `Query Language` 选择 `Flux`。
3. `HTTP URL` 填 `http://106.54.14.129:8086`。
4. `InfluxDB Details` 中填写 `Organization`、`Token`，`Default Bucket` 填 `crawler`。
5. 保存并测试连接。

## 三、导入看板

1. Grafana 进入 `Dashboards` -> `New` -> `Import`。
2. 上传 `dashboards/feapder-crawler.json`。
3. 在 `DS_INFLUXDB` 处选择上一步创建的数据源，完成导入。

导入后顶部有 站点(`project`) -> 爬虫(`spider`) -> 时间 的级联筛选，默认时间范围为过去 30 分钟。看板内置 `bucket` 与 `measurement` 两个常量变量（默认均为 `crawler`），若 `INFLUXDB_BUCKET` 或 `INFLUXDB_MEASUREMENT` 改过名，需同步修改看板顶部对应变量。

## 看板面板

看板分为三个区域，均受顶部 `project` / `spider` / `host` / 时间 筛选约束。

### 请求下载监控

基于 `_classify='document'` 指标，衡量**任务级**下载与解析结果（下载 → validate → parse 整条链路是否跑通）。

| 面板 | 对应指标 | 含义 |
| --- | --- | --- |
| 请求总量 | `download_total` | 进入下载流程的任务数，每处理一个 request 计 1 次 |
| 下载成功 | `download_success` | 下载、校验、解析均无异常，完整跑通的任务数 |
| 下载异常 | `download_exception` | 网络层异常（如连接超时、requests 异常）的任务数 |
| 解析异常 | `parser_exception` | 非 requests 类异常（如 parse 抛错、类型错误等）的任务数 |
| 下载成功率 | `download_success / download_total` | 顶部时间范围内，完整成功的任务占全部任务的比例 |
| 请求下载趋势 | 同上三项的滚动比率 | 固定 5 分钟窗口内的下载成功率、下载异常率、解析异常率折线，详见下文 |

> `download_total - download_success - download_exception - parser_exception` 的差值，包含 `validate` 返回 False、重试未达上限等未单独打异常指标的场景。

### 运行状态监控

基于 `_classify='runtime'`，借助全局的 `hostname` / `pid` tag 区分进程实例。`hostname` 取本机内网 IP（探测失败时回退为机器名）。

| 面板 | 对应指标 | 含义 |
| --- | --- | --- |
| 运行状态 | `heartbeat` | 表格展示各实例的爬虫名、主机、PID、在线状态；最近 60 秒内有心跳则显示「在线」 |
| 内存占用 | `memory` | 各实例进程内存占用（MB）时序曲线，默认每 10 秒上报一次 |

### 请求监控（状态码/耗时）

基于 `_classify='http_status'` 与 `latency`，衡量**HTTP 传输层**响应情况（在收到响应时记录，早于 `validate`）。

| 面板 | 对应指标 | 含义 |
| --- | --- | --- |
| HTTP 状态码分布 | `http_status` | 各 HTTP 状态码（200、404、500 等）的请求数占比饼图 |
| 请求成功率 | 2xx / 全部 `http_status` | 顶部时间范围内，返回 2xx 状态码的请求占全部 HTTP 响应的比例 |
| 状态码趋势 | 2xx / 3xx / 4xx / 5xx 占比 | 固定 5 分钟窗口内各状态码段占比折线，详见下文 |
| 请求响应耗时 P95 | `latency` | 各爬虫请求响应耗时的 P95 曲线，单位秒 |

### 下载成功率 vs 请求成功率

两者名称相近，但统计维度完全不同，排查问题时应分开看：

| | 下载成功率 | 请求成功率 |
| --- | --- | --- |
| 所在区域 | 请求下载监控 | 请求监控（状态码/耗时） |
| 统计层级 | 任务/业务层 | HTTP 传输层 |
| 数据来源 | `_classify='document'` | `_classify='http_status'` |
| 分子 | `download_success` | 2xx 状态码计数 |
| 分母 | `download_total` | 全部 HTTP 状态码计数 |
| 记录时机 | 任务完整跑通后 | 收到 HTTP 响应时（早于 validate） |

**典型差异场景：**

- HTTP 200 但 `validate` 返回 False：请求成功率计为成功，下载成功率不计为成功。
- 连接超时、无 HTTP 响应：计入 `download_total` 和 `download_exception`，通常不产生 `http_status` 点，拉低下载成功率但不影响请求成功率分母。
- 返回 403/500：请求成功率计为失败；若因此未跑完整条链路，下载成功率同样计为失败。
- 重试：每次重试可能产生新的 `http_status` 点，而 `download_total` 按每次 `deal_request` 计 1 次，两个指标的分子分母可能不完全对齐。

### 成功率告警（代码侧）

`setting.py` 中 `WARNING_SUCCESS_RATE` 触发的「请求成功率报警」，与看板三个成功率指标均不同：

| | 成功率告警 | 看板「下载成功率」 | 看板「请求成功率」 |
| --- | --- | --- | --- |
| 数据来源 | `ParserControl` 内存计数 | InfluxDB `document` | InfluxDB `http_status` |
| 公式 | `success / (success + failed)` | `download_success / download_total` | 2xx / 全部状态码 |
| 统计范围 | 进程启动后累计 | 看板时间筛选 | 看板时间筛选 |

**计算公式**（`Scheduler.check_task_status()`，每分钟最多检查一次）：

```
成功率 = success_task_count / (success_task_count + failed_task_count)
```

- 默认 `WARNING_SUCCESS_RATE = 0.8`，低于 80% 告警。
- 默认 `WARNING_SUCCESS_RATE_MIN_COUNT = 100`，`success + failed` 不足 100 时不告警。
- `_total_task_count` 不参与告警计算。

**何时计入 success / failed：**

| 场景 | success | failed |
| --- | --- | --- |
| 下载 → validate → parse 全程无异常 | +1 | — |
| `validate` 返回 False | — | +1（立即计失败，无重试） |
| 异常且重试次数已用尽或 `is_abandoned` | — | +1 |
| 异常但未达重试上限（重新入队） | — | 不加 |

**重试对告警的影响：** 中间失败的重试尝试只增加 `_total_task_count`（及 InfluxDB 的 `download_total`），既不记 failed 也不记 success。URL 最终成功只计 1 次 success；重试用尽只计 1 次 failed。因此**重试后成功的请求不会拉低告警成功率**。

**与看板下载成功率的差异：** 看板按每次 `deal_request` 计 `download_total`，中间重试会拉高分母；告警按 URL 最终结局统计，分母为 `success + failed`。可能出现告警成功率仍高、看板下载成功率偏低的情况（大量重试时）。

相关配置项：`WARNING_SUCCESS_RATE`、`WARNING_SUCCESS_RATE_MIN_COUNT`。详见 `docs/source_code/报警及监控.md`。

### 请求下载趋势（滚动窗口比率）

折线图中每个点的含义：以该点横坐标时间为终点、**固定 5 分钟窗口**内的滚动比率。某点 T 的下载成功率 = `[T-5m, T]` 内 `download_success` 之和 / 同窗口内 `download_total` 之和；下载异常率、解析异常率同理。顶部时间筛选只决定趋势图展示的时间段。

实现要点：

- 查询使用顶部时间筛选作为展示范围。
- Flux 先用 `aggregateWindow(every: 1m, fn: sum)` 按 1 分钟分桶求和，再用 `timedMovingAverage(every: 1m, period: 5m)` 计算固定 5 分钟滚动聚合。
- **图例 Last / 最右端点** 表示最新 5 分钟窗口值，不等同于整个顶部筛选范围的汇总比率。

### 状态码趋势（5 分钟窗口占比）

按 5 分钟窗口展示 2xx / 3xx / 4xx / 5xx 在 HTTP 响应总量中的占比。实现上先用 Flux `aggregateWindow(every: 1m, fn: sum)` 按 1 分钟分桶求和，再通过 `timedMovingAverage(every: 1m, period: 5m)` 生成固定 5 分钟窗口值，最后由看板 transformations 计算各状态码段占比。

## 自定义打点

业务代码可直接打点，自动带上当前 `project` 与 `spider` tag：

```python
from feapder.utils import metrics

metrics.emit_counter("success", count=1, classify="自定义指标")
```

`classify` 对应看板分组，`key` 对应折线名，`count` 对应点数。
