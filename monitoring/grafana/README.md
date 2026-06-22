# feapder 监控（InfluxDB + Grafana）

feapder 内置打点监控，运行时将请求下载、运行状态、响应耗时等指标写入 InfluxDB 1.x，再通过 Grafana 看板可视化。整套监控只需配置数据源地址、导入看板 JSON 即可使用。

## 数据模型

所有打点写入同一个 measurement（默认 `crawler`），通过 tag 区分维度：

| tag | 含义 |
| --- | --- |
| `project` | 站点/项目名，默认取项目目录名，可在 `setting.py` 用 `PROJECT_NAME` 覆盖 |
| `spider` | 爬虫名（爬虫类的 `name`） |
| `_classify` | 指标分组：`document`（下载）、`http_status`（状态码分布）、`latency`（响应耗时）、`runtime`（运行状态，额外带 `hostname`/`pid` tag 区分进程实例）、`<表名>`（入库）、自定义分类 |
| `_key` | 序列名（折线名） |
| `_type` | `counter` / `timer` / `store` |

| field | 含义 |
| --- | --- |
| `_count` | counter 计数 |
| `_duration` | timer 耗时（秒） |
| `_value` | store 快照值（如 `runtime` 的 heartbeat/memory） |

## 一、爬虫侧配置

在项目 `setting.py` 或环境变量中配置 InfluxDB 连接（无认证时账号密码留空即可）：

```python
INFLUXDB_HOST = "106.54.14.129"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = "crawler"
INFLUXDB_USER = ""
INFLUXDB_PASSWORD = ""
# 可选：自定义 measurement 名称，需与 Grafana 看板的 measurement 变量一致
INFLUXDB_MEASUREMENT = "crawler"
# 可选：站点名，默认取项目目录名
PROJECT_NAME = "taobao"
```

对应的环境变量为 `INFLUXDB_HOST` / `INFLUXDB_PORT` / `INFLUXDB_DB` / `INFLUXDB_USER` / `INFLUXDB_PASSWORD` / `INFLUXDB_MEASUREMENT` / `PROJECT_NAME`。

爬虫正常运行即会自动打点，数据库与保留策略（默认 180 天）会在首次运行时自动创建。

## 二、Grafana 配置数据源

1. Grafana 进入 `Connections` -> `Data sources` -> `Add data source` -> `InfluxDB`。
2. `Query Language` 选择 `InfluxQL`。
3. `HTTP URL` 填 `http://106.54.14.129:8086`。
4. `InfluxDB Details` 中 `Database` 填 `crawler`，无认证时 `User`/`Password` 留空。
5. 保存并测试连接。

## 三、导入看板

1. Grafana 进入 `Dashboards` -> `New` -> `Import`。
2. 上传 `dashboards/feapder-crawler.json`。
3. 在 `DS_INFLUXDB` 处选择上一步创建的数据源，完成导入。

导入后顶部有 站点(`project`) -> 爬虫(`spider`) -> 时间 的级联筛选，默认时间范围为过去 30 分钟；若 `INFLUXDB_MEASUREMENT` 改过名，需同步修改看板顶部 `measurement` 变量。

## 看板面板

- 请求下载监控：请求总量、下载成功、下载异常、解析异常、下载成功率、**请求下载趋势**（固定 5 分钟滚动窗口比率）。
- 运行状态监控：按 `name` / `host` / `pid` / `status` 展示实例在线状态、内存占用。
- 请求监控（状态码/耗时）：HTTP 状态码分布、2xx 成功率、状态码趋势（固定 5 分钟窗口占比）、请求响应耗时 P95。

### 请求下载趋势（滚动窗口比率）

「请求下载趋势」折线图中每个点的含义：以该点横坐标时间为终点、**固定 5 分钟窗口**内的滚动比率。某点 T 的下载成功率 = `[T-5m, T]` 内 `download_success` 之和 / 同窗口内 `download_total` 之和；解析异常率、下载异常率同理。顶部时间筛选只决定趋势图展示的时间段。

实现要点：

- 查询使用顶部时间筛选作为展示范围。
- InfluxQL 内层按 1 分钟分桶求和，外层使用 `moving_average(..., 5)` 计算固定 5 分钟滚动聚合。
- **图例 Last / 最右端点** 表示最新 5 分钟窗口值，不等同于整个顶部筛选范围的汇总比率。

### 状态码趋势（5 分钟窗口占比）

「状态码趋势」按 5 分钟窗口展示 2xx / 3xx / 4xx / 5xx 在请求总量中的占比。实现上先用 InfluxQL 按 1 分钟分桶求和，再通过 `moving_average(..., 5)` 生成固定 5 分钟窗口值，最后计算各状态码占比。

## 自定义打点

业务代码可直接打点，自动带上当前 `project` 与 `spider` tag：

```python
from feapder.utils import metrics

metrics.emit_counter("success", count=1, classify="自定义指标")
```

`classify` 对应看板分组，`key` 对应折线名，`count` 对应点数。
