# Feapder Grafana 看板

基于 InfluxDB 2.x（Flux 查询语言）的 feapder 框架监控看板。

## 一、目录结构

```
dashboards/grafana/
├── feapder.json   # Grafana 看板定义（Flux）
└── README.md
```

## 二、前置条件

- InfluxDB 2.x（>= 2.7）
- Grafana（>= 9.0）
- 已经在 InfluxDB 创建好 bucket（默认名为 `feapder`），并生成可写入该 bucket 的 token

## 三、最小化部署示例

### 1）启动 InfluxDB 2.x

```bash
docker run -d --name influxdb2 \
  -p 8086:8086 \
  -e DOCKER_INFLUXDB_INIT_MODE=setup \
  -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=adminpass \
  -e DOCKER_INFLUXDB_INIT_ORG=feapder \
  -e DOCKER_INFLUXDB_INIT_BUCKET=feapder \
  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=my-token \
  influxdb:2.7
```

启动后浏览器访问 `http://localhost:8086`，使用 `admin / adminpass` 登录。

### 2）启动 Grafana

```bash
docker run -d --name grafana -p 3000:3000 grafana/grafana:latest
```

浏览器访问 `http://localhost:3000`，默认账号 `admin / admin`。

### 3）配置 InfluxDB 数据源

在 Grafana 中 `Connections -> Data sources -> Add data source -> InfluxDB`，设置：

- Query Language: `Flux`
- URL: `http://host.docker.internal:8086`（或宿主机实际地址）
- Organization: `feapder`
- Token: `my-token`
- Default Bucket: `feapder`

保存并测试，确认 `Data source is working`。

## 四、导入看板

`Dashboards -> Import -> Upload JSON file`，选择 `dashboards/grafana/feapder.json`，在导入页选择上一步创建好的 InfluxDB 数据源即可。

## 五、看板变量

| 变量 | 类型 | 说明 |
| --- | --- | --- |
| `DS_INFLUXDB` | datasource | InfluxDB 数据源 |
| `bucket` | textbox | 写入的 bucket，默认 `feapder` |
| `spider` | query (multi) | 爬虫名，从 `feapder_download` 等 measurement 的 `spider` tag 读取 |

## 六、Schema 说明

| Measurement | Tags | Fields | 含义 |
| --- | --- | --- | --- |
| `feapder_download` | `spider`, `parser`, `status` | `count` | 下载/解析事件，`status` 取值见下方 |
| `feapder_item` | `spider`, `table`, `field` | `count` | 数据入库事件，`field=__total__` 表示总条数 |
| `feapder_proxy` | `spider`, `event` | `count` | 代理池事件，`event` ∈ `pull` / `use` / `invalid` |
| `feapder_user` | `spider`, `user_id`, `status` | `count` | 账号池事件，`status` 对应 `GoldUserStatus` |
| `feapder_custom` | `spider`, `metric`, 任意自定义 | `count` / `value` / `duration` | 用户自定义打点 |

`feapder_download` 的 `status` 取值：

| 值 | 含义 |
| --- | --- |
| `download_total` | 进入下载流程的请求数 |
| `download_success` | 下载并解析成功 |
| `download_exception` | 网络/HTTP 层错误 |
| `parser_exception` | 解析回调里抛异常 |

## 七、看板面板

- 概览：总请求数、下载成功、下载失败、解析异常、成功率、入库总量
- 下载监控：各状态请求速率、成功率趋势、失败 Top10（按 parser）
- 数据入库：各表入库速率、字段命中统计 Top50
- 代理池：代理事件速率、失效比例
- 账号池：账号状态速率、账号 × 状态 Top50
- 自定义指标：按 `metric × field` 的趋势

## 八、Flux 查询示例

```flux
from(bucket: "feapder")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "feapder_download" and r.spider == "my_spider")
  |> filter(fn: (r) => r._field == "count")
  |> aggregateWindow(every: v.windowPeriod, fn: sum, createEmpty: false)
  |> group(columns: ["status"])
```
