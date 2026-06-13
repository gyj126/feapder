# feapder Grafana 监控

feapder 通过 InfluxDB 1.x 上报监控指标，Grafana 负责可视化展示。

## 前置条件

1. 已部署 InfluxDB 1.x（HTTP 端口默认 8086）
2. 已部署 Grafana，并具备导入仪表盘权限
3. 在爬虫项目 `setting.py` 中配置 InfluxDB 连接信息（参考项目模板）

## InfluxDB 准备

```sql
CREATE DATABASE feapder;
CREATE USER feapder WITH PASSWORD 'your-password' WITH ALL PRIVILEGES;
```

若爬虫进程使用的账号无建库权限，需由管理员预先创建 database 与 retention policy。feapder 首次启动时会尝试自动建库并创建 `{database}_180d` 保留策略。

## Grafana 数据源

1. 进入 **Connections → Data sources → Add data source**
2. 选择 **InfluxDB**
3. 配置项：
   - Query Language: **InfluxQL**
   - URL: `http://<influxdb-host>:8086`
   - Database: `feapder`（与 `INFLUXDB_DATABASE` 一致）
   - User / Password: 与 `setting.py` 一致
4. 点击 **Save & test**

## 导入仪表盘

1. 进入 **Dashboards → New → Import**
2. 上传 [`dashboards/feapder-overview.json`](dashboards/feapder-overview.json)
3. 选择刚创建的 InfluxDB 数据源
4. 导入后在顶部变量中选择爬虫 measurement（即爬虫类名，如 `RedditSpider`）

## 面板说明

| 面板 | `_classify` | 说明 |
|------|-------------|------|
| 请求监控 | `document` | 各 parser 下载总量/成功/异常 |
| 数据监控 | 表名 | 入库字段非空情况与 `total count` |
| 代理池 | `proxy` | total / used_times / invalid |
| 账号池 | `users_*` | 按账号状态分布 |

## 数据模型

- **measurement**: 默认等于爬虫类名（`__class__.__name__`）
- **tags**: `_classify`（分类）、`_key`（指标名）、`_type`（counter/store/timer）
- **fields**: `_count`（counter 聚合值）

## 验证

爬虫运行后，在 InfluxDB 中执行：

```sql
SHOW MEASUREMENTS;
SELECT * FROM "YourSpider" LIMIT 5;
```

若无数据，检查日志是否出现「监控打点未启用，缺少配置」或「监控打点已启用」。
