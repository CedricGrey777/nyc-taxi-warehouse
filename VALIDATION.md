# 验证与历史实测记录

## 2026-11-02 治理补强验收（当前）

本轮补齐日／上车地区粒度对账、距离指标校验、独立实验留档和只读运营报告。数据模型及固定 Q1 数据范围保持不变。

| 核验项 | 当前结果 | 原始证据 |
|---|---|---|
| 新增能力聚焦测试 | **22 passed in 1.90s**：分组分配错误、缺失／多余分组、距离容差和空／非有限值、实验报告独立留档、SQL 加权指标及只读报告输出 | [focused-tests.txt](evidence/2026-11-02-governance/focused-tests.txt) |
| 完整测试，含真实 Spark | **31 passed in 13.89s** | [tests.txt](evidence/2026-11-02-governance/tests.txt) |
| 真实 Q1 分组对账 | 1、2、3 月均通过新增的日／地区记录数、金额和距离校验；发布 run ID 不变，仍为 9,436,037 条接纳记录、259,585,035.59 美元记录收费总额 | [warehouse-reconciliation.json](evidence/2026-11-02-governance/warehouse-reconciliation.json) |
| 只读运营报告 | 91 个日期的日趋势、上车地区 Top 10、工作日／周末指标已从真实已发布汇总导出 | [Markdown 报告](evidence/2026-11-02-governance/business-report.md) · [JSON 结果](evidence/2026-11-02-governance/business-report.json) |

记录数和金额按日／地区精确核对，缺失或多余分组阻止通过。距离采用 `abs(actual - expected) <= 1e-8 + 1e-9 * max(abs(actual), abs(expected))`，同时拒绝事实或汇总中的空值及非有限距离；保留月总对账与维度引用检查。

每次性能实验现在生成独立 `experiment_id`，文件名为 `benchmarks/<month>-<run_id>-<experiment_id>.json`，保留 `run_id` 关联发布。聚焦测试验证同一发布重复保存不会覆盖先前报告。**本轮未重跑真实数据性能实验**，下方 2026-09-09 的 Join 数据及原始证据继续保留。

### 当前复现命令

在项目目录使用 PowerShell 7：

```powershell
docker compose build cli
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q tests/test_warehouse.py tests/test_benchmark.py tests/test_reports.py
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q
foreach ($month in @('2024-01', '2024-02', '2024-03')) {
  docker compose run --rm --no-deps cli check --month $month --data-root /opt/airflow/data
}
docker compose run --rm --no-deps --entrypoint python cli tools/report.py --data-root /opt/airflow/data --format markdown
```

`check` 和报告命令读取已有真实 Q1 发布；新环境需先按 README 处理对应月份。报告的三份查询为 [日趋势](sql/daily_trends.sql)、[地区 Top 10](sql/top_pickup_zones.sql)、[工作日／周末](sql/weekday_weekend.sql)，各距离均值按总距离除以总记录量计算，工作日与周末日均记录量按各自有记录的日期数归一。

## 2026-11-02 首次发布复核（历史版本）

本节保留治理补强前的 9 项测试及首次发布证据；当前源码的 31 项验收结果见上节。

本轮对 Docker 卷 `nyc-taxi-warehouse_taxi-data` 中已有的 Q1 发布执行只读复核，并在独立临时目录重新运行合成数据测试和 CLI。环境沿用 Python 3.11、Spark 3.5.9、Java 17、DuckDB 1.4.5、Airflow 3.3.1。

| 核验项 | 本轮结果 | 原始证据 |
|---|---|---|
| Q1 已发布事实与指标 | 三个月 `check` 均通过；原始 9,554,778 条、接纳 9,436,037 条、隔离／业务排除 118,741 条，接纳记录收费总额 259,585,035.59 美元，与下方历史记录一致 | [warehouse-verification.json](evidence/2026-11-02/warehouse-verification.json) 的 `months` / `checks` |
| 聚焦合同测试 | **5 passed in 0.02s** | [contracts.txt](evidence/2026-11-02/contracts.txt) |
| 完整测试，含真实 Spark | **9 passed in 13.20s** | [tests.txt](evidence/2026-11-02/tests.txt) |
| 合成数据 CLI | `fixtures → run-month → check` 成功；9 条输入＝3 条接纳＋6 条隔离，接纳金额 **25.00 美元** | [cli-demo.txt](evidence/2026-11-02/cli-demo.txt) |
| 镜像依赖 | `python -m pip check` 返回 `No broken requirements found.` | [cli-demo.txt](evidence/2026-11-02/cli-demo.txt) |
| 历史 Join 实验归档 | 从原卷导出 2026-09-09 的实验结果，保留每次耗时、物理计划和算子指标；**本轮未重跑性能实验** | [warehouse-verification.json](evidence/2026-11-02/warehouse-verification.json) 的 `historical_benchmark` |

JSON 只包含月份级质量汇总、来源摘要、发布标识、复核结果以及历史实验的物理计划与算子指标，不含出租车行级明细。本轮复核读取的是已有发布，不代表重新下载并处理整个 Q1；下方 Airflow 调度顺序与性能测量仍属于 2026-09-09 的历史运行。

### 首次发布复核的命令入口

在项目目录以 PowerShell 7 执行；使用已构建的项目镜像，测试和 CLI 均不启动调度服务。下列命令保留原入口，完整测试数量以执行的源码版本为准：

```powershell
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q tests/test_contracts.py
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q
docker compose run --rm --no-deps --entrypoint bash cli -euc '
  python -m pip check
  python -m taxi_warehouse fixtures --output /tmp/taxi-verification/fixtures
  python -m taxi_warehouse run-month --month 2024-01 --data-root /tmp/taxi-verification/warehouse --source /tmp/taxi-verification/fixtures/yellow_tripdata_2024-01.parquet --zones /tmp/taxi-verification/fixtures/zones.csv
  python -m taxi_warehouse check --month 2024-01 --data-root /tmp/taxi-verification/warehouse
'
```

以下命令只适用于已有真实 Q1 发布的数据卷，逐月读取事实与指标进行对账：

```powershell
foreach ($month in @('2024-01', '2024-02', '2024-03')) {
  docker compose run --rm --no-deps cli check --month $month --data-root /opt/airflow/data
}
```

## 2026-09-09 历史实测

以下正文保留 2026-09-09 的实际运行结果。原始数据和完整运行账本保留在 Docker 卷 `nyc-taxi-warehouse_taxi-data`；不提交出租车明细到 Git。

### 功能验收

- Python 3.11、Spark 3.5.9、Java 17、DuckDB 1.4.5、Airflow 3.3.1。
- 聚焦质量测试通过；最终完整测试 **9 passed in 13.06s**，使用真实 Spark。包含提交成功后丢响应、事务回滚、重复源记录保留、未知地区、闰日与跨月补跑。
- Airflow DAG 无导入错误，四阶段实际执行成功。按 **1 月 → 3 月 → 2 月** 运行官方公开文件；3 月和 2 月由项目自己的 Airflow REST 补跑客户端顺序触发。
- 补 2 月前后，1 月与 3 月的业务汇总及发布运行 ID 均不变。
- 自定义 Airflow 镜像执行 `python -m pip check`，无依赖冲突。

| 来源月 | 原始记录 | 接纳记录 | 隔离／业务排除 | 接纳记录收费总额（美元） |
|---|---:|---:|---:|---:|
| 2024-01 | 2,964,624 | 2,928,237 | 36,387 | 80,307,277.95 |
| 2024-02 | 3,007,526 | 2,970,630 | 36,896 | 80,972,470.73 |
| 2024-03 | 3,582,628 | 3,537,170 | 45,458 | 98,305,286.91 |
| 合计 | 9,554,778 | 9,436,037 | 118,741 | 259,585,035.59 |

三个来源月均通过行数、可解析金额和事实—指标对账。这里统计的是公开源记录，不是唯一真实行程、乘客数或公司净收入。隔离集合包含分析范围排除项，不能全部称为错误数据。

### Join 实验

仅比较固定 1 月已接纳的 **2,928,237 条记录**，不包含读取原始下载文件、清洗或发布成本。

- 独立容器限制 2 CPU、6 GiB；Spark `local[2]`、driver heap 3 GiB、4 个 shuffle 分区。
- 关闭 AQE 与自动广播，显式选择 BroadcastHashJoin / SortMergeJoin；实际物理计划核验通过。
- 各预热一次后交错测量 5 次，不缓存 DataFrame；属于热 JVM / OS 缓存条件。
- 两种策略输出的 **1,301 个业务分组完全一致**。

| 策略 | 5 次耗时中位数 |
|---|---:|
| Sort-Merge Join | 1.649809 秒 |
| Broadcast Hash Join | 0.451515 秒 |

完整 SQL operator metrics 和每次耗时位于卷内 `benchmarks/2024-01-df6324a68b5743e3b7f67bb59e7e8a0e.json`，2026-11-02 已将该文件内容导出到 [公开证据](evidence/2026-11-02/warehouse-verification.json) 的 `historical_benchmark`。最后一次观测中，广播策略的聚合 Exchange 写出 47,469 字节；Merge 计划还包含两处大表 Join Exchange，分别写出 23,531,197 与 31,293,282 字节。不能把单个节点计数当成整个系统网络流量。

本次整个实验 JVM 的 Linux `VmHWM` 为 **1,759,168 KiB（约 1.68 GiB）**。它是该 JVM 的峰值 RSS，不是整机或全部服务的峰值，也不等于 Java heap 上限。

结果仅支持这个单机、固定数据和配置下的比较，不外推到分布式生产环境。

### 历史全链路与性能复现

在项目目录使用 Docker：

```powershell
docker compose build scheduler
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q tests/test_contracts.py
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q
docker compose run --rm --no-deps cli check --month 2024-01 --data-root /opt/airflow/data
docker compose run --rm --no-deps cli benchmark --month 2024-01 --data-root /opt/airflow/data --repetitions 5
```

重新执行公开数据调度时按 README 启动服务。性能实验前停止调度服务，避免有另一 Spark 作业参与竞争；`docker compose stop` 保留数据，不要使用删除卷的命令。
