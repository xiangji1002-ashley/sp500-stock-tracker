# S&P 500 Stock Project — Interview Prep & Trade-offs

## 面试官可能问的问题 + 参考答案

> 所有答案都是口语化的，模拟面试现场回答。展示思考过程，承认 trade-off，不回避缺点。

---

## 一、架构设计类

### Q1: 为什么选 Lambda Architecture 而不是 Kappa Architecture？

"一开始我也犹豫过。Kappa 的优势是只维护一套流处理代码，逻辑更简单。但我看了一下我的数据源，发现不太合适。

我的历史数据是从 yfinance 下载的 CSV 文件，这东西天然就是批量的，不是一条一条的事件流。如果用 Kappa，我得先把 CSV 灌到 Kafka 里模拟成流，这就有点 over-engineering 了。

另外我的实时数据源是 Alpaca 的 REST API，也不是事件驱动的，是我主动去轮询的。所以批处理用批处理的方式，实时用微批量轮询，各走各的路，反而更清晰。

当然 Lambda 的缺点是要维护两套代码——一个 batch DAG 和一个 speed DAG。但我通过让两条链路**共享同一个 Gold 层**来降低复杂度，Superset 不需要关心数据是从哪条链路来的。"

---

### Q2: 为什么用微批量而不是 Kafka + Flink 做实时？

"这个我认真想过。Kafka + Flink 能做到秒级延迟，很酷，但对我这个场景来说是杀鸡用牛刀。

首先，我的数据源是 **REST API**，不是事件流。Alpaca 没有给我推数据，是我每 5 分钟去拉一次。就算我用了 Kafka，上游还是轮询，瓶颈不在 Kafka。

其次，我的下游是 **Superset dashboard**，30 秒刷新一次。就算我做到 1 秒延迟，用户看到的还是 30 秒前的数据。所以 5 分钟的微批量延迟完全够用。

第三是**成本**。一个 Glue Job 跑 7 个小时大概花几美元。如果上 Kafka + Flink，光集群就要几十美元一天，还得有人运维。

当然，如果将来要做交易信号触发——比如某只股票跌了 5% 就自动告警——那 5 分钟就太慢了，我会引入 Kafka。但目前的需求不需要。"

---

### Q3: 为什么 Batch 和 Speed 共享同一个 Gold 层（marts）？

"这其实是 Lambda Architecture 的标准做法。两条链路最终都往 marts schema 写数据，Superset 只查 marts 层，不需要知道数据是从哪条链路来的。

具体来说，Batch 写 cumulative、dim_daily 这些历史聚合表，Speed 写 current_day 这张实时 serving 表。它们虽然在同一个 schema 下，但是不同的表，不会冲突。

好处是**下游简单**——dashboard 开发者只需要知道去 marts 下面找表就行，不用管上游的复杂性。"

---

### Q4: 为什么 Speed Layer 跳过了 Bronze 层？

"因为 Speed Layer 的核心目标是**快**，不是**全**。

Batch 链路之所以要经过 Bronze（raw 层），是为了保留原始数据，方便出问题的时候从 raw 重跑。但 Speed 链路的数据是**临时的**——每天开盘前我会清空 current_day_stock_price，重新从 Batch 链路的 fact 表初始化。所以昨天的实时数据根本不需要保留。

如果我把每次 API 拉回来的数据都存到 raw 层，500 只股票每 5 分钟一次，一天就是几万条记录，但这些记录第二天就没用了。不值得。

当然，如果是金融监管要求必须保留每笔实时数据的审计记录，那我会加一个 Kafka topic 做数据回放，但不会写到 Iceberg raw 表里——Kafka 更适合做这种 append-only 的日志。"

---

### Q5: 如果要支持秒级延迟，你会怎么改架构？

"改动还挺大的。首先数据源要从 REST 轮询换成 **WebSocket 订阅**，Alpaca 付费版支持 WebSocket 推送。然后中间加一个 **Kafka** 做消息缓冲——这样即使下游处理不过来，数据也不会丢。

计算引擎要从 Glue 的 while 循环换成 **Flink**，因为 Flink 天然就是处理流数据的，窗口聚合、事件触发什么的都是内置功能。

存储层也要换——Iceberg 的 MERGE INTO 虽然支持 ACID，但每次 commit 都要写新的 metadata 文件，秒级频率会有性能问题。可以换成 **Redis** 做实时查询，或者 **Apache Druid** 做实时 OLAP。

最后 Superset 的 30 秒轮询也要换成 WebSocket 推送，或者直接用 Grafana。

总结就是——整个技术栈都要换一遍。所以我说微批量是当前需求下**性价比最高**的方案。"

---

## 二、技术选型类

### Q6: 为什么用 Iceberg 而不是 Delta Lake 或 Hudi？

"三个都能用，核心功能差不多。我选 Iceberg 主要是因为**引擎兼容性**。

我的查询引擎是 Starburst Galaxy（Trino），Iceberg 是 Trino **原生支持最好**的格式。Delta Lake 虽然 Databricks 用得多，但在 Trino 上的支持不如 Iceberg 成熟——比如 MERGE INTO 在 Trino + Delta 上有些限制。

另外 Iceberg 是 Apache 基金会的项目，不被任何一家公司主导。Delta Lake 是 Databricks 的，Hudi 是 Uber/AWS 的。从开放性来说 Iceberg 更中立。

还有一个实际原因——AWS Glue 内置了 Iceberg 支持，加一个 `--datalake-formats=iceberg` 参数就能用。Delta Lake 在 Glue 上需要额外配置。

当然，如果团队已经在用 Databricks 生态，Delta Lake 是更自然的选择。技术选型不是说哪个绝对好，而是看你的**上下游生态**。"

---

### Q7: 为什么用 Trino (Starburst) 而不是 Athena 或 Spark SQL？

"Athena 其实我考虑过，它也是基于 Trino 的。但有两个问题：第一，Athena 按扫描的数据量收费，我在开发阶段要跑很多查询来调试，费用不可控。Starburst 免费版每个月有足够的额度。

第二，我用 Starburst Galaxy 是因为它有一个很好用的 **Query Editor**，可以直接在网页上写 SQL、看结果、管理 catalog，开发体验比 Athena 好很多。

至于 Spark SQL——Glue 的 Spark 冷启动要 2-3 分钟。我的 staging → fact 转换就是一个简单的列重命名 + INSERT，用 Trino 秒级搞定。为了一个 10 秒钟能完成的 SQL 去启动一个 Spark 集群，不划算。

所以我的原则是：**能用 SQL 做的就用 Trino，只有读 CSV 和 MERGE INTO 这种 Trino 做不了的事情才用 Spark**。"

---

### Q8: 为什么 Glue + Trino 分工，而不是全用 Spark？

"因为 Spark 太重了。每次启动一个 Glue Job，光冷启动就要 2-3 分钟，而且按运行时间收费。

我的 DAG 有 6 个 task，其中 4 个是纯 SQL 操作——列重命名、ARRAY_AGG、AVG、COUNT 这些。如果全用 Spark，我得启动 4 个 Glue Job，每个冷启动 2-3 分钟，就白白等了 8-12 分钟。但用 Trino，4 个 SQL 加起来可能 10 秒就跑完了。

所以我把 Spark 只用在**Trino 做不了的事情**上：读 CSV 文件写入 Iceberg（第一个 task）和微批量 MERGE（实时链路）。其他的全交给 Trino。

这也省了钱——Glue 按分钟收费，Trino 免费版够用。"

---

### Q9: 为什么不用 Snowflake？

"其实我一开始也考虑过 Snowflake，因为它确实很方便，建个表写个 SQL 就完事了。但后来想了想，觉得有几个问题：

第一，**Snowflake 是封闭的生态**。数据存在它自己的内部存储里，不是开放格式。如果哪天我想换引擎，比如用 Spark 跑个 ML 任务，数据还得导出来。但我用 S3 + Iceberg，数据就在 S3 上，Spark 能读、Trino 能读、Flink 也能读，不绑定任何一个引擎。

第二，**成本模型不透明**。Snowflake 按 credit 收费，一个 warehouse 跑着就在烧钱，哪怕你只是跑个简单 SQL。我现在用 Starburst 免费版做 SQL，Glue 按分钟收费只在需要 Spark 的时候才花钱，其他时候成本是零。

第三，说实话，**用 Snowflake 做这个项目面试的时候没什么好讲的**。面试官问你为什么选 Snowflake，你只能说'因为方便'。但用 S3 + Iceberg + Trino，我可以聊存储格式的 trade-off、存算分离的好处、为什么 Glue 和 Trino 要分工——这些才是数据工程师该懂的东西。

当然，如果是公司生产环境，团队已经在用 Snowflake，我肯定会用 Snowflake，没必要自己搭一套。但作为一个学习项目，我更想理解底层是怎么运作的。"

---

### Q10: 为什么用 yfinance 而不是付费 API（Polygon/Bloomberg）？

"很简单，**够用就行**。

我只需要 S&P 500 的日线数据——开盘、最高、最低、收盘、成交量。yfinance 完全能满足，而且免费。Polygon 能给你分钟线、Tick 数据，但我不需要那么细的粒度。Bloomberg 就更别说了，两千美元一个月，完全超出 capstone 项目的预算。

当然 yfinance 有缺点——数据延迟一天，不提供盘中数据。所以实时报价我用了 Alpaca Markets API，它的免费版支持 IEX 数据源的实时报价，虽然不是 SIP（全市场）数据，但对 dashboard 监控来说够用了。

如果是生产环境做量化交易，那肯定得用 Polygon 或者 Bloomberg，因为数据质量和实时性是关键。但对于学习项目，没必要花那个钱。"

---

## 三、数据建模类

### Q11: 为什么用 Medallion Architecture（raw/stg/marts）？每层的职责是什么？

"Medallion 分三层主要是为了**可恢复性**。

举个例子：假设我的 staging → fact 的转换逻辑有 bug，算出来的 close_price 是错的。如果只有一层，我得重新下载 CSV、重新上传 S3、重新跑 Glue Job——整个链路全部重来。但有了分层，我只需要从 raw 层重跑 staging → fact 的 SQL 就行，raw 层的数据是完好的。

具体来说：
- **raw（Bronze）** 就是 CSV 原样存着，不做任何转换。这是我的'安全网'。
- **stg（Silver）** 做清洗——列重命名（open → open_price）、类型转换（string → double）。这一层的数据是干净的、标准化的。
- **marts（Gold）** 做业务聚合——ARRAY_AGG 生成价格数组、计算 historic_low/high。这一层是给 dashboard 直接查的。

面试的时候有人可能会问'你为什么不把 raw 和 stg 合成一层？'——因为 raw 层我是用 Glue（Spark）写的 CTAS，stg 层是用 Trino SQL 转的。如果合在一起，要么全用 Spark（浪费），要么全用 Trino（Trino 不能读 CSV）。分开正好各司其职。"

---

### Q12: current_day_stock_price 为什么只有 8 列？指标在哪算的？

"这个表最开始我是设计成 22 列的，把所有指标都预算好存进去——什么日涨跌幅、90 天均价变化、365 天均价变化，全都在 Glue Job 里算完再 MERGE 进表。

但后来开发的时候发现一个问题：**我老是要改指标公式**。比如一开始用 90 天窗口，后来觉得 60 天更好，又想加一个成交量的指标。每改一次公式，我就得改 Glue Job 的代码、重新部署、重跑——这个周期太长了，改一个公式要 10 分钟才能看到效果。

所以我就换了个思路：**表里只存原始的 8 个字段**（ticker、日期、最新价、OHLCV、更新时间），所有的衍生指标都放到 Superset 的 Virtual Dataset 里用 SQL 算。这样改公式就是改一条 SQL 的事情，秒级生效。

当然这个方案也有缺点——每次 dashboard 刷新都要 JOIN fact 表重新算指标，查询会慢一点。但我们只有 500 只股票，JOIN 的开销几乎可以忽略，所以这个 trade-off 是划算的。

如果数据量大到几万只股票，那我可能会改回预算方案，或者用物化视图（Materialized View）来缓存计算结果。"

---

### Q13: cumulative_stock_price 里的 ARRAY_AGG 有什么用？

"这个想法其实来自课程里的 'big array' 模式。简单说就是把每个 ticker 的所有历史收盘价**聚合成一个数组**存起来。

为什么要这样做？因为如果我想在 dashboard 上给每只股票画一个迷你走势图（sparkline），直接查 fact 表的话，500 只股票每只查一次，每次 GROUP BY + ORDER BY，查询量很大。但如果我预先 ARRAY_AGG 好了，一次查询把 500 行拿回来，前端直接用数组画图，快很多。

当然缺点是每天要**全量重建**这张表——DELETE + INSERT 全部 500 行。但 500 行的 ARRAY_AGG 在 Trino 上也就几秒钟的事情，完全可以接受。

如果数据量大到几十万只股票，全量重建就不现实了，可能要改成增量更新——每天只 append 当天的 close_price 到数组末尾。但 500 只的规模下，全量重建更简单可靠。"

---

### Q14: fact 表和 dim 表的区别是什么？

"最简单的理解：**fact 表是明细，dim 表是汇总**。

fact_daily_stock_price 有 15 万多行，每天每只股票一行，记录的是每天的 OHLCV 原始数据。你要做时间序列分析、画折线图、按日期筛选，就查 fact 表。

dim_daily_stock_price 只有 500 行，每只股票一行，存的是聚合后的指标——最新价、历史最低、历史最高。你要做概览、做排名、做 KPI 卡片，就查 dim 表。

还有一个 dim_ticker_details，存的是公司信息——名字、行业、市值。这些信息不随时间变化（或者变化很慢），所以是维度表。

在 dashboard 里，我经常需要 fact JOIN dim_ticker_details 来给数据加上公司名和行业标签。这就是星型模型（star schema）的经典用法。"

---

## 四、数据质量类

### Q15: 你做了哪些 DQ 检查？如果失败了怎么办？

"目前做了比较基础的检查：ticker、close_price、trade_date 这三列的空值检查，以及 fact 表和 staging 表的行数是否一致、dim 表的行数是否和 distinct ticker 数一致。

说实话这个 DQ 做得不够完善。现在只是 print 结果，没有真正 fail DAG。如果我在生产环境做，会改几个地方：

第一，DQ 失败要 **raise Exception** 终止 DAG，不能只打印日志就过了。
第二，要加 **Airflow 告警**——邮件或者 Slack 通知 on-call。
第三，可能会引入 **Great Expectations** 或者 **dbt test** 做更系统化的 DQ，比如检查数值范围（close_price 不能为负）、检查数据延迟（今天跑的数据是不是昨天的）。

这是一个我知道需要改进的地方，但 capstone 项目时间有限，先做了最基础的。"

---

### Q16: staging → fact 的 INSERT 是幂等的吗？

"是的。我用的是**先删后插**模式：

```sql
DELETE FROM fact WHERE trade_date = DATE('{ds}');
INSERT INTO fact SELECT ... FROM staging WHERE snapshot_date = DATE('{ds}');
```

先把当天的数据删掉，再重新插入。这样无论 DAG 跑一次还是跑十次，结果都是一样的。

为什么不直接 INSERT？因为如果 DAG 失败后重跑，同一天的数据会插入两次，造成重复。先删后插就避免了这个问题。

其实更优雅的做法是用 MERGE INTO（upsert），但 Trino 的 MERGE INTO 对 Iceberg 的支持在某些版本有限制，所以我选了更稳妥的 DELETE + INSERT。"

---

### Q17: 如果 yfinance 某天没数据（非交易日），DAG 怎么处理？

"download_daily_data task 会检查 yfinance 返回的 DataFrame 是否为空。如果是非交易日（周末、假日），yfinance 不返回数据，DataFrame 是空的，我就直接 return 跳过上传。

但说实话现在的处理有点粗糙——后面的 task（load_staging、staging_to_fact 这些）还是会跑，只是没有新数据可以处理。更好的做法是用 Airflow 的 **BranchPythonOperator**，在 download 之后判断是否是交易日，如果不是就直接跳到 DAG 末尾。

另外一个改进方向是加一个**交易日日历**，在 DAG 调度层面就排除掉非交易日，而不是跑了之后再判断。Python 有个库叫 `exchange_calendars` 可以做这个。"

---

## 五、实时链路类

### Q18: MERGE INTO 是怎么工作的？为什么用 MERGE 不用 INSERT？

"current_day_stock_price 这张表每个 ticker 只有一行，我需要每 5 分钟**更新**这行的价格，而不是**新增**一行。

如果用 INSERT，每 5 分钟加 500 行，一天下来就有 5 万行——全是重复的 ticker，只是价格不一样。dashboard 查的时候还得 GROUP BY 取最新的，很浪费。

MERGE INTO 就是 upsert：ticker 已存在就 UPDATE 价格，不存在就 INSERT。这样 500 只股票永远只有 500 行，dashboard 直接 SELECT 就行。

Iceberg 支持 MERGE INTO 的 ACID 事务，所以即使 Superset 在查这张表的同时 Glue 在 MERGE，也不会读到脏数据。这就是我选 Iceberg 的一个重要原因——如果用普通 Parquet 文件，并发读写可能会出问题。"

---

### Q19: 如果 Alpaca API 挂了，微批量会怎样？

"当前的处理比较简单——API 返回非 200 状态码的时候，我 print 一条错误日志然后 continue 跳到下一轮。Job 不会挂掉，5 分钟后会重新尝试。

但这个处理有几个问题我知道需要改：

第一，没有**指数退避**。如果 API 持续不可用，每 5 分钟重试一次其实是在浪费资源。应该第一次等 5 分钟，第二次等 10 分钟，第三次等 20 分钟。

第二，没有**告警**。API 挂了 1 小时都没人知道。应该加 CloudWatch 告警或者 Airflow callback，连续 N 次失败就通知 on-call。

第三，没有**断路器（circuit breaker）**。如果 API 连续失败超过某个阈值，应该主动终止 Job，而不是一直空跑。

这些在生产环境是必须做的，capstone 项目里我先做了最基础的容错。"

---

### Q20: 为什么每天要初始化 current_day_stock_price？

"因为这张表存的不只是'当前价格'，还有'当前价格相对于基准的变化'。比如日涨跌幅是 current_price 减去 close_price_last_day，90 天变化是 current_price 减去 avg_90d。

这些基准值——昨天的收盘价、90 天均价、365 天均价——每天都不一样。昨天的基准今天就过时了。

所以每天开盘前，我要把表清空，从 fact 表重新算出最新的基准值填进去。然后微批量跑起来后，每 5 分钟用 Alpaca 的实时价格减去这些基准，算出实时涨跌幅。

如果不初始化，微批量还在用昨天的基准算，比如昨天 AAPL 收盘 185，今天收盘 190，但如果基准还是前天的 180，算出来的涨跌幅就是错的。

当然在我们改成 8 列方案后，初始化变得更简单了——只需要 INSERT ticker、trade_date 这些基础字段，指标在 Superset 里算。"

---

## 六、运维/部署类

### Q21: DAG 跑失败了怎么排查？

"我的排查流程一般是这样的：

第一步，打开 Astronomer 的 DAG 页面，看哪个 task 变红了。这就能定位到是哪个环节出的问题——是下载数据失败了，还是 Glue Job 挂了，还是 Trino SQL 报错了。

第二步，点进那个 task 看 log。如果是 Trino SQL 的 task，日志里会有具体的 SQL 错误信息。如果是 Glue Job 的 task，日志里会有 Glue Run ID，我拿这个 ID 去 AWS Glue Console 看 CloudWatch 的详细日志。

第三步，修复问题后，在 Airflow UI 里 **Clear** 那个失败的 task。Airflow 会从失败的那个 task 开始重跑，前面成功的 task 不会重跑。这就是为什么 DAG 设计成多个小 task 而不是一个大 task 的好处——失败了只需要重跑一小段。

最常见的失败原因是 Starburst 的 free cluster 休眠了，第一次查询超时。加了 retries=1 之后基本就解决了。"

---

### Q22: Glue Job 的冷启动时间是多少？对实时性有影响吗？

"Glue Job 的冷启动大概 2-3 分钟。对批处理链路没什么影响——每天跑一次，不在乎这几分钟。

但对实时链路有点影响。微批量的 Glue Job 在 9:25 AM 启动，加上冷启动，大概 9:28 AM 才能开始第一次轮询。美股 9:30 开盘，所以其实刚好赶上。这也是我把 DAG 调度设在 9:25 而不是 9:30 的原因——提前 5 分钟启动，等 Glue 冷启动完刚好开盘。

如果对冷启动很敏感，可以用 Glue 的 **warm pool** 功能——预留一些 worker 保持热状态。但这要额外花钱，capstone 项目里不值得。"

---

### Q23: Starburst Galaxy 的 free cluster 会自动休眠，你怎么处理？

"这个确实踩过坑。Free cluster 5 分钟不活动就休眠，第一次查询要等 30-60 秒唤醒。有时候 DAG 的 Trino SQL task 会因为等太久而超时。

我的处理方式是给 task 加了 retries=1——第一次超时失败，Airflow 自动重试，这时候 cluster 已经醒了，第二次就能成功。

更好的做法是在 DAG 开头加一个 'warmup' task，先发一个 SELECT 1 把 cluster 唤醒，等它 ready 了再跑后面的真正 SQL。但这有点 hacky。

生产环境的话，用付费版 cluster，auto-suspend 时间设长一点，或者干脆不 suspend，问题就解决了。"

---

## 七、扩展性类

### Q24: 从 500 只扩展到 12,000 只，需要改什么？

"主要改这几个地方：

**数据下载**：yfinance 一次能下 500 只没问题，12000 只可能要分批——比如每批 500 只，分 24 批下载。需要加并发控制和 rate limiting。

**Glue Job**：Worker 从 2 个增加到 5-10 个。12000 只股票的 CSV 大概有几百 MB，Spark 处理起来不会太慢，但需要更多内存。

**Alpaca API 轮询**：这个是最大的瓶颈。Alpaca 免费版有 200 请求/分钟的限制。12000 只股票分 24 批，每批一个请求，一轮就要 24 个请求。5 分钟轮询一次的话绰绰有余，但如果想更频繁就可能触发限流。

**Iceberg 表**：需要加分区。现在 500 行没分区无所谓，12000 行的 MERGE INTO 可能要按 trade_date 分区来提升性能。

**Superset**：影响不大，dashboard 上还是只展示 Top 50，Row Limit 不变。

核心的架构和代码逻辑不需要大改，主要是**调参数和加分区**。"

---

### Q25: 如果要加一个新指标（比如 RSI），怎么加？

"这取决于 current_day_stock_price 的设计。

我们现在用的是 8 列方案——表里只存原始价格，指标在 Superset Virtual Dataset 里算。所以加 RSI 就是在 Superset 里改一条 SQL：

```sql
-- 在 Virtual Dataset 的 SQL 里加一列
, AVG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date ROWS 14 PRECEDING) AS rsi_avg_14d
```

改完 SQL 点保存，dashboard 立刻就能看到新指标，不需要动任何后端代码。

如果我们用的是 22 列方案（预算在表里），那就要：
1. ALTER TABLE 加列
2. 改 Glue Job 的计算逻辑
3. 改 MERGE SQL
4. 重新部署 Glue Job
5. 重跑一次

所以 8 列方案在指标迭代上**快得多**。这也是我从 22 列改成 8 列的主要原因。"

---

### Q26: 如果要支持多个国家的股市，怎么调整？

"架构不需要大改，主要是三个方面：

**数据源**：每个市场需要不同的 API。美股用 yfinance + Alpaca，日股可能用 jpx 的 API，A 股可能用 tushare。每个市场加一个下载脚本就行。

**调度时间**：不同市场的交易时间不一样——东京 9:00-15:00 JST，上海 9:30-15:00 CST，纽约 9:30-16:00 ET。每个市场一个 DAG，schedule 设成对应的本地时间。

**表隔离**：可以按市场分 schema——`marts.us_current_day`、`marts.jp_current_day`。或者加一个 `market` 列做筛选，但分 schema 更清晰。

Dashboard 上加一个 **Market 筛选器**让用户切换就行。

最大的挑战其实不是技术，而是**数据源的差异**——每个市场的 API 返回格式不同、字段名不同、交易规则不同（A 股有涨跌停、日股有午休）。需要在 Silver 层做好标准化。"

---

## 八、关于这个项目的 Why

> "我对股票市场一直很感兴趣，平时会关注标普 500 的走势。做这个项目的初衷是想把自己学到的数据工程技术应用到一个我真正感兴趣的领域。同时我想实践 Lambda Architecture —— 同时处理批量历史数据和近实时数据 —— 因为这是现代数据平台的核心模式。通过这个项目，我从零搭建了一个完整的数据湖管道，涵盖了数据采集、ETL、数据建模、编排、可视化的全流程。"
