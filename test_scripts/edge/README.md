# binlog 边界形态判据

五类源端形态，改造前每一类都是**静默**出问题——数据没到目标库，任务却显示健康、位点照常前进。

## 拓扑

`dr-mysql-a(33320)` 作源、`dr-mysql-b(33321)` 作目标。三进程编排复用 XA 判据的
`test_scripts/xa/xalib.py`，本目录只加"任意建表 DDL"和"源端全局参数改了必复原"。

```bash
mvn -pl migration-capture,migration-extract,migration-increment,migration-full -am package -DskipTests
python3 test_scripts/edge/edge_e2e.py                 # 期望 15/15
python3 test_scripts/edge/edge_e2e.py --only savepoint # 只跑一个用例
```

## 五类形态

| 用例 | 源端形态 | 改造前 | 改造后 |
|---|---|---|---|
| `compression` | `binlog_transaction_compression=ON` | 整个事务被打包成一个 `TRANSACTION_PAYLOAD`，连接器只投外层 → **整事务丢失** | capture 侧拆包，逐个内层事件照常处理 |
| `partial-json` | `binlog_row_value_options=PARTIAL_JSON` | MySQL 发 `PARTIAL_UPDATE_ROWS_EVENT`，连接器不认识 → **UPDATE 丢失** | extract 以 E3020 停机上报（连接器确实不支持这个编码，判据是"绝不静默丢"） |
| `generated` / `generated-full` | 表带 STORED/VIRTUAL 生成列 | 行事件带着生成列的值，apply 照单全收 → MySQL **3105 fail-stop** | 列与值成对剔除，生成列交给目标库自己算 |
| `savepoint` | 事务中间有 `SAVEPOINT` | extract 当成 DDL 清掉 tx_id → 源事务**被切成两个**目标事务 | savepoint 视为事务内语句，tx_id 不断 |
| `schema-drift` | 抽取积压时 `ALTER TABLE` | 老事件按新表定义解析 → **整行错位**且看不出异常 | 用事件自带的列名（`binlog_row_metadata=FULL`）自愈；拿不到列名时以 E3021 停机 |

## 判据怎么定的

用**水位标记**而不是 sleep：每做完一件事插一行普通数据，等它到达目标库，说明它之前的所有
事件都已经流完整条链路。这时候断言"某些行不在目标库/没错位"才站得住脚，否则只是没等够。

原子性类判据（savepoint 不切开事务）用高频采样：commit 之后每 50ms 查一次目标库，
只要观测到"部分到达"就判失败。

## 两条容易踩的坑

**`xalib.rows_of` 把列名写死成 XA 判据的表结构（id/tag/amt）**，在别的表上会直接 SQL 报错，
而 `wait_until` 会把异常当成"还没到"一路等到超时——判据永远失败还看不出原因。本目录用
`edgelib.ids_of`（只取主键）。

**改源端全局参数必须用 `SourceVar` 上下文管理器**，判据挂了也要复原，否则污染后面所有用例
（`binlog_transaction_compression` 尤其明显：留着不关，后面每个用例都在测压缩链路）。

## 相关配置

| 配置 | 默认 | 说明 |
|---|---|---|
| `capture.deserialization.failure.policy` | `FAIL_STOP` | 反序列化失败的处置；`SKIP` 是旧行为（跳过=静默丢数据） |
| `extract.unknown.event.policy` | `FAIL_STOP` | 未知事件类型的处置；`SKIP` 放行 |
| 源端 `binlog_row_metadata` | 建议 `FULL` | 列名随事件下发，DDL 漂移可自愈；MINIMAL 下列数不符会以 E3021 停机 |
| 源端 `binlog_row_value_options` | 必须为空 | 非空时向导预检直接拦（error） |
