# binlog 边界形态：七项静默故障的修复

2026-08-11

承接 LOB 受限内存同步与 XA 事务同步两项改造，按同一思路继续排查：**A 类**是 binlog 里真实
存在、但状态机不认识的事件形态（XA 那一类）；**B 类**是协议/内存/schema 的边界（LOB 那一类）。
七项里五项已在真链路上复现过，两项是代码级确认。

共同的病症是**静默**：数据没到目标库，任务显示健康、位点照常前进、`error_status` 是空的。

## 修了什么

### ④ 未知事件不再静默丢弃（这一项收益最大）

两处都改成"不认识就停"，而不是跳过：

* **capture**：`onEventDeserializationFailure` 以前只打一行日志，而连接器在这个回调之后是
  `continue`——事件被永久跳过。现在写 `error_status`(E3019) 并退出进程。
  开关 `capture.deserialization.failure.policy=SKIP` 可退回旧行为。
* **extract**：事件分发改成**白名单**——列出"不带数据、丢了也没后果"的类型，
  其余一律 fail-stop(E3020)。之所以是白名单：MySQL 每个大版本都在加事件类型，黑名单漏一个
  就是一次静默丢数据。开关 `extract.unknown.event.policy=SKIP`。

这一条本身就兜住了②，也会兜住将来任何新的事件类型。

### ① 压缩 binlog（`binlog_transaction_compression=ON`，MySQL 8.0.20+）

整个事务在 binlog 里只剩一个 `Transaction_payload` 事件（实测 8.0.44 ZSTD，内层的
BEGIN/Table_map/Write_rows/Xid 位点全等于外层），而连接器**只把外层投给监听器**，
内层要调用方自己 `getUncompressedEvents()` 展开。改造前整个事务连一行都到不了下游。

现在 capture 在事件入口拆包，逐个内层事件走原有处理。内层沿用外层位点，续传口径不变。

### ② JSON 差量（`binlog_row_value_options=PARTIAL_JSON`）

MySQL 发的是 `PARTIAL_UPDATE_ROWS_EVENT`，连接器没有这个类型的反序列化器。
实测 UPDATE 连同同一条语句里改的普通列一起消失。

连接器确实不支持这个编码，所以修的是"别静默丢"：运行时由④以 E3020 停机上报，
向导预检把该参数非空判成 **error 阻断**（Debezium / DTS / DMS 的口径都是要求它为空）。

### ③ 生成列（STORED / VIRTUAL）

binlog 行事件**带着**生成列算好的值（实测 `includedColumns` 覆盖全部列），而目标库拒绝显式
写入：`ERROR 3105`。改造前增量一开工就 fail-stop。

值不能从事件里直接删——行值按全列顺序排，删一个后面全错位——所以是**列名与值成对剔除**：

* 增量类型化管道：`INSERT` 列清单、`UPDATE` 的 SET 列剔除；WHERE 用的前镜像保持完整
  （生成列可以是主键的一部分）。
* 增量文本路径：同样按一组下标同时裁列名、列类型、行值。
* 全量：在 `migrateTableData` 入口一次性从 `TableInfo` 的列清单里摘掉——SELECT 列表、
  瘦扫描列表、INSERT 列表、主键下标、分片键下标、大字段计划全都按同一份列清单的下标互相
  对齐，任何一处单独过滤都会让行值整体错位。仅同引擎 mysql→mysql；异构目标那边是普通列，
  值照常搬才对。

### ⑤ SAVEPOINT 打断源事务边界

ROW 格式下 `SAVEPOINT` 随事务一起进 binlog（实测：`BEGIN → 行事件 → SAVEPOINT \`sp1\` →
行事件 → XID`；`ROLLBACK TO` 不进，被回滚的行事件在写盘前就丢了）。改造前它落进
"DDL 隐式提交"分支把 `currentTxId` 清掉，savepoint 之后的行事件全部丢 tx_id，
事务一致模式下一个源事务被切成两个目标事务——正是该模式要防的"半个事务"。
Spring 的 `PROPAGATION_NESTED`、各类 ORM 的嵌套事务都会产生 savepoint。

现在 extract 把它视为事务内语句保持 tx_id；apply 侧把 `SAVEPOINT` / `ROLLBACK TO` /
`RELEASE SAVEPOINT` 一并识别成事务语句丢弃（原样执行会报 1305）。

### ⑦ 双向同步的自增碰撞

active-active 下两端各自分配自增 ID，两条**内容不同**的行拿到同一个 id，复制到对端时
撞主键 → upsert 覆盖 / 被当成"幂等重放"忽略，一侧的数据静默消失（冲突裁决管的是 UPDATE 的
写写冲突，覆盖不到这种情况）。

预检新增"双向自增错开"：仅 BIDIRECTIONAL 且同步对象里确实有自增列时检查，要求两端
`auto_increment_increment` 相同且 ≥2、`auto_increment_offset` 互不相同且落在 [1, 步长] 内
（MySQL 对 offset > increment 的配置是**直接忽略 offset** 的，写成 3/5 看着错开、实际同源）。

### ⑥ 积压期间 ALTER TABLE 导致列错位

列清单取自**当前**的 `information_schema`，事件却是过去某一刻的。`parseValueList` 对数量不符
的处理是"多的截断、少的补 null"，一句告警都没有。

现在优先用**事件自带的列名**（`binlog_row_metadata=FULL`，MySQL 8.0.1+）——那是与行值同一
时刻的权威信息，加列/删列/改名都能自愈；类型/精度仍只能从 information_schema 拿，所以按
**列名**而不是下标去查。拿不到列名（MINIMAL）且列数对不上时以 E3021 停机。
预检加一条 `binlog_row_metadata=FULL` 的建议（warning，不阻断）。

## 查过但确认没问题的

* **并行应用下的 DDL 顺序**：DDL 走 barrier，`flushBatch()` 先把批 drain 干净再串行处理；
  串行路径也会先 `commitPendingTx` 再转换。没有"DDL 抢在前面 DML 之前落"的问题。
* **`binlog_format=MIXED/STATEMENT`**：向导的 `checkBinlogFormat` 是 error 级阻断，进不了任务。

## 判据

| 层次 | 覆盖 |
|---|---|
| `test_scripts/edge/edge_e2e.py` | **15/15**，三进程真链路：压缩事务 / PARTIAL_JSON 停机 / 生成列增量+全量 / SAVEPOINT 原子性 / schema 漂移自愈 |
| `MySQLTransactionBoundaryTest` | 新增 SAVEPOINT 不打断 tx_id（确定性单测，不依赖采样时序） |
| `BidiAutoIncrementStaggerTest` | 自增错开判定 5 例（含 offset>increment 与两端步长不同这两个不显然的情况） |
| `test_scripts/xa/xa_e2e.py` | 12/12，回归确认 XA 链路未受影响 |
| `./test.sh all` | 引擎 + 后端全部通过（后端含错误码三份目录一致性门禁） |

## 新增错误码

| 码 | 含义 |
|---|---|
| E3019 | binlog 事件反序列化失败（跳过=静默丢数据，已停机） |
| E3020 | 不支持的 binlog 事件类型 |
| E3021 | 列布局与源库当前定义不一致 |

## 没做自动化覆盖的部分

`binlog_row_value_options` 与 `binlog_row_metadata` 两条**预检项**只有编译与代码走查，
没有自动化测试——仓库约定单测不连外部库，而这两条本质上就是"连上源库读一个全局变量"。
它们防的运行期故障（E3020）已有 E2E 覆盖。自增错开那条的判定逻辑已抽成纯函数并有单测。
