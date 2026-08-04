# 增量一致性语义：用户可选的「事务一致 / 最终一致」（2026-08-02）

## 1. 要解决的问题

增量投递此前只有一套全局灰度开关（`apply.transaction.mode`，agent 级环境变量，默认 `EVENT`），
用户在页面上看不到、也选不了。而两类链路的诉求本来就是相反的：

- 灾备、订阅：备库/下游任何时刻都不能读到"半个事务"（转账扣了没入账），**宁可慢**；
- 普通同步：只要最终数据一致就行，**吞吐优先**。

现在把它变成建任务时的一次显式选择，选完写进任务，**创建后不可修改**。

## 2. 语义定义

| | 事务一致性 TRANSACTIONAL | 最终一致性 EVENTUAL |
|---|---|---|
| 目标库提交顺序 | 与源库事务提交顺序**一致** | 不保证 |
| 每个源事务 | 在目标端仍是一个事务，内容一致（不拆不并） | 可被打散、合并 |
| 增量应用 | 串行 | 按「表 + 主键」冲突矩阵并发 |
| 中间时刻可读到半个源事务 | 否 | 可能（追平后收敛） |
| 默认给谁 | 订阅、灾备 | 同步 |

冲突矩阵（最终一致模式的并发判据）：

- 不同表的 DML → 一定可并发；
- 同表、主键不同 → 可并发；
- 同表、**主键相同**的 INSERT/UPDATE/DELETE → 冲突键相同，落到同一个 worker 上按 seqno 严格保序。

## 3. 实现落点

### 3.1 任务属性（创建即定死）

- Flyway `V10__add_consistency_mode.sql`：`workflows.consistency_mode`，存量行回填 `EVENTUAL`
  （那是它们此前**实际**跑的语义，升级不该改变在跑任务的投递语义）。
- `WorkflowService.createWorkflow(..., consistencyMode)`：空值按任务类型取默认，非法值直接拒绝
  （不静默落成默认，否则用户以为选上了）。双向灾备的反向影子任务**继承**正向的语义。
- `WorkflowService.updateConfig(...)`：传入与已存值不同即抛错。前端只做只读展示，
  服务端这一道是防直接调接口绕过去的。
- 随 TaskCreatedMessage 下发给 agent；`RecoveryService` 的恢复 SQL 也 select 了这一列——
  漏了它，agent 重启重写 config 会让任务悄悄退回另一套语义。

### 3.2 引擎参数（ConfigService → 每个任务的 config.properties）

TRANSACTIONAL：

```properties
sync.consistency.mode=TRANSACTIONAL
apply.transaction.mode=TRANSACTION
increment.apply.parallelism=1
subscribe.transaction.topic.enabled=true
```

`parallelism=1` 是**刚性**的：多 worker 各自提交只能保住每个连通分量内部有序，
"目标提交顺序 = 源提交顺序"就不成立了。只要"每个源事务在目标端是一个原子事务"、
不在意全局提交顺序的，可以设 `apply.transaction.strict.order=false` 换回按表连通分量的并发投递。

EVENTUAL：

```properties
sync.consistency.mode=EVENTUAL
apply.transaction.mode=EVENT
increment.apply.parallelism=4            # 并发线程数
increment.apply.commit.batch.sql=200     # 每个线程一次目标事务最多攒多少条 SQL
increment.apply.batch.size=500           # 单批读入的事件数
increment.apply.conflict.granularity=ROW # ROW=表+主键，TABLE=表
```

四个值都有默认、都写进任务自己的配置文件，可按链路改；agent 级环境变量
（`INCREMENT_APPLY_PARALLELISM` 等）仍可覆盖，用于灰度/排障。

### 3.3 冲突矩阵调度（ContinuousIncrementMain）

`shardBatch()` 由"按表 hash 分片"推广为"按冲突键的并查集连通分量分片"：

- 冲突键：ROW 粒度 = `库.表 + 主键值`（主键取自类型化管道的 `ParameterizedDml.rowKey`）；
  TABLE 粒度 = `库.表`；
- 一个执行组横跨多个键（多表事务、一个事件改多行）时，这些键并成一个连通分量整体同片，
  否则同一组会被拆到两个连接上；
- **降级规则**：一张表上只要有一条 DML 算不出主键（无主键表、走文本路径的事件、列数不齐回退），
  整张表压回表级键。行级键与表级键在同一张表上并存会永远算不到一起，冲突就漏了。

worker 侧新增攒批提交：`commit.batch.sql > 1` 时多个执行组共用一个目标事务，攒够 N 条再提交。
失败即整块回滚，返回**本块第一条**的 seqno，位点退到那里，重放靠应用侧幂等语义收敛
（与既有的并行 fail-stop 低水位口径一致）。

## 4. 已知取舍

- **ROW 粒度 + 业务唯一索引**：流量里若存在"删一行、再插一行复用同一唯一键"的模式，
  行级并发可能把两条颠倒，后到的 INSERT 撞唯一键会被当成重复键忽略掉。
  这类表把 `increment.apply.conflict.granularity` 设成 `TABLE`。
- **表名映射把两张源表合并到同一张目标表**时，两张源表被判为不冲突。
  这与改动前的按表分片口径一致（那时也只看源表名）。
- Mongo / Redis / Elasticsearch 三条链路的增量是单进程顺序应用，暂不受该选择影响；
  字段照常记录与下发，语义差异待这些引擎支持并发应用后再落地。

## 5. 判据

- `ConflictMatrixShardingTest`（6）：同表同主键必须同片保序、同表不同主键并发、不同表并发、
  无主键/文本路径整表降级、TABLE 粒度退回按表分片。
- `TransactionShardingTest`（5）：原事务一致分片规则不回归。
- `ConsistencyModeConfigTest`（6，agent）：三种任务类型的默认值、显式选择、非法值回落，
  以及"事务一致必须连带把并发关掉"。
- `ConsistencyModeImmutableTest`（6，backend）：类型默认、显式选择归一、非法值拒绝、
  改配置时试图修改即报错、回传相同值不算修改。
