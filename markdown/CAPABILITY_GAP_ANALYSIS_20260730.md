# 数据同步平台：一致性 / 恢复能力 / 长任务可靠性 深度评估 + 大厂对标与补齐方案

日期：2026-07-30　范围：`migration-*` 九个引擎模块 + `migration-agent` + `java-backend`（约 72K 行 Java）
方法：全链路代码通读 + 现网环境实测（故障注入、事务原子性专项、放大率量化）

---

## 0. 结论摘要

现有实现在**最终一致性**这条线上做得相当扎实：位点 fail-stop、幂等应用、僵死看门狗、
死信裁决、双向防回环，故障注入套件覆盖 5 种源 × 3 类任务。本轮复跑 `sql_resume.py mysql`
（4 分钟写入 + 2 次 SIGKILL）**整表指纹完全一致**（21497 行 / BIT_XOR 2659432041 双侧相等）。

但有三个层次的问题，越往下越结构性：

| 层次 | 问题 | 性质 |
|---|---|---|
| **A. 正确性** | 源事务原子性在目标端完全丢失（已实测证明） | 语义缺陷，灾备场景下是硬伤 |
| **B. 恢复能力** | capture 重启永远从"任务起始位点"重放；熔断器无半开态；agent 硬崩后子进程孤儿化可双写 | 短任务看不出来，长任务必炸 |
| **C. 长跑可靠性** | 日志/表延迟文件/THL 放大均无界，实测 10 分钟任务产出 283MB 日志 | 运行数天即耗尽磁盘 |
| **D. 吞吐**（2.11，第 1 批期间新发现） | 多行事件的目标写入次数是**行数的平方**：落地 500 行付出 62,104 次写入 | 批量写入越大越狠，是对标万级 TPS 的天花板 |

**第 1 批（B 层的位点重放 + 孤儿双写）已实施完成并实测通过，见 §7**；
过程中还揪出并修掉了一个被孤儿进程长期掩盖的静默丢数据缺陷（§2.3b）。

**第 4 批（P1-1 集群化/故障转移 + P1-4 双向冲突消解）已实施完成并实测通过，见 §10**：
任务不再"广播抢单"而是按负载指派给具体 agent 并持租约，持有者硬崩后由其它 agent
**自动接管续跑**（实测 SIGKILL 后改派并追平，两端指纹一致）；双向写写冲突从"谁后到谁覆盖、
两端各留一半"变成**确定性收敛**（实测 6 个并发冲突行两端 100% 收敛到同一值，且冲突有记录）。

**第 3 批（B 层的熔断自愈 + 2.4 静默丢文件 + C 层的资源治理）已实施完成并实测通过，见 §9**：
依赖不可用不再永久打死任务（新增 RECONNECTING 态 + 三态熔断，恢复后自愈）、
反复崩溃有了 E3007 而不再伪装成健康、转换失败与文件损坏一律 fail-stop 不再静默吞掉整个 THL 文件、
日志与表延迟文件从无界变有界且默认不落行值。

**第 2 批（D 层的 N² 写放大 + A 层的事务一致性投递）已实施完成并实测通过，见 §8**：
写放大从 124× 降到 **1.00×**（1000 行 → 1000 次目标写入，未修时理论值 200,000 次）；
`apply.transaction.mode=TRANSACTION` 下源事务与目标提交点 **150:150 严格 1:1**、
34,413 次不变量抓拍**零破缺**（EVENT 默认路径保持原样，对照组仍是 300 提交点 / 17.7% 破缺）。

对标大厂后，**缺失的关键能力**按优先级是：事务一致性投递 → 集群化/任务级 HA →
位点与资源的全生命周期治理 → 双向冲突消解 → 可观测性与 SLA 闭环。

---

## 1. 架构速览（便于定位下文改动点）

```
                 ┌── SQL 三段管线（mysql / pg / oracle / tidb 源）────────────────┐
源库 ──binlog/WAL/redo/TiCDC──> capture ──*.cap 文本行──> extract ──*.thl 分帧──> increment ──> 目标库
                 │              (子进程)                (子进程)              (子进程)        │
                 │                 ↑ ProcessGuard 守护 · 崩溃自动重启 · 活性文件僵死看门狗    │
                 └──────────────────────────────────────────────────────────────┘
                                              └──> subscribe ──> Kafka（订阅任务）

单进程引擎：migration-mongo（全量+Change Streams）· migration-redis（PSYNC）· migration-elastic
控制面：java-backend（REST/JPA/MySQL 元库）──Kafka(29092)──> migration-agent（线程池 + 子进程管理 + H2 本地态）
```

关键位点载体：

| 组件 | 位点存放 | 重启后是否真的从这里续 |
|---|---|---|
| capture（mysql/oracle） | 写 `binlog_output/capture_position.properties`，读 `config.properties` 的 `capture.binlog.*` | **否**（见 2.2） |
| capture（pg） | 同上，但服务端复制槽 `confirmed_flush_lsn` 兜底 | 部分是 |
| capture（tidb） | `capture_position.properties`，读写同一文件 | 是 |
| extract | `thl_output/.extractor_seqno` + `.extract_progress` | 是 |
| increment | H2 `checkpoint/increment_checkpoint` + `.increment_progress` | 是 |
| subscribe | `checkpoint/.subscribe_progress`（flush 后才推进） | 是 |

---

## 2. 实测发现的缺陷

### 2.1 【严重·正确性】源事务原子性在目标端完全丢失　【已实施，见 §8】

**结论**：一个包含 N 个行事件的源事务，会在目标库被拆成 **N 个独立事务** 落地。
目标端在中间时刻可以读到"半个事务"。

**代码根因**：`ContinuousIncrementMain.processThlFile()` 以**单个 THL 事件**为事务边界，
每个事件独立 `commit()`（[ContinuousIncrementMain.java:633](migration-increment/src/main/java/com/migration/increment/ContinuousIncrementMain.java:633)）。
源 binlog 的 XID（提交点）虽然被 extract 翻译成了 `operation=COMMIT`
（[MySQLBinlogExtractor.java:163](migration-extract/src/main/java/com/migration/extract/MySQLBinlogExtractor.java:163)）、
再由转换器产出一条 `"COMMIT;"` 文本（[THLToSqlConverter.java:525](migration-increment/src/main/java/com/migration/increment/THLToSqlConverter.java:525)），
但执行时被**显式跳过**（[ContinuousIncrementMain.java:856](migration-increment/src/main/java/com/migration/increment/ContinuousIncrementMain.java:856)）——
事务边界信息一路传到了最后一米，然后被丢掉。并行应用路径（`flushBatch`）按表 hash 分片，
同一事务的多表写入还会落到**不同 worker 的不同连接**上，原子性更无从谈起。

**实测证明**（新增 `test_scripts/fault_injection/txn_atomicity.py`）：
源库两行账户 `sum(bal)` 恒为 2000，执行 150 个「每个含 2 条 UPDATE」的转账事务。

```
尺子1｜目标库 binlog 提交点计数（确定性）
  源:  300 个行事件 / 150 个事务          → 2 行事件 : 1 提交点
  目标: 218 个行事件 / 218 个提交点(Xid)  → 1 行事件 : 1 提交点   ← 每行一个事务
尺子2｜不变量抓拍
  目标读取 5164 次，其中 1530 次读到 sum=1999（≈30%）——即"扣款已落、入账未落"
```

**影响面**（这不是"理论问题"）：
- **灾备**：备库任意时刻都可能停在半个事务上。计划外倒换后业务直接读到不平的账，
  且**无法自愈**——不是 RPO 窗口内丢数据（可接受的灾备语义），是已同步区间内的**原子性破坏**。
- **读写分离 / 只读副本**：下游报表、风控读到不一致快照。
- **订阅**：Kafka 消息不带任何事务标识（`CdcEvent` 只有 seqno/table/before/after，
  见 [ContinuousSubscribeMain.java:1157](migration-subscribe/src/main/java/com/migration/subscribe/ContinuousSubscribeMain.java:1157)），
  下游无法重组源事务。Debezium 有 `source.txId` + 事务元数据 topic，这里完全没有。

> 注：单条多行 INSERT（一个 binlog 行事件带 N 行）仍在一个事务内，所以"批量插入"看不出问题；
> 一旦是多语句事务就必现。

---

### 2.2 【严重·恢复能力】capture 每次重启都从「任务起始位点」重放

**代码根因**：capture 把推进后的位点写进 `binlog_output/capture_position.properties`
（[MySQLBinlogCapture.java:791](migration-capture/src/main/java/com/migration/capture/MySQLBinlogCapture.java:791)），
但**启动时读的是 `config.properties` 的 `capture.binlog.file/position`**
（[MySQLBinlogCapture.java:98](migration-capture/src/main/java/com/migration/capture/MySQLBinlogCapture.java:98)）。
全仓检索：`capture_position.properties` 只有 TiCDC 会读回，MySQL/Oracle/PG 都是**只写不读**；
而 `capture.binlog.position` 只在 `AbstractTaskExecutor.updateMysqlCheckpointConfig()`
任务初始化时写一次，运行期从不回写。GTID 路径（`capture.gtid.set`）同理。

**实测证据**（本轮 `sql_resume.py mysql` 任务 `6e8081f4…` 的日志）：

```
10:14:39 Capture初始化完成 … binlogPosition=31487318     ← 首次启动
10:17:48 SIGKILL capture
10:18:20 Capture初始化完成 … binlogPosition=31487318     ← 重启，位点分毫未动
（此时 capture_position.properties 里已经是 43310545，前进了 ~12MB）
```

**后果**（幂等应用能保证最终一致，但代价在长任务上是灾难性的）：

| 任务已运行 | 一次 capture 崩溃的代价 |
|---|---|
| 5 分钟（测试） | 重读几十 MB binlog，几分钟追平——看起来"没问题" |
| 7 天（生产） | 重读 7 天全量 binlog；若源库 `binlog_expire_logs_seconds` 已清理该文件，**capture 直接起不来，任务永久不可恢复** |

本轮实测的放大率，两条链路都量到了：

- **同步链路**：源侧真实变更 = 20000 存量 + 1915 次增删改，产出
  **184,144 条 THL 事件 / 83MB THL / 43MB cap / 283MB 日志**，停写后追平耗时 ~7 分钟。
- **订阅链路**（3 次崩溃 / 3 分钟）：写入真值 9013 条，**Kafka 实收 15,037 条 = 1.67× 重复放大**。
  订阅是 at-least-once，下游本就要去重，所以现有用例的"不丢"判据看不出这一项；
  但如果任务已跑了一周，一次 capture 崩溃就会把**一周的事件全部重投一遍**到下游 Kafka——
  这不是"少量重复"，是下游要么被打爆、要么在去重表上撞穿。

Oracle（`capture.redo.scn`）同样问题。PG 因为服务端复制槽 `confirmed_flush_lsn` 兜底，
实际不会整段重放；但 PG 反过来有另一个风险：`setFlushedLSN(receiveLsn)` 在**写完 .cap 文件即推进槽**
（[PostgresWalCapture.java:536](migration-capture/src/main/java/com/migration/capture/PostgresWalCapture.java:536)），
而 `.cap` 只 `flush()` 到 page cache 未 fsync——**宿主机断电时槽已前进而数据未落盘 = 永久丢失**。

---

### 2.3 【严重·恢复能力】agent 硬崩后子进程孤儿化，重启即双写

- `ProcessManager` 只在被显式 `stop()` 时 `destroy()` 子进程；JVM 的 shutdown hook 里
  `agent.stop()` 走的是正常路径，`kill -9` / OOM / 宿主重启时**子进程全部存活成孤儿**。
- agent 重启后 `TaskRecoveryService.recoverUnfinishedTasks()` 按
  `WHERE status IN ('STARTING','FULL_MIGRATING','INCREMENT_RUNNING',…)` **全量拉起**
  （[RecoveryService.java:33](migration-agent/src/main/java/com/migration/agent/service/RecoveryService.java:33)），
  `startMigrationAgentThread()` 不做任何存在性检查（[TaskProcessService.java:37](migration-agent/src/main/java/com/migration/agent/service/TaskProcessService.java:37)）。
- 结果：同一个 taskId 出现**两套 capture/extract/increment**，写同一个 `binlog_output/`、
  同一个 `thl_output/`、同一个 H2 checkpoint。seqno 交错、位点互相覆盖、THL 文件内容错乱。

这一点连项目自己的运维脚本都承认——`restart_agent.sh` 里写着
「子进程随 agent 退出不会自动收敛，一并清理」，靠 `pkill` 兜底。**但 `pkill` 只在人为重启时执行。**

同一根因还导致**无法水平扩容**：多个 agent 共用一个 Kafka consumer group 分发新任务，
但 recovery 查询没有 agent 归属列/租约，任何一个 agent 启动都会把**所有** RUNNING 任务再拉一遍。
即当前架构事实上只能单 agent 运行，控制面无 HA。

---

### 2.3b 【严重·正确性】agent 恢复任务时丢掉目标库名 → 增量把 DML 写回源库

> 这一条是实施第 1 批（P0-3）时**新发现**的，见 §7.5。之前一直被 2.3 的孤儿进程掩盖着：
> agent 崩了以后孤儿子进程带着正确的内存态继续同步，恢复出来的第二套进程写错库也看不出来
> （表现只是"重复双写"）。孤儿一收敛，它立刻变成**静默丢数据**。

`RecoveryService` 的恢复查询 select 了 `source_db_name` 却**没有 `target_db_name`**，
`RecoveryTask.toTaskMessage()` 也从未 `setTargetDbName()`。而连接串通常不带库名
（`mysql://user:pass@host:port`），`ConfigService.updateConfig()` 解析它时会把
`target.db.database` **覆盖成空**，本指望随后用 `taskMessage.getTargetDbName()` 填回来。

于是 agent 每次重启恢复后 `target.db.database=`（空），increment 退化成用**源库名**限定 DML：

```
执行参数化SQL (seqno=5790): INSERT INTO `pos_src`.`pos_load` (...)   ← 该写 pos_tgt
```

**实测复现两次**：agent SIGKILL 重启后写入的 500 行全部落回源库（因主键相同、
`ON DUPLICATE KEY UPDATE` 静默吸收，连报错都没有），目标库停在旧行数，
任务状态始终是 `INCREMENT_RUNNING`。已修复并加单测锁死（§7.5）。

---

### 2.4 【中·正确性】THL 文件处理异常 → 静默跳过整个文件剩余事件　【已实施，见 §9】

[ContinuousIncrementMain.java:703-713](migration-increment/src/main/java/com/migration/increment/ContinuousIncrementMain.java:703)：

```java
} catch (Exception e) {
    logger.error("处理THL文件出错: {}, 跳过至下一个文件. 错误: {}", fileName, e.getMessage());
    if (isLatestFile) processedFiles.put(fileName, lastExecutedSeqno);
    else              processedFiles.put(fileName, -1L);   // ← 标记"已处理完"，永不重读
    saveProgress();
}
```

`try` 块罩住了整个事件循环，SQLException 在内层已处理，能逃到这里的是：THL 反序列化失败、
转换器的运行时异常（NPE / 越界 / 未知类型）。一个事件的转换器 bug ⇒ **该 THL 文件（最大 50MB）
剩余全部事件被永久丢弃**，且不写死信、不上报 FAILED、任务状态保持 `INCREMENT_RUNNING`。
这与同文件里精心设计的 fail-stop（`aborted` 分支绝不标 -1）自相矛盾——fail-stop 只保护
SQL 执行失败，不保护转换失败。

### 2.5 【中·恢复能力】熔断器只有 CLOSED/OPEN，没有半开，永不自愈　【已实施，见 §9】

[CircuitBreaker.java:13](migration-agent/src/main/java/com/migration/agent/resilience/CircuitBreaker.java:13) 的
`State` 枚举只有两个值，`reset()` 是 public 但**全仓无人调用**（ProcessGuard 只调
`retryPolicy.reset()`，是另一个对象）。一旦连续 5 次启动失败：`allowRequest()` 恒 false →
`attemptRecovery()` 返回 false → `guarding.set(false)` → 守护线程退出 → **该进程再也不会被拉起**。

重试退避 5s→10s→20s→40s→80s 合计约 2.5 分钟。也就是说：**目标库一次超过 2.5 分钟的
计划内维护窗口，就会把任务永久打死，必须人工 retry。**

反向的洞同样存在：若子进程每次能存活 >5s 再退出，`waitForStartup()` 判定成功 →
`recordSuccess()` + `retryPolicy.reset()` → 无限崩溃重启循环，且每次都上报
`INCREMENT_RUNNING`「进程已自动重启恢复」。**永久 crash-loop 在监控上完全不可见**——
僵死看门狗又恰好被 `guardsHealthyForStallCheck()` 在重启窗口内关掉了。

### 2.6 【中·长跑】三处无界增长　【已实施，见 §9】

1. **表级延迟文件**：`recordTableLatency()` 对**每个事件**向
   `binlog_output/table_latency/<table>.tsv` 追加一行，**从不裁剪**
   （[ContinuousIncrementMain.java:967](migration-increment/src/main/java/com/migration/increment/ContinuousIncrementMain.java:967)）。
   读侧 `TableLatencyService.loadFromFiles()` 每次都**整文件读进内存**再截断保留最后 N 条——
   文件越大，热力图接口越慢、越吃内存。实测：单个测试任务 46,268 行 / 2.6MB；
   按 5000 行/秒的生产流量估算约 **17 GB/天/表**。
2. **日志**：per-task `logback.xml` 把 `com.migration` 设成 **DEBUG**，且增量对每条 SQL
   打 INFO（`logger.info("执行SQL (seqno={}): {}", …)`）。实测 10 分钟任务产出 **283MB**。
   单任务 `totalSizeCap` 10GB，N 个任务就是 N×10GB。
   顺带一个合规问题：**行数据明文落盘**——`执行SQL` 打的是拼好值的完整 DML，
   `执行参数化SQL` 打的 `dml` 也含参数值。DTS/DMS 默认都不记录行值。
3. **THL / cap 放大**：见 2.2，崩溃重放导致的放大没有任何上限保护。

### 2.7 【中·灾备】双向同步只有冲突"检测"，没有冲突"消解"　【已实施，见 §10】

- 防回环做得不错：应用事务先写 `__sync_origin` 标记行，对端 capture 见标记即跳过整个事务
  （前向单遍状态机，[MySQLBinlogCapture.java:566](migration-capture/src/main/java/com/migration/capture/MySQLBinlogCapture.java:566)）。
- 但**写写冲突**（两端同时改同一行）没有任何策略：谁后到谁覆盖，且两条通道各自
  `ON DUPLICATE KEY UPDATE`，最终收敛结果**取决于到达顺序而非业务时间序**。
- `DataValidationService` 里的 `BIDIRECTIONAL_WRITE` 只是事后按"最近更新时间"做启发式**探测**，
  既不阻断也不修复。
- 双向模式下 **DDL 被完全丢弃**（capture 在 bidi 分支对 `QueryEventData` 直接 `return`），
  注释里说"需各节点带外协调"——对标 DTS 双向同步的 DDL 单向传播能力，这是明确缺口。

### 2.8 【低·可观测】任务状态可以倒退（实测命中）　【已修复，见 §11.2】

本轮 `dr_resume.py mysql2mysql` 的状态序列实测为：
`FULL_MIGRATING → FULL_COMPLETED → FULL_MIGRATING → INCREMENT_RUNNING`——**回退了一次**。

根因是两处叠加：
- agent 侧 `executeFullMigration()` 先 `interrupt()` 进度监控线程再发 `FULL_COMPLETED`，
  但监控线程可能已进入循环体、正在构造/发送一条 `FULL_MIGRATING`，两条消息竞争；
- backend 侧 `KafkaConsumerService` 只挡终态（COMPLETED/FAILED/CONFIGURING）和 SWITCHING，
  **没有生命周期单调性校验**（[KafkaConsumerService.java:104](java-backend/src/main/java/com/synctask/service/KafkaConsumerService.java:104) 无条件 `setStatus`）。

后果：UI/告警/`ValidationTaskService.getIncrementalWorkflows()` 这类按状态取任务的逻辑
会短暂看到错误状态；对自动化编排（依赖任务、调度）是隐患。
修法：给 `WorkflowStatus` 定义阶段序号，`setStatus` 前拒绝低阶段覆盖高阶段
（FAILED/SWITCHING/STOPPED 等显式终止态除外）；agent 侧监控线程发送前再确认一次 `stopped`。

### 2.9 【低·性能】全量吞吐偏低　【已修复，见 §11.1】

`dr_resume.py mysql2mysql` 全量阶段：**200,000 行 / 686 秒 ≈ 291 行/秒**（单表、无限速配额约束、
两个独立 MySQL 实例）。逐页 `SELECT … LIMIT` + 逐行 `INSERT` 的路径没有用
`rewriteBatchedStatements` / `LOAD DATA LOCAL INFILE` / `COPY`（PG）这类批量通道。
对标 DTS/DMS 的万级行/秒，全量迁移一张亿级表需要数天，这本身就会把 2.2 的位点重放风险放大。
建议在 `DataMigration` 的写侧引入按方言的批量通道（MySQL `LOAD DATA`、PG `COPY`、
Oracle 数组绑定），预期 1~2 个数量级提升。

### 2.10 【低·一致性】全量阶段无一致性快照　【已实施，见 §11.3】

`DataMigration` 全程 autocommit + 逐页 `WHERE pk > last ORDER BY pk LIMIT n`，
没有 `START TRANSACTION WITH CONSISTENT SNAPSHOT` / `REPEATABLE READ` / 任何快照点。
**最终**能收敛（增量位点在全量之前 + 幂等 upsert 重放），但：
- 中间态目标库长期处于"跨时间点拼接"的状态，全量阶段无法对外提供任何一致性保证；
- 不存在"全量完成即等价于某个 LSN/GTID 的一致快照"这个语义，因此**做不了
  "全量结束点校验"**，只能等增量追平后再校验。

---

### 2.11 【严重·性能】多行事件的写放大是**行数的平方**　【已修复，见 §8.1】

> 第 1 批实施期间量到的新缺陷，**第 2 批已修**：拆分时按行切片逐行元数据，
> 实测放大率从 124× 降到 **1.00×**（`write_amplification.py`，见 §3）。

`ContinuousExtractMain` 把一个 N 行的 binlog 行事件拆成 N 个"每行一条"的 THL 事件
（[ContinuousExtractMain.java:540-558](migration-extract/src/main/java/com/migration/extract/ContinuousExtractMain.java:540)），
拆分时逐键复制 metadata，只排除了 `rows_data` 与 `multi_row`：

```java
if (!"rows_data".equals(key) && !"multi_row".equals(key)) {
    rowEvent.addMetadata(key, entry.getValue());   // ← rows_typed（N 行全量）也被复制进来了
}
rowEvent.addMetadata("row_data", rowsData.get(i));  // 文本路径正确：只带第 i 行
```

而增量端的类型化值管道读的是 **`rows_typed`**
（[TypedDmlConverter.java:122](migration-increment/src/main/java/com/migration/increment/TypedDmlConverter.java:122)），
不是 `row_data`。于是拆出来的 N 个事件里**每一个都带着全部 N 行**，各自生成 N 条 SQL：

**N 行的源事件 → N 个 THL 事件 × N 条 SQL = N² 次目标写入。**

实测吻合到个位数（`position_resume.py` 单次 agent 崩溃恢复窗口内）：

| 源事件行数 | 156 | 44 | 154 | 46 | 100 | 合计 |
|---|---|---|---|---|---|---|
| THL 事件数（=ΣN） | 156 | 44 | 154 | 46 | 100 | **500** |
| 目标写入次数（=ΣN²） | 24336 | 1936 | 23716 | 2116 | 10000 | **62104** |

日志里的 `执行参数化SQL` 恰好 62,104 条 —— 落地 **500 行**数据付出了 **62,104 次**写入，放大 124×。

**这解释了此前几个一直没解释清的现象**：
- 增量应用吞吐只有 ~20–30 行/秒（本轮三次运行一致）——不是限速、不是网络，是在做 N² 次写；
- §2.2 里"20000 存量 + 1915 次变更产出 184,144 条 THL 事件"的量级；
- 为什么 `sql_resume.py` 从没暴露过：它按**单行**写入（N=1，N²=N），放大倍数恰好为 1。

**批量写入越大，放大越狠**：一次 1000 行的 `INSERT ... VALUES (...)` 会变成 100 万次目标写入。
这对"对标 DTS/DMS 万级行每秒"是决定性的天花板。

修法（第 2 批，**已实施**）：拆分时把 `rows_typed` / `rows_before_typed` 一并**按行切片**（只放第 i 行的
单元素列表），而不是排除它们——排除会让类型化管道整体回退到文本路径，丢掉它当初修掉的
5 类值保真缺陷（见 `value-conversion-unified-in-typetranslator`）。切片在
`ContinuousExtractMain.splitMultiRowEvent()` 一处完成，对四个 extractor 一视同仁（它们产出的
是同一份 metadata 契约），UPDATE 前后镜像随 `rows_before_typed` 一并切。落地细节见 §8.1。

---

## 3. 实测记录

| 用例 | 结果 | 备注 |
|---|---|---|
| `sql_resume.py mysql --minutes 4`（2 次 SIGKILL：capture、extract） | **通过 3 / 失败 0** | 源目标整表指纹完全相等（21497 行 / BIT_XOR 2659432041）；但追平耗时 ~7 分钟，见 2.2 放大率 |
| `sub_resume.py mysql --minutes 3`（3 次 SIGKILL：subscribe、capture、extract） | **通过 5 / 失败 0** | 三进程全部自愈；写入真值 9013 条 Kafka 缺 0 条；按投递顺序回放 6151 行与源逐行相等。**但 Kafka 实收 15,037 条（1.67× 重复）**，现有判据不覆盖重复放大 |
| `dr_resume.py mysql2mysql --phase incre --minutes 3`（2 次 SIGKILL） | **通过 4 / 失败 0** | 全量 200000 行两端 xor 相等；增量崩溃后最终 202028 行 xor 相等。副产物：全量耗时 **686s ≈ 291 行/秒**（见 2.9）；状态序列出现 **FULL_COMPLETED → FULL_MIGRATING 回退**（见 2.8） |
| `txn_atomicity.py --txns 150`（**本轮新增**） | **失败 1** | 目标 218 行事件 : 218 提交点（源 300:150）；5164 次抓拍中 1530 次读到半个事务 |
| `position_resume.py --rows 800`（**第 1 批新增，修复后**） | **通过 5 / 失败 0** | capture 从落盘位点续传（102909673，任务起始位点是 102548470）；静默窗口重放行变更 **0 条**（旧行为应≈8 条）；agent SIGKILL 后 3 个子进程全部自杀无孤儿；重复启动的 capture 被锁挡住 exit=9；最终 2600 行两端 xor 相等 |
| `sql_resume.py mysql --minutes 3`（**第 1 批后回归**） | **通过 3 / 失败 0** | 2 次 SIGKILL（capture、extract）后 22913 行两端指纹相等（BIT_XOR 126448788），无回归 |
| `sub_resume.py mysql --minutes 2`（**第 1 批后回归**） | **管线部分通过，判据未跑完** | 任务正常进入 SUBSCRIBE_RUNNING，2 次 SIGKILL 后 subscribe/capture/extract **三个子进程全部自愈**；但消息内容校验步骤在本机跑不了——Kafka 生产者用 snappy 压缩（`ContinuousSubscribeMain` 的 `COMPRESSION_TYPE_CONFIG`），而本机 python3.14 下 kafka-python 缺 snappy 解码库（`UnsupportedCodecError`）。属**测试环境依赖缺失**，与本批改动无关；恢复该用例需 `brew install snappy && pip3 install --break-system-packages python-snappy` |
| `write_amplification.py --batch 200 --batches 5`（**第 2 批新增，修复后**） | **通过 1 / 失败 0** | 5 条 200 行的 INSERT = 1000 行 → 目标 binlog **1000** 个行事件、增量日志 **1000** 条实际执行 SQL，放大率 **1.00×**（未修理论值 200×，ΣN²=200,000）；追平 ~4s（修复前是 ~20–30 行/秒）；两端指纹 `(1000, 1831589905)` 相等 |
| `txn_atomicity.py --txns 150 --mode TRANSACTION`（**第 2 批门禁**） | **通过 1 / 失败 0** | 目标 300 行事件 / **150 个提交点**，与源事务数严格 1:1；34,413 次不变量抓拍中读到半个事务 **0 次** |
| `txn_atomicity.py --txns 150 --mode EVENT`（**对照组，默认路径**） | **通过 1 / 失败 0** | 判据按模式取反：仍是 300 提交点、24,896 次抓拍中 4,406 次破缺（17.7%）——默认行为零变更，且这把尺子依然抓得住缺陷 |
| `subscribe_txn_metadata.py --txns 20`（**第 2 批新增**） | **通过 5 / 失败 0** | 40/40 条消息带 `transaction` 块；20 个源事务各恰好 2 条消息且共享同一 `transaction.id`；`total_order` 事务内从 1 递增；事务标记 topic BEGIN 20 / END 20，`event_count` 均为 2 |
| `sql_resume.py mysql --minutes 3`（**第 2 批后回归，TRANSACTION 模式**） | **通过 3 / 失败 0** | 2 次 SIGKILL（capture、extract）后 23207 行两端指纹相等（BIT_XOR 1845963337）——按事务提交不影响崩溃续传语义 |
| `position_resume.py --rows 800`（**第 2 批后回归，默认模式**） | **通过 5 / 失败 0** | 重启位点 205785312 vs 配置起始 205288696；静默窗口重放 0 条；agent SIGKILL 后 3 个子进程全部自杀；2600 行两端 xor 相等（第 1 批能力无回归） |
| `selfheal_reconnect.py --rows 400`（**第 3 批新增**） | **通过 4 / 失败 0** | 连杀 3 次 → 错误码 E3007（任务仍是运行态，不判死）；移走 increment jar → **RECONNECTING**（旧行为是熔断打开后守护线程退出、任务判死）；放回 jar 后 **自行**回到 INCREMENT_RUNNING；自愈后 401 行两端指纹相等 |
| `resource_governance.py --rows 3000`（**第 3 批新增**） | **通过 5 / 失败 0** | 行值明文日志 **0** 行；日志 **21KB/千行**（治理前同一用例 1118KB/千行，降 53×，阈值 400KB）；表延迟 tsv 最大 366 行（上限 200，容忍 2×，旧行为无上限）；热力图接口仍返回 4 张表；删除任务后生成 `.terminal` 终态标记 |
| `write_amplification.py --batch 200 --batches 5`（**第 3 批后回归**） | **通过 1 / 失败 0** | 放大率仍 1.00×，执行 SQL 1000 条，两端指纹 `(1000, 1831589905)` 相等 |
| `txn_atomicity.py --txns 150 --mode TRANSACTION`（**第 3 批后回归**） | **通过 1 / 失败 0** | 300 行事件 / **150 提交点**（1:1），8051 次抓拍 0 次读到半个事务 |
| `sql_resume.py mysql --minutes 3`（**第 3 批后回归**） | **通过 3 / 失败 0** | 2 次 SIGKILL 后 22219 行两端指纹相等（BIT_XOR 3120551778）——改了整条崩溃恢复路径后无回归 |
| `position_resume.py --rows 800`（**第 3 批后回归**） | **通过 5 / 失败 0** | 五把尺子全过，2600 行两端 xor 相等（第 1 批能力无回归） |
| `agent_failover.py --rows 600`（**第 4 批新增，双 agent**） | **通过 6 / 失败 0** | 两台 agent 注册心跳；任务指派给 `agent-c38dab7c`（lease_epoch=1）；SIGKILL 持有者后 **改派给 agent-failover-b**（epoch 1→3）；接管方拉起子进程；601 行两端指纹相等 |
| `bidi_conflict.py --rows 6`（**第 4 批新增，双向灾备**） | **通过 5 / 失败 0** | 冻结两向 increment 制造真并发：6 个冲突行**两端 100% 收敛到同一值**（owner=B，即源事件时间戳较晚的一端）；12 条冲突记录可查；两端旁路表均有行级元数据；非冲突行照常同步 |
| `sql_resume.py mysql --minutes 3`（**第 4 批后回归**） | **通过 3 / 失败 0** | 2 次 SIGKILL 后 23504 行两端指纹相等（BIT_XOR 1983926410）——集群化改了下发/恢复路径后无回归 |
| `txn_atomicity.py --txns 120 --mode TRANSACTION`（**第 4 批后回归**） | **通过 1 / 失败 0** | 120 事务 / **120 提交点**，事务原子性保持 |

现有故障注入套件的**判据盲区**（建议补齐）：
- 只测"不丢 + 可收敛"，**不测重复量级**——2.2 的整段重放因此在 CI 里完全隐形；
- 只测"最终一致"，**不测中间态一致性**——2.1 的事务原子性因此从未被发现；
- 崩溃注入间隔 50~90s、任务总时长 3~5 分钟，**测不出与"已运行时长"成正比的退化**
  （位点重放量、日志/tsv 体积、熔断器耗尽）。建议加一个 `--soak` 长跑模式（≥2 小时）
  并断言「第 N 次崩溃的追平耗时 ≈ 第 1 次」。

新增脚本：[test_scripts/fault_injection/txn_atomicity.py](test_scripts/fault_injection/txn_atomicity.py)
（两把独立尺子：目标库 binlog 提交点计数 = 确定性判据；不变量抓拍 = 直接证据）、
[write_amplification.py](test_scripts/fault_injection/write_amplification.py)、
[subscribe_txn_metadata.py](test_scripts/fault_injection/subscribe_txn_metadata.py)（后两个见 §8.4）。

---

## 4. 对标大厂

对标对象：阿里云 DTS、AWS DMS、Oracle GoldenGate（OGG）、Debezium + Flink CDC。

| 能力 | 本平台 | DTS | DMS | OGG | Debezium |
|---|---|---|---|---|---|
| 多源多目标异构链路 | ✅ 9 类引擎 | ✅ | ✅ | ✅ | ✅ |
| 断点续传 + 幂等 | ✅ | ✅ | ✅ | ✅ | ✅ |
| **事务一致性投递** | ❌ 每行一事务 | ✅ 默认保序保事务（高吞吐模式可选放开） | ⚠️ 可选，开 batch apply 即放弃 | ✅ 核心能力（事务级投递 + CDR） | ⚠️ 上游给全 tx metadata，下游 sink 是否事务落地看实现 |
| **位点持续持久化** | ❌ 只记首启 | ✅ | ✅ | ✅ | ✅ offset topic |
| **任务级 HA / 集群调度** | ❌ 单 agent | ✅ | ✅ 多副本 | ✅ | ✅ Kafka Connect |
| 全量一致性快照 | ❌ | ✅ | ✅ | ✅ | ✅ |
| 全量批量装载通道 | ❌ 逐行 INSERT（实测 ~291 行/秒） | ✅ | ✅ | ✅ | — |
| DDL 同步 | ✅ AUTO/SKIP/MANUAL + gh-ost 识别 | ✅ | 部分 | ✅ | 部分 |
| 双向同步防回环 | ✅ | ✅ | — | ✅ | — |
| **双向冲突消解策略** | ❌ 仅检测 | ✅ | — | ✅ CDR | — |
| 数据校验 + 修复 | ✅ CHECKSUM+逐行+repair | ✅ | ✅ | ✅ veridata | ❌ |
| 预检 | ✅ 18 项 | ✅ | ✅ | — | — |
| 限流 / 背压 | ✅ 行/秒 + 文件水位 | ✅ | ✅ | ✅ | ✅ |
| RPO/RTO 可视化 | ✅ | ✅ | ✅ | ✅ | 部分 |
| 死信 / 人工裁决 | ✅ | ✅ | ✅ | ✅ | ✅ DLQ |
| **端到端加密 + 行值不落日志** | ⚠️ THL 可加密，但行值明文进日志 | ✅ | ✅ | ✅ | ✅ |
| **归档/时间点回溯（PITR）** | ❌ | ✅ | — | ✅ | — |
| 分库分表汇聚 / 拆分 | ⚠️ 有库表名映射，无聚合路由 | ✅ | 部分 | ✅ | — |
| 数据脱敏 | ✅（订阅侧） | ✅ | — | — | ✅ SMT |

**差距集中在三块**：事务语义、集群化、位点与资源治理。功能广度（引擎种类、校验、预检、
死信、脱敏、双向）其实已经很接近商用产品；缺的是**"能长期无人值守跑在生产上"的那部分**。

---

## 5. 补齐方案（按优先级，落到类/文件级）

### P0-1　事务一致性投递（Transactional Apply）　【已实施，见 §8】

**目标**：源事务 → 目标事务 1:1；订阅消息携带事务标识。

**改法**（三段各改一点，向后兼容，开关 `apply.transaction.mode=EVENT|TRANSACTION`，默认先 EVENT）：

1. **extract 侧打事务边界**——好消息是**四种源的事务信息在 capture 阶段就已经拿到了**，
   只是没往下传：

   | 源 | 现状 | 要做的 |
   |---|---|---|
   | MySQL | XID 事件已透传，extract 产出 `operation=COMMIT`（[MySQLBinlogExtractor.java:163](migration-extract/src/main/java/com/migration/extract/MySQLBinlogExtractor.java:163)）；capture 写的记录里就带 `XidEventData{xid=…}` | 解析出 xid 值，回填成 `tx_id`；**双向模式下 capture 现在把 XID/BEGIN 直接 `return` 丢掉了，必须改成"照常写出、只跳过被标记事务的 DML"**（PG 侧已经是这个做法，照抄即可） |
   | PostgreSQL | BEGIN/COMMIT 已产出 `operation=BEGIN/COMMIT`（[PostgresWalExtractor.java:152](migration-extract/src/main/java/com/migration/extract/PostgresWalExtractor.java:152)） | 直接可用，补 `tx_id`=xid |
   | Oracle | LogMiner 查询已 `SELECT … XID …` + `COMMITTED_DATA_ONLY`（[OracleRedoCapture.java:504](migration-capture/src/main/java/com/migration/capture/OracleRedoCapture.java:504)） | XID 变化即事务边界，补 `tx_id`/`tx_last` |
   | TiDB | 同一事务多行共享 `commitTs`（[TiCDCExtractor.java:29](migration-extract/src/main/java/com/migration/extract/TiCDCExtractor.java:29)） | commitTs 变化即边界 |

   统一给每个 THL 事件补三个 metadata：`tx_id` / `tx_seq`（事务内序号）/ `tx_last`。
   兼容：老 THL 无这些 metadata 时 increment 自动退回 EVENT 模式，不影响存量任务。

2. **increment 侧按事务提交**（`ContinuousIncrementMain`）
   - 串行路径：把「读事件 → 单事务 commit」改成「累积到 `tx_last` 或达到
     `apply.transaction.max.rows`（防超大事务撑爆）再 commit」；checkpoint 在 commit 之后
     按**事务末条 seqno** 落盘（现有 fail-stop 语义天然适配：失败即整事务回滚 + 位点不推进）。
   - 并行路径：分片键从 `table.hashCode()` 改为 `tx_id.hashCode()`——**同一事务的所有行进同一个
     worker/同一连接**，跨事务并发。这样既保住原子性，又保留并行收益；表间依赖靠
     `FOREIGN_KEY_CHECKS=0` + 幂等（与现状一致）。冲突事务（写同一行的两个事务分到不同 worker）
     用「按主键区间二级排序 + 低水位提交」处理，或简单起见在 TRANSACTION 模式下把
     `applyParallelism` 上限收敛到按 tx 分片。
   - 超大事务（`tx_rows > 阈值`）降级为分批提交并在日志/指标里标记，避免目标库事务日志爆掉。

3. **subscribe 侧补事务元数据**（`ContinuousSubscribeMain.CdcEvent`）
   - 消息体加 `source.txId` / `transaction: {id, total_order, data_collection_order}`（对齐 Debezium）；
   - 可选开一个 `<prefix>.transaction` topic，投 `BEGIN{txId}` / `END{txId,eventCount}`，
     让下游能做事务重组。

**验收**：`txn_atomicity.py` 在 TRANSACTION 模式下——目标提交点数 ≈ 源事务数、抓拍破缺次数为 0。

> 实施时修正了上面并行路径的设计：**纯按 `tx_id` 分片是错的**——两个事务写同一张表被分到不同
> worker 后，同表的先后顺序会颠倒（详见 §8.3）。最终改成「按事务分组 + 对表做并查集，
> 同一连通分量进同一 worker」。

**实测结果**：目标 300 行事件 / **150 提交点**（源 150 事务，严格 1:1）、34,413 次抓拍 **0 次**破缺。

---

### P0-2　位点持续持久化 + 位点保护　【已实施，见 §7】

**目标**：任何一次子进程重启，最多重放"上次落盘位点之后"的量，不再整段重放；
源端日志被清理时能明确报错而非无限重试。

1. **capture 启动位点改为「持久化位点优先」**
   在 `MySQLBinlogCapture.doInitialize()`（Oracle/PG 同构改造）加载顺序改为：
   `binlog_output/capture_position.properties` → 无则 `config.properties` 的
   `capture.binlog.*` → 无则源库当前位点。TiCDC 已经是这个语义，直接对齐它。
   - **主备倒换必须清掉这个文件**：`FailoverService.cleanFailoverFiles()` /
     `AgentMain.STALE_POSITION_KEYS_ON_FAILOVER` 已经在清 `binlog_output/` 整个目录，
     天然满足；补一条单测锁死这个不变量。
2. **位点落盘改为原子写 + fsync**：现在 `savePosition()` 直接 `FileOutputStream` 覆写，
   崩在写一半会读到半个文件。改成 `tmp + fsync + rename`（`writeApplyQueueDepth()` 已有这个写法，
   抽成 `migration-common` 的 `AtomicFileWriter` 复用）。
3. **`.cap` / `.thl` 写入补 fsync 选项**（`durability.fsync=true`，默认关）。
   PG 链路必须开：`setFlushedLSN()` 之前若未 fsync，宿主断电即永久丢数据。
   或者更好——**把槽推进从"写完 .cap"改为"increment 已应用"**，用一个
   `binlog_output/applied_lsn` 反馈文件驱动 `setFlushedLSN`，实现端到端背压。
4. **位点有效性预检**：capture 启动时先 `SHOW BINARY LOGS` / `pg_replication_slots` /
   `V$ARCHIVED_LOG` 校验目标位点是否仍在保留期内；不在则直接
   `writeErrorStatus("E3006","源端日志已被清理，位点不可用，需重新初始化全量")`，
   由 agent 上报 FAILED，而不是让 ProcessGuard 无限重启。

---

### P0-3　任务级单实例互斥 + 孤儿进程收敛　【已实施，见 §7】

**目标**：同一 taskId 全局只能有一套子进程；agent 硬崩不产生双写。

1. **任务目录文件锁**：子进程启动时对 `files/<taskId>/.task.lock` 取
   `FileChannel.tryLock()`（`migration-common` 新增 `TaskInstanceLock`），拿不到就退出并打印
   持锁 PID。capture/extract/increment/subscribe/mongo/redis/elastic 七个 main 各加三行。
2. **子进程随父进程死**：`ProcessManager.start()` 时给子进程传
   `-Dagent.watchdog.pid=<agentPid>`，子进程起一个守护线程每 5s
   `ProcessHandle.of(agentPid).isPresent()`，父没了就自杀。
   （比 `pkill` 可靠，也不依赖运维脚本。）
3. **agent 启动先收孤儿**：`AgentMain` 在 `recoverUnfinishedTasks()` **之前**，
   扫描 `files/*/` 下的锁文件与 `pgrep -f "task.id="`，把不属于当前 agent 的残留子进程清掉
   （或直接复用它们——但复用需要 attach 语义，第一版直接清更简单）。
4. **恢复加租约**：`workflows` 表加 `agent_id` + `lease_expire_at`，
   `RecoveryService.getUnfinishedTasks()` 只捞「无租约 或 租约过期」的任务并 CAS 抢占；
   agent 每 30s 续租。这一条同时是 P1-1 集群化的地基。

---

### P1-1　控制面/执行面集群化与故障转移　【已实施，见 §10】

在 P0-3 租约的基础上：

- `workflows` 增 `agent_id`/`lease_expire_at`/`lease_epoch`；agent 启动注册到
  `agents` 表（host/port/capacity/heartbeat_at）。
- 任务下发从「Kafka 广播 + 谁抢到算谁」改为「backend 按 capacity 选 agent，写
  `agent_id` 后再投 Kafka（key=agentId 保证分区亲和）」。
- agent 心跳超时（如 90s）→ backend 把它名下任务的租约作废 → 其它 agent 抢占续跑
  （从各自 checkpoint 续传，语义上等价于一次进程崩溃恢复，现有恢复路径直接复用）。
- `WorkflowService` 里 4 处硬编码的 `AGENT_BASE_URL=http://localhost:8083` 改为按
  `workflow.agent_id` 查 `agents` 表路由。

---

### P1-2　真正的熔断/自愈策略　【已实施，见 §9】

- `CircuitBreaker` 补 `HALF_OPEN` + `openTimeoutMs`（默认 60s）：OPEN 到期后放行一次探测，
  成功回 CLOSED、失败回 OPEN 并指数延长（上限如 30 分钟）。
- `RetryPolicy` 分两级：**短期重试**（现有指数退避，用于瞬时抖动）+ **长期重连**
  （无限次、固定 5~30 分钟间隔，用于目标库长时间维护）。任务状态相应分成
  `RECONNECTING`（可自愈，不算失败）与 `FAILED`（需人工）。
- **crash-loop 检测**：ProcessGuard 记录滑动窗口内的重启次数
  （如 10 分钟内 >5 次），触发 `E3007 进程反复重启` 上报，而不是每次都报"已自动重启恢复"。
- 僵死看门狗在 crash-loop 期间不再全局静默：把 `guardsHealthyForStallCheck()`
  的「有进程不在 RUNNING 就整体跳过」改成「按进程各自判定 + 记录连续不健康时长」。

---

### P1-3　转换失败进死信，不再静默丢文件　【已实施，见 §9】

改 `ContinuousIncrementMain` 的 catch 分支（2.4）：

- 逐事件 try/catch 包住 `typedDmlConverter.convert()` / `sqlConverter.convertToSql()`；
- 转换异常按 `increment.convert.error.policy` 处理：
  `FAIL_STOP`（默认，与 SQL 失败对齐：不推进位点、上报 FAILED、等人工裁决）
  或 `DEAD_LETTER`（写 `deadletter.jsonl` + 推进位点 + 计数上报）。
- 只有**文件级不可读**（reader 构造失败）才允许跳过，且必须 `sendFailedStatus`，
  绝不能静默标 -1。

---

### P1-4　双向冲突消解（CDR）　【已实施，见 §10】

- 新增 `sync.bidi.conflict.policy`：`LWW_SOURCE_TS`（按源事件时间戳，默认）/
  `NODE_PRIORITY`（固定主端优先）/ `CUSTOM_SQL`（按表配表达式）/ `ERROR`（冲突即 fail-stop 送人工）。
- 实现落点：`ContinuousIncrementMain.applyEventTx()` 里，UPDATE/DELETE 改为带条件的
  `... WHERE pk=? AND __sync_ts <= ?`（需要目标表隐藏列或旁路 `__sync_rowmeta` 表记录
  每行的最后写入源与时间戳）。旁路表方案不侵入用户表结构，代价是一次额外写。
- 冲突计入指标 + 落 `conflict.jsonl` 供 UI 展示，复用现有死信页面。
- 双向 DDL：新增 `sync.bidi.ddl.direction=NONE|A_TO_B`，允许指定单向传播 DDL，
  capture 的 bidi 分支从"一律丢弃 QueryEventData"改为按方向放行。

---

### P2-1　长跑资源治理　【已实施，见 §9】

1. **表延迟改环形缓冲**：`recordTableLatency()` 写定长环形文件
   （固定 N 条 × 定长记录，覆盖写 + 头部游标），或直接改成写
   agent 的 `MetricsPersistenceService`（已有 H2 + 保留期 30 天）而不落 tsv。
   `TableLatencyService.loadFromFiles()` 相应改为只读尾部 N 条。
2. **日志降噪**：per-task `logback.xml` 的 `com.migration` 从 DEBUG 改 INFO；
   逐事件 SQL 日志降为 TRACE 并**默认不打行值**（只打 `table/op/pk-hash/seqno`），
   行值需要 `logging.include.row.values=true` 显式开启并在 UI 上标注合规风险。
   预计日志量下降 2 个数量级（283MB → 个位数 MB）。
3. **磁盘水位保护**：agent 巡检 `files/<taskId>` 总大小，超过
   `task.disk.quota.mb`（接 `ResourceQuotaService.maxStorageMb`）时先触发背压
   （复用 `backpressure.signal`），再超则 fail-stop 上报，避免把宿主磁盘写满拖垮所有任务。
4. **任务终态清理**：任务 COMPLETED/删除后按保留期清 `files/<taskId>`
   （当前 `files/` 已累积 12GB / 204 个任务目录，全是历史测试残留）。

---

### P2-2　全量批量装载 + 状态单调性　【已实施，见 §11】

- `DataMigration` 写侧按方言分流：MySQL 走 `LOAD DATA LOCAL INFILE`（或至少
  `rewriteBatchedStatements=true` + `addBatch/executeBatch`）、PG 走 `COPY … FROM STDIN`、
  Oracle 走数组绑定。现有的分片并行、断点续传（`migration_progress` 按表/按 PK 区间）语义不变。
- `WorkflowStatus` 加阶段序号 + `KafkaConsumerService` 拒绝低阶段覆盖高阶段（见 2.8）。

---

### P2-3　全量一致性快照　【已实施，见 §11】

- MySQL：全量搬运前 `FLUSH TABLES WITH READ LOCK` → 取 `SHOW MASTER STATUS`/`gtid_executed`
  → 开 `START TRANSACTION WITH CONSISTENT SNAPSHOT`（每个并行 worker 用
  `SET TRANSACTION SNAPSHOT` 或共享同一 RR 事务）→ `UNLOCK TABLES` → 全量在该快照内完成。
  无锁场景可退化为「仅取 GTID 快照 + 各表 RR 事务」（一致性略弱但无锁，做成可选项）。
- PG：`pg_export_snapshot()` + 各 worker `SET TRANSACTION SNAPSHOT '<id>'`，
  与复制槽创建时返回的 `consistent_point` 天然对齐。
- Oracle：`AS OF SCN <startScn>` 闪回查询。
- 收益：全量结束点即等价于一个明确的 LSN/GTID/SCN，可以做"全量完成即校验"，
  也让 2.8 的中间态问题消失。

---

### P2-4　可观测性与 SLA 闭环　【已实施，见 §11】

- 指标补齐：`replication_lag_seconds`（源最新事件时间 − 已应用事件时间，现有
  `calculateRpo()` 已接近，但依赖心跳事件，需补"源库 `NOW()` − 已应用事件时间"的绝对口径）、
  `capture_replay_bytes`（重放放大量，直接暴露 2.2 类问题）、
  `restart_count_10m`、`conflict_count`、`deadletter_count`、`disk_usage_bytes`。
- `AlertRuleService.extractMetricValue()` 目前只支持 `RPO_MS`/`RTO_MS`，扩到上述指标。
- 补一条**端到端探针**：定期向源库心跳表写一个带 UUID 的标记，测量它出现在目标库/Kafka 的时间
  —— 这是唯一能同时证明"链路真的通"和"延迟多少"的指标，比现有各段自报的活性文件更可信。

---

## 6. 建议的实施顺序

| 阶段 | 内容 | 理由 |
|---|---|---|
| **第 1 批** ✅ | P0-2 位点持久化 + P0-3 单实例互斥 | 改动小、风险低、消除两类"长任务必炸"；且是后续一切的前提 —— **已完成，见 §7** |
| **第 2 批** ✅ | **2.11 N² 写放大**（先做，改动最小收益最大） + P0-1 事务一致性投递 | 2.11 是把 `rows_typed` 按行切片，几行代码换来两个数量级吞吐；P0-1 改动最大但价值最高，先上 `TRANSACTION` 开关灰度，`txn_atomicity.py` 做门禁。两者都在同一段应用路径上，一起改一次回归 —— **已完成，见 §8** |
| **第 3 批** ✅ | P1-2 熔断自愈 + P1-3 转换死信 + P2-1 资源治理 | 都是独立小改动，可并行 —— **已完成，见 §9** |
| **第 4 批** ✅ | P1-1 集群化 + P1-4 冲突消解 | 需要元数据表变更（Flyway V3+）与较多联调 —— **已完成，见 §10** |
| **第 5 批** ✅ | P2-2 批量装载与状态单调 + P2-3 一致性快照 + P2-4 可观测闭环 | 锦上添花，但决定能不能对外承诺 SLA —— **已完成，见 §11**（全量 291→38,365 行/秒） |

每批都应在 `test_scripts/fault_injection/` 里补对应的判据脚本，
并入 `e2e_smoke.py` 的 CI 门禁场景表。

---

## 7. 第 1 批实施记录（P0-2 位点持久化 + P0-3 单实例互斥）

日期：2026-07-30。全量构建 `mvn clean install` 通过，单测 **377 通过 / 0 失败**。

### 7.1 新增的三个公共原语（`migration-common`）

| 类 | 作用 |
|---|---|
| [AtomicFileWriter](migration-common/src/main/java/com/migration/common/io/AtomicFileWriter.java) | 位点类小文件的 **tmp + fsync + rename** 原子落盘。原来的 `new FileOutputStream(posFile)` 是"先截断再写"，崩在中间会读到空文件或半行——重启即当作"无位点"退化成整段重放 |
| [CapturePositionStore](migration-common/src/main/java/com/migration/common/position/CapturePositionStore.java) | `capture_position.properties` 的统一读写口，落实**「已落盘位点优先于 config 起始位点」**这条规则；`capture.position.prefer.persisted=false` 可退回旧行为 |
| [TaskInstanceLock](migration-common/src/main/java/com/migration/common/proc/TaskInstanceLock.java) / [ParentWatchdog](migration-common/src/main/java/com/migration/common/proc/ParentWatchdog.java) / [ChildProcessBootstrap](migration-common/src/main/java/com/migration/common/proc/ChildProcessBootstrap.java) | 任务级文件锁（内核在进程死亡时无条件释放，PID 文件做不到）+ 父进程看门狗 |

### 7.2 P0-2　位点持久化

- **四种源的启动位点一律改为「已落盘优先」**：MySQL（file+pos 与 **GTID 集**）、PG（LSN）、
  Oracle（SCN）——TiCDC 原本就是这个语义，此次对齐三者。
  GTID 尤其关键：连接器在流式过程中持续维护 `client.getGtidSet()`，只在 XID/COMMIT 时并入已提交事务，
  因此落盘的永远是**提交前缀**，续传至多重放最后一个事务，不会丢。
- **`savePosition()` 全部改走原子写**（MySQL/PG/Oracle/TiCDC 四处）。
- **位点有效性预检**（`capture.position.precheck.enabled=true`）：启动时校验续传位点是否还在源端保留期内——
  MySQL 查 `SHOW BINARY LOGS` / `@@global.gtid_purged`（用连接器自带的 `GtidSet.isContainedWithin` 判包含），
  PG 比 `pg_replication_slots.restart_lsn`，Oracle 比 `V$LOG`/`V$ARCHIVED_LOG` 的最早 SCN。
  不可用即写 `binlog_output/error_status` 上报新错误码 **E3006**，由 agent 判 FAILED，
  不再让 ProcessGuard 对一个注定失败的进程无限重启。
- **顺带修掉一条静默丢数据路径**：`PostgresWalCapture.ensureReplicationSlot()` 原先在槽显示 active 时
  **drop 掉再重建**。槽是 WAL 保留的唯一凭据，重建后 `restart_lsn` 之前的 WAL 立刻被回收，
  随后拿旧 LSN 续传时服务端**不报错**、直接从新槽位置开始发，中间的变更静默消失。
  改为只踢掉旧 walsender 后端并轮询等待释放（最多 ~10s）；有续传位点却始终释放不掉时判 E3006 失败，
  而不是靠重建槽换取"能起来"。

**配套不变量**：主备倒换必须清掉已落盘位点（旧源的 GTID 拿到新源会触发从 binlog 最开头整段重放）。
`cleanFailoverFiles` / `cleanupFailoverArtifacts` 清空整个 `binlog_output/` 天然满足，
新增 [FailoverPositionCleanupTest](migration-agent/src/test/java/com/migration/agent/service/FailoverPositionCleanupTest.java) 锁死。

### 7.3 P0-3　单实例互斥 + 孤儿收敛

三道闸，缺一不可：

1. **文件锁**：capture / extract / increment / subscribe / full / mongo / redis / elastic 八个 main
   启动即抢 `files/<taskId>/.<role>.lock`，抢不到打印持锁 PID 后 **exit 9**。
2. **父进程看门狗**：`ProcessManager` 与 `MigrationTaskManager`（仅全量的独立启动路径）都传
   `-Dagent.watchdog.pid/start`，子进程每 5s 探活，agent 消失即自杀（比 `restart_agent.sh` 里的
   `pkill` 可靠——那只覆盖"通过脚本重启"这一条路径）。
3. **启动收孤儿**：[OrphanChildReaper](migration-agent/src/main/java/com/migration/agent/resilience/OrphanChildReaper.java)
   在 `recoverUnfinishedTasks()` **之前**扫锁文件收残留子进程。顺序不能反：
   先恢复会双写，而锁又被孤儿占着导致新进程一直起不来直到熔断。
   不解析命令行（macOS 上 `ProcessHandle.info().arguments()` 拿不到），改读锁文件里的
   `pid|启动时刻`，并用启动时刻 + 命令名两道校验排除 PID 复用误杀。

### 7.4 新增判据脚本

[test_scripts/fault_injection/position_resume.py](test_scripts/fault_injection/position_resume.py) —— 五把尺子：
崩溃后**停写静置**期间的重放行变更事件数（正确续传≈0，整段重放≈历史全量）、
capture 重启后的起始位点是否等于落盘位点、SIGKILL agent 后子进程是否自行退出、
手工重复启动 capture 是否被锁挡住（exit 9）、以及最终源/目标逐指纹一致。

前三把尺子正是 §3 指出的判据盲区——现有套件只判"不丢+可收敛"，
所以整段重放和孤儿双写在 CI 里一直是隐形的。

### 7.5 实施过程中新发现并修掉的缺陷（§2.3b）

新判据脚本第一次跑通"agent 崩溃 → 子进程自杀 → agent 重启恢复"这条完整路径后，
立刻抓到一个**此前无法被观察到**的严重缺陷：恢复时 `target_db_name` 丢失，
increment 把 DML 全部写回源库（详见 §2.3b）。

为什么以前发现不了：孤儿子进程带着正确的内存配置继续同步，
恢复出来的第二套进程写错库只表现为"重复双写"、数据仍然会收敛。
P0-3 让孤儿真正收敛之后，这条路径才第一次成为**唯一**的数据通路，缺陷随即暴露成静默丢数据。
这也说明 P0-3 不是"加一道保险"那么简单——它把一条从未被真正执行过的恢复路径变成了主路径，
所以必须连带把这条路径上的问题一起修完。

修复三处：
1. `RecoveryService` 的两条恢复查询补 select `target_db_name`；
2. `RecoveryTask` 增加 `targetDbName` 字段，`toTaskMessage()` 传下去；
3. `ConfigService` 里"连接串解析会把 target.db.database 覆盖成空"的位置补注释说明这个尖角。

新增 [RecoveryTargetDbNameTest](migration-agent/src/test/java/com/migration/agent/service/RecoveryTargetDbNameTest.java)：
既断言 `RecoveryTask → TaskMessage` 带得上库名，也真跑一遍 `ConfigService.updateConfig`
断言写出的 `target.db.database` 非空——后者才是真正会丢数据的那一层。

---

## 8. 第 2 批实施记录（2.11 N² 写放大 + P0-1 事务一致性投递）

日期：2026-07-30。全量构建 `mvn clean install` 通过，单测 **392 通过 / 0 失败**（第 1 批为 377，新增 15）。
开关：`apply.transaction.mode=EVENT|TRANSACTION`，**默认 EVENT，存量任务行为零变更**。

### 8.1　2.11　多行事件按行切片（先做，改动最小收益最大）

`ContinuousExtractMain` 新增 [splitMultiRowEvent()](migration-extract/src/main/java/com/migration/extract/ContinuousExtractMain.java)，
把「逐键复制、只排除 2 个键」换成一份**逐行元数据键白名单**：

```java
PER_ROW_METADATA_KEYS = {rows_data, rows_data_before, rows_typed, rows_before_typed,
                         row_data, row_data_before, multi_row}
```

白名单内的键按行切片（第 i 行包成**单元素列表**放回**同名键**下，下游读法完全不变），
白名单外的键（表名、库名、位点、时间戳…）仍按引用共享。

两个尖角：

- **切片前先校验长度**：`rowListOf()` 在某个逐行列表的长度与 `rows_data` 不等时**整键丢弃并告警**，
  而不是按下标硬切——长度对不上时硬切等于把 A 行的类型化值配到 B 行上，会写出静默错数据。
- **保持同名键**：不能改名成 `row_typed`，否则 `TypedDmlConverter` 读不到 `rows_typed` 会整体
  回退文本路径，丢掉当初修的 5 类值保真缺陷。

单测 [MultiRowSplitTest](migration-extract/src/test/java/com/migration/extract/MultiRowSplitTest.java)（5 例）：
按行切片、前镜像同切、长度不匹配丢弃、单行事件不拆、共享元数据按引用复制。

**效果**：1000 行 → 目标 binlog **1000** 个行事件（修复前该量级是 200,000），放大率 **1.00×**。

### 8.2　P0-1(1)　extract 侧下发事务边界

新增公共契约 [TxnMetadata](migration-common/src/main/java/com/migration/common/txn/TxnMetadata.java)
（`tx_id` / `tx_last` / `tx_source_id` 三个键 + `txIdOf()` / `isTxLast()` 两个读取口），
extract、increment、subscribe 三段共用，避免字符串键各写各的。

四种源的事务边界形态并不一致，分成两类处理：

| 源 | `tx_id` 取值 | 边界识别 |
|---|---|---|
| MySQL | **BEGIN 事件的 `binlogFile:position`** | 显式：XID 事件打 `tx_last`，`tx_source_id` 回填 xid 值 |
| PostgreSQL | `pg:<xid>` | 显式：COMMIT 事件打 `tx_last` |
| Oracle | `ora:<XID>` | 隐式：LogMiner `COMMITTED_DATA_ONLY` 没有提交事件，靠 `tx_id` 变化 |
| TiDB | `tso:<commitTs>` | 隐式：同上，靠 `commitTs` 变化 |

MySQL 的 `tx_id` **不用 xid 值**：xid 要到事务末尾的 XID 事件才知道，而行事件在此之前就要写出去；
BEGIN 的位点在事务内恒定且天然唯一，正好当 id 用（xid 仍作为 `tx_source_id` 附带，便于对账）。
DDL（非 BEGIN 的 QUERY）显式清空 `currentTxId`——它在 MySQL 里是隐式提交，自成一个事务；
TiCDC 的 DDL 也刻意不打标，让它在增量侧充当一道天然屏障。

**顺带修掉一处前置缺陷**：`MySQLBinlogCapture` 在双向模式下把 BEGIN/XID 直接 `return` 丢掉了，
事务边界根本传不下来。改成只对非 BEGIN 的 QUERY（DDL）提前返回，BEGIN/XID 照常写出、
同时喂给 `BidiLoopGuard.onTransactionBoundary()`——防回环语义不变，边界信息得以下传。

单测 [MySQLTransactionBoundaryTest](migration-extract/src/test/java/com/migration/extract/MySQLTransactionBoundaryTest.java)（5 例）。

### 8.3　P0-1(2)　increment 按事务提交

**串行路径**：把「一个事件一个事务」改成「一个源事务一个目标事务」。

- 读游标改为 `applyCursor() = max(lastExecutedSeqno, pendingTxHighSeqno)`——**已读入未提交**的事件
  不能再被重复读到，但**已提交低水位**才是 checkpoint。两者必须分开（与并行路径当初踩过的坑同源）。
- `commitPendingTx()` 里才 `commit()` + 落 checkpoint（按事务末条 seqno），
  失败即 `rollbackPendingTx()` 整事务回滚且位点不推进——沿用现有 fail-stop 语义。
- 三条收口路径缺一不可：`tx_last`（显式）、`tx_id` 变化（隐式）、**空闲 flush**
  （`apply.transaction.idle.flush.ms`，默认 3s）。第三条是给 Oracle/TiDB 兜底的：
  没有提交事件时最后一个事务会一直悬着，既锁着目标行又不推进位点。
  超时按**提交**处理并告警，而不是回滚——回滚会导致下轮重读同一批数据、原地打转。
- `apply.transaction.max.rows`（默认 50000）强制切分超大事务，避免目标库事务日志被撑爆。
- **跨 THL 文件的事务**：文件读完时若事务仍开着，不能把该文件标成 `-1`（已处理完）——
  否则重启后会跳过事务的前半段。改成记 `lastExecutedSeqno` 并保留文件。
- 人工裁决的 skip-event 之前**强制先提交**，确保 checkpoint 永远不会越过未提交的写入。

**并行路径**：分片键**不能**只用 `tx_id`。两个事务写同一张表被分到不同 worker 时，
同表的先后顺序会颠倒（tx1 INSERT X、tx2 UPDATE X → UPDATE 先跑，幂等写吞掉 "0 rows affected"，
最终值错成 tx1 的旧值），这是比原子性更隐蔽的破坏。

最终做法：按 `tx_id` 分组成 `TxGroup` → 对每个组内涉及的表做**并查集合并** → 按连通分量的根分片。

- 同一事务的所有行必进同一 worker（原子性）；
- 任何两个共享表（含传递共享）的事务也进同一 worker（同表顺序）；
- 表集合互不相交的事务照常并行（并发度退化为「不相交表集合数」，是这条约束下的上界）。

批次切分点从「按表 hash」改成「遇到新事务才切」（`startsNewTransaction()` 在入批**前**判定），
失败时返回 `group.firstSeqno` 让低水位回退到**整个失败事务之前**。
单测 [TransactionShardingTest](migration-increment/src/test/java/com/migration/increment/TransactionShardingTest.java)（5 例）
锁死上述四条性质 + EVENT 模式仍按表分片。

### 8.4　P0-1(3)　subscribe 侧事务元数据

`CdcEvent` 加 `txId` / `txSourceId` / `txOrder`，消息体按 Debezium 口径产出：

```json
"source": { ..., "txId": "binlog.000042:1234" },
"transaction": { "id": "binlog.000042:1234", "total_order": 1, "data_collection_order": 1 }
```

`txOrder` 在 `convertToCdcEvent` 的操作分支**之后**才分配，被过滤掉的事件不会占掉一个序号
（否则下游看到的 `total_order` 会跳号，无从判断是丢消息还是被过滤）。
可选的事务标记 topic `<prefix>.<taskId>.transaction`（`subscribe.transaction.topic.enabled`，默认关）
投 `BEGIN{txId}` / `END{txId, event_count}`，让下游能做事务重组。

### 8.5　配置下发

`ConfigService` 新增 `writeEnumPropFromEnv()`（大小写不敏感的白名单校验 + 非法值告警），
四个新开关随任务 `config.properties` 下发：
`apply.transaction.mode` / `apply.transaction.max.rows` / `apply.transaction.idle.flush.ms` /
`subscribe.transaction.topic.enabled`。

### 8.6　新增判据脚本

| 脚本 | 尺子 |
|---|---|
| [write_amplification.py](test_scripts/fault_injection/write_amplification.py) | 三把：目标库 binlog 行事件数 / 源行数（确定性，阈值 1.5×）、增量日志实际执行 SQL 条数、两端 BIT_XOR 指纹 |
| [txn_atomicity.py](test_scripts/fault_injection/txn_atomicity.py)（扩展） | 加 `--mode EVENT\|TRANSACTION`，按模式取反判定；自动以对应环境变量重启 agent 并在 `finally` 还原 |
| [subscribe_txn_metadata.py](test_scripts/fault_injection/subscribe_txn_metadata.py) | 四把：每条消息带 `transaction` 块、同一源事务共享 id 且条数正确、id 跨事务不重、`total_order` 从 1 递增、标记 topic 的 `event_count` |

两个测试脚手架上的坑，值得记一笔：

- **`执行SQL (seqno=N): COMMIT;` 不是写入**。写放大尺子2 起初把它算进去，1000 行数出 1758 条。
  那是 XID 被转成 `"COMMIT;"` 文本后在执行时跳过的日志，得按语句文本剔掉 COMMIT/BEGIN/ROLLBACK。
- **±1 交替转账的收敛判据会提前返回**。偶数个事务后余额回到初值，`tgt == src` 在事务只应用了一半时
  就已成立，量到的是 216 行事件 / 108 提交点。加一列单调递增的 `ver` 才能等到真正追平（300/150）。
  订阅侧的消息消费也换成了 Kafka 容器自带的 `kafka-console-consumer`——本机 python3.14 解不了 snappy。

---

## 9. 第 3 批实施记录（P1-2 熔断自愈 + P1-3 转换死信 + P2-1 资源治理）

日期：2026-07-30。全量构建 `mvn clean install` 通过，单测 **413 通过 / 0 失败**（第 2 批为 392，新增 13 例）。
新增一个任务状态 **RECONNECTING** 与四个错误码 **E3007/E3008/E3009/E3010**，
`workflows.status` 是 MySQL ENUM，随 Flyway [V7__add_reconnecting_status.sql](java-backend/src/main/resources/db/migration/V7__add_reconnecting_status.sql) 扩容。

### 9.1　P1-2　三态熔断 + 长期重连（2.5 的正面）

[CircuitBreaker](migration-agent/src/main/java/com/migration/agent/resilience/CircuitBreaker.java) 补 `HALF_OPEN`：
OPEN 到期后放行**一次**探测，成功回 CLOSED，失败回 OPEN 且打开时长按 `openTimeoutMultiplier`
指数延长到 `circuit.breaker.open.timeout.max.ms`（默认 60s → 30min 封顶）。

[ProcessGuard](migration-agent/src/main/java/com/migration/agent/resilience/ProcessGuard.java) 的恢复路径改成两级：

| 档位 | 触发 | 间隔 | 任务状态 |
|---|---|---|---|
| 短期重试 | 进程崩溃 | 现有指数退避 5s→80s，共 `retry.max.attempts` 次 | 保持原状态 |
| 长期重连 | 短期预算耗尽 | `max(reconnect.interval.ms, 熔断剩余时长)`，默认 5 分钟起、随熔断退避涨到 30 分钟 | **RECONNECTING** |
| 判失败 | 长期重连也用尽（`reconnect.max.attempts`，默认 12 轮 ≈ 1 小时以上） | — | FAILED |

两个改动看着小但都是必须的：

- **熔断打开不再等于放弃**。旧代码 `attemptRecovery()` 里 `!allowRequest()` 直接 `return false` →
  守护线程 `guarding.set(false)` 退出 → 进程再也不会被拉起。也就是说目标库一次超过
  重试总时长（~2.5 分钟）的计划内维护窗口，就足以把任务永久打死。
- **递归改循环**。旧实现失败时递归调用 `attemptRecovery()`；长期重连是无限轮次的，
  递归会把栈打爆。顺带把睡眠切成 5s 片，`stop()` 仍能秒级收敛。

### 9.2　P1-2　crash-loop 可见性 + 僵死看门狗按进程判定（2.5 的反面）

反向的洞比正向更隐蔽：只要子进程每次能活过 5s，`waitForStartup()` 就判定成功，
于是每次崩溃都上报「进程已自动重启恢复 + INCREMENT_RUNNING」——**永久 crash-loop 在看板上
与健康任务毫无区别**。现在 ProcessGuard 用滑动窗口记重启成功时刻，
窗口（`crashloop.window.ms`，默认 10 分钟）内达到 `crashloop.threshold`（默认 5）即改报
**E3007**，任务状态仍是运行态（进程确实起来了，不该判死），但错误码把真相摆出来。

僵死看门狗同步改掉一处连带静默：[AbstractTaskExecutor](migration-agent/src/main/java/com/migration/agent/thread/AbstractTaskExecutor.java)
原来的 `guardsHealthyForStallCheck()` 是**全局开关**——「有任一受守护进程不在 RUNNING 就整轮跳过」。
只要有一个进程在 crash-loop 里反复重启，其余进程的冻结就被一起静默掉。
改为 `livenessOwnerRestarting(path)` 按文件各自判定：谁在重启只跳过谁的活性文件，
另外把「某进程连续不在运行」的时长记进日志（超过僵死阈值周期性告警），
不再是重启期间什么都不留下。

### 9.3　P1-3　转换失败不再静默丢文件（2.4）

三处改动，核心是**位点绝不越过没成功应用的事件**：

1. 逐事件把 `typedDmlConverter.convert()` / `sqlConverter.convertToSql()` 单独 try 起来，
   交给 `handleConvertFailure()` 按 `increment.convert.error.policy` 处置：
   `FAIL_STOP`（默认）写 **E3009** 的 `error_status` 并停下等人工裁决；
   `DEAD_LETTER` 写死信（`reason=convert-failed`）后推进位点继续。
   两条路径都先把手里打开的目标事务提交掉再动位点——与人工裁决跳过同源的尖角。
2. reader 构造与循环内异常分开接：前者是文件级不可读，后者是流级损坏，
   但**两者都不再标 -1**。统一走 `failStopOnFile()`：回滚未提交事务 → 文件记到已应用的 seqno →
   写 **E3010** → 停止应用循环。旧实现在这里「跳过至下一个文件」，
   一个转换器 bug 就能静默吃掉最大 50MB THL 里的全部剩余事件，任务状态还停在 INCREMENT_RUNNING。
3. 死信记录加 `reason` 字段，人工裁决（`manual-skip`）与转换失败（`convert-failed`）在 UI 上分得开。

### 9.4　P2-1　长跑资源治理（2.6）

| 增长点 | 旧行为 | 现在 |
|---|---|---|
| 任务日志 | per-task logback 固定 `com.migration=DEBUG` + 每行 DML 打 INFO（含行值），实测 10 分钟 283MB，单任务上限 10GB | 默认 INFO（`MIGRATION_TASK_LOG_LEVEL` 可临时调回 DEBUG），逐行 SQL 降到 TRACE 且**默认不打行值**（`logging.include.row.values=true` 才打，并告警合规风险），单任务上限 2GB / 保留 7 天 |
| 表延迟 tsv | 每事件追加一行、从不裁剪；读侧整文件读进内存再取最后 60 条 | 写侧按 `increment.table.latency.max.lines`（默认 2000）滚动裁剪（攒到 2 倍才重写一次，均摊近 0）；读侧改环形窗口，内存恒定 60 条 |
| 任务目录 | 删了也没人清，本机累积 12GB / 204 个目录 | 终态（delete/terminate）打 `.terminal` 标记，[TaskFilesJanitor](migration-agent/src/main/java/com/migration/agent/service/TaskFilesJanitor.java) 每小时巡检，过 `task.files.retention.hours`（默认 72h）才删；任务重新拉起会撤标记 |
| 磁盘水位 | 无保护，写满宿主盘会把**所有**任务一起拖垮 | 每 60s 量一次 `files/<taskId>`：超过配额×`task.disk.backpressure.ratio` 先写 PAUSE 背压信号（复用 extract↔capture 既有通道）暂停拉取，超过 `task.disk.quota.mb` 报 **E3008** 判失败 |

清理这件事上，"什么都不清" 和 "清错" 是两种事故，后者更贵：
**只清打过终态标记的目录**——PAUSED/FAILED 的任务目录里装着位点与 checkpoint，
删掉就等于把「恢复」变成「从头重来或直接丢数据」。单测 [TaskFilesJanitorTest](migration-agent/src/test/java/com/migration/agent/service/TaskFilesJanitorTest.java)
把这条锁死（没标记的、运行中的、标记被撤销的，一律不动）。

### 9.5　新增判据脚本

| 脚本 | 尺子 |
|---|---|
| [selfheal_reconnect.py](test_scripts/fault_injection/selfheal_reconnect.py) | 四把：反复重启是否报 E3007（而不是一路"已自动重启恢复"）、依赖不可用时是 RECONNECTING 还是被判死、依赖恢复后能否**自己**回到 INCREMENT_RUNNING、自愈后两端指纹是否一致。用"临时移走 increment jar"模拟依赖长时间不可用，并把熔断/重连参数压到秒级 |
| [resource_governance.py](test_scripts/fault_injection/resource_governance.py) | 四把：日志里是否出现行值明文（源数据里埋了标记串）、每千行变更的日志字节数、表延迟 tsv 是否被压在上限 2 倍内且热力图仍能出数、任务删除后是否生成终态标记 |

单测新增 [CircuitBreakerHalfOpenTest](migration-agent/src/test/java/com/migration/agent/resilience/CircuitBreakerHalfOpenTest.java)（5 例）、
[TaskFilesJanitorTest](migration-agent/src/test/java/com/migration/agent/service/TaskFilesJanitorTest.java)（5 例）、
[ConvertFailureAndLatencyRollTest](migration-increment/src/test/java/com/migration/increment/ConvertFailureAndLatencyRollTest.java)（3 例）。

### 9.6　实施过程中新发现并修掉的缺陷

判据脚本第一次跑通就抓到三个，其中两个是本批自己引入的，一个是存量的静默丢数据：

1. **【存量·严重】全量分页跳过主键 ≤ 0 的行**（`DataMigration`，非分片路径）。
   首页无条件用 `WHERE pk > 0` 起扫，`currentLastId` 初始化成 0。于是表里那行 `id=0`
   **从来没被搬过**，日志还是「本页 0 行 / 表 acct 数据迁移完成，成功: 0 / 全量迁移成功完成」——
   静默丢数据且标 COMPLETED。分片路径不受影响（首片下界取 `minId - 1`）。
   改为首次搬运不带下界（只有断点续传才需要），翻完首页后再启用。
   自愈用例里源 401 行 / 目标 400 行、缺的正是 `id=0`，就是这条。
2. **【本批引入】磁盘水位巡检把健康任务打成 FAILED**。`Files.walk` 是惰性遍历，
   任务目录里的 THL/队列深度文件边写边删，撞上刚消失的文件抛的是 `UncheckedIOException`
   （RuntimeException，`catch (IOException)` 接不住），一路冒到 `run()` 的兜底 catch →
   `FAILED(E9999)`。实测就栽在 `extract_queue_depth` 被重写的瞬间。
   现在 `dirSizeBytes` 吞掉所有异常，`checkDiskQuota` 再包一层——巡检永远不该牵连任务。
3. **【本批引入】crash-loop 数不到**。最初按「守护循环发现的崩溃」计数，
   而进程死在启动就绪窗口里（`waitForStartup` 期间被杀/自己退出）走的是「启动失败」分支，
   根本不计数——恰恰是最典型的 crash-loop（起来几秒就死）。实测连杀 3 次只记到 1 次。
   改为按**重启次数**计（与方案措辞一致），并在跨过阈值的那一刻就上报，
   不等下一次重启成功——反复崩溃的进程完全可能卡在重连里再也起不来。

---

## 10. 第 4 批实施记录（P1-1 集群化/故障转移 + P1-4 双向冲突消解）

日期：2026-07-31。全量构建 `mvn clean install` 通过，单测 **426 通过 / 0 失败**（第 3 批 413，新增 13 例）。
元数据表变更走 Flyway [V8__agent_registry_and_lease.sql](java-backend/src/main/resources/db/migration/V8__agent_registry_and_lease.sql)：
新增 `agents` 表 + `workflows.agent_id/lease_expire_at/lease_epoch`。

### 10.1　P1-1　从"广播抢单"到"指派 + 租约"

原来的下发是 **Kafka 广播 + 谁抢到算谁**：所有 agent 同组消费同一个 topic，谁拿到分区谁执行，
后端<b>不知道任务落在哪台机器上</b>。恢复能力早就有（子进程各自 checkpoint 续传），
缺的只是「谁负责这个任务」这条信息——所以 agent 硬崩后它名下的任务**没有任何人接管**，
只能等那台机器被人重新拉起来。

| 环节 | 实现 |
|---|---|
| 注册 / 心跳 | [AgentRegistryService](migration-agent/src/main/java/com/migration/agent/service/AgentRegistryService.java)：启动写 `agents`（agent_id 跨重启稳定，取 `MIGRATION_AGENT_ID` 或 `files/.agent_id`），每 15s 刷 `heartbeat_at` **并给自己在跑的任务续租** |
| 指派 | [AgentClusterService.assign()](java-backend/src/main/java/com/synctask/service/AgentClusterService.java)：按容量占用率挑最闲的存活 agent，写 `agent_id` + 90s 租约后再投 Kafka，消息带 `targetAgentId` |
| 过滤 | agent 收到不是指派给自己的消息直接放行；`targetAgentId` 为空时退回广播语义（单机/旧后端零影响） |
| 接管 | 后端每 20s 巡检：**owner 心跳过期**（进程没了）或**任务租约过期**（进程在、任务线程没了）→ 改派给另一台并下发 `resume`，`lease_epoch` +1 |
| 路由 | `WorkflowService` 里 4 处硬编码的 `AGENT_BASE_URL` 改为按 `workflow.agent_id` 查 `agents` 表；查不到回退默认地址 |

两个"不这么写就出事"的点：

- **启动恢复必须按归属过滤**。`recoverUnfinishedTasks()` 原来把所有未完成任务全捞起来重跑；
  集群里每台 agent 启动都这么干一遍，等于人为制造双写。现在归属别人且租约有效的直接跳过。
- **接管走 `resume` 而不是重新建任务**：读各自 checkpoint 续传，与一次进程崩溃恢复等价，
  不会重做全量。而 P0-3 的任务级文件锁保证：万一老 agent 只是网络分区没真死，
  新老两套子进程也不会同时写目标库。

### 10.2　P1-4　双向写写冲突：从"值互换"到确定性收敛

此前双向只有**防回环**（`__sync_origin` 事务打标），没有**冲突策略**。两端同时改同一行时，
两条通道各自 `ON DUPLICATE KEY UPDATE` 覆盖对方，结果是**值互换**——A 变成 B 的值、
B 变成 A 的值，两端永远不一致，而且看起来还像"同步成功"：不报错、不告警，只有对账才发现。

三层机制：

1. **检测：前镜像守卫**。CDR 开启时 UPDATE 的 WHERE 带上<b>整行前镜像</b>
   （NULL 用 `<=>` / `IS NOT DISTINCT FROM`），"影响 0 行"就说明这一行在本端已被改过。
   零额外查询——正常路径不多花一次 IO。
2. **裁决：[ConflictResolver](migration-increment/src/main/java/com/migration/increment/ConflictResolver.java)**，
   `sync.bidi.conflict.policy` = `LWW_SOURCE_TS`（默认，按**源事件时间戳**比大小）/
   `NODE_PRIORITY`（`sync.bidi.primary.node` 恒赢）/ `ERROR`（冲突即 fail-stop 报 **E3011**）。
   裁决必须**对称**：两个方向是两个独立进程各算各的，规则得让它们选出同一个赢家，
   否则两端会稳定地收敛到不同的值——比"谁后到"更糟。时间戳无从比较时退化为**节点 id 字典序**，
   不看时间但一定收敛。
3. **留痕**：旁路表 `__sync_rowmeta`（表, 行键 → 最后写入节点 + 源时刻）与业务 DML 在
   **同一个目标事务**里更新，崩溃不会错位；冲突落 `conflict.jsonl`，
   经 `/api/agent/conflicts/{taskId}` → 后端 `/api/workflows/{id}/conflicts` 供 UI 复用死信页面展示。

配套：节点标识从"源库名"改成 **host:port/db**——灾备两端库名通常一致（倒换要求），
只用库名会得到两个相同的 id，平局裁决直接失效。
双向 DDL 也开了口子：`sync.bidi.ddl.direction=NONE|A_TO_B`，
只有正向通道放行 DDL（反向影子恒不放行，两边都传会各自建表打架），内部表 DDL 任何方向都不传。

### 10.3　新增判据脚本

| 脚本 | 尺子 |
|---|---|
| [agent_failover.py](test_scripts/fault_injection/agent_failover.py) | 六把：两台 agent 都注册并心跳、任务被明确指派（非广播）、SIGKILL 持有者后被改派、`lease_epoch` 递增、接管方真把子进程拉起来、接管后数据追平指纹一致 |
| [bidi_conflict.py](test_scripts/fault_injection/bidi_conflict.py) | 五把：并发冲突行两端是否收敛到同一值、赢家是否确定一致、冲突是否有记录、旁路表是否有行级元数据、非冲突行是否照常双向同步。用 SIGSTOP 冻住两个方向的 increment 来制造**真并发**（否则一端的写会先同步过去，变成顺序修改） |

单测新增 [AgentClusterServiceTest](java-backend/src/test/java/com/synctask/service/AgentClusterServiceTest.java)（6 例：选派/退回广播/改派/不误抢/租约过期/终态忽略）与
[ConflictResolverTest](migration-increment/src/test/java/com/migration/increment/ConflictResolverTest.java)（7 例，含"平局裁决必须对称"）。

### 10.4　实施过程中新发现并修掉的缺陷

1. **【本批引入·致命】心跳时间戳用了 SQL 的 `NOW()`**。元数据库跑在容器里（UTC），
   后端存活判定用 `LocalDateTime.now()`（JVM 时区），两边差 8 小时 —— 于是
   **任何 agent 都永远显示"不在线"**：选派退回广播、故障转移整个不工作，
   日志里只有一句"没有存活的 agent 注册"，不看这条几乎发现不了。
   改为所有时间戳一律 JVM 侧 `setTimestamp` 绑定。第一次跑 `agent_failover.py` 就是栽在这里。
2. **【本批引入】`aliveAgents()` 就地排序仓储返回的列表**。列表不保证可变，
   `List.of` / 不可变实现会抛 `UnsupportedOperationException`，而它会被巡检的兜底 catch 吞掉——
   表现为"故障转移悄悄不工作"。单测直接抓到，改为拷贝后排序。
3. **【测试脚手架】被杀的 agent 若是脚本的子进程且没 `wait()`**，会以 `<defunct>` 僵尸留在进程表里，
   而 `ProcessHandle.isAlive()` 对僵尸仍返回 true——它的子进程的父进程看门狗永远不触发，
   孤儿继续写目标库（生产上 init 立刻回收，不存在此问题）。脚本里补了回收。

---

## 11. 第 5 批实施记录（P2-2 批量装载/状态单调 + P2-3 一致性快照 + P2-4 可观测闭环）

日期：2026-08-01。全量构建 `mvn clean install` 通过，单测 **439 通过 / 0 失败**（第 4 批 426，新增 13 例）。
元数据表变更走 Flyway [V9__sla_metrics.sql](java-backend/src/main/resources/db/migration/V9__sla_metrics.sql)：
`workflows` 增 6 个 SLA 指标列。

### 11.1　P2-2(1)　全量批量装载：291 行/秒 → 38,365 行/秒

原来的写侧是"逐行 `addBatch` + 每 1000 行 `executeBatch`"，但 JDBC 默认<b>不合并语句</b>——
一批 1000 行仍是 1000 次往返，§2.9 实测 291 行/秒的瓶颈全在这里。

改动只有两件事，都收敛在 [BatchWriter](migration-full/src/main/java/com/migration/full/migration/BatchWriter.java)：

| 项 | 做法 |
|---|---|
| 语句重写 | 目标连接挂 `rewriteBatchedStatements=true`（MySQL/TiDB）/ `reWriteBatchedInserts=true`（PG），驱动把一批单行 INSERT 合成一条多值 INSERT；Oracle 的 `executeBatch` 本身即数组绑定，不需要参数 |
| 批大小 | `migration.full.bulk.rows`，默认 `batchSize × 5` |

**为什么不用 `LOAD DATA LOCAL INFILE` / `COPY FROM STDIN`**（报告 P2-2 原本的首选）：那两条通道都是
**文本协议**，值要先渲染成字符串再由服务端解析，等于绕开 PreparedStatement 的类型绑定。
本项目增量链路正是因为文本管道踩过 5 类值保真缺陷（见 [value-conversion 收敛]），
后来才统一到类型化绑定；为一档吞吐把全量退回文本管道不划算，何况 LOAD DATA 还要服务端
`local_infile=ON`。实测语句重写通道已经拿到 **58×**，文本通道的边际收益不值这个风险。

两个"不这么写就出事"的点：

- **`SUCCESS_NO_INFO(-2)` 必须算成功**。语句一旦被重写，`executeBatch` 返回的就是 SUCCESS_NO_INFO
  而非逐行影响数；沿用原先"负数即失败"的口径，开启重写后<b>每一次全量都会把全部行报成失败</b>。
  只有 `EXECUTE_FAILED(-3)` 才是真失败。判据脚本专门有一把尺子盯这个。
- **批失败必须按行重放**。重写之后一行主键冲突会让<b>整条多值 INSERT</b> 失败，而原逻辑只是
  warn 一句继续下一页——等于静默丢掉一整批（最多 5000 行）。现在失败后逐行重放，只跳过真正冲突的行。
  顺带修掉原有的另一个丢数据口子：**目标连接重建时已 addBatch 的行随旧 statement 一起消失，
  计数却照常推进**；现在保留行缓冲，重连后重放。

### 11.2　P2-2(2)　任务状态单调性（§2.8）

两侧各堵一半：

- agent 侧：`fullMonitorDone` 标志 + 监控线程**发送前二次确认**。只 interrupt 挡不住已经进入
  循环体、正在构造 FULL_MIGRATING 的那一轮——它会在 FULL_COMPLETED 之后才发出去。
- backend 侧：[WorkflowStatus.phase()](java-backend/src/main/java/com/synctask/entity/WorkflowStatus.java)
  给生命周期状态定阶段序号，`KafkaConsumerService` 拒绝低阶段覆盖高阶段。倒换/重连/失败/暂停
  是**控制态**（`PHASE_CONTROL`），任何方向都放行。

一个容易做错的取舍：被挡下的消息**只丢"状态"这一个字段**，同一条消息里的进度/表信息/RPO 照常应用——
迟到消息里的进度依然是真实观测值，连带丢掉会让进度条卡住。

顺手修掉一个被外层 catch 吞掉的老 NPE：`buildStatusLogMessage` / `determineLogLevel` 对
`newStatus == null` 直接 `switch` 抛 NPE。这条路径本来就存在（agent 的过程/通知类状态），
表现是进度存进了库、但这一条既不写任务日志也不推 WebSocket。单调性规则会让它更频繁地被走到。

### 11.3　P2-3　全量一致性快照

[ConsistentSnapshot](migration-full/src/main/java/com/migration/full/snapshot/ConsistentSnapshot.java)，
`migration.full.snapshot.mode` 三档：

| 模式 | 行为 |
|---|---|
| `NONE` | 完全的旧行为 |
| `GTID_ONLY`（默认） | 只在搬运前记一次位点（GTID / binlog 坐标 / LSN / SCN），不加锁、不改读取路径。零风险，换来"这次全量对应哪个位点"的可观测性 |
| `CONSISTENT` | 真快照 |

各库手法不同，差别决定了实现形态：

- **MySQL 没有"把快照导出给别的会话"的能力**，只能在 `FLUSH TABLES WITH READ LOCK` 期间把
  <b>所有读连接</b>的 `START TRANSACTION WITH CONSISTENT SNAPSHOT` 一起开出来再解锁（mydumper 的做法）。
  因此读连接必须**预建成池并全程复用**——而默认路径是<b>每页新建连接</b>（为释放 Oracle 会话 PGA，
  避免 ORA-04036）。解锁之后再开的会话，快照点已经不是记下的那个位点了。
  池大小按最坏并发算：表级并行度 × 单表分片数。
- **PostgreSQL** 有 `pg_export_snapshot()`，快照可被任意会话导入，每页新建连接的模型原样保留
  （借出时导入快照，归还时提交）。
- **Oracle** 用闪回 `AS OF SCN`，是逐查询生效的，同样不需要固定连接。

一条硬规则：**快照是增强项，不能让全量起不来**。源库缺 RELOAD 权限、连不上、版本不支持——
任何一种都安静降级为无快照全量（数据仍最终一致），只丢"全量结束点"这个语义，绝不把任务打失败。

位点落到 `files/<taskId>/full_snapshot_position`，这才让"全量完成即校验"成为可能：
此前全量结束点不对应任何一个 LSN/GTID/SCN，只能等增量追平后再校验。

### 11.4　P2-4　可观测性与 SLA 闭环

新增 6 个指标，全链路打通到告警：agent 采集（[SlaMetricsCollector](migration-agent/src/main/java/com/migration/agent/service/SlaMetricsCollector.java)）
→ 随状态消息上报 → `workflows` 表（Flyway V9）→ `AlertRuleService` 可设阈值 → dashboard 展示。

| 指标 | 来源 | 为什么值得单独有一个 |
|---|---|---|
| `replication_lag_ms` | 源库当前时刻 − 已应用事件的源端时刻 | **和现有 `rpo_ms` 不是一回事**：rpo 是"最新捕获事件 − 已应用事件"，源库空闲时分子分母都不动、**恒为 0**——链路整段卡死也是 0。绝对口径用源库时钟做分子，卡多久涨多久，这才是能签 SLA 的那个数 |
| `capture_replay_bytes` | capture 启动时算"上轮跑到过的最远位点 − 本轮起始位点" | 直接暴露 §2.2 那类"每次重启整段重放"。健康时接近 0；一旦变成几十上百 MB，就是位点没被用上，而这类故障不报错、不告警 |
| `restart_count_10m` | `ProcessGuard.restartCountInWindow()` | crash-loop 此前只存在于 agent 日志 |
| `conflict_count` / `deadletter_count` | `conflict.jsonl` / `deadletter.jsonl` 行数（按文件长度缓存，不重复数） | 双向冲突与人工裁决的量此前只能翻文件 |
| `disk_usage_bytes` | 任务目录字节数 | 长跑资源治理的可观测面 |

源库时钟不是每次采集都去查：按 `sourceNow − localNow` 的**偏移量**缓存 60 秒，源库短暂不可达时
沿用上一次偏移（指标不该因为一次连接抖动而跳变）。

**端到端探针**（[E2eProbeService](migration-agent/src/main/java/com/migration/agent/service/E2eProbeService.java)）默认**关闭**：
它要在用户源库里建 `__sync_probe` 表并持续写入，这是对源库的副作用，必须显式同意（`probe.enabled`）。
开启后 `ConfigService` 会把探针表**并入任务的同步范围**——表级同步下不并入的话，标记行根本不会被捕获，
探针只会一直报超时。它是唯一同时证明"链路真的通"和"延迟多少"的指标：各段自报的活性文件都有
"段内自洽但整条链路不通"的盲区（比如 THL 一直在产出、位点一直在推进，目标库却因某个过滤条件一行没落）。

### 11.5　新增判据脚本

| 脚本 | 尺子 |
|---|---|
| [bulk_snapshot.py](test_scripts/fault_injection/bulk_snapshot.py) | 九把：值保真（逐行 MD5 汇总与源端一致，证明没退回文本通道）、成功计数不塌（SUCCESS_NO_INFO）、吞吐提升、快照位点落盘、CONSISTENT 下"快照点之后写入的行不得进来"、"快照点之前已提交的行必须全部搬到"。实测 **668 → 38,365 行/秒（56×）**，快照隔离 0 泄漏 |
| [sla_metrics.py](test_scripts/fault_injection/sla_metrics.py) | 七把：绝对延迟/磁盘占用有值、SIGKILL capture 后重启次数涨上来、重放放大量有值、新指标类型能建告警规则、**告警规则真的按新指标触发**（指标→落库→告警全链路） |

判据脚本的两个坑：

1. **快照判据必须以"快照建立时刻"为界，不是"进程启动时刻"**。JVM 启动 + 建表要一两秒，
   这期间写进源库的行本来就在快照之内、理应被搬走。第一版按进程启动时刻算，直接误报
   "漏入 100 行"。位点文件第一段就是快照时刻（epoch ms），拿它当界才对。
2. **对照组要用 `GTID_ONLY` 而不是 `NONE`**：判据需要快照时刻，而 NONE 根本不记位点。
   GTID_ONLY 只记位点、不隔离读取，正好是"知道位点、数据却不属于那个位点"的旧状态——
   实测它漏入 200 行，证明尺子确实有效（否则 CONSISTENT 的 0 泄漏可能只是没赶上时序）。

单测新增 [BatchWriterTest](migration-full/src/test/java/com/migration/full/migration/BatchWriterTest.java)（4 例）、
[ConsistentSnapshotTest](migration-full/src/test/java/com/migration/full/snapshot/ConsistentSnapshotTest.java)（4 例）、
[StatusMonotonicityTest](java-backend/src/test/java/com/synctask/service/StatusMonotonicityTest.java)（5 例）。

### 11.6　实施过程中的坑

1. **批失败后的计数口径**。逐行重放遇主键冲突时若按"跳过"计（原逐行路径的口径），
   一批里只要有一行冲突整批就都不计数——进度条平白少掉一整批。冲突意味着目标端已有该行，
   对全量的语义（行已就位）就是成功，改为计成功。
2. **多库模式下批量参数会丢**。驱动参数挂在 `DatabaseConfig` 上，而多库模式会派生 per-db 配置，
   不显式 `copyJdbcOptionsFrom` 就只有单库模式提速。
3. **本机 JDK 24 跑不了后端单测**（Mockito inline mockmaker 无法 instrument
   `SimpMessagingTemplate` / `KafkaProducerService`），与本批改动无关；用 `start.sh` 同款 JDK 21 即可。
   另外 `SimpMessagingTemplate` 本身 mock 不了，测试里用真实实例 + mock 的 `MessageChannel`
   （`send` 必须返回 true，否则模板抛 `MessageDeliveryException`）。
