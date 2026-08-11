# 位点持续持久化设计（Durable & Continuous Checkpointing）

> 2026-08-05 ｜ 对标 阿里云 DTS / AWS DMS / Oracle GoldenGate / Debezium
> 前置：[CAPABILITY_GAP_ANALYSIS_20260730.md](CAPABILITY_GAP_ANALYSIS_20260730.md) §7（第 1 批 P0-2 已把
> **capture 读位点**做成"持久化优先"）、V8 集群化租约、[SHARDING_ROUTE_DESIGN_20260803.md](SHARDING_ROUTE_DESIGN_20260803.md)（跨实例 leg 位点独立）

---

## 0. 一句话

第 1 批解决的是"**同一台机器上重启，别整段重放**"；本设计解决的是"**位点本身别只活在那台机器的磁盘上**"——
把 8 种各自为政的本地位点载体收敛成一个模型、一套读写口，并让它**持续上卷到元数据库**，
使 V8 的跨机故障转移真正具备"接管即续传"的语义，同时把位点变成可观测、可校验、可重置的一等资源。

---

## 1. 现状盘点（逐个核过代码）

### 1.1 位点载体清单

| # | 链路段 | 载体路径（相对任务目录 `files/<taskId>/`） | 写入方 | 落盘时机 | 原子性 | 跨机可用 |
|---|---|---|---|---|---|---|
| 1 | capture 读位点 | `binlog_output/capture_position.properties` | `MySQLBinlogCapture:1002` / `PostgresWalCapture:1169` / `OracleRedoCapture:1275` / `TiCDCCapture:496` | 每 1000 事件或 5s | ✅ `AtomicFileWriter` | ❌ |
| 2 | 任务起始位点 | `checkpoint/checkpoint.mv.db`（H2） | `AbstractTaskExecutor.initMysqlCheckpoint:449` | **仅首启一次** | ⚠️ H2 默认 500ms 延迟刷盘 | ❌ |
| 3 | apply 已应用位点 | `checkpoint/increment_checkpoint.mv.db`（H2） | `SeqnoCheckpointManager:79`，由 `ContinuousIncrementMain.advanceCheckpoint:2395` 调 | **每事件** MERGE | ⚠️ 同上 | ❌ |
| 4 | extract 产出位点 | `thl_output/.extractor_seqno` | extract 进程 | 每批 | ⚠️ 文本覆写 | ❌ |
| 5 | 全量快照位点 | `full_snapshot_position` | `SnapshotPosition.write` | 全量开始一次 | ✅ | ❌ |
| 6 | 全量表级断点 | `CheckpointRecorder`（H2）+ 进度文件 | migration-full | 表/分片粒度 | ⚠️ | ❌ |
| 7 | Mongo resume token | `checkpoint/mongo_resume_token.json` | `MongoSyncMain:1032` / `MongoSubscribeMain:471` | 每批 | ✅ tmp+rename | ❌ |
| 8 | ES binlog 位点 | `checkpoint/elastic_binlog_position.json` | `ElasticSyncMain:891` | 每批 | ✅ tmp+rename | ❌ |
| 9 | 订阅位点 | `checkpoint/.subscribe_progress` | `ContinuousSubscribeMain:183` | flush 后 | ⚠️ | ❌ |
| 10 | Redis 增量 | **无** | — | — | — | ❌ |

**结论**：4 种格式（properties / H2 / JSON / 自定义文本）、3 套落盘策略、10 个写入点，
**没有一个出得了这台机器的磁盘**。每接一种新引擎就重写一遍，[已经踩过的坑](CAPABILITY_GAP_ANALYSIS_20260730.md)
（"位点先于 flush 推进即丢"、"位点 -1 覆盖 seqno"）也就每条链路各踩一次。

### 1.2 现在能力的边界

第 1 批做完之后，**同机重启**这条路是通的：capture 从落盘位点续，实测重放行变更 0 条。
但它只覆盖载体 #1；#2/#3 这两个 H2 库既没被纳入"持久化优先"，也没有原子性保证，
更没有任何一个载体离开过本机磁盘。

---

## 2. 差距（对标大厂）

| 能力 | 本平台 | DTS | DMS | OGG | Debezium |
|---|---|---|---|---|---|
| 位点续传（同机） | ✅ 第 1 批 | ✅ | ✅ | ✅ | ✅ |
| **位点跨节点可用** | ❌ 本地磁盘 | ✅ 服务端存储 | ✅ | ✅ checkpoint table | ✅ offset topic |
| **位点写入 fencing** | ❌ | ✅ | ✅ | ✅ | ✅ epoch |
| **位点历史 / 重置回溯** | ❌ 单行覆盖 | ✅ 可重置位点重跑 | ⚠️ | ✅ | ✅ 可改 offset |
| **位点有效性在线巡检** | ⚠️ 仅启动预检一次 | ✅ | ✅ | ✅ | ⚠️ |
| 位点可观测 | ⚠️ 只能问 agent | ✅ 控制台 | ✅ | ✅ | ✅ |

### G1【P0·致命】跨机接管 = 静默丢数据

V8 的注释写着"接管方走既有的崩溃恢复路径（从各自 checkpoint 续传）"——这个前提**只在同一台机器上成立**。
换一台 agent 接管时 `files/<taskId>/` 是空的，于是：

```
AbstractTaskExecutor.initMysqlCheckpoint:454   loadCheckpoint() → null
                                    :474       getCurrentPositionFromSource()   ← 取"源库此刻的位点"
                                    :476       saveCheckpoint(currentPosition)
```

**崩溃时刻到接管时刻之间的全部变更被直接跳过**，且不报错、不告警、进度条一路 100%。
这是全平台目前最严重的一处静默数据丢失：故障转移越成功，丢得越干净。

### G2【P0】位点写入无 fencing

`workflows.lease_epoch` 已经有了，但**位点面完全没用上**。网络分区下老 agent 未死、
子进程还在跑，它会继续写自己的位点文件；一旦老 agent 的目录被将来的"位点上卷"纳入，
或运维把老机器目录同步回来，旧位点就会覆盖新位点——重放到旧点是安全的，
**但如果覆盖发生在接管方已经跑过头之后，就是真丢**。

### G3【P1】位点无历史，不可重置/回溯

三个 H2 表全是 `MERGE INTO ... WHERE id=1`：单行覆盖，写完即失去上一版。
误操作（错误的跳过裁决、错误的重置）之后只能整任务重做全量。
能力对比表里的 PITR ❌ 就卡在这里。

### G4【P1】无统一模型，无单调守卫

10 个写入点各写各的，没有任何一处校验"新位点是否 ≥ 旧位点"。
位点回退不会报错，只会表现为"数据重复/顺序错乱"，事后极难归因。

### G5【P1】位点有效性只在启动时校验一次

`capture.position.precheck.enabled` 只在 capture 启动时跑一次（`SHOW BINARY LOGS` /
`pg_replication_slots` / `V$ARCHIVED_LOG`）。任务**运行中**源端日志被清理、复制槽被删、
Mongo resume token 超出 oplog 窗口、TiCDC gc 掉——全部要等到下一次重启才炸，
而那时已经无法恢复，只能重做全量。

### G6【P2】位点不可观测于后端

`CheckpointVisualizationService` 直读本地文件、后端 `/{id}/checkpoint` 代理 agent。
**agent 一挂，位点就完全看不见**——恰恰是最需要判断"还能不能续"的时刻。

---

## 3. 不变量（实现时逐条锁死）

| # | 不变量 | 违反后果 |
|---|---|---|
| **I1** | 位点绝不越过尚未持久化的数据：先 flush/commit 数据 → 再推位点 | 丢数据（订阅链路踩过） |
| **I2** | 位点单调不回退；回退只允许经"显式重置"路径，且必须留审计 | 重复/乱序，且无从归因 |
| **I3** | 位点写入必须携带 `(agent_id, lease_epoch)`，低 epoch 一律拒绝 | 脑裂互相覆盖 |
| **I4** | 本地位点是热路径的唯一权威；中心库不可用不得阻塞或失败任务 | 元数据库抖动 = 全平台停摆 |
| **I5** | 中心位点**必须落后或等于**本地位点，绝不允许上卷"尚未落到本地的位点" | 接管方从超前的位点续 = 丢数据 |
| **I6** | 主备倒换 / 重做全量必须**同时**作废本地与中心位点 | 旧源 GTID 拿到新源 → 从 binlog 最开头整段重放 |
| **I7** | 中心有位点而本地没有（= 接管场景）时，回灌失败必须 fail-stop，**不得**退化成"取源库当前位点" | 就是 G1 那条静默丢数据 |

---

## 4. 方案

### 4.1 三层结构

```
        子进程（capture / extract / increment / full / mongo / es / redis / subscribe）
              │  ① 热路径：只写本地，原子写，不感知中心库
              ▼
   ┌──────────────────────────────────────────────┐
   │ 本地层  LocalCheckpointStore（migration-common）│  files/<taskId>/checkpoint/positions/<stage>.json
   └──────────────────────────────────────────────┘
              │  ② agent 周期扫描（默认 3s），只读不写
              ▼
   ┌──────────────────────────────────────────────┐
   │ agent 层  CheckpointUploader / CheckpointHydrator │  带 (agent_id, lease_epoch) 做 fencing
   └──────────────────────────────────────────────┘
              │  ③ JDBC（复用 agent.properties 的 mysql.db.*，与 AgentRegistryService 同一条路）
              ▼
   ┌──────────────────────────────────────────────┐
   │ 中心层  task_checkpoints + task_checkpoint_history │  元数据 MySQL
   └──────────────────────────────────────────────┘
```

**为什么不让子进程直连元数据库**：① 热路径每事件一次远程写会把 apply 吞吐打回原形；
② 元数据库抖动会直接变成任务失败，违反 I4；③ 8 个模块都要发一份元数据库凭据，
攻击面和配置面都翻倍。上卷放在 agent 侧，还能顺带复用它已经持有的 `lease_epoch`。

**上卷落后是安全方向**：中心位点比本地旧 → 接管方多重放一小段 → 幂等应用兜底。
反过来才致命，所以 I5 要在 uploader 里用"只读已落盘文件"这条硬约束保证（绝不读内存里的当前位点）。

### 4.2 统一位点模型

```java
// migration-common: com.migration.common.position.CheckpointRecord
taskId        String      // MERGE_LEG 是独立 workflow 行，天然一 leg 一条，不会互相覆盖
stage         Stage       // CAPTURE | EXTRACT | APPLY | FULL | SUBSCRIBE
streamKey     String      // 单流固定 "-"；订阅按 topic-partition 时才有别的值
engine        String      // mysql | postgresql | oracle | tidb | mongodb | redis | elasticsearch
kind          Kind        // BINLOG_FILE_POS | GTID_SET | LSN | SCN | TSO | RESUME_TOKEN | REPL_OFFSET | SEQNO | KAFKA_OFFSET
payload       Properties  // 引擎原生位点，原样保存，不做归一（归一必然丢信息）
monotonicKey  long        // 唯一被"比较"的字段，见 4.3
sourceTs      long        // 该位点对应的源端事件时间，算 RPO 用
updatedAt     long        // JVM 侧时间戳
```

`payload` 原样、`monotonicKey` 可比 —— 这个拆分是关键：GTID 集、resume token 这类位点
**本身不可比**，硬要归一成一个数就会丢掉续传所需的信息；而单调守卫又必须有个可比的标量。
两者分开存，各司其职。

**载体格式用 properties 而不是 JSON**（实施时的修正）：migration-common 被所有模块依赖，
往它里面加 JSON 依赖会把 Spring BOM 的版本仲裁牵进每一个子进程（mongo 链路踩过驱动降级混包）。
外层记元信息、`pos.*` 前缀记引擎原生位点的两层 properties 零依赖，且能原样 round-trip
GTID 集里的 `:` `,` 和 resume token 的 JSON 文本（已有单测锁死）。
中心库 `payload` 列存的就是 `pos.*` 那一层序列化出来的文本。

### 4.3 monotonicKey 映射

| 引擎 / 位点 | monotonicKey |
|---|---|
| MySQL binlog | `binlogFileSeq << 32 \| position`（`mysql-bin.000123` → 123） |
| MySQL GTID 集 | 不可比 → 用同一时刻的 file:pos 兜底；无 file:pos 时置 0 并**跳过单调校验**（记 warn） |
| PostgreSQL LSN | `(segment << 32) \| offset`，复用 `CheckpointManager.parseLsnToLong:288` |
| Oracle SCN / TiDB TSO | 数值本身 |
| Mongo resume token | `clusterTime.seconds << 32 \| inc` |
| Redis repl offset | offset 本身（replId 变化 = 换主，视为重置） |
| THL seqno / apply seqno | seqno 本身 |
| Kafka offset | `partition << 48 \| offset`（订阅按 topic-partition 一行） |

### 4.4 中心表（Flyway V13）

```sql
CREATE TABLE task_checkpoints (
    task_id        VARCHAR(64)  NOT NULL,
    stage          VARCHAR(16)  NOT NULL,
    stream_key     VARCHAR(128) NOT NULL DEFAULT '-',  -- 订阅按 topic-partition、未来多流用；单流固定 '-'
    engine         VARCHAR(32)  NOT NULL,
    kind           VARCHAR(32)  NOT NULL,
    payload        TEXT         NOT NULL,
    monotonic_key  BIGINT       NOT NULL DEFAULT 0,
    source_ts      DATETIME(3)  DEFAULT NULL,
    agent_id       VARCHAR(64)  NOT NULL,
    lease_epoch    INT          NOT NULL DEFAULT 0,   -- fencing token
    updated_at     DATETIME(3)  NOT NULL,             -- JVM 侧绑定，禁止 SQL NOW()
    PRIMARY KEY (task_id, stage, stream_key),
    INDEX idx_ckpt_updated (updated_at)
) ENGINE=InnoDB;

CREATE TABLE task_checkpoint_history (  -- 采样 + 重置审计
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL, stage VARCHAR(16) NOT NULL, stream_key VARCHAR(128) NOT NULL DEFAULT '-',
    payload TEXT NOT NULL, monotonic_key BIGINT NOT NULL DEFAULT 0,
    source_ts DATETIME(3) DEFAULT NULL, recorded_at DATETIME(3) NOT NULL,
    reason VARCHAR(32) NOT NULL,          -- SAMPLE | RESET | FAILOVER | SNAPSHOT
    operator VARCHAR(64) DEFAULT NULL,    -- RESET 时的操作人
    INDEX idx_ckpt_hist (task_id, stage, recorded_at)
) ENGINE=InnoDB;
```

**写入的守卫压在 SQL 里**，不靠应用层"先查再写"（并发下中间有窗口）：

```sql
UPDATE task_checkpoints SET payload=?, monotonic_key=?, ..., lease_epoch=?, updated_at=?
 WHERE task_id=? AND stage=? AND stream_key=?
   AND (? > lease_epoch OR (? = lease_epoch AND ? >= monotonic_key));
-- 0 行 → 该行还不存在，INSERT ... ON DUPLICATE KEY UPDATE task_id=task_id（撞并发插入即视为未写成）
```

两条规则：

- **更高 epoch 无条件放行**。它是租约的合法新主，位点本来就是从中心库回灌走的；即便偏旧
  也只意味着多重放（安全方向），而拦住它会让位点**永久卡死**。
- 同 epoch 内 `>=` 才放行；`monotonic_key = 0`（不可比形态）时守卫自动降级为不校验。

判"被拒"不能只看 affected rows：UPDATE 因"值一个字节没变"返回 0 行、
INSERT 又撞主键，这跟真的被守卫拒绝在返回值上长得一样（而且 affected rows 的语义还取决于
驱动的 `useAffectedRows`）。所以 0 行时补一次 SELECT 判断当前行是否本来就接受这次写入，
再决定是否计入 `checkpoint_reject_total` 并告警——否则会刷一屏假告警。

### 4.5 启动决策表（回灌）

任务启动 / 接管时，`CheckpointHydrator` 在**拉起任何子进程之前**执行：

| 本地 | 中心 | 判定 | 行为 |
|---|---|---|---|
| 有 | 无 | 首次启用中心位点，或上卷还没跑过 | 用本地；**立刻**同步上卷一次 |
| 有 | 有 | 同机重启 | 取 `monotonicKey` 大者（正常本地 ≥ 中心；中心大 = 本地被回滚/损坏，用中心） |
| 无 | 有 | **跨机接管 / 本地目录被清** | 回灌中心 → 本地，成功后才启动子进程 |
| 无 | 无 | 真·首启 | 现有逻辑（取源库当前位点），并**在启动子进程前**同步写入中心一行 |

最后一行是 G1 的补丁核心：首启位点必须**当场**进中心库，
否则"首启后 3 秒内崩溃 + 接管"仍会落进"中心无行 → 又取源库当前位点"的丢数据窗口。

第三行的失败处理即 **I7**：回灌失败（元数据库不可达 / payload 损坏）→ `E3014` fail-stop，
**绝不**继续走"取源库当前位点"。宁可任务起不来等人来看，也不能静默丢一段数据。
（读中心库失败会重试 3 次、间隔 1s：这一步的失败会挡住任务启动，不能被一次网络抖动带偏。）

### 4.5.1　只回灌"源端坐标"，seqno 类位点一律丢弃【实施时发现的关键修正】

原设计想当然地把所有 stage 的位点一视同仁地回灌，**这会造成比丢数据更严重的丢数据**：

> THL 的 seqno 是 `thl_output/` 里的**本机文件坐标**。接管方的 THL 目录是空的、会从 0 重新编号。
> 把"已应用到 seqno=5000"灌到一台 THL 从 0 开始的机器上，increment 的
> `readEventAfter(applyCursor())` 会把新产出的 seqno ≤ 5000 的事件**全部跳过**。

所以回灌按**坐标是否与机器无关**来筛：

| kind | 可回灌 | 理由 |
|---|---|---|
| `BINLOG_FILE_POS` / `GTID_SET` / `LSN` / `SCN` / `TSO` / `RESUME_TOKEN` / `REPL_OFFSET` | ✅ | 源端自己的坐标，换台机器照样定位得到 |
| `SEQNO` / `KAFKA_OFFSET`（EXTRACT / APPLY / SUBSCRIBE） | ❌ | 本机 THL 文件坐标，换台机器就是错的 |

接管方只拿源端位点，整条 THL 管线从那里重新产出，seqno 从 0 自洽重排——
这正是"至少一次 + 幂等应用"该有的样子。APPLY/SUBSCRIBE 的位点仍然上卷，
但只用于**可观测**（agent 挂了也能在后端看到应用到哪了），永不参与回灌。

中心库里只有 seqno 类位点、没有任何源端坐标时，判 `FAILED` 而不是放行——
那种情况下续不上，只能让人决定重做全量，绝不能偷偷从源库当前位点开始。

### 4.5.2　回灌要落到"老载体"，且挂在所有执行器的入口

子进程的续传读路径一个字都没改，所以回灌必须写回它们认得的文件：
`capture_position.properties`、agent 侧 H2 起始位点（灌上它，`initXxxCheckpoint`
就完全不会去碰源库）、`mongo_resume_token.json`、`elastic_binlog_position.json`。
ES 与 MySQL capture 的位点形态完全一样（binlog file+pos），靠 payload 里的
`carrier` 标记区分该写哪个文件。

挂钩点是 `AbstractTaskExecutor.run()` 而**不是** `initCheckpoint()`：
Mongo/ES/Redis 这些单进程链路根本不调 `initCheckpoint()`，只在那里挂会让它们在跨机接管时
悄悄从"源库当前位点"重来——正是本设计要消灭的那类故障。

### 4.6 时序（跨机接管）

```
agent-A                              元数据库                        agent-B
  │ capture 落盘位点 (5s)                │                                │
  │ uploader 上卷 (3s) ────────────────►│ task_checkpoints(epoch=7)      │
  │ ✗ SIGKILL                           │                                │
                                        │◄── 租约过期，改派 epoch=8 ─────│
                                        │                                │ hydrate: 读中心 → 写本地
                                        │                                │ 起 capture（从回灌位点续）
  │（老进程若未死）上卷 epoch=7 ────────►│ 被 fencing 拒绝                │ 上卷 epoch=8 ✅
```

**接管重放窗口 = 上卷间隔 × 源端速率**（默认 3s）。这是"至少一次"的重放量，不是丢数据量；
把它调小只减少重放，调大不会丢——**丢数据只可能发生在位点跑到数据前面时**（I1/I5）。

### 4.7 位点在线巡检（G5）

agent 侧 `CheckpointHealthChecker` 每 60s 对**运行中**任务校验当前位点是否仍在源端保留期内，
复用 capture 启动预检那套查询（`SHOW BINARY LOGS` / `pg_replication_slots` / `V$ARCHIVED_LOG` /
oplog 窗口 / TiCDC gc-ttl）。剩余保留期低于阈值（默认 30min）→ 告警 `E3006` 的**预警版**，
让人有时间延长保留期，而不是等重启时才发现已经无法恢复。

### 4.8 位点重置 / 回溯（G3）

后端 `POST /api/workflows/{id}/checkpoint/reset`，任务必须处于 PAUSED/FAILED：
- 入参：`{stage, target: {type: "HISTORY_ID"|"TIMESTAMP"|"RAW", value}}`
- 从 `task_checkpoint_history` 找到目标位点 → 写审计行（`reason=RESET, operator=<user>`）→
  更新 `task_checkpoints`（**这是唯一允许 monotonic 回退的路径**）→ 清本地位点 → 下次启动自动回灌。
- 采样策略：uploader 每 `checkpoint.history.sample.interval.s`（默认 300s）额外写一条 history，
  保留 `checkpoint.history.retention.hours`（默认 72h），由后端巡检清理。

---

## 5. 分批实施

### B1　统一位点内核 + 本地存储归一（无行为变化）　【已实施】

| 文件 | 改动 |
|---|---|
| [CheckpointRecord.java](../migration-common/src/main/java/com/migration/common/position/CheckpointRecord.java) | 新增，4.2 的模型（两层 properties 载体） |
| [MonotonicKey.java](../migration-common/src/main/java/com/migration/common/position/MonotonicKey.java) | 新增，4.3 的折算；折不出来一律 `UNKNOWN` |
| [LocalCheckpointStore.java](../migration-common/src/main/java/com/migration/common/position/LocalCheckpointStore.java) | 新增，`files/<taskId>/checkpoint/positions/<stage>.properties`，基于 `AtomicFileWriter`；带节流；从老 `capture_position.properties` 反推（升级前就在跑的任务不必等重启就能上卷） |
| [CapturePositionStore.java](../migration-common/src/main/java/com/migration/common/position/CapturePositionStore.java) | 加一个带 taskId 的 `save` 重载：四种 capture 都汇到这个出口，统一载体只需在这里挂一次 |
| capture ×4 / increment apply ×3 / mongo / es / subscribe | 在原有落盘**之后**追加一次统一载体写入，原落盘时机一个字没动（I1 相关的已修缺陷不回归） |

单测：`MonotonicKeyTest`（7 例，序关系与 UNKNOWN 降级）、`LocalCheckpointStoreTest`（7 例，
GTID/JSON 原样 round-trip、节流、四种老载体反推、残缺记录当作无位点）。

### B2　中心持久化 + 回灌 + fencing【解决 G1/G2】　【已实施】

| 文件 | 改动 |
|---|---|
| [V13__task_checkpoints.sql](../java-backend/src/main/resources/db/migration/V13__task_checkpoints.sql) | 新增两张表 |
| [CentralCheckpointStore.java](../migration-agent/src/main/java/com/migration/agent/checkpoint/CentralCheckpointStore.java) | 新增，4.4 的写入守卫 + 读取 + 作废 + 取 `lease_epoch` |
| [CheckpointUploader.java](../migration-agent/src/main/java/com/migration/agent/checkpoint/CheckpointUploader.java) | 新增，每 3s 只读**已落盘**文件上卷（I5）；指纹去重；失败 best-effort（I4）；停机前补一拍 |
| [CheckpointHydrator.java](../migration-agent/src/main/java/com/migration/agent/checkpoint/CheckpointHydrator.java) | 新增，4.5 决策表 + 4.5.1 的可回灌筛选 + 4.5.2 的老载体还原 + 首启位点立刻进中心库 |
| [CheckpointCleaner.java](../migration-agent/src/main/java/com/migration/agent/checkpoint/CheckpointCleaner.java) | 新增，倒换/重做全量时一处收口清掉统一载体 + 中心行 + 上卷缓存 |
| [AbstractTaskExecutor.java](../migration-agent/src/main/java/com/migration/agent/thread/AbstractTaskExecutor.java) | `run()` 入口先回灌（覆盖全部执行器）；`FAILED` 即 fail-stop；三处"取源库当前位点"后立刻 `publishInitialPosition` |
| [AgentMain.java](../migration-agent/src/main/java/com/migration/agent/AgentMain.java) | 初始化三件套；停机顺序：先补一拍上卷 → 再释放租约（顺序反了会被新主的 epoch 拒掉） |
| [SyncErrorCodeMapper.java](../migration-agent/src/main/java/com/migration/agent/util/SyncErrorCodeMapper.java) / [SyncErrorCode.java](../java-backend/src/main/java/com/synctask/entity/SyncErrorCode.java) | 新增 `E3014 位点回灌失败`（关键词映射要排在泛化的 checkpoint 规则之前） |
| [FailoverService.java](../migration-agent/src/main/java/com/migration/agent/service/FailoverService.java) / `AgentMain.cleanupFailoverArtifacts` | 清本地位点时**一并删中心行**并留 `reason=FAILOVER` 的 history（I6） |

单测：`CentralCheckpointStoreGuardTest`（7 例，跑在内存 H2 上：fencing / 单调 / 高 epoch 放行 /
不可比形态降级 / 未变更不算被拒 / 作废留档）、`CheckpointHydratorTest`（8 例，决策表全覆盖，
含 **seqno 绝不回灌** 与 **判不出就 fail-stop**）、`FailoverPositionCleanupTest` 扩展。
`./test.sh engine` 与 `./test.sh` 全绿。

### B3　位点历史 / 重置 / 可观测【解决 G3/G6】　【已实施】

| 文件 | 改动 |
|---|---|
| [V14__checkpoint_reset.sql](../java-backend/src/main/resources/db/migration/V14__checkpoint_reset.sql) | `task_checkpoints` 加 `reset_at`（见下方"为什么非要这一列"） |
| [CheckpointCentralService.java](../java-backend/src/main/java/com/synctask/service/CheckpointCentralService.java) | 中心位点的读 / 历史 / 重置 / 降级视图 / 采样清理（JdbcTemplate，不引 JPA 实体避免 `ddl-auto: validate` 的类型仲裁） |
| [WorkflowController.java](../java-backend/src/main/java/com/synctask/controller/WorkflowController.java) | `GET /{id}/checkpoint/history`、`POST /{id}/checkpoint/reset`（带审计） |
| [WorkflowService.java](../java-backend/src/main/java/com/synctask/service/WorkflowService.java) | `/{id}/checkpoint` 在 agent 不可达时**降级读中心表**，响应带 `source=central/degraded=true` |
| [CheckpointHydrator.java](../migration-agent/src/main/java/com/migration/agent/checkpoint/CheckpointHydrator.java) | 认 `reset_at`：中心比本地新即**强制覆盖本地**（`RESET_APPLIED`） |
| [admin-dashboard.js](../admin-dashboard.js) | 降级横幅 + 位点历史表 + "重置到此" |

**为什么非要 `reset_at` 这一列**：重置是在后端做的，而位点的实际使用方是 agent 本地的老载体。
只改中心库，agent 一看"本地有位点"就走同机重启分支，重置永远不生效。有了 `reset_at`，
回灌判据多一条"中心的重置时刻比本地新 → 强制覆盖"，而且回灌后本地记下同一个时刻，不会反复覆盖。
判据用**时刻**而不是位点大小：重置几乎总是往回调，用大小判会跟单调守卫的方向打架。

**清理只删 `SAMPLE`**：`RESET`/`FAILOVER` 是审计——"位点什么时候被谁动过"往往很久以后才被问起，
跟着采样数据一起删掉就再也查不到了。

单测：`CheckpointCentralServiceTest`（8 例，内存 H2：跑着的任务不许重置 / 按 id 与按时间点重置 /
无可用历史要报错 / 行缺失时补插 / 清理保留审计 / 降级视图必须自报是降级数据）。

### B4　在线巡检 + Redis 部分重同步【解决 G5 + 载体 #10】　【已实施】

| 文件 | 改动 |
|---|---|
| [RetentionStatus.java](../migration-common/src/main/java/com/migration/common/position/RetentionStatus.java) | `binlog_output/retention_metric` 的读写口（单行文本，跨进程零依赖） |
| [MySQLBinlogCapture](../migration-capture/src/main/java/com/migration/capture/MySQLBinlogCapture.java) / [PostgresWalCapture](../migration-capture/src/main/java/com/migration/capture/PostgresWalCapture.java) / [OracleRedoCapture](../migration-capture/src/main/java/com/migration/capture/OracleRedoCapture.java) | 每 60s 巡检一次保留期余量 |
| [CheckpointVisualizationService](../migration-agent/src/main/java/com/migration/agent/service/CheckpointVisualizationService.java) / dashboard | 把巡检结果接进位点视图，WARN/LOST 出横幅 |
| [RedisSyncMain](../migration-redis/src/main/java/com/migration/redis/RedisSyncMain.java) | 落 `replid+offset` 到统一位点；重启先试 `PSYNC` 部分重同步 |

**巡检放在 capture 里而不是 agent 里**：预检那套引擎相关的查询本来就在 capture，
而且 capture 手里已经有源库连接与当前位点；放到 agent 还要再发一份凭据过去。

**判据一律"越小越危险"**：MySQL = 位点文件之前还留着几个 binlog；Oracle = 当前 SCN 之前还留着几个
redo/归档；PG 没有等价的"还剩几个"，改用 `wal_status`（PG13+ 的权威答案：`lost`→LOST、
`unreserved`→WARN），更老的版本如实记 UNKNOWN 而不是猜一个。
**只预警不阻断**：位点真丢了由启动预检的 E3006 拦；把正在正常同步的任务打成 FAILED 更糟。

**Redis 之前压根没有增量位点**：进程一重启就整库重来（清空目标 + 全部键重新 RESTORE）。
现在重启先试部分重同步，源端 backlog 覆盖得住就一个键都不用重搬；覆盖不住时源端回 `+FULLRESYNC`，
走的还是原来那条路（`PreRdbSyncEvent` → 清目标库 → 全量），所以失败没有额外代价。
只在**确实进入过增量**时才尝试——统一位点文件是在 `applyCommand` 里写的，它存在本身就等于
"上次跑到过增量阶段"，不必再引入别的标记。部分重同步成功时不会有 RDB 阶段，
所以 `phase` 要在发起前就摆成 `INCREMENT`，否则进度文件一直显示 FULL、agent 判活跟着错。

---

## 6. 配置项

```properties
# —— agent.properties ——
checkpoint.central.enabled=true                  # 关掉即完全回到本地位点行为（单机部署可关）
checkpoint.central.upload.interval.ms=3000       # 上卷间隔 = 接管重放窗口
checkpoint.central.upload.batch.size=50
checkpoint.hydrate.fail.stop=true                # I7；置 false 回到旧的"取源库当前位点"（仅排障用，会丢数据）
checkpoint.history.sample.interval.s=300
checkpoint.history.retention.hours=72
checkpoint.health.check.interval.ms=60000
checkpoint.health.retention.warn.minutes=30
```

---

## 7. 尖角与坑（实施时必须带着这张表）

1. **元数据库时间戳绝不用 SQL `NOW()`**：容器 UTC、JVM 本地时区，差 8 小时会让"位点永远看起来是新鲜的/永远过期"。一律 JVM 侧 `setTimestamp` 绑定（V8 的故障转移就栽在这上面）。
2. **上卷只读已落盘文件**（I5）。任何"顺手把内存里更新的位点也传上去"的优化都会制造丢数据。
3. **回灌必须在拉起子进程之前**，且要和 P0-3 的任务级文件锁配合：先拿锁、再回灌、再启动，否则老进程可能在回灌之后又把旧位点写回本地。
4. **倒换清位点要清到中心**（I6）。`FailoverCleanupInvariantTest` 现在只锁本地目录，必须扩。
5. **GTID 集不参与单调比较**（4.3），但它必须**整体替换**，不能与 file:pos 各自独立更新——两者是同一时刻的快照，混用会定位到不存在的点。
6. **MERGE_LEG 各 leg 位点独立**，父任务的整体 RPO 只能取 max，中心表不要试图给父任务合成一个"全局位点"（不存在全局一致点，见分片设计 §425）。
7. **H2 默认 500ms 延迟刷盘**：#2/#3 两个 H2 库的位点在崩溃时最多丢 500ms 的推进——方向安全（重放），但 B1 归一后应以 `LocalCheckpointStore` 的原子写为准。
8. **apply 侧每事件一次 MERGE** 是当前吞吐的隐性成本；B1 并行写 JSON 时不要变成"每事件两次 fsync"，新载体按"每 N 事件 / 每 T 毫秒 + 事务边界强制"落盘，且**必须在提交之后**（I1）。
9. **首启位点要当场进中心库**（4.5 最后一行），否则留下一个几秒的丢数据窗口。
10. **改 migration-common 后 fat jar 要 `clean install`**，否则子进程用的是旧类；
    只跑 `mvn -pl migration-agent test` 也会拿到仓库里的旧 common，必须带 `-am`（或直接 `./test.sh engine`）。
11. **seqno 类位点绝不可回灌**（4.5.1）——这是实施中发现的最大一处设计错误，
    照原设计做出来会制造一个比"重放"严重得多的静默丢数据。
12. **回灌挂在 `run()` 而不是 `initCheckpoint()`**（4.5.2）：Mongo/ES/Redis 单进程链路
    根本不调 `initCheckpoint()`。
13. **停机顺序**：先补一拍上卷，再 `agentRegistry.stop()` 释放租约。反过来的话，
    租约一放后端立刻改派，这一拍上卷会撞上新主更高的 epoch 被 fencing 拒掉。
14. **别拿 affected rows 判"被守卫拒绝"**（4.4）：值没变的 UPDATE 与真的被拒在返回值上一模一样，
    而且语义还取决于驱动的 `useAffectedRows`。要补一次 SELECT 才分得清，否则告警全是假的。
15. **`checkpoint.central.enabled=true` 是默认值**，意味着 agent.properties 里的
    `mysql.db.*` 从"只影响集群功能"升级成"影响任务能否启动"。内网 IP 遗留这类老问题
    会从"任务卡 PENDING"变成"任务报 E3014"——错误文案里已经直接指向这个配置。

---

## 8. 验收判据

B1~B4 的单测已全绿（见 §5）。故障注入脚本
[checkpoint_durability.py](../test_scripts/fault_injection/checkpoint_durability.py) 已写好，
**尚未在本机执行过**——它会 SIGKILL agent-A 并重建 `ckpt_src/ckpt_tgt` 两个库，
需要先用新 jar 重启后端与 agent（后端启动时 Flyway 会应用 V13/V14）：

```bash
./build.sh && ./start.sh && python3 test_scripts/fault_injection/checkpoint_durability.py
```

它真的会起**第二个 agent 实例**（独立工作目录 + 空的 `files/` + 链过去的模块 jar + 8084 端口 +
独立 `MIGRATION_AGENT_ID`），因为"另一台机器"的本质就是那个空目录；用同一个 agent 重启测不出这条。

判据：

| # | 用例 | 判据 |
|---|---|---|
| 1 | 同机重启回归 | 起始位点 == 落盘位点；重放行变更 0 条（`position_resume.py` 的既有判据不得回归） |
| 2 | **跨机接管** | agent-A 跑到一半 SIGKILL → agent-B 接管 → 两端数据 xor 相等；重放量 ≤ 上卷间隔 × 速率 |
| 3 | 接管对照组 | `checkpoint.central.enabled=false` 时**必须能复现丢数据**（证明用例真的在测这条路径） |
| 4 | fencing | 伪造 `lease_epoch-1` 的上卷被拒，中心行不变，`checkpoint_reject_total` +1 |
| 5 | 单调守卫 | 写入更旧的 `monotonic_key` 被拒 |
| 6 | 回灌 fail-stop | 中心有行 + 元数据库不可达 → 任务 FAILED 且错误码 `E3014`，**不得**出现"取源库当前位点"日志 |
| 7 | 倒换 | 倒换后中心行被删且 history 留 `reason=FAILOVER` |
| 8 | 重置回溯 | 重置到 T-5min 的 history 位点后重跑，两端最终收敛（用带单调 `ver` 列的判据，别用 ±1 交替转账） |
| 9 | 保留期预警 | 人为 `PURGE BINARY LOGS` 逼近位点 → 60s 内出现预警 |
| 10 | 全链路覆盖 | 8 条链路（mysql/pg/oracle/tidb/mongo/es/redis/subscribe）各跑一遍 2 号用例 |

---

## 9. 范围边界（本设计不做）

- **不改**各链路现有的落盘时机与 fail-stop 语义（I1 相关缺陷是一条条修出来的，不重构）。
- **不做**位点的跨任务/跨集群复制，也不做异地多活的位点仲裁。
- **不把**元数据库变成热路径依赖（I4）——中心库挂掉，任务照跑，只是接管能力退化回今天的水平。
- **不做**全量表级断点（载体 #6）的中心化：全量重跑是幂等的、代价可接受，收益不足以抵掉复杂度。
