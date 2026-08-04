# 全量批量装载通道 + 全量一致性快照（2026-08-03）

对标大厂 DTS 的两项全量能力，覆盖**全部同步任务与灾备任务**：

1. **批量装载通道**：每条链路的全量写侧都走批量装载，档位可选、可降级、可观测；
2. **一致性快照**：给"全量结束点 = 源端某个位点"这个语义，覆盖能做到的每一条链路。

---

## 一、动手前的现状

| 链路 | 全量写通道 | 快照 |
|---|---|---|
| RDBMS 同步/灾备（migration-full） | 驱动语句重写 + 批失败按行重放 | 有（NONE/GTID_ONLY/CONSISTENT） |
| MongoDB 同步/灾备 | bulkWrite 硬编码 1000/批 | 无（只有 change stream 起始 token） |
| MySQL→ES | _bulk 硬编码 1000/批 | 无 |
| Redis→Redis | **逐 key RESTORE，一个 key 一次 RTT** | 天然有（RDB），但没记录 |
| Kafka 订阅 | 无全量阶段 | 不适用 |

致命的一条：`ConfigService` 里 grep 不到任何 `migration.full.bulk.*` / `migration.full.snapshot.mode`——
这两组键**从来没有人写过**，引擎只能吃编译期默认值，`CONSISTENT` 档位在产品上实际不可达。

---

## 二、批量装载通道

### 统一抽象（migration-common `com.migration.common.bulk`）

- `BulkLoadOptions`：一组键管全部引擎——`migration.full.bulk.{enabled,mode,rows,bytes}`；
- `BulkLoadChannel<T>` / `JdbcBulkChannel`：统一的 `add / isFull / flush → {ok, fail}` 契约；
- `BulkLoadStats`：行数、批数、**批失败数、重放行数**（批失败率高 = 吞吐悄悄退化成逐条写，只看行速率看不出来）。

**flush 的语义是"尽力送达并如实计数"**，不是全或无：批级失败一律降级为逐条重放，
只把真正失败的条目计入 fail。整批抛异常等于把已经写进目标端的行也算没写。

### 各目标端的档位

| 档位 | 目标端 | 做法 |
|---|---|---|
| `AUTO`（默认） | 全部 | JDBC 走语句重写；Mongo/ES/Redis 走各自原生批量 API。**等于升级前的行为** |
| `BATCH` | JDBC | `rewriteBatchedStatements` / `reWriteBatchedInserts` |
| `COPY` | PostgreSQL | `COPY ... WITH (FORMAT binary)` |
| `DIRECT_PATH` | Oracle | `INSERT /*+ APPEND_VALUES */` 直接路径装载 |

新增的三条：

- **PG 二进制 COPY**（`JdbcCopyChannel` + `PgBinaryCopyEncoder`）。选二进制而非文本 COPY，是因为
  文本 COPY 要把值渲染成字符串再由服务端解析，等于绕开类型绑定——增量链路正是因为文本管道踩过
  5 类值保真缺陷才收敛到类型化写入。**两道防线保证编码器的完备性只影响性能、不影响正确性**：
  目标表有任何一列不在支持类型集内 → 开通道时就拒绝、降级 BATCH；运行时遇到不认识的 Java 值类型
  → 整批回退到 INSERT 重放。
- **Oracle direct-path**。只在**独占写入**时启用：直接路径装载持表级排他锁，单表 PK 分片并行下
  几个 worker 写同一张表只会互相阻塞（不报错，只是悄悄变慢）。逐行重放语句故意**不带**提示。
- **Redis pipeline**（`RedisRestoreChannel`）。原先每个 key 一次 RTT，键多的库时间几乎全花在网络等待上。
  `syncAndReturnAll` 拿逐条结果，个别 key 失败只计这几个 key。逻辑库切换前必须先 flush ——
  pipeline 与 SELECT 共用同一条连接。

### 三条链路的其它补强

- **字节阈值**（Mongo/ES/JDBC 均加）：只按条数攒批时，宽行会顶穿 Mongo 的 48MB 消息上限、
  ES 的 `http.max_content_length`、MySQL 的 `max_allowed_packet`，且报错与批大小无关、极难定位。
- **ES 背压重试**：429（`es_rejected_execution_exception`）表达的是"现在太忙，稍后再来"，
  不是数据错误。原实现把它与真失败同等对待，目标端一忙就整任务失败。现在 HTTP 层 429/503 重投整批，
  条目级 429/503 只重投被拒的条目（已成功的不能重投）。
- **ES 装载窗口**：全量期间 `refresh_interval=-1` + `number_of_replicas=0`，结束（**含失败路径**）恢复并 `_refresh`。
- **Mongo insertMany 快路径**：目标集合为空时用 `InsertOneModel`（省掉 ReplaceOne 每条的 `_id` 匹配）；
  判空用 `countDocuments(limit 1)` 而不是 `estimatedDocumentCount()`——后者读元数据估算值，
  非正常关闭后可能报 0，据此走 insert 会让续搬的每条都撞重复键。重复键（11000）按"目标端已有"计成功。

---

## 三、一致性快照：哪些链路能做，怎么做

判据两条：源端能否提供 (a) 跨查询/跨会话复用的读一致点，(b) 能与增量位点对齐的坐标。

| 源端 | 机制 | 锁 | 状态 |
|---|---|---|---|
| MySQL | FTWRL + 预建会话池 `START TRANSACTION WITH CONSISTENT SNAPSHOT` | 全局读锁（短） | 已有 |
| PostgreSQL | `pg_export_snapshot()` | 无 | 已有 |
| Oracle | 闪回 `AS OF SCN` | 无 | 已有 |
| **TiDB** | `AS OF TIMESTAMP TIDB_PARSE_TSO(tso)` 历史读 | **无**（MVCC） | 新增 |
| **MongoDB** | 快照会话 `ClientSessionOptions.snapshot(true)`（5.0+） | 无 | 新增 |
| **MySQL→ES** | 复用 MySQL 快照（全量单连接单线程） | 同 MySQL | 新增 |
| **Redis** | PSYNC 的 RDB 本身就是某个复制偏移上的一致镜像 | 无 | 新增（只补位点暴露） |
| Kafka 订阅 | 无全量阶段 | — | 不适用 |

### 两条新链路的"历史窗口"问题与降级

TiDB 与 MongoDB 的快照都依赖历史版本保留，而两者的**默认值都撑不住一次正经全量**
（TiDB `tikv_gc_life_time` 默认 10 分钟；MongoDB `minSnapshotHistoryWindowInSeconds` 默认 300 秒）。
与其让全量跑到一半失败，不如建立时就检查并**降级为只记位点**，同时把该调的参数打进日志——
这与本模块既有的"快照是增强项，不能让全量起不来"原则一致。参数读不到（缺权限）时按未知处理、不降级。

### MySQL→ES 的额外收益

原实现是"先记 master 位点，再开始全量"，两步之间有一个窗口。现在改为**先建快照、位点取自快照点**
（`ConsistentSnapshot` 始终记录 binlog 坐标，供只能按 file:pos 起读的链路对齐），窗口消失。

### 统一位点契约

所有引擎写同一个文件 `files/<taskId>/full_snapshot_position`（`ts|mode|dbType|position`）。
`SnapshotPosition` 负责读写；解析时只切前 3 个分隔符——PG 的位点自带 `;`、MySQL 的 GTID 集自带 `,:`。
agent 一处读取即覆盖全部链路 → `/api/checkpoint` → 任务详情的位点链路多出「⓪ 全量快照点」一段。

---

## 四、配置下发（此前完全断开的一环）

`workflows` 新增三列（V11）→ `Workflow` → `TaskCreatedMessage` → agent `TaskMessage` →
`ConfigService.applyFullLoadOptions` 写 `migration.full.bulk.*` 与 `migration.full.snapshot.mode`
→ 四个引擎共读同一份 config.properties。`RecoveryService` 同步带回这三列，
否则 agent 重写配置会把用户选的档位悄悄退回默认。

**默认值按源端给**（后端 `defaultSnapshotMode` 与 agent 侧同规则）：
MySQL 源默认只记位点（真快照要 RELOAD 权限 + 全局读锁，不该默认替用户承担）；
TiDB / PG / Oracle / MongoDB / Redis 默认真快照（都不加全局锁）。
存量任务回填 `AUTO` + `GTID_ONLY`——这正是它们此前实际在跑的档位。

**可改性**：与一致性语义（创建即定死）不同，这两项**任务启动前都可以改**——
它们只影响全量怎么读怎么写，不改变增量投递语义。由 `updateConfig` 的 `CONFIGURING` 状态校验统一挡住。

UI：同步任务在配置向导第 1 步，灾备任务在灾备配置弹窗，均带源端相关的代价说明。

---

## 五、验证

- 单测 497 项全绿（`./test.sh all`，9 个模块；本轮新增 26 项）：装载档位解析与降级、PG 二进制编码逐字节比对（numeric 的
  base-10000 weight/dscale、时间纪元 2000-01-01）、通道选路与 direct-path 分片降级、
  快照位点文件契约、TiDB GC 时长解析（`500ms` 不能被当成 `500m`）、后端/agent 两侧的档位下发。
- 全工程 `clean install` 通过（fat jar 已重建）。
- **未做**：需要真实库的行为（COPY 实际吞吐、direct-path 表锁、TiDB/Mongo 快照在长全量下的表现），
  留给 `test_scripts/` 的 E2E 判据脚本。COPY 与 direct-path 均为显式 opt-in，默认档位不受影响。
