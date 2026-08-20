# PITR / 归档回溯 设计方案（F-03）

> 状态：**设计，未实现**。本文件的目的是把"要做什么、改哪里、怎么验"写到能直接开工的粒度。

## 1. 为什么现在能做

对标表里 `归档 / 时间点回溯（PITR）` 一直标着 ❌，而 DTS 与 GoldenGate 都把它列在正面。
过去做不了是因为缺两块地基，现在两块都有了：

| 地基 | 现状 | PITR 需要它做什么 |
|---|---|---|
| 有序、带时间戳的变更日志 | THL：`seqno` 单调、`sourceTstamp` 是**源端时钟**（延迟指标那轮修过） | 定位"回放到哪一条" |
| 按位点还原当时的表结构 | `schema-timeline`：按 binlog 位点索引 + DDL 施加式推演 | 用**当时**的结构解释**当时**的事件 |

第二块是最难的部分，而它已经解决了。"用现在的结构解释过去的事件"这个坑，
在 schema drift 判据里实测过：积压期 `RENAME COLUMN` 会把值写成 `0x` 十六进制且零报错。
没有时序库的 PITR 一定会重蹈它。

## 2. 范围界定

**做**：把一条同步链路的目标库恢复到过去某个时刻的状态（或导出该时刻的全量快照）。

**不做**（明确排除，避免范围蔓延）：
- 不是数据库自身的 PITR 替代品（那要 binlog + 全量备份，是 DBA 的事）
- 不做"回放到任意分支"（多版本并行），只做单一时间轴
- 不覆盖 Redis / ES 目标（它们没有事务与一致性快照语义）

## 3. 数据模型

### 3.1 归档层

现状：THL 被 `ContinuousIncrementMain` 消费完即删除（保留最近 N 个作安全余量），
`TaskFilesJanitor` 在任务终结 72h 后清目录。**PITR 需要的正是这批被删掉的文件。**

改法是加一个**归档去向**，而不是延长保留期——延长保留期会让活跃任务的磁盘无界增长，
而磁盘水位背压会把任务判失败。

```
files/<taskId>/thl_output/*.thl        ← 热区，消费完即删（现状不变）
        │  consumed
        ▼
archive/<taskId>/<yyyyMMdd>/<seqnoRange>.thl.zst   ← 冷区，按保留策略过期
archive/<taskId>/index.jsonl                        ← 索引，见下
```

- **压缩**：THL 实测 gzip 有 37 倍压缩比（类描述符换成 `ThlCodec` 之后仍有大量重复的
  库表名与列名）。用 zstd 而不是 gzip：解压速度快一个数量级，而回放是解压密集的。
- **不加密单独做**：归档文件复用 `ThlEncryptionService`（S-05 那套），
  开关跟随 `SYNCTASK_STAGING_ENCRYPTION`。

### 3.2 索引

回放的第一步是"给定时间 T，找到从哪个文件的哪一条开始"。全扫归档不可接受。

`archive/<taskId>/index.jsonl`，每归档一个文件追加一行：

```json
{"file":"20260819/000001-010000.thl.zst",
 "seqnoFrom":1,"seqnoTo":10000,
 "tsFrom":"2026-08-19T10:00:00.123","tsTo":"2026-08-19T10:04:31.900",
 "eventIdFrom":"binlog.000035:4","eventIdTo":"binlog.000035:8812340",
 "bytes":1048576,"sha256":"…"}
```

- 用 `sourceTstamp` 而不是归档时刻：回放的语义是"源库在 T 时刻的状态"。
- `sha256` 让"归档文件被改过"可检测——PITR 的产出会被当作事实来源，不能默默用坏文件。
- jsonl 追加写：崩溃最多丢最后一行，而最后一行对应的文件还在热区，重新归档即可。

### 3.3 与 schema-timeline 的关系

`schema_history.jsonl` 已经按位点记录了结构版本链。归档时**一并快照**它到
`archive/<taskId>/schema/<seqnoRange>.jsonl`——否则任务删除后时序库跟着没了，
归档的 THL 就再也解释不了。

## 4. 回放

### 4.1 两种形态

| 形态 | 语义 | 目标 |
|---|---|---|
| **恢复到点**（restore-to-point） | 从基线全量 + 回放到 T | 一个新库/新 schema，不动现有目标库 |
| **导出快照**（export-at） | 只产出 T 时刻的数据，不落库 | CSV / Parquet，给审计与取证 |

**恢复到点绝不覆盖现有目标库**——那是不可逆操作，而 PITR 的使用场景本身就是
"出事了要看看当时是什么样"。写到新库，让人自己比对。

### 4.2 起点问题

回放需要一个基线。三种来源，按可用性择优：

1. **全量快照**：任务首次全量完成时的 `full_snapshot_position` + 目标库当时的备份（若有）
2. **周期性物化点**：归档过程中每 N 条事件（或每日）在冷区落一份物化快照
3. **从头回放**：只有增量归档、没有基线时，从第一条归档事件开始

方案二是关键——没有物化点，回放一个跑了三个月的任务要重放上亿条事件。
物化点做成**可配的归档副产品**（`archive.materialize.every.events`，默认关）。

### 4.3 回放引擎

**复用 `ContinuousIncrementMain`，不写第二套应用逻辑。** 理由：
类型转换、冲突处理、LOB 引用、库表名映射这些语义已经在那里趟平了，
重写一套必然与主链路漂移，而漂移的表现是"回放出来的数据和当时不一样"——
这正是 PITR 最不能出的错。

需要给它加三个入口参数：
- `replay.source.dir` —— 从归档目录读而不是热区
- `replay.stop.at.tstamp` / `replay.stop.at.seqno` —— 回放终点（到点即停，不等新事件）
- `replay.schema.timeline` —— 用归档的结构版本链而不是活的那份

## 5. 改动点（类/文件级）

| 模块 | 改动 |
|---|---|
| `migration-common` | 新增 `archive/ThlArchiver`（压缩+落冷区+写索引）、`archive/ArchiveIndex`（读写 jsonl、按时间/seqno 定位） |
| `migration-increment` | `ContinuousIncrementMain` 消费完 THL 后调 `ThlArchiver` 而不是直接删；加上面三个 replay 入口参数 |
| `migration-agent` | 新增 `ReplayTaskExecutor`（复用 `AbstractTaskExecutor` 的进程管理与看门狗）；`TaskFilesJanitor` 增加冷区保留策略 |
| `java-backend` | `POST /api/workflows/{id}/pitr/restore`、`GET /api/workflows/{id}/pitr/timeline`（可回溯范围）；新增 `PitrService`；`@Audited` 记录（PITR 会读走全量业务数据，必须留痕） |
| 前端 | 任务详情页加"时间点回溯"：时间轴选点 + 目标库选择 + 预估耗时 |
| Flyway | `V21__pitr_jobs.sql`：回放作业表（状态机与现有任务一致） |

## 6. 判据设计

判据要能证明"回放出来的就是当时的样子"，而不只是"回放跑完了"。

`test_scripts/pitr/pitr_e2e.py`：

1. 建 mysql→mysql 任务，写入 N 批数据，**每批之间记录源端指纹与时刻** `(T_i, fp_i)`
2. 期间穿插 DDL（加列、改列名、改类型）——这是 schema-timeline 必须被用上的地方
3. 任务跑完并归档
4. 对每个 `T_i` 做一次 restore-to-point 到新库，断言新库指纹 == `fp_i`
5. **反向断言**：restore 到 `T_i` 的结果 ≠ `fp_{i+1}`（否则"回放到点"是假的，只是跑到了最后）
6. 篡改一个归档文件的一个字节，断言校验能发现（`sha256` 那条）

指纹用 `SUM(CRC32(...))`——不能用 XOR，理由见 `fingerprint_semantics.py`。

## 7. 分阶段

| 阶段 | 内容 | 可独立验收 |
|---|---|---|
| 1 | 归档层：压缩落冷区 + 索引 + schema 快照 | 归档文件可读、索引可定位 |
| 2 | 保留策略与磁盘治理：冷区过期、水位 | 长跑不撑爆盘 |
| 3 | 回放引擎：replay 三参数 + 到点即停 | 能回放到任意 seqno |
| 4 | 物化点 | 长任务回放耗时可接受 |
| 5 | 控制面 + 前端 + 审计 | 端到端可用 |

阶段 1~3 就能覆盖"取证"这个最主要的诉求（导出某时刻的数据给审计看），
阶段 4~5 是把它做成产品功能。**建议先做 1~3 并停下来看反馈**——
PITR 的实际用法差异很大，做完前三阶段再定后两阶段的形态更省。

## 8. 已知风险

- **归档使冷区无界增长**：必须与阶段 2 同批出，否则第一个长跑任务就会把盘写满。
- **回放的目标库写入权限**：restore-to-point 要建新库，控制面得有相应凭证；
  不能复用同步任务的目标凭证（那是给同步用的，不该被回放借去建库）。
- **`.cap` 不归档**：归档的是 THL 而不是 `.cap`。二者信息量的差异需要确认——
  若某些边界形态（压缩 binlog、PARTIAL_JSON）的信息只在 `.cap` 里，回放会失真。
  这一条**必须在阶段 1 之前查清**。
