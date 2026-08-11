# 源库 XA 事务的增量同步（mysql → mysql）

2026-08-11

## 一句话

源库的 XA 事务，**要等它在源库真正提交，目标库才提交**；源库回滚掉的，目标库一行都不能有。

## 问题

改造前全仓库没有一处认识 XA（`XA START/END/PREPARE/COMMIT/ROLLBACK`、`XA_PREPARE` 事件类型
全部零命中）。而 MySQL 的 XA 事务在 binlog 里是**拆成两段**写的（实测 8.0.44）：

```
Query      XA START X'..',X'..',1     ← 行事件在 prepare 时刻就落 binlog 了
Table_map / Write_rows / ...
Query      XA END   X'..',X'..',1
XA_prepare XA PREPARE X'..',X'..',1
 ……（中间穿插任意多个其它事务，实测隔了 7KB）……
Query      XA COMMIT X'..',X'..',1    ← 提交决议在这里，也可能是 XA ROLLBACK
```

没有 `BEGIN`、没有 `Xid`；一阶段提交（`XA COMMIT … ONE PHASE`）复用 `XA_prepare` 事件，
`onePhase=true`，没有独立的 `XA COMMIT` Query 事件。

于是三处都是错的：

| # | 故障 | 机理 |
|---|---|---|
| ① | **应用连接被打死、任务永久停摆** | `XA START X'..'` 在 `SqlClassifier` 里落进 `OTHER` → `THLToSqlConverter` 兜底路径原样打到目标库 → 在目标端**执行成功**，把应用连接推进 XA ACTIVE 态 → 随后的 `COMMIT` 与 `ROLLBACK` 双双报 `1399 XAER_RMFAIL` → 写 E3004 fail-stop，位点不推进，重试永远撞同一堵墙 |
| ② | **回滚的 XA 在目标库留下永久幻影行** | 行事件在 prepare 时刻就被应用，源库随后 `XA ROLLBACK`，没有任何机制撤回，也没有告警 |
| ③ | **XA 事务在目标库不是原子的** | 行事件拿不到 `tx_id`（`XA START` 被当成 DDL 走了"隐式提交"分支），事务一致模式下也退化成逐行提交 |

## 方案：缓冲到提交点再下发

`XA START … XA PREPARE` 之间的原始 `.cap` 行整段扣下落盘，等决议：

* `XA COMMIT` → 按**普通事务**形态重放进 THL（同一个 `tx_id`，末尾补一个 `XID` 事件带 `tx_last`）
* `XA ROLLBACK` → 整段丢弃
* `XA_prepare(onePhase=true)` → 等价于 XID，立即重放

下游（increment / subscribe）**完全不需要认识 XA**——它看到的就是一个发生在提交点的普通事务。

### 为什么推迟到 XA COMMIT 不会打乱顺序

XA 分支在 commit 之前一直持有它改过的行锁与表 MDL：prepare 与 commit 之间的任何事务都碰不到
同一行、也做不了同表 DDL，读到的更是 XA 提交前的旧值。所以"在 XA COMMIT 处应用"恰恰比
"在 prepare 处应用"**更**贴合源库自己的可见性顺序。

### 三处改写

重放出来的事件有三处被改写，都是为了让下游看起来就是一个普通事务：

* `tx_id` 统一钉在分支上，末尾补 `XID` 事件带 `tx_last`——增量端据此原子提交；
* `binlog_file/binlog_position` 与 `eventId` 改写成 **XA COMMIT 的位点**——行事件原本的位点
  停在 prepare 时刻，照原样下发会让应用端位点**倒退**，重启后从更早的位点重放一大段；
  原位点保留在 `xa_prepare_position`；
* `sourceTstamp` 取提交时刻——数据是在 XA COMMIT 那一刻才在源库可见的，用 prepare 时间算
  延迟会把"事务一直没提交"错记成同步延迟。

### 崩溃语义

分支落盘文件 `<sha256(xid)>.part`（收集中）→ prepare 时 fsync + rename 成 `.xa`（已就绪）。

* **已 prepare 的分支**跨 extract 重启从 `.xa` 重新纳管；
* **收集中的分支**（`.part`）重启时一律删除——靠 extract 把 `.cap` 读取进度**冻结在分支起点**
  （`heldProgressSnapshot`），重启后从 `XA START` 重新收集一遍。同理，收集期间不标 `.cap`
  文件 completed、也不清理 `.cap`；
* 重放中途崩溃 → 该分支下一轮重放一遍，整条链路本来就是"至少一次"，应用端按主键幂等吸收。

### 配额

未决分支会占磁盘，且源库那边可能真的卡死了。超限**停任务上报 E3018**，绝不静默丢分支：

| 配置 | 默认 |
|---|---|
| `sync.xa.enabled` | `true`（关掉退回旧行为，只给对照测试用） |
| `sync.xa.branch.max.bytes` | 1GB |
| `sync.xa.pending.max.branches` | 256 |
| `sync.xa.pending.max.bytes` | 2GB |
| `sync.xa.pending.warn.minutes` | 60（只 warn 不停） |

指标落 `files/<taskId>/binlog_output/xa_pending_branches`、`xa_pending_oldest_ms`。

## 改了哪些

| 文件 | 改动 |
|---|---|
| `migration-extract/…/XaTransactionBuffer.java` | **新增**。分支状态机 + 落盘 + 恢复 + 配额 |
| `migration-extract/…/MySQLBinlogExtractor.java` | 分配 seqno **之前**拦截；`drainXaReplay` 流式重放；重放期跳过位点跳过判定与事务打标 |
| `migration-extract/…/ContinuousExtractMain.java` | 每行之后 drain 重放；进度冻结快照；E3018 上报；未决分支指标 |
| `migration-increment/…/SqlClassifier.java` | XA 语句识别成 `TRANSACTION/XA`，不再落进 `OTHER` |
| `migration-increment/…/THLToSqlConverter.java` | 丢弃 XA 控制语句与 `XA_PREPARE` 事件（第二道闸） |
| `migration-capture/…/MySQLBinlogCapture.java` | 双向防回环的事务边界补上 XA START/COMMIT/ROLLBACK 与 `XAPrepareEventData` |
| `java-backend/…/DiagnosticService.java` | 预检加"未决 XA 分支"（WARNING，不阻断） |
| `java-backend/…/SyncErrorCode.java` + `admin-dashboard.js` | E3018 三份目录同步 |

## 判据

* 单测 `XaTransactionSyncTest` 7 项（两阶段提交/回滚、一阶段、跨重启恢复、半截分支丢弃、
  不消耗 seqno、不阻塞其它事务）
* 单测 `SqlClassifierTest` 新增 2 项（XA 语句分类、普通事务不受影响）
* E2E `test_scripts/xa/xa_e2e.py` **12/12**（capture→extract→increment 三进程真链路）
* 对照 `test_scripts/xa/xa_baseline.py` **3/3 复现**改造前的数据面故障

## 已知边界

**抽取起点落在某个分支的 `XA START` 之后**（首次增量取当前位点、或续传位点正好在分支中间）：
这一段的行事件没被扣下、已按普通事件下发，若源库最终回滚该分支，目标库会残留几行。
窗口只有启动瞬间正在 prepare 的那个分支，且 extract 会打 WARN 点名（`收到 XA PREPARE 但没有
对应的收集中分支`）提示做一致性校验。预检的"未决 XA 分支"一项会在启动前先把这类分支报出来。

**XA 事务里不会有 DDL**：MySQL 本身不允许（隐式提交会报错），所以缓冲里只会是 DML。
