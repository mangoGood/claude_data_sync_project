# 流量复制与回放任务设计方案

> 范围：**仅 MySQL**（源库 MySQL 复制、目标库 MySQL 回放）。
> 与「实时同步管理 / 灾备任务管理 / 数据订阅管理」并列的第四类任务。
> 本文的捕获机制选型带**本机实测证据**（synctask-mysql，MySQL 8.0.44），见 §2.6。

---

## 0. 现状体检：能复用多少，不能复用什么

### 0.1 能整块复用的（这决定了本功能的工作量下限）

新功能没有必要重建一套任务生命周期。现有的四类任务全部落在同一张 `workflows` 表上，靠
`task_type` 区分，围绕它已经有一整套**与任务语义无关**的基础设施：

| 能力 | 落点 | 复用方式 |
|---|---|---|
| 任务 CRUD / 分页筛选 | `WorkflowController` + `WorkflowService.getWorkflowsByUserIdAndFilters` | `taskType` 传 `TRAFFIC_CAPTURE` / `TRAFFIC_REPLAY` 即可，Specification 已支持 |
| 启动 / 暂停 / 恢复 / 停止 / 删除 / 重试 | `WorkflowService.launchWorkflow` + Kafka `sync-task-created` | 加两个分支即可 |
| 派发到 agent + 集群租约 | `AgentClusterService.assign`、`agent_id/lease_*` | 零改动 |
| agent 侧子进程守护 / 熔断 / 僵死看门狗 | `AbstractTaskExecutor` + `ProcessGuard` | 新增执行器继承即可 |
| 连接串加密落库 | `EncryptedStringConverter`（AES-GCM） | 零改动 |
| 传输加密（SSL/TLS） | `source_ssl_*` / `target_ssl_*` + `SslMaterial` | 零改动，见 §8.4 |
| 审计 / 配额 / 配置版本 / 预检门禁 | `AuditLogService` / `ResourceQuotaService` / `runPrecheckGate` | 零改动 |
| 实时指标代理 | 后端 `callAgentJson` → agent `:8083` | 新增两个指标文件即可 |
| 前端页面骨架 | `switchPage` + `dashboard-*.js` ES module 模式 | 照 `dashboard-dr.js` 复刻 |

**结论：任务框架一行不用重写，真正要新写的只有「捕获引擎 + 录制文件格式 + 回放引擎」三件事。**

### 0.2 不能复用的（必须认下来）

1. **捕获链路完全不同。** 现有 5 种 capture（binlog / WAL / redo / oplog / TiCDC）抓的都是**已提交的数据变更**。
   本功能要的是**语句流**，且包含 `SELECT`——binlog 里根本没有 SELECT，任何"在 binlog 上改改"的方案都是死路。
   这是一条**全新的捕获通道**，不进 `.cap`/THL，不走 extract/increment。
2. **没有位点可续。** binlog/WAL 是持久化日志，任务停了再起能从位点补上；语句流是**易失**的，
   捕获停摆期间源库执行的语句**永久拿不回来**。因此"断点续传"这个词在捕获侧不成立，
   只能诚实地记成**时间轴空洞**（§3.3）。这一点若不在设计里写死，实现时一定会有人按
   binlog 的思路做出一个"看起来能续、实际静默丢一段"的东西。
3. **回放是写目标库的破坏性操作。** 灾备/同步写的是"源库已经发生过的数据变更"，
   回放写的是"任意 SQL，含 DDL/DCL"。安全边界要单独设计（§8）。

### 0.3 规模底数

- `workflows` 表 60+ 列，`WorkflowService` 1987 行，`admin-dashboard.js` 402 KB。
  → 新增字段**不再往 `workflows` 里塞**，走 1:1 侧表（§5.2）。
- 错误码有**三份**互不引用的目录，`SyncErrorCodeCatalogTest` 是 CI 门禁 → 新错误码必须同时改三处（§9.2）。

---

## 1. 目标与非目标

### 1.1 目标

**流量复制任务（TRAFFIC_CAPTURE）**

- 从任务启动时刻起，按源库**真实到达顺序**记录 MySQL 源库上执行的全部语句：
  `SELECT` / DML / DDL / DCL / TCL（BEGIN·COMMIT·ROLLBACK）/ `SET` / `USE`。
- 每条语句记录：**相对任务启动的时间偏移（微秒）**、来源会话、当时的默认库、来源账号、语句原文。
- 产出一个**可下载、可再上传、自描述**的录制文件（`.trfz`）。
- 用户手动停止 → 文件封口（写 manifest + SHA-256）。

**流量回放任务（TRAFFIC_REPLAY）**

- 选定一个录制文件 + 一个目标 MySQL 库。
- 按录制中的**顺序**与**时间间隔**在目标库执行：录制里偏移 5s 的 DDL，在回放启动后 5s 执行；
  偏移 10s 的 DML 在 10s 执行；偏移 15s 的 SELECT 在 15s 执行。
- 同一源会话内的语句严格保序且落在**同一条目标连接**上（事务、临时表、`SET`、`USE` 才有意义）。
- 产出回放报告：执行条数 / 错误 TOP / 慢语句 TOP / **时间轴偏差**分布。

### 1.2 非目标（v1 明确不做）

| 不做 | 原因 |
|---|---|
| MySQL 以外的源/目标 | 用户明确限定；且 PG/Oracle 的语句捕获机制完全不同（PG 走 `log_statement`+日志文件，Oracle 走 audit trail），不是同一套代码 |
| 库名/表名改写 | 回放要求目标库与源库**同名 schema**。改写任意 SQL（含多表 JOIN 的 SELECT）远超 `DdlIdentifierRewriter` 现有能力，是独立一期 |
| 结果集比对（逐行） | v1 只比 `ROWS_SENT`/`ROWS_AFFECTED`（可选），逐行校验属于"数据对比任务"的职责 |
| 加速/减速回放之外的压测编排（并发放大 N 倍） | 放大倍数会破坏"按原时间间隔"这个核心语义，另开档位再议 |
| 二进制协议 prepared statement 的参数级还原 | 不需要——`Execute` 行里 MySQL 已经给出**参数替换后的完整 SQL**（§2.6 实测） |

---

## 2. 捕获机制选型（全篇最关键的一个决策）

### 2.1 候选

| 方案 | 能拿到 SELECT | 完整性 | 侵入性 | 结论 |
|---|---|---|---|---|
| A. binlog | ✗ | — | 低 | **直接出局**，没有 SELECT |
| B. `general_log` + `log_output=TABLE` | ✓ | 完整、不丢 | 改 2 个全局变量 | **选它做主通道** |
| C. `performance_schema.events_statements_history_long` | ✓ | 环形缓冲**会丢**；`SQL_TEXT` 默认截断 1024 字节 | 需开 consumer | 降级通道 / 富化 |
| D. 审计插件（MySQL Enterprise Audit / Percona / MariaDB） | ✓ | 完整 | 要装插件、要重启 | 留扩展点，v1 不做 |
| E. tcpdump 抓包解析 MySQL 协议 | ✓ | 完整 | 要部署在库机、要 root | 留扩展点，v1 不做 |

### 2.2 为什么是 B（general_log → TABLE）

- **纯 JDBC 可达。** agent 与源库不必同机——这是本工程的基本前提（agent 是集群化的）。
  `log_output=FILE` 时日志落在**库机磁盘**上，agent 读不到；`TABLE` 落 `mysql.general_log`，
  一条 `SELECT` 就能拿。
- **不丢。** 它是 MySQL 在语句**到达时**同步写的，不是采样、不是环形缓冲。
- **语句原文完整**，不像 C 那样被 `performance_schema_max_sql_text_length` 截成 1024 字节。

### 2.3 为什么 C 不能当主通道（实测结论，不是推测）

```
performance_schema_max_sql_text_length = 1024          -- SQL_TEXT 被截断
events_statements_history_long        = NO (consumer 默认关闭)
```

一条超过 1KB 的 SQL 被截断后**语法就不完整了**，回放时必然报错——而且是"看起来抓到了、
实际执行不了"的那种失败。C 只能做**可选富化**（§2.5）。

### 2.4 抽象出 `TrafficSource` 接口

尽管 v1 只实现 B，捕获侧仍按接口设计，让 D/E 将来能平移进来：

```java
public interface TrafficSource {
    void open(Properties cfg) throws Exception;   // 开启捕获（B：改全局变量 + 建轮转表）
    List<RawStatement> poll() throws Exception;   // 一批语句，**已按源库真实顺序**
    void close();                                 // 关闭并**还原源库状态**
    SourceFingerprint fingerprint();              // server_uuid/version/sql_mode/time_zone/charset
}
```

`GeneralLogTrafficSource` 是 v1 唯一实现。

### 2.5 双层记录：主通道 + 可选富化

| 层 | 来源 | 内容 | 是否默认开 |
|---|---|---|---|
| 主 | `mysql.general_log` | 顺序、到达时刻、会话、账号、语句原文 | 是 |
| 富化 | `performance_schema.events_statements_history` | `MYSQL_ERRNO` / `ROWS_SENT` / `ROWS_AFFECTED` / `TIMER_WAIT`（源端耗时） | 否（`traffic.capture.enrich=false`） |

富化**永远只是尽力而为**，绝不参与排序与完整性判定。开了富化，回放报告才能回答
"这条语句在源库本来就报错"和"源库返回 100 行、目标库返回 3 行"。

> **富化的连接键是个坑**：`general_log.thread_id` 是**连接 id（processlist id）**，
> 而 `events_statements_*.THREAD_ID` 是 performance_schema 的**内部线程 id**，两者不相等。
> 必须经 `performance_schema.threads(PROCESSLIST_ID → THREAD_ID)` 中转。
> 直接 join 会静默匹配到错误的会话或匹配不上——而"匹配不上"在尽力而为的语义下不会报错。

### 2.6 实测证据（synctask-mysql，MySQL 8.0.44）

在本机源库上实际验证了六件事，每一件都直接决定了实现细节：

**① `general_log` 表结构与精度**
```
event_time timestamp(6)   -- 微秒精度，正好够做时间轴
argument   mediumblob     -- 16MB，语句原文不截断
ENGINE=CSV                -- 无索引、无主键
```

**② 语句类型覆盖 —— 全都拿得到**
```
command_type  argument
Connect       root@localhost on  using Socket
Query         CREATE DATABASE IF NOT EXISTS trf_probe
Init DB       trf_probe
Query         INSERT INTO t1 VALUES (1,'a')
Query         SELECT * FROM t1 WHERE id=1          ← SELECT 拿得到
Prepare       SELECT v FROM t1 WHERE id=?
Execute       SELECT v FROM t1 WHERE id=1          ← 参数已替换，可直接回放
Query         BEGIN / UPDATE ... / COMMIT
Query         GRANT SELECT ON trf_probe.* TO 'trfprobe'@'%'
```
→ `Execute` 行给出的是**参数替换后的完整 SQL**，所以 prepared statement 不需要任何特殊处理：
**回放时用 `Execute`，丢弃 `Prepare`**。

**③ 报错的语句也会被记录**
```
Query   SELECT * FROM no_such_table      -- 源库返回 1146，日志里照样有这一行
```
→ 录制里存在"源库本来就失败"的语句，回放时它在目标库也失败是**符合预期**的，
不能算回放错误（这正是 §2.5 富化的价值：拿到 `MYSQL_ERRNO` 才分得清）。

**④ 口令被 MySQL 抹掉 —— 这是一条硬边界**
```
Query   CREATE USER IF NOT EXISTS 'trfprobe'@'%' IDENTIFIED BY <secret>
```
→ **带口令的 DCL 无法忠实回放**，`<secret>` 是 MySQL 自己写进日志的占位符。
设计必须显式处理：捕获时打 `redacted=true` 标记，回放时**跳过并计入报告**，
而不是把 `<secret>` 当字面量执行（那会建出一个口令是 `<secret>` 的账号）。

**⑤ 日志表只能 TRUNCATE / RENAME，不能 DELETE**
```
DELETE FROM mysql.general_log WHERE ... ;
ERROR 1556 (HY000): You can't use locks with log tables
```
→ 轮转只能靠**改名换表**（见 §2.7）。

**⑥ `SET SESSION sql_log_off=1` 能彻底消掉自噪声**

捕获进程自己的轮询 SELECT 也会被写进 `general_log`——若不处理，
**每次轮询都会给下一次轮询造出新数据，自激**。实测：
```
-- 采集连接执行 SET SESSION sql_log_off=1 之后：
SELECT 'THIS_SHOULD_NOT_BE_LOGGED_1';     → 日志里没有
SELECT COUNT(*) FROM mysql.general_log;    → 日志里没有
-- 只剩固定的 3 行：Connect、驱动的 version_comment 探测、SET 语句自身
```
→ 采集连接建立后第一件事就是 `SET SESSION sql_log_off=1`；
残留的 3 行按 `thread_id` 过滤掉即可（常数条，不随时间增长）。

### 2.7 轮转：RENAME 换表（实测可行，且**无缝**）

`mysql.general_log` 会无限增长在**源库的 datadir** 上。轮转必须做，而 DELETE 不可用。
实测 **RENAME 在 `general_log=ON` 状态下就能做，MySQL 会立刻改写新表，一条不丢**：

```sql
CREATE TABLE mysql.general_log_trf_next LIKE mysql.general_log;
RENAME TABLE mysql.general_log         TO mysql.general_log_trf_read,
             mysql.general_log_trf_next TO mysql.general_log;
-- general_log_trf_read 此刻已冻结 → 整表读干净 → DROP
```
实测：换表后新表继续收到语句（`AFTER_SWAP_MARKER` 落在新表），旧表 34 行完整保留。

**读取策略**：`SELECT ... FROM mysql.general_log_trf_read`（**不加 ORDER BY**）。
CSV 引擎是平铺文件，全表扫描即写入顺序 —— 这比 `ORDER BY event_time` 更好：
- 省掉大表 filesort；
- 同一微秒内的多条语句有**稳定的全序**，而 `ORDER BY` 在时间戳相同时顺序未定义。

轮转触发条件（取先到者）：`traffic.capture.rotate.rows`（默认 20 万）
/ `traffic.capture.rotate.interval.ms`（默认 10s）。

### 2.8 `argument` 必须按字节读

`general_log` 表字符集是 **utf8mb3**，而 `argument` 是 `mediumblob`——原始字节完好。
实测：`INSERT INTO t1 VALUES (2,'中文🚀emoji')` 的 `HEX(argument)` 含 `F09F9A80`（🚀，4 字节 UTF-8），
但若让服务端 `CONVERT(argument USING utf8mb4)`、或让驱动按表字符集转成 String，就变成 `???`。

→ **`rs.getBytes("argument")` + 自己按 UTF-8 解码**，绝不让服务端或驱动代劳。
这与本仓库既有的一整类静默损坏教训同源（见 `[[silent-loss-audit-2026-08-11]]`：PG 空串变 `'NULL'`、
Oracle CSF 4000 字节）——**任务全绿，数据已经错了**。

---

## 3. 录制文件格式

### 3.1 布局

任务运行期在 agent 上是分段的（便于流式写、便于崩溃后只丢最后一段的尾巴）：

```
files/<captureTaskId>/traffic/
  manifest.json                 # 头 + 分段清单 + 空洞清单 + 统计（封口时写全）
  seg-00000001.trf.gz           # JSONL + gzip
  seg-00000002.trf.gz
  ...
```

对用户下载/上传则打成**一个文件** `<taskName>-<taskId>.trfz`（zip，内含 manifest + 全部分段）——
用户口径里"生成一个文件"就是它。

### 3.2 `manifest.json`

```jsonc
{
  "format": "synctask-traffic/1",
  "captureTaskId": "…", "captureTaskName": "订单库-上午高峰",
  "t0Wall": "2026-08-20T10:00:00.000000+08:00",   // 时间轴原点（绝对时刻，仅供展示）
  "endWall": "2026-08-20T10:30:00.000000+08:00",
  "source": {                                      // 回放前的兼容性校验依据
    "serverUuid": "…", "version": "8.0.44", "serverId": 1,
    "sqlMode": "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,…",
    "timeZone": "+08:00", "charset": "utf8mb4", "collation": "utf8mb4_0900_ai_ci",
    "lowerCaseTableNames": 0
  },
  "filter": { "databases": ["order_db"], "classes": ["SELECT","DML","DDL"], "sampleRate": 1.0 },
  "segments": [
    {"seq":1,"file":"seg-00000001.trf.gz","records":198432,
     "firstN":1,"lastN":198432,"firstT":0,"lastT":612334221,
     "bytes":18234112,"sha256":"…"}
  ],
  "gaps": [ {"fromT":612334221,"toT":734001998,"reason":"TASK_PAUSED"} ],   // §3.3
  "stats": {"total":198432,"select":151002,"dml":47001,"ddl":12,"dcl":3,
            "redacted":3,"sessions":88,"maxConcurrentSessions":41},
  "sealed": true, "sha256": "…"
}
```

`source` 段不是装饰：回放前要拿它跟目标库比对（§4.6），`sqlMode` / `timeZone` / `charset`
不一致时同一条 SQL 在两边的行为就是不一样的。

### 3.3 时间轴空洞（`gaps`）—— 必须诚实

捕获任务被暂停 / 进程崩溃重启 / 源库重启期间，**语句流是真的丢了，且拿不回来**。
不能像 binlog 那样"从位点续上"。因此：

- 每次恢复捕获 → 在 manifest 里追加一条 `gaps` 记录；
- UI 的时间轴上把空洞画成灰条；
- **回放遇到空洞：按原时长静默等待**（保持后续语句的绝对偏移不变），并在报告里点名。
  另一档 `traffic.replay.gap.policy=COMPRESS` 把空洞压成 0，用于"我只想尽快跑完"的场景。

### 3.4 记录行（JSONL，每行一条）

```jsonc
{"n":1024,               // 全局序号：**排序的唯一权威**（物理读序），单调递增
 "t":15000123,           // 相对 t0 的偏移，微秒。回放调度只看它
 "s":2691,               // 源会话 id（general_log.thread_id）
 "c":"Q",                // Q=Query E=Execute I=InitDB C=Connect D=Quit
 "k":"SELECT",           // 语句类别：SELECT/DML/DDL/DCL/TCL/SET/USE/OTHER
 "db":"order_db",        // 该会话此刻的默认库（捕获侧跟踪 Connect/Init DB/USE 得出）
 "u":"app@10.0.0.5",     // 来源账号（审计 + 可选按账号过滤）
 "q":"SELECT * FROM t1 WHERE id=1",
 "rd":false,             // redacted：MySQL 抹了口令（<secret>），不可回放
 "e":{"errno":0,"rows":1,"aff":0,"us":831}}   // 富化，可缺省
```

字段名压到 1~2 字符是有意的：一条记录的固定开销约 60 字节，
一个 10k QPS 的库跑 30 分钟是 1800 万条——键名多 20 字节就是多 360 MB。
gzip 后实测量级约为原始 SQL 文本的 1/8~1/12。

**写入耐崩**：分段文件按行追加，每 `traffic.capture.flush.ms`（默认 1s）flush；
恢复时若最后一行不完整则**整行丢弃**（与 `.cap 半行` 的处理一致），并把该分段的
`records`/`lastN` 按实际读到的重算。manifest 在任务运行中不落 `sealed:true`，
崩溃恢复时由分段扫描重建。

### 3.5 体量护栏

无界录制会把 agent 磁盘写满。三条上限，任一触发即**自动封口并把任务置 COMPLETED**（不是 FAILED）：

- `traffic.capture.max.duration.ms`（默认 2 小时）
- `traffic.capture.max.bytes`（默认 20 GB）
- `traffic.capture.max.records`（默认 1 亿）

另有 `traffic.capture.sample.rate`（默认 1.0）：按**会话**哈希采样而非按语句采样——
按语句采样会把事务切碎、把 `SET`/`USE` 采丢，录制直接失去可回放性。

---

## 4. 回放引擎

### 4.1 总体结构

```
 manifest + 分段
      │
      ▼
 ┌──────────┐   按 n 顺序读，一条不跳
 │ Reader   │
 └────┬─────┘
      │  record
      ▼
 ┌──────────────┐   计算绝对截止时刻 deadline = t0Replay + t/speed
 │ Scheduler    │   —— 单线程，全局唯一的时间轴权威
 └────┬─────────┘
      │  投递到该会话的队列
      ▼
 ┌──────────────────────────────────────┐
 │ SessionRunner[s]  1 源会话 : 1 目标连接 : 1 线程 │
 │  串行执行 → 保证会话内严格保序          │
 └──────────────────────────────────────┘
      │
      ▼   执行结果
 ┌──────────┐
 │ Reporter │  指标文件 + replay_errors.jsonl
 └──────────┘
```

**跨会话的顺序不由代码保证，而由时间轴保证**——这正是源库当时的真实情形：
两个会话之间本就没有顺序约束，只有各自的到达时刻。

### 4.2 时间精度

用户的例子精确到秒，但真实录制里语句间隔常常是毫秒级。因此：

- **绝对截止时刻，不做累加睡眠。** `deadline_i = t0Replay + t_i/speed`。
  用 `sleep(间隔)` 累加会把每次的调度误差累积成秒级漂移。
- 距 deadline > 2ms 用 `LockSupport.parkNanos`，最后 2ms 自旋 `Thread.onSpinWait()`。
  `Thread.sleep` 在 Linux 上的实际精度约 1~15ms，够不上。
- 记录**每条语句的实际偏差** `t_actual - t_planned`，聚合成 P50/P95/P99/max 进报告。
  这个指标就是"回放到底像不像"的答案，必须一等公民对待。

### 4.3 会话模型

- 源 `s`（thread_id）首次出现 → 建一条目标连接 + 一个单线程执行器。
- 记录里的 `C`(Connect) / `D`(Quit) 用于建/销连接；没有 Connect 就懒建（录制从中途开始很常见）。
- **`USE` / `SET` / `BEGIN` / `COMMIT` / `ROLLBACK` 永远回放**，即使用户只勾了"DML"。
  它们是会话状态，不是负载。漏放一条 `SET autocommit=0`，其后的所有 DML 语义就变了。
  → 语句类别分两组：
  - **负载类**（用户可筛）：`SELECT` / DML / DDL / DCL
  - **会话状态类**（强制回放）：`SET` / `USE` / TCL / `Init DB`
- 连接数上限 `traffic.replay.max.sessions`（默认 200）。超限策略：
  LRU 淘汰**空闲且不在事务中**的会话；找不到可淘汰的 → 该语句记 `SESSION_EXHAUSTED` 跳过并计数。
  绝不淘汰事务中的会话（会造成目标库上一个悬挂事务）。
- 会话建立时按 manifest 的 `source` 段设置 `sql_mode` / `time_zone` / `character_set_client`，
  让两边的语句语义对齐。

### 4.4 落后了怎么办（`traffic.replay.lag.policy`）

目标库比源库慢是常态。三档：

| 档位 | 行为 | 用途 |
|---|---|---|
| `WAIT`（默认） | 保序等待，允许整体落后，落后量进指标 | 用户要的"按原间隔"忠实回放 |
| `SKIP` | 已过 deadline 超过 `lag.skip.ms`（默认 5000）的语句直接丢弃并计数 | 压测：宁可丢也要维持压力形状 |
| `STRETCH` | 检测到落后就整体拉长时间轴（后续 deadline 顺延） | 只关心顺序、不关心绝对节奏 |

无论哪档，**会话内保序永不破坏**：同一会话上一条没跑完，下一条一定不会先跑。

### 4.5 错误处理

| 情形 | 处置 |
|---|---|
| 源库本来就报错（富化 `errno != 0`），目标库同样报错 | 记 `EXPECTED_ERROR`，不算问题 |
| 源库成功、目标库报错 | 记 `REPLAY_ERROR` 进 `replay_errors.jsonl`，**继续跑** |
| `rd=true`（口令被抹） | 记 `UNREPLAYABLE_REDACTED`，跳过 |
| 命中危险语句黑名单（§8.2） | 记 `BLOCKED`，跳过 |
| 连接断开 | 该会话重连一次；再失败则该会话标记失败，其余会话继续 |
| 错误率超 `traffic.replay.abort.error.rate`（默认 0.5） | 整个任务 fail-stop，避免在目标库上继续制造破坏 |

回放**不做 fail-stop-per-statement**：一条 SQL 失败就停任务的回放没有使用价值，
因为回放的目的恰恰是"找出哪些语句在新环境里跑不通"。

### 4.6 启动前的兼容性校验（硬拦截）

对比 manifest `source` 与目标库实测值：

| 项 | 不一致时 |
|---|---|
| `serverUuid` 相同（回放到源库自己） | **error，硬拦截**（需显式勾选强制，且留审计）——见 §8.1 |
| 大版本不同（8.0 → 5.7） | warning |
| `sqlMode` 不同 | warning（回放时按录制值覆盖会话） |
| `timeZone` 不同 | warning（同上） |
| `lowerCaseTableNames` 不同 | **error**：大小写语义不同，DDL 会以不同的名字落地 |
| 录制涉及的 schema 在目标库不存在 | warning（含 `CREATE DATABASE` 的录制是合法的） |

---

## 5. 任务模型与数据库变更

### 5.1 `task_type` 与状态

- `task_type`：新增 `TRAFFIC_CAPTURE` / `TRAFFIC_REPLAY`。列是 `VARCHAR(20)`，够用，无需改列。
- `WorkflowStatus`：新增 `TRAFFIC_CAPTURING` / `TRAFFIC_REPLAYING`，`phase()` 均返回 **60**
  （与 `INCREMENT_RUNNING`/`SUBSCRIBE_RUNNING` 同阶段：都是"运行态"，一个任务不会兼有）。
- ⚠️ `workflows.status` 在 MySQL 里是 **ENUM**，新增取值必须 `ALTER TABLE ... MODIFY COLUMN`，
  否则写入被**截断成空串**（V7 新增 `RECONNECTING` 时踩过，见该迁移的注释）。

### 5.2 新增表（Flyway `V22__traffic_replay.sql`）

不往 `workflows` 加列——这 14 个字段只对 6 类任务里的 2 类有意义，而该表已有 60+ 列。

```sql
-- 任务配置（1:1）
CREATE TABLE traffic_task_config (
  task_id                VARCHAR(36) PRIMARY KEY,          -- = workflows.id
  -- 捕获侧
  capture_backend        VARCHAR(20)  DEFAULT 'GENERAL_LOG',
  capture_databases      TEXT         DEFAULT NULL,        -- JSON 数组，空=全部
  capture_classes        VARCHAR(100) DEFAULT 'SELECT,DML,DDL',
  capture_users          TEXT         DEFAULT NULL,        -- JSON 数组，按来源账号过滤
  capture_sample_rate    DECIMAL(5,4) DEFAULT 1.0000,
  capture_enrich         TINYINT(1)   DEFAULT 0,
  capture_max_duration_ms BIGINT      DEFAULT 7200000,
  capture_max_bytes      BIGINT       DEFAULT 21474836480,
  capture_max_records    BIGINT       DEFAULT 100000000,
  -- 源库原始开关值，用于**还原**（§8.3）
  src_general_log_before VARCHAR(10)  DEFAULT NULL,
  src_log_output_before  VARCHAR(30)  DEFAULT NULL,
  -- 回放侧
  replay_source_task_id  VARCHAR(36)  DEFAULT NULL,        -- 录制来自哪个捕获任务
  replay_upload_id       VARCHAR(36)  DEFAULT NULL,        -- 或来自上传
  replay_speed           DECIMAL(6,3) DEFAULT 1.000,
  replay_classes         VARCHAR(100) DEFAULT 'SELECT,DML',
  replay_lag_policy      VARCHAR(20)  DEFAULT 'WAIT',
  replay_gap_policy      VARCHAR(20)  DEFAULT 'PRESERVE',
  replay_max_sessions    INT          DEFAULT 200,
  replay_compare         VARCHAR(20)  DEFAULT 'NONE',      -- NONE/ROWCOUNT
  replay_allow_dcl       TINYINT(1)   DEFAULT 0,
  replay_allow_dangerous TINYINT(1)   DEFAULT 0,
  created_at             DATETIME     DEFAULT CURRENT_TIMESTAMP,
  updated_at             DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) COMMENT='流量复制/回放任务配置';

-- 录制文件目录（供回放选择 + 下载）
CREATE TABLE traffic_recordings (
  id             VARCHAR(36) PRIMARY KEY,
  capture_task_id VARCHAR(36) NOT NULL,
  user_id        BIGINT      NOT NULL,
  agent_id       VARCHAR(64) DEFAULT NULL,     -- 文件实际躺在哪台 agent 上
  name           VARCHAR(200) NOT NULL,
  t0_wall        DATETIME(6) DEFAULT NULL,
  end_wall       DATETIME(6) DEFAULT NULL,
  duration_ms    BIGINT      DEFAULT 0,
  record_count   BIGINT      DEFAULT 0,
  byte_size      BIGINT      DEFAULT 0,
  session_count  INT         DEFAULT 0,
  gap_count      INT         DEFAULT 0,
  sha256         VARCHAR(64) DEFAULT NULL,
  sealed         TINYINT(1)  DEFAULT 0,
  source_fingerprint TEXT    DEFAULT NULL,     -- manifest.source 原样
  is_deleted     TINYINT(1)  DEFAULT 0,
  created_at     DATETIME    DEFAULT CURRENT_TIMESTAMP,
  KEY idx_capture_task (capture_task_id),
  KEY idx_user (user_id, is_deleted)
) COMMENT='流量录制文件目录';
```

回放的错误明细**不入库**，落 agent 侧 `replay_errors.jsonl`，由后端代理读取——
与既有 deadletter / conflicts 的做法一致（`/api/agent/deadletter`），
避免一次回放的百万级错误行冲垮元数据库。

---

## 6. 代码落点

### 6.1 新模块 `migration-traffic`

```
migration-traffic/
  pom.xml                                    # 加入根 pom <modules>
  src/main/java/com/migration/traffic/
    TrafficMain.java                         # Main-Class；--mode capture|replay 分派
    capture/
      TrafficSource.java                     # 接口（§2.4）
      GeneralLogTrafficSource.java           # general_log 主通道 + RENAME 轮转
      SessionSchemaTracker.java              # Connect/Init DB/USE → 每会话默认库
      StatementClassifier.java               # SELECT/DML/DDL/DCL/TCL/SET/USE/OTHER
      PerfSchemaEnricher.java                # 可选富化（注意 PROCESSLIST_ID 中转）
      TrafficWriter.java                     # 分段 JSONL+gzip、flush、manifest
      SourceGuard.java                        # 还原源库开关（§8.3）
    replay/
      RecordingReader.java                   # manifest + 分段流式读、坏尾行丢弃
      ReplayScheduler.java                   # 绝对 deadline + parkNanos/自旋
      SessionRunner.java                     # 1 源会话 : 1 目标连接 : 1 线程
      DangerousStatementFilter.java          # 黑名单（§8.2）
      ReplayReporter.java                    # 指标文件 + replay_errors.jsonl
    model/TrafficRecord.java, RecordingManifest.java, SourceFingerprint.java
```

复用 `migration-common`：`CredentialCipher`（解密 config 口令）、`ChildProcessBootstrap`
（单实例互斥 + 父进程看门狗）、`MdcUtil`、`AtomicFileWriter`（指标文件）、
`ssl/SslMaterial`（源/目标 TLS）。

### 6.2 agent 侧

- `AgentConfig`：`jar.traffic.path = migration-traffic/target/migration-traffic-1.0.0.jar`
  + `getTrafficJarPath()`。
- `ConfigService.updateConfig`：`TRAFFIC_*` 分支，写 `traffic.*` 全套属性 +
  `props.setProperty("task.type", …)`。
- 新增 `thread/TrafficCaptureTask.java` / `thread/TrafficReplayTask.java`，均继承
  `AbstractTaskExecutor`：
  - `getRunningStatus()` → `TRAFFIC_CAPTURING` / `TRAFFIC_REPLAYING`
  - `doRun()` → 单个 `ProcessGuard` 守一个子进程（`setMainArgs(new String[]{"--mode","capture"})`）
  - `initCheckpoint()` **必须整体跳过**——语句流没有位点，硬套会去查 binlog 位点并写
    checkpoint，纯属副作用（且会让"跨机接管"的回灌逻辑误判）。
  - `stallLivenessFiles()` → 见 §7.2
- `MigrationAgentThread.createExecutor()`：在 `SUBSCRIBE` 分支前后加两个 `taskType` 分支。
- `TaskMessageHandler`：`isSingleProcessEngine()` 增加 `TRAFFIC_*` 判定（走
  `startMigrationAgentThread`，不能落到 legacy `MigrationTaskManager` 的 SQL 全量引擎）。
- `AgentHttpServer`：`/api/traffic/recordings/{taskId}`（清单）、
  `/api/traffic/recordings/{taskId}/bundle`（打包下载）、`/api/traffic/replay-errors/{taskId}`。
- `TaskFilesJanitor`：`traffic/` 目录纳入清理策略（录制文件按 `traffic_recordings.is_deleted` 保留）。

### 6.3 后端

- `entity/TrafficTaskConfig.java`、`entity/TrafficRecording.java` + 两个 Repository。
- `service/TrafficTaskService.java`：配置读写、录制目录查询、
  录制文件下载（代理到**录制所在 agent**，用 `traffic_recordings.agent_id` 而非当前任务的 agent）。
- `controller/TrafficController.java`（§7.1）。
- `WorkflowService.launchWorkflow`：`TRAFFIC_CAPTURE` 免检 `targetConnection` 与 `syncObjects`；
  `TRAFFIC_REPLAY` 免检 `sourceConnection`，改为必须有录制来源。
- `MetadataService`：新增 `trafficPrecheck`（§9.1）。
- `KafkaConsumerService`：处理捕获任务的"自动封口 → COMPLETED"与录制元数据上报。

### 6.4 前端

- `admin-dashboard.html`：菜单 `<div class="menu-item" data-page="traffic" onclick="switchPage('traffic')">流量复制与回放</div>`
  （放在"数据订阅管理"之后）+ `trafficPage` 容器 + 两套向导模态框。
- `admin-dashboard.js`：`switchPage` 的隐藏列表与分支加 `trafficPage`；
  `__dash` 里补 traffic 需要的共享函数。
- 新增 `dashboard-traffic.js`（ES module，照 `dashboard-dr.js` 的模式：
  自有作用域、`window.__dash` 取依赖、onclick 用到的函数挂 `window`）。
- `build-frontend.mjs` 的 `ENTRIES` 追加 `{ file: 'dashboard-traffic.js', kind: 'module' }`。

---

## 7. API 与可观测

### 7.1 REST（`/api/traffic`）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/traffic/config/{taskId}` | 读任务配置 |
| PUT | `/api/traffic/config/{taskId}` | 写配置（仅 `CONFIGURING` 放行，与既有 `updateConfig` 同规矩） |
| GET | `/api/traffic/recordings` | 当前用户的录制文件列表（供回放向导选择） |
| GET | `/api/traffic/recordings/{id}` | 录制详情（含 manifest 摘要、空洞、统计） |
| GET | `/api/traffic/recordings/{id}/download` | 下载 `.trfz`（代理到录制所在 agent） |
| POST | `/api/traffic/recordings/upload` | 上传 `.trfz`，落库 + 分发到目标 agent |
| DELETE | `/api/traffic/recordings/{id}` | 逻辑删除 + 通知 agent 回收磁盘 |
| GET | `/api/traffic/replay-errors/{taskId}` | 回放错误分页（代理 agent） |
| GET | `/api/traffic/replay-report/{taskId}` | 回放报告聚合（偏差分位数 / 错误 TOP / 慢语句 TOP） |
| POST | `/api/traffic/precheck` | 启动前预检（§9.1） |

任务生命周期**完全复用** `/api/workflows/*`（创建/启动/暂停/停止/删除/重试），不另起一套。

### 7.2 指标文件与活性（照本仓库既有约定）

```
files/<taskId>/metrics/traffic_capture_rate       # 语句/秒
files/<taskId>/metrics/traffic_capture_records    # 累计条数
files/<taskId>/metrics/traffic_capture_bytes      # 累计字节
files/<taskId>/metrics/traffic_srclog_backlog     # 源库轮转表待读行数 ← 追不上的早期信号
files/<taskId>/metrics/traffic_capture_liveness   # 无条件每轮改写
files/<taskId>/metrics/traffic_replay_progress    # 已回放 / 总数
files/<taskId>/metrics/traffic_replay_skew_us     # 当前时间轴偏差
files/<taskId>/metrics/traffic_replay_errors      # 累计错误
files/<taskId>/metrics/traffic_replay_liveness    # 无条件每轮改写
```

> **看门狗只认 `*_liveness`。** `[[fault-injection-resume-hardening]]` 与
> `[[latency-metric-truthfulness]]` 的教训：拿"有数据才更新"的业务指标当活性判据，
> 源库空闲时会误杀，上游冻结时又会被下游排空掩盖。
> `traffic_capture_liveness` 必须**与是否抓到语句无关**，每轮循环无条件改写。
>
> 反过来，`traffic_srclog_backlog` 持续上涨 = 捕获追不上源库产生速度，
> 是这条链路特有的、必须单独暴露的健康信号（binlog 链路的对应物是 `capture_queue_depth`）。

---

## 8. 安全与运维风险（本功能真正的难点）

### 8.1 回放到源库自己 —— 必须硬拦截

回放会在目标库执行 DML/DDL。若用户误选了源库，等于**把源库上刚发生的一切再做一遍**：
`INSERT` 变成重复插入、`UPDATE ... WHERE id=1 SET n=n+1` 变成二次累加、`DROP TABLE` 变成真的删。

→ 预检对比 `server_uuid`，相同即 **error 级硬拦截**。
参照既有的灾备"源目标隔离"检查（`MetadataService` 已有同类逻辑）。
强制放行需显式勾选 + 审计留痕（与 `launchWorkflow(force=true)` 同规矩）。

### 8.2 危险语句黑名单

默认拦截（`replay_allow_dangerous=0`）：

```
DROP DATABASE / DROP SCHEMA          -- 见 [[capability-review-2026-08-09]]：DROP DATABASE 直穿目标库
DROP USER / RENAME USER
SET GLOBAL / SET PERSIST             -- 改的是目标实例的全局状态，不是"业务流量"
SHUTDOWN / RESET MASTER / RESET REPLICA / PURGE BINARY LOGS
FLUSH PRIVILEGES / FLUSH TABLES WITH READ LOCK
INSTALL PLUGIN / UNINSTALL PLUGIN
GRANT ALL ... ON *.*                 -- 提权
```

命中即 `BLOCKED` 跳过并在报告里点名。DCL 整类默认关（`replay_allow_dcl=0`），
且带 `<secret>` 的 DCL **即使开了也无法执行**（§2.6 ④）。

### 8.3 源库开关的还原 —— 这是最大的运维风险

捕获要把源库的 `general_log` 打到 `ON`、`log_output` 打到 `TABLE`。若任务异常终止而没还原：

- 源库**每条语句**都继续往 `mysql.general_log` 写 → datadir 持续膨胀 → **把源库磁盘写满**；
- 性能持续受损；
- 没有任何人知道是谁开的。

**四道保险，缺一不可：**

1. **落库记录原值。** 开启前把 `general_log` / `log_output` 的原值写进
   `traffic_task_config.src_general_log_before` / `src_log_output_before`（不是只放内存）。
2. **子进程 shutdown hook** 还原（覆盖正常停止 / SIGTERM）。
3. **agent 侧兜底：`SourceGuard` 心跳。** 捕获子进程每 5s 刷 `traffic_capture_liveness`；
   `TrafficCaptureTask` 发现活性停滞超 `traffic.capture.guard.timeout.ms`（默认 60s）
   且守护已放弃 → **agent 自己连源库还原开关**，再置任务失败。
4. **agent 启动时扫尾。** `RecoveryService` 里增加一步：查出所有非终态的 `TRAFFIC_CAPTURE` 任务，
   若其进程已不在（agent 硬崩后重启的情形），逐个连源库按落库的原值还原。

> 这条正好对应 `[[fault-injection-resume-hardening]]` 里"启动即僵死是看门狗盲区"的形态：
> **进程起来了、立刻卡住，于是既没干活也没还原**。保险 3 与 4 就是专为它准备的。

### 8.4 传输与静态安全

- 源/目标连接直接复用既有 `source_ssl_*` / `target_ssl_*` 与 `SslMaterial`，无需新代码。
- **录制文件本身含明文 SQL，其中必然有业务数据**（`INSERT ... VALUES ('张三','13800138000')`）。
  → 下载接口做属主校验 + 审计；
  → 落盘复用 `capture.encryption.enabled` 同一套（`CredentialCipher`）作为可选项
     `traffic.recording.encryption.enabled`，默认跟随全局暂存加密开关。
- 录制里的 `<secret>` 已由 MySQL 抹掉，但 `SET PASSWORD` 之类**也会被抹**——
  这一点对我们有利，不必自己做脱敏。真正需要注意的是 `INSERT` 里的业务敏感字段，
  可复用订阅侧已有的 `DataMaskingService`（`column.mask.*`）做可选脱敏。

### 8.5 对源库的性能影响

`general_log=ON` 会给每条语句加一次 CSV 写 + 一把互斥锁。高 QPS 库上开销是实打实的。
设计上的应对：

- 预检**明确警示**并要求用户显式勾选确认（不是默认放行的 warning）；
- 提供按会话采样（`capture_sample_rate`）——但要说明采样会牺牲可回放性（§3.5）；
- 提供 `capture_backend=PERF_SCHEMA` 低侵入降级档（不改全局变量），
  并**如实标注**它会截断 >1KB 的 SQL 且在高负载下丢事件——这一档只适合"看看有哪些语句"，
  不适合回放；
- 文档与 UI 都写清楚：生产库上的长时间捕获应走 D（审计插件）方案，v1 不提供。

---

## 9. 预检与错误码

### 9.1 预检项（`POST /api/traffic/precheck`）

**捕获任务**

| 检查 | 级别 |
|---|---|
| 源库连通 | error |
| 账号具备 `SUPER` 或 `SYSTEM_VARIABLES_ADMIN`（改 `general_log`/`log_output`） | error |
| 账号具备 `SELECT` on `mysql.general_log` + `CREATE`/`DROP`/`ALTER` on `mysql`（轮转换表） | error |
| `general_log` 当前已是 ON 且 `log_output` 含 TABLE | info（说明不需要我们改，也不需要还原） |
| 源库当前 QPS 估算（`Questions` 两次采样） > 阈值 | **warning + 需显式确认**（§8.5） |
| 源库 datadir 剩余空间 | warning |
| agent 侧磁盘剩余 vs `capture_max_bytes` | error（不够直接拦） |
| 富化开启但 `events_statements_history` consumer 关闭 | warning（自动降级为不富化） |

**回放任务**

| 检查 | 级别 |
|---|---|
| 录制文件存在、`sealed=true`、SHA-256 校验通过 | error |
| 目标库连通 + 写权限 | error |
| **目标 `server_uuid` == 录制源 `server_uuid`** | **error（硬拦截，§8.1）** |
| `lower_case_table_names` 不一致 | error |
| `sql_mode` / `time_zone` / 大版本 不一致 | warning |
| 录制涉及的 schema 在目标库缺失 | warning |
| 录制含 DDL/DCL 但用户未勾选 | info（说明会被过滤掉多少条） |
| 录制的最大并发会话数 > `replay_max_sessions` | warning（说明会触发 LRU 淘汰） |
| 目标库 `max_connections` < `replay_max_sessions` | error |

### 9.2 错误码（三份目录必须同步）

`SyncErrorCodeCatalogTest` 扫的是三处**互不引用**的写法，漏一份不会有任何编译或运行期报错，
只会让用户在页面上看到「未知错误 (E31xx)」：

1. **引擎/agent 侧的发出点**——字符串字面量（`writeErrorStatus("E3120", …)`），本功能在 `migration-traffic` 与 `TrafficCaptureTask`/`TrafficReplayTask` 里；
2. `java-backend` 的 `SyncErrorCode` 枚举（人读的目录）；
3. `admin-dashboard.js` 的 `ERROR_CODE_MAP`（失败详情区**真正查表的地方**）。

| 码 | 名称 | 含义 |
|---|---|---|
| `E3120` | 源库语句日志开启失败 | 账号缺 `SYSTEM_VARIABLES_ADMIN`，或源库为只读副本 |
| `E3121` | 源库语句日志轮转失败 | `RENAME TABLE mysql.general_log` 被拒（缺 `mysql` 库权限）；不处理会让源库日志表无限膨胀 |
| `E3122` | 捕获落后源库产生速度 | `traffic_srclog_backlog` 持续超阈值；再不干预就会因轮转表堆积而影响源库 |
| `E3123` | 录制文件损坏 | manifest 缺失/SHA-256 不符/分段序号不连续 |
| `E3124` | 回放目标即录制源库 | `server_uuid` 相同，会把源库上的操作再做一遍 |
| `E3125` | 回放错误率超阈值 | 超 `traffic.replay.abort.error.rate`，已停止以免继续破坏目标库 |
| `E3126` | 源库语句日志未能还原 | 任务已结束但 `general_log` 仍为 ON——**必须人工确认**，否则源库磁盘会被写满 |

（`E3120` 起是空段，与既有 `E3101/E3102`（Elastic）不冲突。新增码顺延、不复用历史空位。）

---

## 10. 实施批次

| 批次 | 内容 | 可验收的产出 |
|---|---|---|
| **B1 捕获内核** | `migration-traffic` 模块骨架 + `GeneralLogTrafficSource`（含 `sql_log_off` 自噪声抑制、RENAME 轮转、字节级 `argument` 读取）+ `StatementClassifier` + `SessionSchemaTracker` + `TrafficWriter` | 独立跑子进程即能对一个 MySQL 产出 `.trf.gz` + manifest |
| **B2 源库守护** | `SourceGuard` 四道保险（§8.3）+ 体量护栏 + 空洞记录 | 杀进程 / 杀 agent / 断网 三种方式终止，源库开关都能还原 |
| **B3 回放内核** | `RecordingReader` + `ReplayScheduler`（绝对 deadline + parkNanos）+ `SessionRunner` + 危险语句黑名单 + `ReplayReporter` | 5s DDL / 10s DML / 15s SELECT 的用户样例，偏差 P99 < 50ms |
| **B4 平台接入** | Flyway V22 + 两个 entity/repo + `TrafficTaskService` + `TrafficController` + agent 执行器/ConfigService/AgentHttpServer + 状态枚举 ALTER | 能从 API 全流程建任务→启动→停止→下载文件→建回放→启动 |
| **B5 前端** | 菜单 + 列表 + 两套向导 + 详情抽屉（时间轴 / 空洞 / 回放报告）+ `build-frontend.mjs` 接入 | 页面全流程可操作 |
| **B6 预检与错误码** | `trafficPrecheck` + 7 个错误码三份目录同步 | `SyncErrorCodeCatalogTest` 绿 |
| **B7 判据套件** | `test_scripts/traffic/` + `autotest/suites/traffic.py` | §11 |

B1~B3 是本功能的全部技术风险所在；B4~B6 是照既有模式复刻，风险低但量大。

---

## 11. 判据（`test_scripts/traffic/traffic_e2e.py`）

对齐既有判据脚本的形态（直驱引擎子进程 + 真实 MySQL，参考 `test_scripts/column_processing/`）。

| # | 用例 | 判据 |
|---|---|---|
| 1 | 用户样例：5s DDL / 10s DML / 15s SELECT | 三条都在目标库执行；实际偏移与 5/10/15s 之差 < 200ms |
| 2 | 语句类别全覆盖 | SELECT/INSERT/UPDATE/DELETE/CREATE TABLE/ALTER/GRANT 各至少 1 条被录到 |
| 3 | 自噪声 | 捕获运行 60s，录制里**不含**捕获自身的轮询 SQL |
| 4 | 4 字节 UTF-8 | 录制里 `🚀` 原样，回放后目标表内容与源表一致 |
| 5 | 顺序保真 | 单会话 1000 条自增 `UPDATE n=n+1`，回放后目标值 == 1000（顺序错就不等） |
| 6 | 多会话交织 | 3 会话各 500 条，录制中各会话内部序号严格递增 |
| 7 | 事务边界 | `BEGIN … ROLLBACK` 的中间 DML 不落目标库 |
| 8 | prepared statement | JDBC `PreparedStatement` 的语句以 `Execute`（参数已替换）录入并成功回放 |
| 9 | 口令抹除 | `CREATE USER … IDENTIFIED BY` 录成 `rd=true`，回放跳过并计入报告 |
| 10 | 源端报错语句 | `SELECT * FROM 不存在的表` 被录；回放报错但不计入 `REPLAY_ERROR`（需开富化） |
| 11 | 轮转不丢 | 把 `rotate.rows` 调到 100，灌 10 万条，录制条数 == 灌入条数 |
| 12 | 时间轴空洞 | 捕获中途 pause 30s 再 resume，manifest 出现 1 条 gap，回放耗时含这 30s |
| 13 | 崩溃耐受 | `kill -9` 捕获子进程，最后一行半行被丢弃，manifest 可重建，其余记录完好 |
| 14 | **开关还原（三种终止方式）** | 正常停止 / `kill -9` 子进程 / `kill -9` agent 后重启 —— 三种情况下源库 `general_log` 均回到 OFF、`log_output` 回到原值 |
| 15 | 回放到源库自己 | 预检返回 error 且 `launch` 被拒（`E3124`） |
| 16 | 危险语句拦截 | 录制含 `DROP DATABASE`，默认配置下被 `BLOCKED`，目标库该库仍在 |
| 17 | 倍速 | `speed=2.0` 时总耗时 ≈ 录制时长的一半，会话内顺序不变 |
| 18 | 落后策略 | 目标库人为加锁制造阻塞，`SKIP` 档跳过数 > 0 且任务不失败；`WAIT` 档零跳过、偏差指标上涨 |
| 19 | 文件往返 | 下载 `.trfz` → 删除 agent 上的原文件 → 上传 → 回放成功，SHA-256 一致 |

> 判据脚本的两个既有坑（`[[lob-test-suite]]` / `[[capability-review-2026-08-09]]`）：
> **子进程 stdout 必须持续排空**（否则子进程阻塞在写日志上），
> **判据要跑 fat jar 而不是只 `mvn compile`**。

---

## 12. 风险与取舍

| 风险 | 处置 |
|---|---|
| `general_log` 拖慢源库 | 预检显式确认 + 采样档 + 低侵入降级档；文档写明生产建议走审计插件 |
| 忘记还原开关写满源库磁盘 | 四道保险（§8.3）+ 判据 14 三种终止方式全覆盖 + `E3126` 单独报警 |
| 捕获追不上高 QPS 源库 | `traffic_srclog_backlog` 指标 + `E3122`；轮转表堆积到阈值即主动降级为采样或停任务 |
| 回放误伤目标库 | `server_uuid` 硬拦截 + 危险语句黑名单 + DCL 默认关 + 错误率熔断 |
| 录制文件泄露业务数据 | 属主校验 + 审计 + 可选落盘加密 + 可选列脱敏 |
| 库名不同导致回放失败 | v1 明确要求同名 schema，预检提示；改写留作二期 |
| 会话数超目标库上限 | 预检比对 `max_connections`；运行期 LRU 淘汰（不淘汰事务中会话） |
| 时间精度不足 | 绝对 deadline + parkNanos + 自旋；偏差分位数作为一等指标暴露，做不到就让用户看得见 |

**明确接受的不完美：**

1. 捕获停摆期间的语句**永久丢失**，只记空洞。这是语句流的物理性质，不是实现缺陷。
2. 带口令的 DCL **不可回放**。MySQL 在写日志时就抹了，任何方案都拿不到。
3. 回放不保证与源库**同样的结果**——目标库的数据基线、并发度、优化器统计信息都不同。
   回放保证的是「同样的语句、同样的顺序、同样的时间间隔」，结果差异正是要被报告出来的东西。

---

## 附：与本仓库既有教训的对齐

| 既有教训 | 本设计的对应处 |
|---|---|
| 值转换让服务端/驱动代劳 → 静默损坏 | §2.8 `argument` 按字节读，自己 UTF-8 解码 |
| 拿业务指标当活性判据 → 空闲误杀 / 上游冻结被掩盖 | §7.2 `*_liveness` 与是否抓到语句无关，每轮无条件写 |
| 启动即僵死是看门狗盲区 | §8.3 保险 3：活性停滞即由 agent 还原源库并判失败 |
| 子进程崩了没人拉起 → 任务显示正常、实际什么都没干 | 复用 `ProcessGuard`；守护放弃即判任务失败（照 `SubscribeTask` 的处理） |
| `.cap` 半行 | §3.4 分段最后一行不完整则整行丢弃，manifest 由分段扫描重建 |
| DROP DATABASE 直穿目标库 | §8.2 危险语句黑名单 |
| 错误码三份目录漂移 | §9.2 三处同步，`SyncErrorCodeCatalogTest` 是 CI 门禁 |
| 改 common 后 fat jar 要 clean install | B7 判据必须跑 fat jar |
| 判据脚本子进程 stdout 不排空会阻塞 | §11 脚注 |
| 指纹绝不能用 XOR 聚合 CRC32 | §3.2 录制文件用 SHA-256，不自造聚合 |

---

## 13. 交付记录（2026-08-20，B1~B7 全部完成）

七个批次按序实施，每批实现完即在真实 MySQL 上验证。**下面每一条"实测发现"都是验证跑出来的，不是设想。**

### B1 捕获内核

`migration-traffic` 模块 + `GeneralLogTrafficSource` + `StatementClassifier` +
`SessionSchemaTracker` + `TrafficWriter` + `CaptureFilter`。36 个单测。

验证跑出 4 个缺陷，全部修掉：

1. **SIGTERM 会丢数据、且不还原源库开关。** JVM 收到 SIGTERM 后<b>只等 shutdown hook、不等 main 线程</b>；
   hook 里只置 stop 标志就返回，JVM 随即退出，主循环被拦腰砍断。后果是双份的：
   最后一批已读到的语句还在缓冲区里直接丢掉，`source.close()` 从未执行、源库 general_log 一直开着。
   → hook 改为阻塞等 `awaitFinished(30s)`。
2. **无默认库的 Connect 行被解析成 `"using Socket"`。** 实测格式是
   `root@localhost on  using Socket`（两个空格），`" using "` 恰好紧贴 `" on "` 之后，
   旧的边界判断回退去取整个尾巴。后果：每个无库连接的默认库都被记成 `using Socket`，
   回放时这些会话会去 USE 一个不存在的库。
3. **Connect/Quit 被计进语句类别直方图**，用户跑 3 条语句却看到 `other: 34`。
4. **连接池会话的默认库全是未知。** 池里的连接在捕获开始前就建好了，它们的
   `Connect`/`Init DB` 行永远不会再出现 → 补 `information_schema.PROCESSLIST` 播种。
   不补的话，连接池（生产常态）场景下所有非限定表名语句在回放时都会撞
   "No database selected"。

### B2 源库守护 + 体量护栏 + 续录

`TrafficSourceState`（落 migration-common，agent 要用而 agent 不依赖引擎模块）、
`GuardedCaptureRunner`、`RecordingRecovery`、`--mode restore`。

验证跑出 2 个缺陷：

1. **被 kill 的分段被判成 0 条、且新段号退回 1 覆盖了它。**
   `BufferedReader` 按 8KB 填充，填充时底层抛 `EOFException`（gzip 无 trailer），
   **这一块里已经解出来的内容会连同缓冲区一起丢掉**——实测 18 行完整数据被判成 0 条，
   紧接着新分段以同样的段号写下去，把文件覆盖。改为按块读、边读边切行；
   段号取 manifest 与磁盘上实际最大值之中更大的那个。
2. 时间轴空洞的起点因缺陷 1 恒为 0。

### B3 回放内核

`RecordingReader` / `ReplayScheduler` / `SessionRunner` / `DangerousStatementFilter` /
`ReplayReporter` / `TrafficReplayRunner`。累计 62 个单测。

验证跑出 2 个缺陷：

1. **`Init DB` 记录存的是裸库名，回放时被当 SQL 执行 → 1064 语法错。**
   捕获侧改存等价的可执行形态 `USE \`db\``，回放侧对 `c=I` 只切库不执行原文
   （兼容老录制里的裸库名）。
2. **时间轴偏差量在了派发侧，不是执行侧。** 会话队列有上万深度，派发线程可以准点把语句
   全丢进队列（偏差 ≈ 0），而目标库还在慢吞吞执行、实际落后几分钟。
   实测 speed=8 压过头时：<b>派发偏差 P99=4ms，执行偏差 P50=776ms、max=1.4s</b>。
   只报前者等于报了一个永远好看的数字。改为主指标取执行侧，派发侧作为排障用的次要指标。
   顺带把会话创建提到 `awaitTurn` 之前——建连的几十毫秒本来会原样计进第一条语句的偏差。

### B4 平台接入

Flyway `V22`（状态 ENUM 扩容 + `traffic_task_config` + `traffic_recordings` + 审计动作）、
两个 entity/repo、`TrafficTaskService`、`TrafficController`、
agent 侧 `TrafficCaptureTask`/`TrafficReplayTask`/`TrafficSourceGuardService`/`TrafficRecordingFetcher`、
`ConfigService` 展开、`AgentHttpServer` 四个端点。

验证跑出 2 个缺陷：

1. **改错了地方：`TaskMessageHandler` 是死代码，真正的任务分派在 `AgentMain` 里。**
   流量任务被路由去跑 SQL 全量迁移（migration-full），随即以 "exit code 1" 失败——
   报错里没有任何字样能让人联想到"路由错了"。两处都补上。
2. **`stopAllProcesses()` 不认识流量任务的 ProcessGuard，捕获子进程根本停不掉。**
   它变成孤儿继续跑，源库 general_log 一直开着。实测现象：任务显示"已结束"、
   源库日志表还在涨。改为覆盖基类已有的 `stopExtraProcesses` 钩子，
   且顺序必须是**先停进程、再兜底还原**。

### B5 前端

菜单项 + `trafficPage` + 两套向导 + 录制文件列表 + 回放报告 + `dashboard-traffic.js` +
构建入口。用浏览器实际驱动了完整流程（建任务 → 配置 → 启动 → 停止 → 自动同步录制 →
建回放 → 选录制 → 启动 → 看报告），无控制台报错。

验证跑出 1 个缺陷：模态框用错了约定——本仓库是外层 `.modal` + 内层 `.modal-content`
靠 `.show` 切换，我写成了 `.modal-overlay` + `.modal` + `.active`，弹窗永远不显示。

### B6 预检与错误码

`TrafficPrecheckService`（复制侧 6 项、回放侧 7 项）+ `POST /api/traffic/precheck/{id}` +
7 个错误码（E3120~E3126）落三份目录，`SyncErrorCodeCatalogTest` 门禁通过。

验证跑出 3 个缺陷：

1. **`TrafficTaskService` ↔ `TrafficPrecheckService` 循环依赖，整个后端起不来。**
   预检改为直接注入仓储（它本来也只需要读配置）。
2. **`lower_case_table_names` 按字符串比 → `"0.0" != "0"`。**
   指纹是 JSON，Jackson 把数字读成 Double。这会把<b>每一次合法回放</b>都误拦下来。
3. **拿"配置的会话上限"跟 `max_connections` 比。** 默认上限 200 > MySQL 默认 151，
   于是默认配置永远预检失败。改为比"这份录制**实际**要用多少条连接"（录制峰值并发）。

### B7 判据套件

* `test_scripts/traffic/traffic_e2e.py` —— 引擎级，7 个用例 **49 项全通过**；
* `autotest/suites/traffic.py` —— 平台级，3 条用例 **37 项断言全通过**，
  并入 `standard` 用例集与 `traffic` 分组。

顺带修掉一个真实的产品问题：**源库互斥锁抢不到就立刻判死**。锁是会话级的，
"上一个捕获任务刚停、连接还没被服务端回收"是正常的几秒窗口，立刻失败会让
"停掉再重启同一个任务"这种最普通的操作报错。改为有界等待 20s。

### 最终实测数字

| 项 | 结果 |
|---|---|
| 用户原样场景（5s DDL / 10s DML / 15s SELECT） | 目标库实测间隔 **5.021s / 4.992s** |
| 时间轴偏差（1 倍速，执行侧） | P50 **8ms** / P95 **43ms** / P99 **43ms** |
| 会话内顺序保真 | 1000 条 `n=n+1` → 目标值正好 **1000** |
| 4 字节 UTF-8 | `中文🚀emoji` 原样往返 |
| `DROP DATABASE` | 被拦下并记入报告，目标库该库仍在 |
| 回放到录制源库自己 | 预检 FAIL + 启动被拒（E3124） |
| `kill -9` 后源库开关 | 状态文件留存 → 兜底还原成功 |
| 单元测试 | 引擎 62 + 后端 160，全绿 |
