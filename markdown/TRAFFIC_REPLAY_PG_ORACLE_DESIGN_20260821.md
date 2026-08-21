# 流量复制与回放：PostgreSQL / Oracle 扩展设计方案

> 前置：MySQL 版已交付（`markdown/TRAFFIC_REPLAY_DESIGN_20260820.md`，B1~B7，commit `4a6088f`）。
> 本文只讲**新增 PG / Oracle 两种引擎**要做什么：哪些能原样复用、哪些必须重写、
> 哪些"以为一样、其实完全不一样"。
>
> **本文所有标注「实测」的结论都是在本机真库上跑出来的**，不是查文档得来的：
> - PostgreSQL **18.1**（本次专门起的一次性探针容器，用完已删）
> - PostgreSQL **16.13**（`dr-pg-a`，用于权限与降级形态验证，改动已全部还原）
> - Oracle **AI Database 26ai Free 23.26.2.0.0**（`oracle_db`，`Unified Auditing=TRUE`，
>   探针用的审计策略/用户/表已全部清理）
>
> 没实测的部分在 §11 单独列出来，实现前必须先补测——不要把它们当结论用。

---

## 0. 一句话结论

**三种引擎的捕获通道之间没有一行可共用的代码，但录制格式、调度器、会话模型、
守护与还原骨架可以整块复用。**

真正的工作量在三件事，按风险从高到低：

1. **录制格式必须升到 v2**：MySQL 之外的两种引擎**都不把绑定参数替换进 SQL 文本**
   （实测，§2.3⑤ / §3.3③）。v1 的记录行没有放参数的地方，不升格式就只能录下
   `SELECT * FROM t WHERE id = $1` 这种回放必然报错的半截语句。
2. **回放侧要从"MySQL 一种方言"拆出 `TargetDialect`**：切库（`USE` vs 连接即绑库 vs
   `ALTER SESSION SET CURRENT_SCHEMA`）、会话环境对齐（`sql_mode` vs `search_path` vs `NLS_*`）、
   身份指纹、危险语句黑名单，四处全都不一样。
3. **两个新的 `TrafficSource` 实现**，各自带一套完全不同的源端开关与还原语义。

反过来说，**`ReplayScheduler`（绝对 deadline + parkNanos）、`SessionRunner` 的 1:1:1 会话模型、
分段 JSONL+gzip 写入、崩溃恢复、体量护栏、四道保险的骨架、指标文件与看门狗约定
一行都不用重写**——这些是与引擎无关的。

---

## 1. 三引擎对照表（全文最重要的一页）

| 维度 | MySQL（已交付） | PostgreSQL | Oracle |
|---|---|---|---|
| 捕获通道 | `mysql.general_log`（`log_output=TABLE`） | 服务端日志文件（`jsonlog`/`csvlog`），经 `pg_read_binary_file()` 读 | `UNIFIED_AUDIT_TRAIL`（统一审计） |
| 拿得到 SELECT | ✓ | ✓ | ✓ |
| 语句原文完整性 | 完整（`mediumblob`） | 完整（日志字段无长度上限） | 完整（`SQL_TEXT` 是 **CLOB**，实测 32600） |
| **绑定参数形态** | **已替换进 SQL**（`Execute` 行） | **不替换**：`$1` 留在文本里，值在 `detail` 的 `Parameters:` 里 | **不替换**：`:b1` 留在文本里，值在 `SQL_BINDS` 里 |
| 口令 | **库自己抹**（`IDENTIFIED BY <secret>`） | **明文进日志**（`PASSWORD 'S3cretPass'`） | **库自己抹**（`IDENTIFIED BY *`） |
| 会话内定序键 | 物理读序 | `session_id` + `line_num` | `SESSIONID` + `ENTRY_ID` |
| 全局定序 | CSV 平铺文件的扫描序 | 日志文件的字节序 | `EVENT_TIMESTAMP` + 会话内序号 |
| 时间精度 | **微秒** | **毫秒** | **微秒**（`TIMESTAMP(6)`） |
| 时区 | 服务端 `time_zone` | `log_timezone`（实测 `Etc/UTC`） | **UTC**（另有 `EVENT_TIMESTAMP_UTC`） |
| 会话身份 | `thread_id` | `session_id`（如 `6a87cfdd.147`） | `SESSIONID`（3.0e17 量级 NUMBER） |
| 默认 schema 怎么来 | 自己跟踪 `Connect`/`Init DB`/`USE` | **每行自带 `dbname`**，不用跟踪 | 连接用户即默认 schema，另跟踪 `ALTER SESSION SET CURRENT_SCHEMA` |
| 源端错误码 | 要开 `performance_schema` 富化 | **免费**：`error_severity`+`state_code` 同在日志里 | **免费**：`RETURN_CODE` 就是 ORA 号 |
| 源端耗时 | 富化（`TIMER_WAIT`） | 免费（`duration: N ms` 行，§2.3⑩） | 无（审计不带耗时） |
| **捕获位点可续** | ✗ 停了就永久丢 | **✓（文件名 + 字节偏移）** | **半可续**（时间戳+会话序号游标，取决于审计未被清理） |
| 要改的源端开关 | `general_log`、`log_output` | `log_statement`、`log_destination`、`log_min_duration_statement`、`log_duration` | 创建并启用一条审计策略 |
| **硬前置** | 无 | **`logging_collector` 是 postmaster 参数，关着就必须重启实例** | 需 `AUDIT_ADMIN` 角色 |
| 不还原的后果 | `mysql.general_log` 撑爆源库 datadir | 日志文件撑爆日志盘 | 审计记录撑爆 **AUDSYS/SYSAUX** |
| 身份互锁依据 | `server_uuid` | `pg_control_system().system_identifier` | `V$DATABASE.DBID` + `V$PDBS.CON_UID` |

> 一眼能看出来的三件事：
> **① 参数不替换是 PG 和 Oracle 的共同点，也是格式必须升级的唯一原因；
> ② 口令处理三家三个样，PG 是唯一需要我们自己脱敏的；
> ③ PG 的捕获是三家里唯一"真有位点"的——语句流不可续这条铁律对 PG 不成立。**

---

## 2. PostgreSQL 捕获

### 2.1 候选与淘汰

| 方案 | 拿得到 SELECT | 完整性 | 结论 |
|---|---|---|---|
| A. 逻辑复制 / WAL | ✗ | — | **直接出局**，与 binlog 同一个理由 |
| B. `log_statement=all` + 服务端日志文件 | ✓ | 完整、不采样 | **选它做主通道** |
| C. `pg_stat_statements` | ✓ | 归一化后的模板（`$1`），**没有执行序、没有到达时刻** | 出局，做不了回放 |
| D. `pg_stat_activity` 轮询 | ✓ | 采样，短语句必漏 | 出局 |
| E. `pgaudit` 扩展 | ✓ | 完整 | 输出仍然落日志文件 = B 的超集；要装扩展。留扩展点，v1 不做 |

**B 的关键可行性**：PG 允许用 SQL 读自己的服务端文件——
`pg_ls_logdir()` 列目录、`pg_current_logfile()` 拿当前文件、
`pg_read_binary_file(path, offset, length)` 按**字节偏移**增量读。
这意味着 agent 与源库**不必同机**（本工程的基本前提），纯 JDBC 可达。

### 2.2 选定形态

```
log_destination      = jsonlog   （PG ≥ 15；PG 13/14 退回 csvlog）
logging_collector    = on        （前置，必须本来就是 on）
log_statement        = all
log_min_duration_statement = 0   （可选，只为拿源端耗时）
log_duration         = off       （必须显式关掉，见 §2.3⑩）
```

读取：`pg_current_logfile('jsonlog')` → `pg_read_binary_file(file, offset, chunk)` →
按行切 → 每行一个 JSON 对象。位点 = `(文件名, 已读字节偏移)`。

### 2.3 实测证据（PostgreSQL 18.1，除非另注）

**① `logging_collector` 是 postmaster 参数——这是 PG 侧唯一的硬前置**

```
name              | setting | context
logging_collector | off     | postmaster      ← 官方 docker 镜像默认就是 off
log_destination   | stderr  | sighup
log_statement     | none    | superuser
```

`postmaster` 上下文 = **只能重启实例才能改**。关着的时候日志走 stderr（在容器里就是
容器 stdout），**没有任何文件可读**。我们不去重启用户的数据库，所以这一条是预检的
`error`，附带确切的补救动作。

**② 而 `log_statement` / `log_destination` 是可以热改的，且对已存在的会话也生效**

`ALTER SYSTEM SET log_statement='all'` + `pg_reload_conf()`，实测：

```
（先把 log_statement 置 none，开一个会话）
SELECT 'EXISTING_SESSION_BEFORE';   → 日志里 0 命中
（在这个会话不断开的情况下，另开连接 ALTER SYSTEM + reload）
SELECT 'EXISTING_SESSION_AFTER';    → 日志里 1 命中
```

→ **连接池里那些"捕获开始前就建好的连接"会立刻开始被记录**。
MySQL 那边为此要去 `information_schema.PROCESSLIST` 播种默认库（B1 缺陷 4），
PG 不需要这个补丁。

**③ `ALTER SYSTEM` 会被命令行参数静默压过——改完必须回读**

探针容器最初带 `-c log_destination=stderr` 启动，此时：

```
ALTER SYSTEM SET log_destination='csvlog';  → 返回 ALTER SYSTEM（成功）
SELECT pg_reload_conf();                    → t
SELECT name,setting,source FROM pg_settings WHERE name='log_destination';
  log_destination | stderr | command line     ← 没生效，且没有任何报错
```

命令行 > `postgresql.auto.conf`（ALTER SYSTEM 写这里）。生产上等价的形态是
`postgresql.conf` 里的 `include` 文件或启动脚本参数。
→ **下发完必须回读 `pg_settings.setting` 与 `source` 逐项校验**，
不一致即 `E3128` 失败退出。否则就是"改了、没生效、录出一个空文件、全程零报错"。

**④ `logging_collector=off` 时设 `jsonlog` 会被接受，然后什么都不产生**（PG 16.13 实测）

```
show logging_collector;                       → off
ALTER SYSTEM SET log_destination='jsonlog';   → 成功
SELECT pg_reload_conf();                      → t
SELECT setting,source FROM pg_settings ...    → jsonlog | configuration file   ← 值确实变了
SELECT pg_current_logfile();                  → （空）                          ← 但没有文件
（服务端日志里只有一行 LOG: parameter "log_destination" changed to "jsonlog"，无警告）
```

→ 这是 ③ 之外的第二个静默陷阱，检测点是 **`pg_current_logfile()` 返回空**。

**⑤ 扩展协议的参数不替换进 SQL——PG 与 MySQL 最大的分歧**

```jsonc
{"session_id":"6a87cfdd.147","line_num":2,"error_severity":"LOG",
 "message":"execute <unnamed>: SELECT $1::text, $2::int ",
 "detail":"Parameters: $1 = 'p1', $2 = '7'"}
```

MySQL 的 `Execute` 行给的是**参数已替换的完整 SQL**（可以直接扔给目标库执行）；
PG 给的是 `$n` 占位符 + 另一份参数清单。→ **回放必须绑参**，录制格式必须能装参数（§4）。

参数渲染规则（实测）：

```
Parameters: $1 = '', $2 = 'has,comma and ''quote''', $3 = 'NULL'
            ↑空串     ↑逗号在值里、单引号成对转义        ↑这是字符串"NULL"，带引号
```

→ 解析规则：**带引号 = 文本值（`''` 还原成 `'`）；裸 `NULL` = SQL NULL**。
这正是 `[[silent-loss-audit-2026-08-11]]` 里"PG 空串被写成字符串 `'NULL'`"那一类坑的同源形态，
只不过这次是在读的一侧。

**⑥ PG 不抹口令——录制文件是凭据泄露面**

```
{"message":"statement: CREATE USER trfprobe WITH PASSWORD 'S3cretPass'"}
```

MySQL 和 Oracle 都由数据库自己抹掉（`<secret>` / `*`），**PG 明文写进日志**。
→ PG 侧必须由我们自己脱敏：捕获时对 `CREATE|ALTER ROLE|USER ... PASSWORD '...'`、
`ENCRYPTED PASSWORD '...'` 做正则替换，落盘前就把值换成占位符并打 `rd=true`。
**脱敏必须发生在写盘之前**，不能等到展示层——录制文件会被下载、会被上传到别的机器。

**⑦ 自噪声：会话级 `SET log_statement='none'` 有效，与 MySQL 的 `sql_log_off` 同构**

```
（同一会话，两条独立语句）
SET log_statement='none';            → 这一条自己会被记（常数条，可按 session_id 过滤）
SELECT 'NOISE_TEST_2';               → 日志里 0 命中  ✓
```

但有个边界：**同一个 simple query 里 `SET ...; SELECT ...` 串在一起时，整串一起被记**
（实测 `SHOULD_NOT_BE_LOGGED_1` 被记了 1 次）——PG 是在处理查询串**之前**判定是否记录的。
→ 采集连接建立后，第一件事就是**单独**发一条 `SET log_statement='none'`，
并且顺带把 `log_min_duration_statement=-1`、`log_duration=off` 也置掉（否则耗时行照样是噪声）。

**⑧ 有真正的位点：文件名 + 字节偏移**

```
SELECT pg_current_logfile('jsonlog');        → log/postgresql-2026-08-21_041107.json
SELECT pg_read_binary_file('log/…json', 0, 120);      → 前 120 字节
SELECT pg_read_binary_file('log/…json', 999999999, 100, true);  → 0 字节（越过 EOF 不报错）
SELECT length(pg_read_binary_file('/绝对路径/…json'));           → 超级用户可读绝对路径
```

→ **PG 捕获重启后能从上次的字节偏移继续读**，只要那个日志文件还没被轮转清掉。
MySQL 版设计里"语句流是易失的、停摆即永久空洞"这条铁律，**对 PG 只在
'日志文件已被删除' 时才成立**。这是本次扩展里最值得写进产品说明的一条差异：
PG 捕获任务的暂停/崩溃恢复**通常不产生时间轴空洞**。
（跨文件轮转时按 `pg_ls_logdir()` 的修改时间序补齐中间文件，位点结构变成
`(文件名, 偏移)` 的有序列表。）

**⑨ 失败的语句会产生两行——不去重就会重放两遍**

```jsonc
{"line_num":3,"error_severity":"LOG","message":"statement: SELECT * FROM nope_json;"}
{"line_num":4,"error_severity":"ERROR","state_code":"42P01",
 "message":"relation \"nope_json\" does not exist",
 "statement":"SELECT * FROM nope_json;","cursor_position":15}
```

→ 只有 `error_severity=LOG` 且 `message` 以 `statement: ` / `execute …: ` 开头的行才是语句；
`ERROR` 行**只用来给上一条同会话语句补 `state_code`**（这就是 PG 版的"富化"，且免费）。
把 ERROR 行也当语句录进去，回放时每条失败语句都会被执行两次。

**⑩ 与 `log_min_duration_statement` 同开时 PG 不重复语句文本**

```jsonc
{"line_num":1,"message":"statement: SELECT 'DUP_PROBE_A'"}
{"line_num":2,"message":"duration: 0.182 ms"}          ← 单独一行，不带文本
```

→ 按 `session_id` + 相邻 `line_num` 配对即可拿到源端耗时。
**但形态是随开关组合变的**：只开 duration 不开 statement 时是
`duration: N ms  statement: …`（文本回来了）；再把 `log_duration=on` 打开会变成
`parse`/`bind`/`execute` 三连——**同一条语句被记三次**。
→ 捕获必须**强制一套已知组合并记录原值**（§2.5），解析器同时要认识并丢弃 `parse:`/`bind:` 行。

**⑪ jsonlog 优于 csvlog，且不是"风格偏好"**

csvlog 里一条多行语句就是**物理多行**：

```
2026-08-21 04:09:18.917 UTC,"postgres","probedb",207,…,"statement: SELECT 1 AS multi
  , 2 AS line ",,,,,,,,,"psql","client backend",,0
```

→ csvlog 必须用带引号状态机的 CSV 解析器，且列是**按位置**取的（列集合随 PG 版本增删）。
jsonlog 把换行转义成 `\n`，**一行一条记录、按键名取值**，两个问题一起消失。
→ **PG ≥ 15 一律用 jsonlog**；PG 13/14 才退回 csvlog，并按版本维护列位表。

**⑫ 会话身份与默认库直接可用**

`session_id`（`启动时刻.PID` 的十六进制，如 `6a87cfdd.147`）在会话生命周期内稳定；
`line_num` 会话内单调递增；**每行都带 `dbname` 与 `user`**。
→ PG 不需要 MySQL 那个 `SessionSchemaTracker`（PG 的连接终生绑定一个库，切不了）。
需要跟踪的只有 `search_path`（靠录制里的 `SET search_path` 语句复现）。

### 2.4 最小权限集（逐条实测，PG 16.13）

不要求超级用户。实测下面这组授权刚好够用，**少一条就会在某个环节失败**：

```sql
GRANT pg_monitor          TO trfcap;   -- pg_ls_logdir()
GRANT pg_read_server_files TO trfcap;  -- log_directory 为绝对路径时读文件
GRANT EXECUTE ON FUNCTION pg_read_binary_file(text,bigint,bigint,boolean) TO trfcap;
GRANT EXECUTE ON FUNCTION pg_current_logfile() TO trfcap;
GRANT EXECUTE ON FUNCTION pg_reload_conf()     TO trfcap;
GRANT ALTER SYSTEM ON PARAMETER
      log_statement, log_destination, log_min_duration_statement, log_duration TO trfcap;  -- PG ≥ 15
GRANT SET ON PARAMETER
      log_statement, log_min_duration_statement, log_duration TO trfcap;                   -- 会话级消噪
```

实测踩到的两处**分别独立**的拒绝（不是同一条权限）：

```
（只给了 pg_read_server_files + ALTER SYSTEM ON PARAMETER 时）
SET log_statement='none';          → ERROR: permission denied to set parameter "log_statement"
select count(*) from pg_ls_logdir(); → ERROR: permission denied for function pg_ls_logdir
（补 GRANT SET ON PARAMETER + pg_monitor 之后两条都通过）
select pg_current_logfile();       → ERROR: permission denied for function pg_current_logfile
                                     ← pg_monitor 也不含它，必须单独 GRANT EXECUTE
```

> `ALTER SYSTEM ON PARAMETER` / `SET ON PARAMETER` 是 **PG 15** 才有的授权语法。
> PG 13/14 上这两项只能靠超级用户 —— 预检要按版本给出不同的补救文案。

### 2.5 源端还原（PG 版的"最大运维风险"）

捕获期间源库每条语句都在往日志盘写。不还原的后果与 MySQL 同级：**把日志盘写满**。

落库/落盘要记的原值（比 MySQL 多两个）：
`log_statement`、`log_destination`、`log_min_duration_statement`、`log_duration`，
外加 `logging_collector` 的原值（只读，用于判断"是不是我们开的"——**我们从不改它**）。

还原动作：对每个被我们改过的参数执行 `ALTER SYSTEM SET <name> = <原值>`
（原值为 `default` 来源时用 `ALTER SYSTEM RESET <name>`），再 `pg_reload_conf()`，
**然后回读校验**（同 §2.3③，还原也会被命令行压过——只是这种情况下"没生效"反而是安全的，
但必须如实上报，否则用户以为还原了）。

四道保险与 MySQL 版完全一致（子进程 shutdown hook / agent 任务收尾 / 看门狗判定守护放弃 /
agent 重启扫尾），判据依据仍是录制目录里的 `source_state.properties` 还在不在。

---

## 3. Oracle 捕获

### 3.1 候选与淘汰

| 方案 | 拿得到 SELECT | 完整性 | 结论 |
|---|---|---|---|
| A. LogMiner / redo | ✗ | — | **出局**，同 binlog |
| B. **统一审计**（`CREATE AUDIT POLICY` → `UNIFIED_AUDIT_TRAIL`） | ✓ | 完整，含 SQL 原文与绑定值 | **选它做主通道** |
| C. `V$SQL` / `V$SQLAREA` | ✓ | 共享游标，**没有逐次执行的序与时刻** | 出局 |
| D. `V$ACTIVE_SESSION_HISTORY` | ✓ | 1 秒采样 + 需 Diagnostics Pack 许可 | 出局 |
| E. 10046 trace（`DBMS_MONITOR` + `V$DIAG_TRACE_FILE_CONTENTS`） | ✓ | 完整、含绑定与微秒时刻 | 要解析 raw trace 格式，成本远高于 B。留扩展点 |
| F. 传统审计（`AUDIT_TRAIL=DB,EXTENDED`） | ✓ | `SQL_TEXT` 只有 VARCHAR2(4000) | 出局：12c 起被统一审计取代，23ai 已移除 |

### 3.2 选定形态

```sql
CREATE AUDIT POLICY <p> ACTIONS ALL ONLY TOPLEVEL;   -- ONLY TOPLEVEL 排除递归 SQL，必须带
AUDIT POLICY <p> BY <被录用户列表>;                    -- 或 EXCEPT <采集账号>
```

读取：按 `(EVENT_TIMESTAMP, SESSIONID, ENTRY_ID)` 游标增量查 `UNIFIED_AUDIT_TRAIL`。

### 3.3 实测证据（Oracle AI Database 26ai Free 23.26.2.0.0）

**① 一条策略就把全部语句类别拿全了**

```
ACTION_NAME    SQL_TEXT                                        RETURN_CODE  SQL_BINDS
LOGON          （空）                                                 0
CREATE TABLE   CREATE TABLE trf_t1 (id NUMBER PRIMARY KEY, …)         0
INSERT         INSERT INTO trf_t1 VALUES (1, 'a')                     0
COMMIT         COMMIT                                                 0
SELECT         SELECT * FROM trf_t1 WHERE id = 1                      0
EXECUTE        BEGIN :b1 := 2; :b2 := 'zh中文emoji'; END;              0
INSERT         INSERT INTO trf_t1 VALUES (:b1, :b2)                   1   #1(1):2 #2(9):zh中文emoji
SELECT         SELECT * FROM trf_t1 WHERE id = :b1                    0   #1(1):2
UPDATE         UPDATE trf_t1 SET v='b' WHERE id=1                     0
ROLLBACK       ROLLBACK                                               0
SELECT         SELECT * FROM no_such_table_here                     942
CREATE USER    CREATE USER trf_probe_user IDENTIFIED BY *              0
ALTER USER     ALTER USER trf_probe_user IDENTIFIED BY *               0
DROP USER      DROP USER trf_probe_user                                0
LOGOFF         （空）                                                 0
```

**② `SQL_TEXT` / `SQL_BINDS` 都是 CLOB，不截断**

```
COLUMN_NAME  DATA_TYPE     DATA_LENGTH
SQL_TEXT     CLOB          32600
SQL_BINDS    CLOB          32600
EVENT_TIMESTAMP      TIMESTAMP(6)
EVENT_TIMESTAMP_UTC  TIMESTAMP(6)
SESSIONID / ENTRY_ID / STATEMENT_ID / RETURN_CODE   NUMBER
```

对比传统审计 `DBA_AUDIT_TRAIL.SQL_TEXT` 的 `VARCHAR2(4000)`——那是会静默截断的，
正是 `[[silent-loss-audit-2026-08-11]]` 里 "Oracle CSF 4000 字节" 同一类坑。选统一审计就没这问题。
（读 CLOB 时按 `getCharacterStream` 流式取，不要 `getString` 一次性拉 32K×N 行。）

**③ 绑定值必须按声明长度切片，不能按分隔符切**

```
#1(1):2 #2(9):zh中文emoji
 ↑序号 ↑字符长度 ↑值
```

值里可以有空格、可以有 `#`。按 `#` 或空格 split 就会在第一个带空格的字符串上错位。
→ **解析器读到 `#n(len):` 后，从冒号起精确取 `len` 个字符**，这是唯一无歧义的解析。

**④ NULL 绑定记成 `#1(0):`**

```
INSERT INTO trf_t1 VALUES (99, :bn)     binds=[ #1(0): ]     （:bn 绑的是 NULL）
```

长度 0 即 NULL。Oracle 里 `''` 本来就是 NULL，所以这里没有歧义——
但**回放时必须绑 SQL NULL 而不是空串**，绑错了在 `NOT NULL` 列上会报错、在可空列上会静默写错。

**⑤ `RETURN_CODE` 直接就是 ORA 错误号，富化免费**

实测 `942`（表不存在）、`955`（名字已被使用）、`1`（唯一约束冲突）。
→ Oracle 侧不需要 MySQL 那个 `performance_schema` 富化通道，
"源库本来就报错"这件事天然可判。

**⑥ Oracle 抹口令，与 MySQL 同款**

`CREATE USER … IDENTIFIED BY *`、`ALTER USER … IDENTIFIED BY *`
→ 带口令的 DCL **不可忠实回放**，`rd=true` 跳过并计入报告。这条与 MySQL 版逻辑完全复用。

**⑦ 可见性：无需 FLUSH，实测 50ms 内可查**

```
04:17:10.203  源会话执行 SELECT 'MARKER_LATENCY_PROBE'
04:17:10.281  UNIFIED_AUDIT_TRAIL 里已能查到（count=1）
```

`UNIFIED_AUDIT_TRAIL` 视图包含内存队列，不必先 `DBMS_AUDIT_MGMT.FLUSH_UNIFIED_AUDIT_TRAIL`。
**但这不等于可以直接按时间戳往前推游标**——见 §3.4。

**⑧ 自噪声要靠"审计范围"排除，不是靠过滤**

实测 sqlplus 自己的探测语句 `SELECT DECODE(USER,'XS$NULL',XS_SYS_CONTEXT(…),USER) …`
被完整记了下来。采集连接同理。
→ 策略用 `AUDIT POLICY p BY <业务用户>`（白名单）或 `EXCEPT <采集账号>`（黑名单），
让审计从源头就不产生我们自己的行。**这比 MySQL 的 `sql_log_off` 更干净**：
MySQL 那边还剩 3 行常数噪声要按 thread_id 滤掉，Oracle 这边一行都不会产生。

**⑨ 还原的非对称坑：`NOAUDIT` 必须镜像 `AUDIT` 的范围**

```sql
AUDIT POLICY trf_probe_pol BY app_user;    -- 启用时带了 BY
NOAUDIT POLICY trf_probe_pol;              -- 还原时没带 BY
DROP AUDIT POLICY trf_probe_pol;
SELECT count(*) FROM audit_unified_enabled_policies WHERE policy_name='TRF_PROBE_POL';
  → 1     ← 还在！策略没被停掉，DROP 也没真正生效
```

补上 `NOAUDIT POLICY trf_probe_pol BY app_user;` 之后才归零。
→ **Oracle 版"开关不还原"的具体形态就是这个**：看着执行成功了，其实审计还开着，
审计记录继续往 AUDSYS 写。还原代码必须**记录启用时用的确切范围并原样镜像**，
且还原后**必须回查 `audit_unified_enabled_policies` 确认归零**，不能只看 SQL 有没有报错。

**⑩ 会话身份是大整数**

`SESSIONID` 实测量级 `3.0299E+17`。→ 读的时候用 `BigDecimal`/`long`，
**绝不能按 `int` 读**；录制里 `s` 字段沿用 `long` 正好够（long 上限 9.2e18）。
RAC 下会话身份要带 `INSTANCE_ID` 一起做键。

**⑪ PL/SQL 块的文本形态天然可回放**

审计里 SQL 语句**不带**结尾分号（`COMMIT`、`SELECT * FROM trf_t1 WHERE id = 1`），
而 PL/SQL 块**带**（`BEGIN :b1 := 2; … END;`）。这恰好就是 Oracle JDBC 要求的形态
（普通 SQL 带分号会 ORA-00911，PL/SQL 块必须带 `END;`）→ 原样执行即可，不要自作主张去加/删分号。

**⑫ 空 `SQL_TEXT` 的行要按 `ACTION_NAME` 还原**

`LOGON`/`LOGOFF` 的 `SQL_TEXT` 为空（→ 映射成会话的建立/销毁，即 `c=C`/`c=D`）；
某些隐式 `COMMIT` 的 `SQL_TEXT` 也为空（→ 按 `ACTION_NAME` 合成 `COMMIT`）。
直接把空文本当"没有语句"丢掉，会丢掉事务边界。

### 3.4 读游标：为什么不能只按时间戳推进

审计记录是**生成后写入**的，`EVENT_TIMESTAMP` 是记录创建时刻。
一条长语句的记录在语句结束时才生成，因此**理论上存在"时间戳更早的记录、更晚可见"的窗口**。
若游标只是 `WHERE event_timestamp > :last`，这类记录会被永久跨过——
典型的"任务全绿、数据已经丢了"。

设计：

1. 只读 `event_timestamp <= systimestamp - INTERVAL 'lagSec' SECOND` 的记录（默认 `lagSec=3`），
   把可见性窗口留出来；
2. 游标持久化 `(lastTs, 该毫秒内已消费的 (SESSIONID, ENTRY_ID) 集合)`，
   下一轮用 `>= lastTs` 重叠读 + 按 `(SESSIONID, ENTRY_ID)` 去重；
3. `traffic_srclog_backlog` 的 Oracle 口径 = `count(*) WHERE event_timestamp > lastTs`。

> 这条与 `[[latency-metric-truthfulness]]`、`[[silent-loss-audit-2026-08-11]]` 是同一族教训：
> **"读到哪儿了"和"生成到哪儿了"不是一回事，中间那段就是静默丢数的地方。**

### 3.5 审计记录清理：Oracle 版的"写满磁盘"

审计行落 `AUDSYS`（默认在 SYSAUX 表空间）。捕获期间 `ACTIONS ALL` 的写入量与源库 QPS 同量级，
**不清理就是把 SYSAUX 撑爆**，后果比 MySQL 的 datadir 膨胀更严重（SYSAUX 满会影响整库）。

- 捕获进程按已消费游标定期
  `DBMS_AUDIT_MGMT.SET_LAST_ARCHIVE_TIMESTAMP` + `CLEAN_AUDIT_TRAIL(USE_LAST_ARCH_TIMESTAMP=>TRUE)`；
- **只清理我们已经读完并落盘的时间点之前的记录**，绝不清到游标之后；
- 清理失败 → `E3130`，且这是**必须打断任务**的错误（继续跑就是继续撑爆源库）；
- 用户可选"不清理"（合规场景下审计记录本身是资产），此时预检必须把增长量估算摆出来。

### 3.6 最小权限集

```sql
GRANT AUDIT_ADMIN TO trfcap;          -- 建/启用/停用/删除审计策略
GRANT AUDIT_VIEWER TO trfcap;         -- 读 UNIFIED_AUDIT_TRAIL
GRANT SELECT ON V_$DATABASE  TO trfcap;   -- 身份指纹（DBID）
GRANT SELECT ON V_$PDBS      TO trfcap;   -- 身份指纹（CON_UID）
GRANT SELECT ON V_$INSTANCE  TO trfcap;
GRANT EXECUTE ON DBMS_AUDIT_MGMT TO trfcap;  -- 清理（选择清理时才需要）
```

---

## 4. 录制文件格式 v2

### 4.1 为什么必须升版本

v1 的记录行没有放绑定参数的地方。PG 与 Oracle 都不替换参数（§2.3⑤ / §3.3③），
不升格式就只能录下 `SELECT * FROM t WHERE id = $1` —— 回放必然报错，
而且是"录得好好的、放不出来"的那种失败。

### 4.2 manifest 变化

```jsonc
{
  "format": "synctask-traffic/2",          // v1 = 只有 MySQL，无 engine 字段
  "engine": "postgresql",                  // mysql | postgresql | oracle   ← 新增
  "captureBackend": "PG_JSONLOG",          // GENERAL_LOG | PG_JSONLOG | PG_CSVLOG | ORA_UNIFIED_AUDIT
  "t0Wall": "…", "endWall": "…",
  "source": {                              // 按引擎分族，键集合不同
    // mysql:      serverUuid / version / serverId / sqlMode / timeZone / charset / collation / lowerCaseTableNames
    // postgresql: systemIdentifier / version / dbName / serverEncoding / lcCollate /
    //             searchPath / dateStyle / intervalStyle / timeZone / standardConformingStrings
    // oracle:     dbid / conUid / conName / version / nlsDateFormat / nlsTimestampFormat /
    //             nlsNumericCharacters / nlsSort / nlsComp / dbTimeZone / characterSet
  },
  "checkpoint": {"file":"log/postgresql-….json","offset":183422},  // 仅 PG：可续位点
  "segments": [ … ], "gaps": [ … ], "stats": { … },
  "sealed": true, "sha256": "…"
}
```

**v1 兼容**：没有 `format` 或 `format="synctask-traffic/1"` 的录制，一律按 `engine="mysql"` 读。
老录制必须继续能回放——这是既有用户的资产。

### 4.3 记录行新增字段

```jsonc
{"n":1024, "t":15000123, "s":2691, "c":"E", "k":"SELECT",
 "db":"order_db",                  // MySQL=默认库；PG=dbname；Oracle=CURRENT_SCHEMA
 "u":"app@10.0.0.5",
 "q":"SELECT * FROM t1 WHERE v = $1",
 "b":[{"v":"中文🚀emoji"},{"v":null}],   // ← 新增：绑定参数，按序号排列；null 即 SQL NULL
 "sn":"public,\"$user\"",                // ← 新增：PG 的 search_path / Oracle 的 CURRENT_SCHEMA 快照
 "rd":false,
 "e":{"errno":0,"state":"00000","rows":1,"aff":0,"us":831}}   // ← state 新增：PG SQLSTATE
```

- `b` 缺省即"没有参数"（MySQL 永远缺省，因为参数已经在 `q` 里了）；
- `b[i].v = null` 表示 SQL NULL，与 `""`（空串）**必须严格区分**——
  PG 的 `$1 = NULL` vs `$1 = ''`、Oracle 的 `#1(0):`；
- `sn` 只在变化时写（省字节），回放侧按会话保持最后一次的值。

### 4.4 跨引擎回放：硬拦

`manifest.engine != 目标库引擎` → 预检 `error`（`E3131`），启动被拒。
SQL 方言不可能自动翻译，"让它跑跑看"只会在目标库上制造一堆半成功的破坏。

---

## 5. 回放引擎改造

### 5.1 抽出 `TargetDialect`

现在 `SessionRunner` 里写死了 MySQL 的四件事：`USE \`db\``、
`SET SESSION sql_mode/time_zone` + `SET NAMES`、`server_uuid` 互锁、MySQL 的危险语句黑名单。
抽成接口：

```java
public interface TargetDialect {
    String  jdbcUrl(Properties props, String db);          // 连接串（PG 要带 stringtype=unspecified）
    void    applySessionEnvironment(Connection c, SourceFingerprint fp);
    void    switchSchema(Connection c, String db, String searchPath);  // USE / 不支持 / ALTER SESSION
    String  identity(Connection c);                        // server_uuid / system_identifier / DBID+CON_UID
    boolean isTransactionStart(String sql);                // 事务边界识别（Oracle 没有 BEGIN）
    String  blockReason(String sql);                       // 危险语句黑名单
    void    bind(PreparedStatement ps, List<Bind> binds);  // 绑定语义
}
```

三个实现：`MySqlDialect`（把现有代码原样搬过去，零行为变化）、`PostgresDialect`、`OracleDialect`。
`ReplayScheduler` / `ReplayReporter` / `RecordingReader` / 会话 1:1:1 模型**完全不动**。

### 5.2 PG 回放要点

- **连接终生绑定一个库**：PG 没有 `USE`。录制里同一源会话不可能跨库（物理上不可能），
  所以 `switchSchema` 只做 `SET search_path`，库名在建连时就定死。
- **`stringtype=unspecified` 必须开**：PG 日志里的参数值全是文本、**没有类型信息**。
  按 `setString` 绑到 `WHERE id = $1`（id 是 int）会得到
  `operator does not exist: integer = character varying`。
  连接参数 `stringtype=unspecified` 让服务端自己按上下文推断类型。
- **有参数就走 `PreparedStatement`，没参数走 `Statement`**：
  simple query 里可能是**多条语句串在一起**（实测 `SET …; SELECT …` 被记成一行），
  `Statement.execute` 能原样送出去，`PreparedStatement` 不行。
- 会话环境对齐：`search_path`、`TimeZone`、`DateStyle`、`IntervalStyle`、
  `client_encoding`、`standard_conforming_strings`。
- 事务边界：`BEGIN`/`START TRANSACTION` … `COMMIT`/`ROLLBACK`，与 MySQL 同形。

### 5.3 Oracle 回放要点

- **schema 切换**：`ALTER SESSION SET CURRENT_SCHEMA = <schema>`。
  默认值是登录用户名——回放用的是一个统一配置的目标账号，
  所以**每条会话建立后必须显式设成录制里的 schema**，否则所有非限定表名都会解析到回放账号自己的 schema
  （而且大概率"表不存在"，报一堆假错误）。
- **绑定按序号**：`:b1 :b2` 或 `:1 :2`，`b[i]` 顺序即绑定顺序。
  v1 一律按 `setString` 绑（Oracle 的隐式转换覆盖 NUMBER/VARCHAR2），
  **`b[i].v == null` 时必须 `setNull`**（§3.3④）。
  DATE/TIMESTAMP/RAW 类型的忠实度依赖 NLS 对齐，见 §11 待验证项。
- **会话环境对齐**：`NLS_DATE_FORMAT`、`NLS_TIMESTAMP_FORMAT`、`NLS_TIMESTAMP_TZ_FORMAT`、
  `NLS_NUMERIC_CHARACTERS`、`NLS_SORT`、`NLS_COMP`、`TIME_ZONE`。
  不对齐的后果是实打实的：`TO_DATE('01-02-26')` 在两套 `NLS_DATE_FORMAT` 下是两个不同的日期，
  **不报错、值不一样**。
- **事务边界**：Oracle 没有 `BEGIN`——事务在第一条 DML 时隐式开始，DDL 隐式提交。
  `isTransactionStart` 对 Oracle 返回"首条 DML 即入事务"，`COMMIT`/`ROLLBACK` 出事务。
  会话淘汰（LRU）时"不淘汰事务中会话"的规则据此判定。
- **`autocommit` 必须关**：JDBC 默认 autocommit=true 会把录制里的
  `INSERT…INSERT…ROLLBACK` 变成"两条已提交 + 一条空回滚"，事务语义整个消失。

### 5.4 危险语句黑名单（各自一份）

| MySQL（已有） | PostgreSQL | Oracle |
|---|---|---|
| `DROP DATABASE` | `DROP DATABASE` / `DROP SCHEMA … CASCADE` | `DROP USER … CASCADE` / `DROP TABLESPACE` |
| `DROP USER` / `RENAME USER` | `DROP ROLE` / `ALTER ROLE … SUPERUSER` | `DROP PROFILE` / `ALTER USER … IDENTIFIED BY` |
| `SET GLOBAL` / `SET PERSIST` | `ALTER SYSTEM` | `ALTER SYSTEM` / `ALTER DATABASE` |
| `SHUTDOWN` / `RESET MASTER` | `pg_terminate_backend` / `pg_promote` / `SELECT pg_drop_replication_slot` | `SHUTDOWN` / `ALTER DATABASE … OPEN` / `MOUNT` |
| `FLUSH PRIVILEGES` | `CREATE EXTENSION` / `COPY … FROM PROGRAM`（能在库机执行命令） | `CREATE DIRECTORY` / `DBMS_SCHEDULER` 提交作业 |
| `GRANT ALL ON *.*` | `GRANT ALL ON DATABASE … TO` | `GRANT DBA TO` / `GRANT SYSDBA` |

> PG 的 `COPY … FROM PROGRAM` 值得单拎出来：它在**数据库服务器上执行 shell 命令**。
> 一条录制回放到别人的库上，等于在那台机器上执行任意命令。默认必拦。

### 5.5 身份互锁（回放到源库自己）

| 引擎 | 判据 | 取法 |
|---|---|---|
| MySQL | `server_uuid` | 已有 |
| PostgreSQL | `system_identifier` | `SELECT system_identifier FROM pg_control_system()` —— **实测普通用户即可读**（PG 16.13） |
| Oracle | `DBID` + `CON_UID` | `V$DATABASE.DBID`（实测 `1506701341`）+ `V$PDBS.CON_UID`（实测 `303866416`） |

Oracle 必须**同时**比 DBID 与 CON_UID：同一个 CDB 下的两个 PDB 是两个独立的目标库，
只比 DBID 会把合法的 PDB→PDB 回放误拦；只比 CON_UID 则跨 CDB 时可能撞号。

---

## 6. 源端状态还原：一套骨架，三种形态

`TrafficSourceState`（在 `migration-common`，因为 agent 要用而 agent 不依赖引擎模块）
升级为多引擎：新增 `engine` 字段，原来的 `original.general_log` / `original.log_output`
两个固定键换成一张 **`restore.*` 键值表**，由各引擎自己填。

| 引擎 | 记什么 | 怎么还原 | 怎么验证还原成功 |
|---|---|---|---|
| MySQL | `general_log`、`log_output` 原值 | `SET GLOBAL` 回去 | 回读两个变量 |
| PostgreSQL | `log_statement`、`log_destination`、`log_min_duration_statement`、`log_duration` 原值与来源 | `ALTER SYSTEM SET/RESET` + `pg_reload_conf()` | **回读 `pg_settings.setting`**（§2.3③：下发成功 ≠ 生效） |
| Oracle | 策略名 + **启用时用的确切范围**（`BY`/`EXCEPT` 的实体列表） | `NOAUDIT POLICY p BY <同样的实体>` → `DROP AUDIT POLICY p` | **回查 `audit_unified_enabled_policies` 归零**（§3.3⑨） |

四道保险（子进程 hook / agent 收尾 / 看门狗 / 重启扫尾）与 MySQL 版一字不改，
`--mode restore` 也照旧——它读的是录制目录里的 `source_state.properties`，
只是分派到不同引擎的还原实现。

---

## 7. 代码落点

### 7.1 `migration-traffic`

```
capture/
  TrafficSource.java                  ← 接口不动；RawStatement 增 binds/schema/sqlState 字段
  GeneralLogTrafficSource.java        ← 不动
  PgLogTrafficSource.java             ← 新增：ALTER SYSTEM 下发+回读校验、pg_read_binary_file 增量读、
                                        位点(文件,偏移)、jsonlog/csvlog 双解析、口令脱敏
  PgLogLineParser.java                ← 新增：statement:/execute:/duration:/parse:/bind: 形态识别 + Parameters 解析
  OracleAuditTrafficSource.java       ← 新增：审计策略生命周期、滞后窗口游标+去重、CLOB 流式读、审计清理
  OracleBindParser.java               ← 新增：#n(len): 按长度切片
  StatementClassifier.java            ← 拆出 MySql/Pg/Oracle 三套关键字表（骨架复用）
  SessionSchemaTracker.java           ← 仅 MySQL 用；PG/Oracle 走各自的 schema 字段
replay/
  dialect/TargetDialect.java + MySqlDialect / PostgresDialect / OracleDialect   ← 新增
  SessionRunner.java                  ← 改：方言化 + 绑定参数执行路径
  DangerousStatementFilter.java       ← 改：按引擎选黑名单
  TrafficReplayRunner.java            ← 改：jdbcUrl 与身份互锁走方言
model/
  TrafficRecord.java                  ← 增 b / sn / sqlState
  RecordingManifest.java              ← 增 engine / captureBackend / checkpoint；v1 兼容读
  SourceFingerprint.java              ← 改成按引擎分族（保留 MySQL 字段，新增 PG/Oracle 字段）
```

### 7.2 `migration-common`

`traffic/TrafficSourceState.java`：`engine` + `restore.*` 键值表 + 三套还原实现。

### 7.3 agent

- `TrafficCaptureTask` / `TrafficReplayTask`：按 `source.db.type` 给子进程传 `traffic.engine`；
  `stopExtraProcesses` 已覆盖，不动。
- `TrafficSourceGuardService`：扫尾时按 `source_state.properties` 里的 `engine` 分派。
- ⚠️ **真正的任务分派在 `AgentMain`**（`TaskMessageHandler` 是死代码）——
  这次不新增任务类型，所以不用改；但改 `ConfigService` 时别又只改一处。

### 7.4 后端

- `V23__traffic_multi_engine.sql`：`traffic_task_config` 增 `engine`、`capture_backend` 扩枚举、
  `src_state_before TEXT`（JSON，取代 MySQL 专用的两列，老列保留只读）；
  `traffic_recordings` 增 `engine`。
- `TrafficPrecheckService`：现在第 84 行那句 `!"mysql".equalsIgnoreCase(engine) → FAIL` 拆成三分支（§8）。
- `TrafficTaskService`：录制列表按 engine 过滤，回放向导只列出与目标库同引擎的录制。

### 7.5 前端

- `dashboard-traffic.js` 现在写死了 `sourceType:'mysql'` / `targetType:'mysql'` /
  `mysql://…` 连接串（4 处）→ 改成向导第一步选引擎，连接串按引擎拼。
- 复制向导的告警文案按引擎切换：MySQL 说 `general_log`，PG 说"会改 `log_statement` 并要求
  `logging_collector` 已开"，Oracle 说"会创建审计策略并按需清理审计记录"。
- 录制文件列表加"引擎"列；回放选择录制时**过滤掉异引擎的录制**（配合 §4.4 的硬拦）。

---

## 8. 预检与错误码

### 8.1 新增预检项

**PG 捕获**

| 检查 | 级别 |
|---|---|
| `logging_collector = on` | **error**（关着必须重启实例，附确切补救 SQL） |
| 下发后 `pg_settings.setting/source` 回读一致 | **error**（§2.3③） |
| `pg_current_logfile()` 非空 | **error**（§2.3④） |
| PG ≥ 15（jsonlog 可用） | info；13/14 降级 csvlog 并提示列位表按版本 |
| 权限七项（§2.4）逐项试探 | error，缺哪项报哪项 |
| 日志盘剩余空间 vs 预估写入量 | warning |
| 源库当前 QPS（两次 `pg_stat_database.xact_commit+xact_rollback` 采样） | warning + 需显式确认 |

**Oracle 捕获**

| 检查 | 级别 |
|---|---|
| `V$OPTION` 里 `Unified Auditing = TRUE` | **error** |
| 账号具备 `AUDIT_ADMIN` / `AUDIT_VIEWER` | error |
| 同名策略是否已存在（上一次没还原干净） | **error**，指向 `--mode restore` |
| SYSAUX 剩余空间 vs 预估审计量 | error（不够直接拦） |
| 选择"不清理审计记录"时的增长量估算 | warning + 需显式确认 |
| 12c 以上 | error |

**回放（两种引擎共用）**

| 检查 | 级别 |
|---|---|
| `manifest.engine` == 目标库引擎 | **error（E3131）** |
| 身份互锁（§5.5） | **error（E3124）** |
| PG：目标库 `server_encoding` 与录制不一致 | warning |
| Oracle：目标库字符集与录制不一致 | warning（`AL32UTF8` ↔ 非 UTF 会丢字符） |
| Oracle：`NLS_*` 六项不一致 | warning（回放时按录制值覆盖会话） |
| 录制峰值并发会话数 vs 目标库连接上限 | error（PG `max_connections`、Oracle `sessions`） |

> 最后一条踩过坑：MySQL 版 B6 曾拿"配置的会话上限 200"去比 `max_connections=151`，
> 于是**默认配置永远预检失败**。要比的是"这份录制实际要用多少条连接"。别再犯。

### 8.2 错误码（三份目录必须同步：引擎字面量 / `SyncErrorCode` / `ERROR_CODE_MAP`）

沿用：`E3120`~`E3126`（措辞泛化成"源库语句日志/审计"）。新增：

| 码 | 名称 | 含义 |
|---|---|---|
| `E3127` | 源端语句流通道不可用 | PG `logging_collector=off`，或日志目录不可读 |
| `E3128` | 语句日志开关未生效 | 已下发但回读不一致（被命令行/包含文件压过），或 `pg_current_logfile()` 为空 |
| `E3129` | 审计策略创建或启用失败 | Oracle 缺 `AUDIT_ADMIN`，或同名策略残留 |
| `E3130` | 审计记录清理失败 | `DBMS_AUDIT_MGMT` 报错；再不干预 SYSAUX 会被撑爆 |
| `E3131` | 录制引擎与回放目标不一致 | 跨引擎回放，硬拦 |
| `E3132` | 绑定参数解析失败 | PG `Parameters:` 或 Oracle `SQL_BINDS` 形态不认识；**必须 fail-stop，不能当没参数放过去** |

> `E3132` 为什么是 fail-stop：解析不出参数就把语句当"无参数"录下去，
> 回放时 `$1`/`:b1` 会被目标库当成未绑定占位符——PG 直接报错（还算好），
> Oracle 在某些形态下会用上一次的绑定值，**静默执行一条参数错误的 DML**。

---

## 9. 实施批次

| 批次 | 内容 | 可验收的产出 |
|---|---|---|
| **B1 抽象与格式** | 录制格式 v2（`engine`/`b`/`sn`/`sqlState`）+ v1 兼容读 + `TargetDialect` 抽出 + `MySqlDialect` 平移 | **MySQL 既有 49 项判据全绿、老录制照常回放**（纯重构批次，零行为变化） |
| **B2 PG 捕获** | `PgLogTrafficSource` + `PgLogLineParser` + 口令脱敏 + 位点续读 + 源端还原（含回读校验） | 对一个 PG 产出 `.trf.gz`+manifest；三种终止方式后 `log_statement` 均已还原 |
| **B3 PG 回放** | `PostgresDialect` + 绑定执行路径 + `stringtype=unspecified` | 用户样例（5s DDL / 10s DML / 15s SELECT）在 PG 上跑通，偏差 P99 < 200ms |
| **B4 Oracle 捕获** | `OracleAuditTrafficSource` + `OracleBindParser` + 滞后窗口游标 + 审计清理 + 策略还原（含回查） | 同 B2；策略在 `audit_unified_enabled_policies` 里归零 |
| **B5 Oracle 回放** | `OracleDialect` + `CURRENT_SCHEMA` + NLS 对齐 + autocommit 关闭 | 同 B3，且 `BEGIN…ROLLBACK` 的中间 DML 不落目标库 |
| **B6 平台接入** | Flyway V23 + 预检三分支 + 6 个错误码三份目录 + agent/后端/前端引擎选择 | 页面上能建 PG/Oracle 的复制与回放任务，全流程可操作 |
| **B7 判据套件** | `test_scripts/traffic/traffic_pg_e2e.py`、`traffic_oracle_e2e.py` + `autotest/suites/traffic.py` 扩三条用例 | §10 |

**B1 必须单独成批且先做**：它是纯重构，唯一的验收标准是"MySQL 的一切照旧"。
把它和新引擎混在一起做，出问题时分不清是重构破坏了 MySQL 还是新引擎没写对。

---

## 10. 判据

引擎级（直驱子进程 + 真实库，对齐 `test_scripts/traffic/traffic_e2e.py` 的形态）：

| # | 用例 | PG | Oracle | 判据 |
|---|---|---|---|---|
| 1 | 用户样例：5s DDL / 10s DML / 15s SELECT | ✓ | ✓ | 目标库实测间隔与 5/10/15s 之差 < 200ms |
| 2 | 语句类别全覆盖 | ✓ | ✓ | SELECT/INSERT/UPDATE/DELETE/DDL/DCL 各至少 1 条 |
| 3 | **绑定参数往返** | ✓ | ✓ | 扩展协议/绑定变量语句回放后目标表内容与源表一致 |
| 4 | **NULL vs 空串** | ✓ | ✓ | 绑 NULL 的列在目标库是 NULL，不是 `''`/`'NULL'` |
| 5 | 4 字节 UTF-8 | ✓ | ✓ | `中文🚀emoji` 原样往返 |
| 6 | 自噪声 | ✓ | ✓ | 录制里不含捕获自身的轮询 SQL |
| 7 | 顺序保真 | ✓ | ✓ | 单会话 1000 条 `n=n+1` → 目标值正好 1000 |
| 8 | 事务边界 | ✓ | ✓ | `BEGIN…ROLLBACK` 中间 DML 不落目标库（Oracle 用隐式事务） |
| 9 | 多行语句 | ✓ | — | csvlog 档位下多行 SQL 不被劈成两条 |
| 10 | 失败语句不重放 | ✓ | — | PG 的 `statement:`+`ERROR` 两行只产生 1 条记录（§2.3⑨） |
| 11 | 源端报错语句 | ✓ | ✓ | 源端 SQLSTATE/ORA 号入录制，回放报错不计 `REPLAY_ERROR` |
| 12 | **口令处理** | ✓ | ✓ | PG：录制里**没有明文口令**；Oracle：`rd=true` 且回放跳过 |
| 13 | **开关未生效被抓住** | ✓ | — | 命令行压过 `ALTER SYSTEM` 时任务以 `E3128` 失败，而不是录出空文件 |
| 14 | **`logging_collector=off`** | ✓ | — | 预检 error，任务不启动（不是启动后录 0 条） |
| 15 | **开关/策略还原（三种终止方式）** | ✓ | ✓ | 正常停止 / `kill -9` 子进程 / `kill -9` agent 后重启，均已还原并**回读验证** |
| 16 | 位点续读不产生空洞 | ✓ | — | 捕获暂停 30s 再恢复，期间语句**照样录到**（日志文件仍在），manifest 无 gap |
| 17 | 审计清理 | — | ✓ | 灌 10 万条，AUDSYS 行数不单调增长；清理点永不越过读游标 |
| 18 | 危险语句拦截 | ✓ | ✓ | PG `COPY … FROM PROGRAM`、Oracle `DROP USER … CASCADE` 被 `BLOCKED` |
| 19 | 回放到源库自己 | ✓ | ✓ | 预检 error 且 `launch` 被拒（E3124） |
| 20 | 跨引擎回放 | ✓ | ✓ | PG 录制选 MySQL 目标 → E3131 |
| 21 | 倍速 | ✓ | ✓ | `speed=2.0` 总耗时 ≈ 录制时长一半，会话内顺序不变 |
| 22 | 文件往返 | ✓ | ✓ | 下载 `.trfz` → 删原文件 → 上传 → 回放成功，SHA-256 一致 |

平台级 `autotest/suites/traffic.py`：现有 3 条 MySQL 用例基础上加 PG、Oracle 各一条全链路。

**本机端点**（回放目标必须与录制源是不同实例，产品硬拦 E3124）：
PG 用 `dr-pg-a`(55432) → `dr-pg-b`(55433)；Oracle Free 本机只有一台 `oracle_db`(1521)，
**需要再起一台**（或用同 CDB 下的另一个 PDB —— 那是合法的不同目标，见 §5.5）。

> 判据脚本的既有两个坑照旧：**子进程 stdout 必须持续排空**，**跑 fat jar 而不是 `mvn compile`**。

---

## 11. 本设计尚未实测、实现前必须先补测的点

这些是本文里**唯一没有本机证据**的部分，不要当结论用：

1. **PG 真 NULL 绑定的渲染形态**。实测拿到的是 `$1 = ''`（空串）与 `$1 = 'NULL'`（字符串），
   psql 的 `\bind` 送不出真 NULL。设计按 "裸 `NULL` = SQL NULL" 处理，**必须用 JDBC/psycopg 补一次实测**。
2. **PG 日志轮转跨文件时的位点接续**。`log_rotation_size`/`age` 触发时新文件的命名与
   `pg_current_logfile()` 的切换时机没实测，需验证"旧文件尾巴 + 新文件开头"不丢不重。
3. **PG csvlog 各版本的列位表**（PG 13/14 降级档）。本机只有 16 与 18，都走 jsonlog。
4. **Oracle 高负载下的审计可见性延迟**。实测的 ~50ms 是空闲库上的数字，
   §3.4 的 `lagSec=3` 是保守取值，要在压测下标定。
5. **Oracle DATE / TIMESTAMP / RAW 绑定值在 `SQL_BINDS` 里的渲染格式**，
   以及按 `setString` 绑回去是否还原得了。这决定 v1 要不要限制"只支持字符/数值绑定"。
6. **Oracle RAC**：`UNIFIED_AUDIT_TRAIL` 是本实例视图，多实例要用 `GV$`；v1 暂定单实例。
7. **PG 上 `pgaudit` 已启用的环境**：会额外产生 `AUDIT:` 行，解析器要能识别并忽略（或利用）。

---

## 12. 风险与取舍

| 风险 | 处置 |
|---|---|
| PG `logging_collector` 关着 → 功能不可用 | 预检 error + 明确文案；**不替用户重启数据库** |
| PG 开关下发了没生效 → 静默录空 | 回读校验 + `pg_current_logfile()` 非空 + `E3128`（判据 13/14） |
| PG 录制里含明文口令 | 落盘前正则脱敏（§2.3⑥）+ 属主校验 + 审计 + 可选落盘加密 |
| Oracle 审计撑爆 SYSAUX | 按游标清理 + 预检估算 + `E3130` fail-stop（判据 17） |
| Oracle 策略没还原干净 | `NOAUDIT` 镜像范围 + **回查归零**（§3.3⑨）+ 四道保险（判据 15） |
| 绑定参数解析错 → 静默写错数据 | 按声明长度切片（Oracle）/ 引号状态机（PG）+ 解析失败即 `E3132` fail-stop |
| 跨引擎回放制造破坏 | manifest `engine` 硬拦（E3131）+ 前端过滤 |
| B1 重构打坏已交付的 MySQL 功能 | B1 单独成批，验收标准就是"MySQL 49 项判据全绿 + 老录制照常回放" |

**明确接受的不完美：**

1. **PG 的时间轴精度只有毫秒**（日志时间戳如此）。回放调度仍用微秒坐标，
   但源端偏移的分辨率就是 1ms，报告里如实标注。
2. **Oracle 拿不到源端耗时**（审计不记）。回放报告里 Oracle 的"源端耗时"列为空，
   不去用别的东西凑一个看起来像的数。
3. **PG/Oracle 的绑定值都是文本渲染**，类型信息在源端就丢了。
   v1 靠 `stringtype=unspecified`（PG）与隐式转换 + NLS 对齐（Oracle）逼近，
   不宣称"类型级忠实"。
4. **仍然只支持同引擎、同名 schema 回放**。库名/表名改写与跨引擎翻译都是独立一期。

---

## 13. 交付记录（2026-08-21，B1~B7 全部完成）

七个批次按序实施，每批实现完即在真实库上验证。**下面每一条"实测发现"都是验证跑出来的，不是设想。**
其中五条直接推翻了本文 §2/§3 里基于文档的判断——原文已在对应小节标出更正。

### B1 抽象与格式（纯重构批次）

录制格式升 v2（`engine` / `captureBackend` / `b` 绑定参数 / `sn` schema / `checkpoint`），
抽出 `TargetDialect` 并把 MySQL 逻辑逐行平移过去，`TrafficSourceState` 泛化成三引擎。

验收标准就是"MySQL 的一切照旧"：既有 49 项判据全绿；
另造了一份 **v1 格式的录制**（把 manifest 退回 `synctask-traffic/1`、去掉 engine 字段）
实测能照常回放，32 条记录零错误、4 字节 UTF-8 原样往返。

### B2 PG 捕获

`PgLogTrafficSource` + `PgLogLineParser` + `PgBindParser` + `PgSecretRedactor` + `PgStatementAssembler`。

验证跑出 **3 个缺陷**：

1. **控制面读到的是自己刚设的值。** 采集连接为了消噪自己 `SET log_statement='none'`，
   而 `pg_settings.setting` 返回的是<b>当前会话的有效值</b>——于是"原值"被记成 none、
   "生效校验"永远失败，任务起不来。控制面的读写必须走**独立连接**。
2. **`log_min_duration_statement=0` 才是 parse/bind/execute 三连的来源**，不是本文原先写的
   `log_duration=on`。实测一次扩展协议执行记三行：
   `duration: … parse <unnamed>: …` / `duration: … bind <unnamed>: …`（detail 里带参数）/
   `execute <unnamed>: …`。前两行照单全收就是<b>同一条语句回放三遍</b>。
3. **`parameters:` 前缀大小写不固定。** PG 18 探针给的是 `Parameters:`（大写 P），
   PG 16 在 `log_min_duration_statement=0` 下给的是 `parameters:`（小写）。
   大小写敏感地匹配，一整类语句的绑定参数会**静默变成"没有参数"**。

同时把 §11 待验证项 ①（PG 真 NULL 绑定的渲染）测掉了：JDBC `setNull` 送出的参数
在日志里是**裸 `NULL`**（`$2 = NULL`），与带引号的 `'NULL'` 形态不同，解析规则成立。

### B3 PG 回放

`PostgresDialect` + `PgPlaceholderRewriter`。验证跑出 **1 个缺陷**，而且是致命的：

* **pgjdbc 不认 `$n` 占位符。** 录制里的 `INSERT INTO b3 VALUES ($1,$2,$3)` 直接交给
  `prepareStatement`，报的是 <b>"栏位索引超过许可范围：1，栏位数：0"</b>——
  驱动在 SQL 里一个参数标记都没找到。这句话跟"占位符语法不对"毫无关联，纯靠猜。
  必须改写成 `?` 并按出现顺序展开绑定值（重复引用 `$1` 要绑两次），
  且要跳过字符串字面量、美元引用块与注释里的 `$n`。

修好后实测：6 条记录全成功、零错误，目标库与源库**逐值一致**——
`中文🚀emoji` 原样、NULL 仍是 NULL、空串仍是空串（三者各归各位），
整数列上绑文本靠 `stringtype=unspecified` 通过。

### B4 Oracle 捕获

`OracleAuditTrafficSource` + `OracleBindParser` + `OraclePlaceholders`。
验证跑出 **5 个缺陷**，其中两条推翻了原设计：

1. **`ACTIONS ALL` 抓不到多表 SELECT。** 实测同一批语句里 `SELECT * FROM b4a`、
   `SELECT * FROM app_user.b4a`、`SELECT 1 FROM dual` 都被审计了，唯独
   `SELECT a.v FROM b4a a JOIN b4b b ON …` <b>一行都没有</b>——不报错、不告警，就是没有。
   真实业务负载里 join 遍地都是。补救办法是加对象级 `ACTIONS SELECT ON <schema>.<table>`。
2. **对象级动作不能和 `ACTIONS ALL` 放在同一条策略里。** `ACTIONS ALL, SELECT ON t1, SELECT ON t2`
   与 `ALTER AUDIT POLICY … ADD ACTIONS SELECT ON …` 两种写法都能建成功、都不报错，
   而 join 照样一行不出。**必须拆成两条策略同时启用**。
   代价是一条多表语句按对象数出多行（`ENTRY_ID` 不同、**`STATEMENT_ID` 相同**），靠后者去重。
3. **`SQL_TEXT` 结尾带一个 NUL。** 原样送给 Oracle JDBC 报 ORA-00911 invalid character——
   一条本来完全正常的 INSERT 在回放时莫名其妙失败，而录制文件里看不出来（NUL 在终端上是隐形的）。
4. **`NOAUDIT` 与 `AUDIT` 不对称，而且是两条相反的规则**：
   `BY <user>` 必须原样带上（不带则策略根本停不掉），
   而 `EXCEPT <user>` **不能带**——带了直接 `ORA-46352: NOAUDIT statement with the EXCEPT clause
   is not allowed`，随后的 DROP 也跟着失败，审计就一直开着。
5. **我们自己的审计策略 DDL 会被录进去。** `CREATE/DROP AUDIT POLICY` 与 `AUDIT/NOAUDIT`
   是 Oracle 的**强制审计**动作，`EXCEPT <采集账号>` 挡不住，必须按 `DBUSERNAME` 过滤。
   顺带把 schema 上下文从 `OBJECT_SCHEMA` 改成 `DBUSERNAME`——前者对匿名 PL/SQL 块是 `SYS`，
   照它去 `ALTER SESSION SET CURRENT_SCHEMA=SYS`，之后整条会话的非限定表名全解析错。

另外把 §11 待验证项 ⑤ 的一半测掉了：`SELECT_CATALOG_ROLE` 是必需的——
`ALL_TABLES` 只列出采集账号<b>自己有权访问的表</b>，而采集账号（只有审计权限）
对业务 schema 通常一张表都看不到，结果是"补了个寂寞"，join 照样抓不到且没有任何报错。

### B5 Oracle 回放

`OracleDialect`（`ALTER SESSION SET CURRENT_SCHEMA` + NLS 对齐 + autocommit 关闭 + `:name → ?`）。
验证跑出 **1 个缺陷 + 1 个语义补充**：

* `EXEC :v := …`（审计里是 `BEGIN :v := '…'; END;`）的占位符是**赋值目标**，
  审计的 SQL_BINDS 里当然没有它的值，原样执行必然 ORA-01008。
  这类语句对回放本来也没有意义（真正带值的是紧随其后的 DML），
  新增 `UNREPLAYABLE_NO_BINDS` 单独计一类，免得它把回放错误率顶穿。
* Oracle 的录制原点要用微秒精度：`CAST(… AS DATE)` 会把小数秒截掉，
  秒级原点与微秒级 `EVENT_TIMESTAMP` 混用会给所有偏移带上最多 1 秒的固定误差。

修好后实测 FREEPDB1 → MYAPP_DB：20 条记录、**零回放错误**，
目标库与源库逐值一致（含 NULL 与多字节），`ROLLBACK` 的那条没落地，DDL 也生效了。

### B6 平台接入

Flyway `V23`（两张表加 `engine`、`traffic_task_config` 加 `src_state_before` JSON、
`traffic_recordings` 加 `capture_backend`）、两个 entity、`TrafficTaskService`（引擎跟随任务、
跨引擎选录制直接拒绝）、`TrafficPrecheckService` 三分支、agent 的 manifest 摘要带回引擎、
6 个错误码（E3127~E3132）落三份目录、前端引擎选择与按录制切换目标库表单。

### B7 判据套件

| 套件 | 规模 | 结果 |
|---|---|---|
| `test_scripts/traffic/traffic_e2e.py`（MySQL，既有） | 49 项 | 全通过（B1 重构后无回归） |
| `test_scripts/traffic/traffic_pg_e2e.py`（新） | **57 项** | 全通过 |
| `test_scripts/traffic/traffic_oracle_e2e.py`（新） | **32 项** | 全通过 |
| 引擎单测 | 119 个（新增 57） | 全绿 |
| 后端单测 | 160 个 | 全绿（含错误码三份目录门禁） |

判据套件本身也跑出了一个**产品缺陷**：

* **续录时把自己留下的痕迹当成了"原值"。** 上一轮若被 `kill -9`，源端开关还是我们改过的样子
  （`general_log=ON` / `log_statement=all` / 审计策略还开着）。这一轮再去读"原值"，
  读到的就是自己留下的痕迹，于是收尾时忠实地把它"还原"成开着——**源端从此再也回不去了，
  而且全程零报错**。修法是续录时沿用上一轮落盘的状态文件（`overrideRestoreState`），
  三种引擎都补上了。

### 环境记录：两个与产品无关的本机故障

* **host → `oracle_db` 的 1521 数据面是坏的**：TCP 连得上，字节到不了监听器
  （监听器日志里连一条尝试都没有），而同网段的容器客户端一切正常。
  这是本机 Docker 端口转发的故障，不是产品问题。Oracle 判据因此经
  `test_scripts/traffic/orajava.sh` 在 `oracle_db_network` 里起容器跑引擎。
* PG 判据需要 `docker-compose-synctask-traffic.yml` 起的 `trf-pg-src`(55480) / `trf-pg-tgt`(55481)：
  源库必须 `logging_collector=on`（postmaster 参数，只能启动时给），
  且两端必须是不同实例（产品硬拦同实例回放）。

### 尚未做的一件事

`autotest/suites/traffic.py`（平台级，走 REST API）**没有扩到 PG/Oracle**：
它要求后端与 agent 都加载新代码，而这需要重启本机正在跑的那两个进程。
引擎级判据已覆盖全部新增功能；平台级的接线（引擎落库、预检分支、前端联动）
目前只有单测与页面渲染验证。
