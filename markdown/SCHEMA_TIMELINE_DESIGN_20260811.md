# binlog 表结构时序库：用"事件当时"的结构解析事件

2026-08-11

承接 [binlog 边界形态七项静默故障](BINLOG_EDGE_CASES_20260811.md)。那一轮把
`binlog_row_metadata=FULL` 的列名用了起来，堵住了 schema 漂移里最常见的一种。这一轮做的是
根治：**给链路一份按 binlog 位点索引的表结构时序库**，让解析用的是"这条事件发生当时"的表结构，
而不是"现在"的表结构。

## 病根

MySQL ROW 格式的 binlog 只带列类型码和长度，不带列名、不带 `unsigned`、不带 enum 取值表。
这些语义信息今天全部来自**源库 `information_schema` 的当前定义**
（`MySQLBinlogExtractor.getTableColumns/getTableColumnTypes/getTablePrimaryKeys`，走
`sourceConnection`）。

也就是说：**用"现在的结构"去解释"过去的事件"。**

这个错位在一个地方被放大到最大：`FullMigrationTask` 的执行顺序是
capture 先起 → 跑全量 → **全量完成后才起 extract**。全量跑几个小时，extract 就要拿着
几个小时后的表定义去解析几个小时前的事件。漂移窗口 = 全量耗时。

现有的两层防护各有边界：

| 防护 | 挡得住 | 挡不住 |
|---|---|---|
| `binlog_row_metadata=FULL` 的事件列名 | 加列 / 删列 / 改列名（列名随事件下发，可自愈） | enum/set 取值表、`unsigned`、精度——这些只能从 `information_schema` 拿，拿到的永远是最新版 |
| MINIMAL 下的列数校验（E3021） | 列数变了的结构变更（fail-stop，不静默） | 列数不变的变更：`RENAME COLUMN`、`DROP a`+`ADD b`、`MODIFY` 改类型 |

剩下的那块就是静默数据损坏——写进目标库的是合法值，看不出任何异常。

## 形状

```
capture   启动时发一条 SCHEMA_BASELINE（SHOW CREATE TABLE 原文）→ 之后照旧只写 binlog 事件
extract   CREATE/ALTER 解析 → 施加到结构模型 → 按位点归档版本 → 事件按自己的位点取版本
持久化    files/<taskId>/schema_history.jsonl + 随中心化 checkpoint hydrate
```

parser 和结构模型**全部放在 migration-extract**，capture 只发 SQL 原文。两个理由：capture 是
RPO 的关键路径，不能被解析拖慢；模型放 migration-common 会引入"改 common 后 fat jar 必须
clean install"那个反复踩的坑。

## 三条不可违反的不变量

设计的骨架就是这三条，后面每个阶段都在服务它们。

### 一、单一构造路径

v0 也必须由 CREATE TABLE 解析得出——把 `SHOW CREATE TABLE` 的原文喂给和 ALTER 同一个 parser，
**不允许"v0 走 information_schema、后续走 parser"两条路**。两条路的口径差异会伪装成 DDL bug，
排查代价极高。

附带好处：任务一启动就把源库全部表过了一遍语法，语法缺口在启动时暴露，而不是三个月后
某次 ALTER 才暴露。阶段 1 的预检项就是把这个附带好处变成一道门禁。

### 二、基线只能新于或等于它的位点，绝不能旧于

所以 capture 必须**先读位点、再打基线**：

* 先基线、后读位点 → 中间那条 DDL 既不在基线里、位点也在流的起点之前 → **永久丢失，
  模型从此全错**；
* 先读位点、后基线 → 中间那条 DDL 在基线里、也在流里 → 重复施加 → 由幂等 apply 吸收。

后者可修，前者不可修。同理，capture 被 ProcessGuard 重启后发的基线一定"新于"其位点，
所以还要一条配套规则：**extract 已持久化的时序库优先，capture 基线只是兜底种子**，
只用于填补它不认识的表。

### 三、时序库是"算出来的"，事件自带列名是"证据"

`binlog_row_metadata=FULL` 时，算出的版本必须与事件自带的列名一致，不一致就 fail-stop。

**没有这条交叉校验，一个语法 bug 就是静默数据损坏——比现状更糟**，因为现状至少还有列数校验
兜底。这条是本方案能上线的前提，不是可选项。

## 语法选型（阶段 0 决策）

选了**自己写一份 DDL 专用 ANTLR 语法**，放进已有的 `migration-extract/src/main/antlr4/`
（项目已钉 ANTLR 4.13.1，`antlr4-maven-plugin` 已在 pom 里，仓库里已有两份同风格的 g4）。

淘汰的两个候选：

* **vendor Debezium 的 `MySqlParser.g4`**：那是覆盖 MySQL 全语言（含 DML/查询/事务）的
  ~2500 行语法，我们只要 DDL；而且要从外部下载源文件进构建。
* **Alibaba Druid 的 `SchemaRepository`**（Canal tsdb 的实际后端）：代码量最省，但不是
  ANTLR，且结构模型会变成 Druid 的内部模型，耦合它的实现细节。当前它也不是本项目依赖。

自己写之所以可行，靠一个范围裁剪：**结构精确、表达式不透明**。

| 精确解析 | 不透明消费（按配平括号整段吃掉，只留原文） |
|---|---|
| 列名、数据类型、精度/标度、`unsigned`/`zerofill`、charset/collate | `DEFAULT (表达式)` 的表达式体 |
| `NULL`/`NOT NULL`、`AUTO_INCREMENT`、`COMMENT` | `GENERATED ALWAYS AS (表达式)` 的表达式体 |
| `GENERATED ... STORED/VIRTUAL` 的存储类型 | `CHECK (...)` 约束体 |
| enum/set 的取值表 | 分区子句 |
| `PRIMARY KEY` / `UNIQUE` / `KEY` 的列清单与顺序 | 表选项（`ENGINE=`、`ROW_FORMAT=` 等） |

我们**只需要存下**那些表达式原文，从不求值——所以没有任何理由为它们写语法。
MySQL DDL 语法里最难的部分正是任意表达式，裁掉之后规模从 ~2500 行落到几百行。

实际落地：`migration-extract/src/main/antlr4/com/migration/extract/ddl/MySqlDdl.g4`，
阶段 1 完成时 420 行（含注释与全部词法 token），覆盖 CREATE TABLE。

## 进度

| 阶段 | 状态 | 产出 |
|---|---|---|
| 0 决策与骨架 | ✅ | `TableSchema` / `ColumnSchema` / `TypeRenderMode` / `SchemaJson` / `SchemaTimelineConfig`；错误码 E3022~E3024（三处目录一致，CI 门禁通过） |
| 1 CREATE TABLE 解析 + 自检 | ✅ | `MySqlDdl.g4` / `CreateTableParser` / `SchemaSelfCheck` / `SchemaSelfCheckMain`；单测 33 项通过 |
| 2 ALTER 施加 | ✅ | `DdlApplier` / `SchemaChange`；单测 34 项通过（模块合计 180 项全绿） |
| 3 时序库与位点查询 | ✅ | `SchemaTimeline` / `SchemaHistoryFile` / `SchemaTracker`（extract）+ `SchemaVersionStore`（agent）+ `V18__task_schema_versions.sql`；capture 打基线；单测 39 项 |
| 4 extract 切换 + 交叉校验 | ✅ | `resolveSchema` 三档切换 + E3024 交叉校验 + 分级降级 + 四项指标 + 启动自检；单测 12 项 |
| 5 影子灰度 + 判据 | ✅ | `test_scripts/schema_drift/` 六场景 **13/13 真链路通过**；`DRIFT_MODE=OFF` 对拍 |

## 判据实测到的收益

`test_scripts/schema_drift/` 在 `dr-mysql-a → dr-mysql-b` 真链路上跑出来的对拍
（`DRIFT_MODE=OFF` 是改造前的行为）：

| 场景 | 改造前 | 改造后 |
|---|---|---|
| 积压期 `RENAME COLUMN` | **静默写坏**：值以 `0x…` 十六进制字面量落库，无任何报错 | 正确 |
| 积压期 `DROP a`+`ADD b`（列数不变） | 正确（FULL 事件列名已能自愈） | 正确 |
| 积压期 enum 增删取值 | E3004 停机（`Data truncated`） | 正确 |

第一行是这次改造的核心收益，值得记下它的成因：改造前的自愈只救了列**名**——
`resolveColumns` 用事件自带的列名，`columnMetaByName` 再拿这些名字去**当前**表定义里找类型，
改过名的列找不到、类型退化成空串，`isTextType("")` 为假，于是 capture 交付的
`0x…` 原样落库。**列名自愈了、类型没有**，正是时序库补上的那一维。

## 分阶段

### 阶段 0：决策与骨架

* 语法选型（见上）。
* `TableSchema` 模型：有序列（列名 / `DATA_TYPE` / `COLUMN_TYPE` 全文 / nullable / default /
  charset / collation / generated + STORED\|VIRTUAL / enum-set 取值表）+ 表级（主键列序、
  唯一索引、表 charset）。**字段口径必须与今天 THL 下发的六项元数据一一对得上**
  （`column_names` / `mysql_column_types` / `mysql_column_full_types` / `primary_keys` /
  `enum_set_values` / `generated_columns`），否则切换时会有暗差。
* JSON 序列化（时序库落盘 + 审计两用）。
* 错误码 E3022 / E3023 / E3024，三处同步（后端枚举 / 前端映射 / 判据脚本，有 CI 门禁）。
* 配置开关：`extract.schema.timeline.mode`（OFF / SHADOW / ON）、
  `extract.schema.timeline.fallback`（RESNAPSHOT / FAIL_STOP）。

### 阶段 1：CREATE TABLE 解析 + 启动自检

语法与 listener 只做 CREATE TABLE，产出 `TableSchema`。

* `CREATE TABLE ... LIKE` = 复制模型；`CREATE TABLE ... AS SELECT` 推不出结构，标记该表
  "时序库不可用"走降级。
* **验收方式就是一个自检**：对同步范围内每张表 `SHOW CREATE TABLE` → 解析 → 与
  `information_schema` 逐列比对（列名、`DATA_TYPE`、`COLUMN_TYPE`、主键、生成列、enum 取值）。
  全通过 = 语法对这个源库够用。

这个自检本身就有独立价值，阶段 1 结束即可单独上线——把语法缺口从运行期挪到启动前。

> **落地时的偏差**：原计划把它做成后端向导的一个预检项，实际做成了独立进程
> `SchemaSelfCheckMain`。原因是工程结构：`java-backend` 是与引擎**没有编译依赖**的独立
> Maven 工程（见 `build.sh` 的说明），`migration-agent` 也只依赖 `migration-common`，
> 两边都拿不到 parser。把 parser 复制一份到后端才是真正的坏主意——两份语法各自漂移之后，
> 自检通过与运行期正确就没有关系了。做成子进程后，agent 可以像启动其它引擎进程那样调它，
> 后端预检再代理到 agent，仍然只有一份语法。这条线留到阶段 4 一起接。

### 阶段 2：ALTER 施加到模型

必须改变列布局的：

| 类别 | 语句 |
|---|---|
| 列 | `ADD COLUMN [FIRST\|AFTER x]`、`DROP COLUMN`、`MODIFY COLUMN`、`CHANGE COLUMN`（改名+改型）、`RENAME COLUMN`（8.0）、`ALTER COLUMN SET/DROP DEFAULT` |
| 键 | `ADD/DROP PRIMARY KEY`、`ADD/DROP UNIQUE`、`ADD/DROP INDEX`、`CREATE/DROP INDEX` |
| 表 | `RENAME TABLE a TO b, c TO d`、`ALTER TABLE ... RENAME TO`、`CONVERT TO CHARACTER SET`、`DROP TABLE` |

只需解析通过、不改列布局的：分区 DDL、`ALGORITHM=`/`LOCK=` 子句、`TRUNCATE`、
`ANALYZE`/`OPTIMIZE`、存储程序、trigger/event、库级 DDL。

三条硬要求：

1. **多子句按序施加**。`ALTER TABLE t ADD a INT, DROP b, MODIFY c BIGINT` 必须按书写顺序
   逐条作用在中间态上，不能并行归并。
2. **幂等语义**。ADD 已存在的列 → 跳过 + 计数 + 告警；DROP 不存在的列 → 同理。这是不变量二的
   落点，**必须可观测**——不能像目标端 `SchemaEvolutionService.isIdempotentDdlError` 那样
   默默吞掉（那里把 `unknown column` 也当幂等错误，是另一个待修的问题）。
3. **pt-osc / gh-ost 的 RENAME 交换**。`RENAME TABLE t TO _t_old, _t_new TO t` 是一条语句里的
   原子交换，模型必须正确交换两张表的结构。跟丢一次，之后该表所有事件全错。现有
   `OnlineDdlService` 只负责判断影子表 DDL 要不要下发到目标端，时序库这侧要单独处理。

**落地时补的第四条：兜底规则必须是白名单，不能是"剩下的都不影响结构"。**
实现过程中被单测抓到两次同型问题——语法里的 `alterOther`（"逗号之前随便什么"）和
`otherStatement`（"任意 token 串"）会把**语法坏掉或形态没覆盖的改列 DDL**一并吞掉，
当成"这条不影响结构"跳过。而漏施加一条改列的 ALTER，该表之后的每一个版本都是错的。
两处都改成显式判定：

* `alterOther` 的首 token 不允许是 `ADD`/`DROP`/`MODIFY`/`CHANGE`/`RENAME`/`ALTER`/`CONVERT`，
  确实不改列的分区/索引子句（`ADD PARTITION`、`DROP PARTITION`、`RENAME INDEX`、`ALTER INDEX`）
  逐条列出来放行；
* 落到 `otherStatement` 的语句再按关键字兜一道：形如表级 DDL 却没匹配到任何已知子句的，
  一律报 E3023 解析失败。

这与 extract 的事件分发只放行白名单是同一个道理——黑名单漏一个就是一次静默损坏。

### 阶段 3：时序库与位点查询

* `SchemaTimeline`：`(db, table)` → 按 `(binlogFile, pos)` 升序的版本列表；
  `at(db, table, file, pos)` 取最后一个 `effectiveFrom <= pos` 的版本。
* 落盘 `files/<taskId>/schema_history.jsonl`，append-only，每条存**施加后的完整
  `TableSchema`**（不是只存 DDL）。DDL 稀疏，存全量换来"重启即加载、无需重放"，
  也顺带是一份审计日志。

> **落地时定下来的三件事**：
>
> 1. **中心存储必须独立建表**（`V18__task_schema_versions.sql`），不能塞进
>    `task_checkpoints.payload`——那一列是 `TEXT`（64KB），一张 50 列表的结构 JSON 就有
>    5~6KB，几十张表直接撑爆。新表用 `MEDIUMTEXT`，一行一个版本，唯一键
>    `(task_id, db_name, table_name, monotonic_key)` 让重放天然幂等。
> 2. **上传与回灌由 agent 代劳**，extract 只写本地 jsonl。extract 是子进程，够不着元数据库
>    （与阶段 1 自检那次是同一个约束）。`payload` 对 agent 是**不透明的原始行**——结构格式的
>    知识只留在 migration-extract 一处，agent 只认 `db/tbl/f/p/kind` 这几个键。
>    代价是位点折算函数两边各一份，靠两侧单测钉住同一组样例。
> 3. **capture 侧比预想的简单**：起始位点在 capture 启动之前就由
>    `AbstractTaskExecutor.initMysqlCheckpoint()` 取好并写进 `checkpoint.binlog.*` 了，
>    "先读位点、后打基线"这条不变量现有流程天然满足，capture 只需在 `doStart()` 里
>    对范围内每张表写一条 `SCHEMA_BASELINE`（`SHOW CREATE TABLE` 原文）。
* **跨机接管**：随中心化 checkpoint 一起持久化与 hydrate。不能指望在新机器上重读 `.cap`
  重建——那些文件可能根本不在。这一类坑已经踩过（`d79e7a4` 的跨机接管丢数据），
  务必一次做对。
* `reset_at` 重置 → 清空时序库并重新取基线。
* 保留策略：低于所有消费者最小已提交位点的版本可回收。

### 阶段 4：extract 切换 + 交叉校验

改造集中在四个方法：`resolveColumns` / `getTableColumns` / `getTableColumnTypes` /
`getTablePrimaryKeys`——从"查 `sourceConnection` 的当前定义"改成"按事件位点取版本"。
`sourceConnection` 只留作降级路径。

**分级降级**，不要一刀切：

| 情形 | 处置 |
|---|---|
| 算出的版本与事件自带列名矛盾（FULL） | **FAIL_STOP**，E3024——这种情况真的会写坏 |
| DDL 解析失败 | 该表标记失效 → RESNAPSHOT（退回今天的行为）+ 告警 + E3023 计数 |
| 该位点没有版本（时序库缺失 / 冷启动） | RESNAPSHOT + 告警 |

指标四项：时序库命中率、降级次数、交叉校验失败数、幂等吸收数。灰度期就看这四个数
（`MySQLBinlogExtractor.schemaTimelineMetrics()`，实际落了六项，另两项是 DDL 解析失败数与影子差异数）。

> **落地时补的两点**：
>
> * **交叉校验在 SHADOW 档也照做**。原计划只在 ON 档校验，但灰度期正是要提前知道"切过去
>   会不会报"——SHADOW 下产出仍走旧路径，校验失败却照样 fail-stop，因为那说明时序库跟丢了，
>   继续攒下去的每个版本都是错的，越晚发现越贵。
> * **启动自检接在 extract 启动时**（`extract.schema.selfcheck.enabled`，默认开），
>   默认只报告不阻断：自检失败说明语法有缺口，但运行期已经有分级降级与 E3024 兜着，
>   为它停机会把"一张冷门表解析不了"升级成"整个任务起不来"。
>   要严格把关置 `extract.schema.selfcheck.fail.stop=true`。

### 阶段 5：影子灰度 + 判据

* **`SHADOW` 模式：两条路都算，仍然用旧路径产出，只记录差异。** 差异率跑到 0 再切 `ON`。
  这类改造唯一稳妥的上线方式是让真实流量证明语法覆盖度，而不是靠单测覆盖率。
* `test_scripts/schema_drift/` 六场景：积压期 `RENAME COLUMN`、`DROP a`+`ADD b`（同列数）、
  enum 增删值、MINIMAL 模式、全量中途 `DROP COLUMN`、pt-osc 全程。每项断言
  **"要么正确同步、要么明确报错停下"**，不允许静默写坏。
* 长跑对拍：无 DDL 的静默期，时序库版本应与 `information_schema` 当前版本完全一致，
  不一致即模型漂移告警。

## 其余已知的坑

* **版本注释**：binlog 里的 DDL 常带 `/*!80000 ALGORITHM=INSTANT */`，语法要能吃掉。
* **非限定名的库上下文**：取 QUERY 事件的 `database='...'` 字段，extract 已经在解析
  （`parseQueryEvent`），直接复用。
* **范围外表的 DDL 要早退**，别喂给 parser。共享实例上别的库的 DDL 会制造大量无意义的
  解析失败告警。
* **DDL 事件与位点的先后**：版本的 `effectiveFrom` 取该 DDL 事件的位点，同位点上 DDL 版本
  优先于行事件——边界差一个事件就是整批错位。
* **顺带该修的**：`FullMigrationTask` 里 extract 应与全量并行启动（apply 仍等
  `FULL_COMPLETED`）。它把漂移窗口从"全量耗时几小时"压到"extract 落后量秒级"，
  成本半天，与本方案正交，两者叠加是防御纵深。

## 工期与风险

纯开发约 4~5 周，加影子观察 1~2 周。风险全部集中在阶段 2 的语法覆盖度上，而阶段 1 的预检
自检与阶段 5 的影子模式正好是针对它的两道量化闸门——**这两样任何一样被砍掉，本方案就不该上线**。
