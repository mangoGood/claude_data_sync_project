# 静默丢数据排查（承接 LOB / 表结构时序库 / XA 三项）

日期：2026-08-11　范围：全仓（capture / extract / increment / full / mongo / thl / common）

> **进度**：第 1、2、4、5、6 项**已修并有判据**（本轮），其余 7 项仍是待办。
> 判据：`test_scripts/pg_toast/pg_value_e2e.py` 11/11；单测
> `CapPartialLineTest` 5/5、`PgUnchangedToastTest` 7/7、`UpdateColumnSubsetTypeAlignmentTest` 2/2；
> 回归 `xa_e2e` 12/12、`edge_e2e` 15/15、单测全量 241+122+82 全绿。
> 新增错误码 E3025（extract）/ E3026（increment），三份目录已同步（CI 门禁通过）。

## 排查方法

刚修完的三项有同一个病症：**数据没到目标库，任务显示健康、位点照常前进、`error_status` 是空的**。
按这三项的病理反推出五条判据，逐条扫全仓：

| 判据 | 出处 |
|---|---|
| A. 事件失败/解析不了之后是 `continue`，位点照常推进 | `onEventDeserializationFailure` 静默丢 |
| B. 用**当前**元数据解释**过去**的事件（列名/类型/行值） | 表结构时序库 |
| C. 源端有"值没发过来"的表示法，被当成 NULL / 空串 | LOB、enum/set 空串 |
| D. 分段/续段的形态没认全（整条记录被截断或拆开） | XA 两段式、压缩 binlog |
| E. 黑名单式放行（认识的处理、不认识的丢） | 事件类型改白名单 |

MySQL 主链路这五条都已经收口。**下面 12 项是同族问题在别的链路上的复现**，按严重度排序。
每一项都给了 `文件:行`，可直接对着代码看。

---

## P0：会静默写坏/丢掉业务数据

### 1. ✅已修 PG 未变更的 TOAST 值被当成 NULL，把目标端的大字段**抹掉**

> **实测基线（改造前，pg→pg 真实链路）**：源端 `UPDATE vals SET tag='t1-upd' WHERE id=1`，
> 目标端同一行 40KB 的 `blob_txt` 变成 `<NULL>`。同一轮判据里
> **"全程无 error_status" 是 PASS 的** —— 数据毁了，任务全绿。
>
> **修法**：`u` 用独立标记 `[unchanged]` 下发（`CapTupleMarkers.UNCHANGED`，收敛到 migration-common
> 供两端共用），extract 把该列整个从 SET 列表里摘掉并下发 `update_column_names` /
> `update_before_column_names`；摘不掉的情况（该列是主键、INSERT 里出现、无主键表的 DELETE 前镜像）
> 一律 E3025 停机而不是猜。两个列名清单**只在确实摘了列时**下发，没摘时仍走全宽路径，
> 普通 UPDATE 的行为逐字节不变。
>
> 顺带的收益：`.cap` 里不再重复搬运未变更的大字段（判据里那几条 UPDATE 从 40KB 降到几十字节）。

`PostgresWalCapture.java:909`

```java
} else if (colFlag == 'u') {
    logger.debug("  col[{}] UNCHANGED_TOAST", i);
    values.add(null);          // ← 与真 NULL('n') 合流，信息就此丢失
}
```

pgoutput 的 TupleData 里列标志有三种：`n`=真 NULL、`t`=带值、**`u`=本次未变更的 TOAST 值（不发送）**。
一个行外存储（>2KB 的 text/bytea/json/数组）的列，只要这次 UPDATE 没改它，PG 就发 `u`。

链路走向已逐段确认：

1. capture 写出 `col:[null]`（`PostgresWalCapture.java:822`）
2. extract `parseTupleData` 把 `[null]` 还原成 `null`（`PostgresWalExtractor.java:485`）
3. extract **没有** 下发 `update_column_names`（全仓只有 `OracleRedoExtractor.java:218` 在写这个 key），
   于是 `THLToSqlConverter.generateUpdateSql` 的 SET 列表退化成**全列**（`THLToSqlConverter.java:1086`）
4. 生成 `SET big_col=NULL`（`THLToSqlConverter.java:1151-1155`）；类型化管道同样绑 null

**后果**：`UPDATE t SET flag=1 WHERE id=1` 这种和大字段毫无关系的更新，会把目标端同一行的大字段清成 NULL。
行数校验查不出来，一致性校验也只有在比内容时才发现。这是本次排查里最严重的一条。

**修法**：`u` 必须与 `n` 分开表示（业界做法是占位符，如 Debezium 的 `__unavailable_value`），
下游按"该列不参与 SET"处理 —— 即为 PG 补上 `update_column_names`。
若目标端必须拿到真值，退化为按主键回源查一次。

### 2. ✅已修 PG 空字符串被写成字面量字符串 `'NULL'`

> **实测基线**：源端 `UPDATE vals SET note='' WHERE id=2` 与 `INSERT … note=''`，
> 目标端两处都落成四个字符的 `NULL`。**修法**：删掉 `isEmpty()` 那条捷径，空串按类型正常渲染成 `''`。

`PostgresWalCapture.java:925`

```java
String strValue = new String(colData, StandardCharsets.UTF_8);
if (strValue.isEmpty()) {
    return "NULL";            // ← 空串 '' 与 NULL 混为一谈，且没加引号
}
```

`''` 在 wire 上是 `t` + 长度 0，与 NULL(`n`) 本来是分得开的。这里丢掉了区分，
而且返回的是**不带引号**的 `NULL`，extract 端 `parseTupleData` 只认 `[null]` 前缀，
于是 `"NULL"` 作为普通字符串留下来（`PostgresWalExtractor.java:485-491`），
再经 `formatRowData` 按文本类型加引号 → 目标端落进 4 个字符的 `'NULL'`。

和刚修的 `0623541`（enum/set 取值表丢空串）是同一个错误，换了个链路。

### 3. Oracle LogMiner 的 CSF 续行没处理，长语句被截在 4000 字节

`OracleRedoCapture.java:590`

```sql
SELECT SCN, OPERATION, XID, SEG_OWNER, TABLE_NAME, SQL_REDO, TIMESTAMP, ROW_ID
FROM V$LOGMNR_CONTENTS WHERE OPERATION IN (...)
```

`V$LOGMNR_CONTENTS.SQL_REDO` 单行上限 4000 字节，超了 Oracle 会**把一条语句拆成多行**并置
`CSF=1`（continuation）。查询里既没取 `CSF`，也没有拼接逻辑，于是：

- 宽表/长 VARCHAR2 的 INSERT/UPDATE → 语句在 4000 字节处被切断，`parseSqlRedoSetClause` 解析出**残缺列集**，
  按残缺列写目标端（缺的列静默不写）；
- 续行本身被当成独立事件，正则匹配不到 → `columnValues` 为空 → 走 `sql_redo:` 原样透传分支
  （`OracleRedoCapture.java:752`）→ 下游拿到半截 SQL。

**修法**：`SELECT ... CSF ...`，`CSF=1` 时把后续行的 SQL_REDO 依次拼到当前语句上，
拼完（`CSF=0`）再解析。这是 LogMiner 链路的硬性要求。

### 4. ✅已修 `.cap` 是按行分隔的文本，写入方无原子性、读取方无半行检测

写：五个 capture 一律 `BufferedWriter.write(整条记录) + flush()`
（`PostgresWalCapture.java:637`、`MySQLBinlogCapture.java:887`、`OracleRedoCapture.java:686`、`TiCDCCapture.java:433`）

读：`ContinuousExtractMain.readAndExtractNewLines` 用 `BufferedReader.readLine()`
（`ContinuousExtractMain.java:686`），行读完即 `progress.linesRead++`，**不校验这一行是否以换行结束**。

`BufferedWriter` 的缓冲是 8192 字符：一条记录跨过缓冲边界时会分两次落到文件（先填满的部分，
再 `flush()` 剩下的）。读取方恰好在这两次之间扫到文件，就会：

- 把**前半截**当成完整事件消费掉 —— 若前半截凑够 5 个字段分隔符，`doExtract` 会接受它，
  最后一个字段（行数据）是被截断的 → **静默写半行**；
- `linesRead` 已经跨过去了，**后半截**在下一轮成为独立一行，字段数不足被
  `logger.warn("Invalid WAL event format, skipping")` 丢掉（`PostgresWalExtractor.java:91`）。

对照组说明这不是苛求：**THL 那一跳早就做对了** —— 分帧格式 + `readNextHeader/readPendingPayload`
显式识别"半条记录，本轮当 EOF，下轮重读"（`THLFileReader.java:148-183`）。
capture→extract 这一跳还停在裸文本行上。

**已修**：读取端以换行符为准 —— `countLines` 在文件不以 `\n` 结束时不计末行，
`readAndExtractNewLines` 以进入本轮时数出的完整行数为上限（读的过程中 capture 还在追加，
不能以读到的为准）。半行留到下一轮重读。单测 `CapPartialLineTest` 5/5。
彻底的做法仍是 `.cap` 也上分帧、与 THL 对齐，那是更大的改动，留作后续。

### 5. ✅已修 事件转换出 0 条 SQL 时静默提交、位点照常推进

`THLToSqlConverter` 里 DML 三个入口各有两条"warn 完就返回空列表"的路径：

- `:551` `:566`（INSERT，库表名缺失 / 无行数据）
- `:1059` `:1076`（UPDATE）
- `:1301` `:1316`（DELETE）

而 `ContinuousIncrementMain` 的应用循环**不检查 statements 是否为空**：空列表让 `for` 一次不进，
`txFailed` 保持 false，事务照常提交，位点照常推进（`ContinuousIncrementMain.java:930-971`）。

于是上游任何一次解析退化（比如第 1、3 项造成的空数据事件、第 4 项的半行）都会变成
"一条 warn + 一条数据永久消失"。这是把上游各种毛病放大成静默丢数的**公共出口**。

**已修**：`isSilentlyDropped()` 收口，串行 / 并行批 / 并行 barrier 三条应用路径都过它，
命中即 E3026 停机（`increment.empty.statement.policy=SKIP` 可放过）。

判定只覆盖**文本路径**（`typedDmls == null`）：类型化路径返回空列表是合法的 ——
列过滤把整行排除掉时本来就不该产生 SQL，而文本路径根本不做行过滤（它只打一条
"无法执行行过滤"的告警），所以那里的空结果一定是缺陷。顺带堵掉一个潜在 NPE：
`convertToSql` 在 event_type 缺失时返回 null，旧代码会直接拿去 for 循环。

---

## P1：特定形态下必然丢

### 6. ✅已修 文本回退路径仍在吞掉非主键唯一键冲突

`ContinuousIncrementMain.java:899`

```java
if (errorMsg != null && (errorMsg.contains("Duplicate entry") || errorMsg.contains("1062"))) {
    isRecoverable = true;
    logger.warn("重复键忽略 (seqno={}): {}", event.getSeqno(), errorMsg);
}
```

同一个文件的**类型化路径**在 `:855` 已经分开处理了（`isPrimaryKeyConflict` → 主键冲突才算幂等重放，
非主键唯一键冲突走 E3017 fail-stop），注释里写得很清楚"忽略它会永久丢掉这一行"。
但 `isPrimaryKeyConflict` 全文件只有 `:855` 一处调用 —— **文本路径漏改了**。
类型化管道目前只覆盖部分源→目标组合，没覆盖的组合全走这条文本路径，这个洞是活的。

**已修**：文本路径改用同一套判定（并补上 PG 的 `duplicate key` 文案），
非主键唯一键冲突走 E3017 fail-stop，`increment.unique.conflict.policy=IGNORE` 可放过。
`isPrimaryKeyConflict` 认不出错误文案时按主键处理，不会因为一条没见过的文案把正常任务打停。

### 7. Mongo：单事件应用失败后 resume token 照常前进

`MongoSyncMain.java:672`

```java
} catch (Exception e) {
    // 单事件失败记日志继续（... 下轮 resume 重放可自愈；不因单事件卡死整个流）
    logger.error("应用增量事件失败: {} {}.{}: {}", op, db, coll, e.getMessage());
}
```

注释里的"下轮 resume 重放可自愈"不成立：外层循环紧接着就把 `cursor.getResumeToken()` 落盘
（`MongoSyncMain.java:613-620`），位点**越过了刚失败的这条**，重放永远不会再碰到它。
目标端一次唯一索引冲突、一次 WriteConflict、一次网络抖动 = 永久丢一条文档变更，任务全绿。

**修法**：记住"最后一条成功应用的事件"的 token，失败后就不再推进（或直接 fail-stop，
与 MySQL 链路的 fail-stop 语义对齐）。

### 8. PG 复制槽在重连路径上会被**重建**，中间 WAL 静默丢失

`ensureReplicationSlot` 里对"槽还 active"的分支有完整防护 —— 只踢后端、不删槽，
踢不掉且有续传位点时宁可失败（`PostgresWalCapture.java:493-519`，注释写得很到位）。
但**"槽不存在"的分支直接重建**：

```java
} else {
    stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
}
```

而 `reconnectReplication()`（`:445`）在每次重连时都会调 `ensureReplicationSlot()`。
运行中槽被人删掉 / 被 `max_slot_wal_keep_size` 判 lost 之后：重连 → 建新槽 →
`withStartPosition(currentLsn)` 对新槽无效，服务端**不报错**，从新槽位置开始发 → 中间那段变更消失。
`verifyResumePositionAvailable()` 这道校验只在 `doInitialize` 跑一次，进程不重启就永远不会再跑。

### 9. PG 保留期巡检只在有数据流动时才会执行

`checkRetentionQuietly()` 的唯一调用点在 `processWalMessage` 末尾（`PostgresWalCapture.java:654`）。
它要防的场景恰恰是"槽没了 → 收不到数据"，那时 `processWalMessage` 一次都不会被调用，
巡检自然一次都不跑。等于告警在最需要它的时候必然缺席。

配套问题：`AbstractCapture` 的 `capture_liveness` 是**进程级**心跳（独立线程，只看 `running`，
`AbstractCapture.java:59-82`）。PG/Oracle/Mongo 三个 capture 都不写数据面心跳
（`grep -c heartbeat`：MySQL 45、TiCDC 5、PG/Oracle/Mongo **0**）。
WAL 线程死了或反复重连失败时，进程还在、活性文件照刷，看门狗判健康。

### 10. PG TRUNCATE 被丢弃

`PostgresWalCapture.java:681`

```java
case 'O':
case 'T':   // ← 'T' 是 TRUNCATE
    return "";
default:
    return "";
```

下游其实是认 TRUNCATE 的（`SqlClassifier.java:274`、`:416`）。源端 `TRUNCATE`
在目标端不发生，之后源端重新灌入的数据靠 upsert 合进旧行 —— 两端从此不一致且无告警。
`default:` 分支同样是黑名单式放行，PG 后续版本新增的消息类型会走同一条静默丢弃路径（判据 E）。

### 11. PG/Oracle 的列元数据缓存永不失效——PG 侧的权威信息就在手里却丢掉了

- capture：`relationIdCache` / `tableColumnsCache` / `tableColumnTypesCache` / `tablePrimaryKeysCache`
  全是 `computeIfAbsent`，装进去就再也不更新（`PostgresWalCapture.java:954` `:977` 等）
- extract：`if (!tableSchemaCache.containsKey(cacheKey))` 同样只填一次（`PostgresWalExtractor.java:566`）

值是按**下标**与缓存里的列名/类型对齐的。源端 `ALTER TABLE DROP COLUMN` 之后，
wire 上的 tuple 少一列，而缓存里的列名还是老的 → 被删列之后的所有列**整体错位一格**，
静默写到相邻列里。这正是表结构时序库要根治的问题，只是方向相反（用过去的结构解释现在的事件）。

**PG 这条比 MySQL 好办得多**：pgoutput 在关系定义变化后会重发 Relation('R') 消息，
里面带着**权威的列名与类型 OID**，与行值同处一个流、同一时刻 —— 相当于 MySQL 的
`binlog_row_metadata=FULL`。而 `parseRelationMessage`（`:717`）只取了 schema/table 就把列信息扔了，
也没有借此让缓存失效。extract 侧甚至已经定义了 `RelationMessage` 类和 `relationCache` 字段
（`PostgresWalExtractor.java:33` `:788`），**从未被赋值使用**——像是设计了没做完。

### 12. TiCDC：解析失败与未知类型都是"记一行日志然后丢"

`TiCDCExtractor.java:99`　canal-json 解析失败 → `logger.warn(... 跳过)` → `return null`
`TiCDCExtractor.java:111`　未知事件类型 → `logger.debug("忽略未知 TiCDC 事件类型")` → `return null`

黑名单式放行 + DEBUG 级日志。MySQL 链路已经改成白名单 + `extract.unknown.event.policy=FAIL_STOP`，
TiCDC 链路没跟上。

---

## 附：非丢数但会拖垮长跑

`.cap` 的"处理完成"标记实际上**永远不会被置位**：`processFileIncremental` 在
`totalLinesInFile <= progress.linesRead` 时提前返回（`ContinuousExtractMain.java:470`），
而只要有新行，文件字节数必然也变了 → `fileStoppedGrowing` 恒为 false →
`stableCheckCount` 永远到不了 3（`:483-497`）。

实测佐证：`files/*/thl_output/.extract_progress` 共约 100 个真实任务，
**每一条记录的 completed 字段都是 false**，包括早就轮转走、几天没动过的文件。后果：

- `cleanupCompletedCapFiles()` 从未生效，`.cap` 无限堆积；
- 每轮扫描（默认 3s）对**每个**未完成的 `.cap` 调一次 `countLines()` 整文件重读，
  重读量随保留数据线性增长，总开销是平方级。

---

## 剩余待办

本轮已修 1、2、4、5、6。剩下的按价值排：

1. **3、11** —— 各自要一段实现（Oracle CSF 续行拼接 / PG 用 Relation 消息驱动 schema 更新），
   建议各配判据脚本（`test_scripts/oracle_csf/`、`test_scripts/pg_toast/` 加 schema 漂移场景）；
2. **7~10、12** —— 单点修复，逐条对着上面的行号改即可；
3. 附录那条（`.cap` 的 completed 永远置不上）是长跑资源问题，独立于丢数。

### 判据脚手架的坑（本轮实测撞到的，写下一个判据前先看）

- **光把值造大不会触发未变更 TOAST**：PG 推到行外存储之前会先压缩，`repeat('X',40000)`
  压完只剩几十字节、老老实实待在行内，pgoutput 照发完整值，判据全绿但什么也没测到。
  必须 `ALTER COLUMN … SET STORAGE EXTERNAL`（或用不可压缩内容），
  并**断言 TOAST 附属表真的有字节**再往下跑。
- **判据目录要整个删重建**：只删 `.cap`/`.thl` 而留下位点/进度文件，本轮就会从上一轮的
  LSN 续传，看到的是上一轮的数据。另外 macOS 上 `os.remove` 删不掉目录项，用 `shutil.rmtree`。
- 沿用 [[lob-test-suite]] 的老规矩：子进程 stdout 必须持续排空；判据跑 fat jar，
  只 `compile` 不 `package` 等于跑旧代码。
