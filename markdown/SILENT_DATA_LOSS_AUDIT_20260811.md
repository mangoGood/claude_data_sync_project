# 静默丢数据排查（承接 LOB / 表结构时序库 / XA 三项）

日期：2026-08-11　范围：全仓（capture / extract / increment / full / mongo / thl / common）

> **进度：12 项全部已修**，附录那条也一并处理。两轮提交（`baa1f54` + 本轮）。
>
> | 判据 | 改造前 | 改造后 |
> |---|---|---|
> | `test_scripts/pg_toast/pg_value_e2e.py` | 6/11 | **11/11** |
> | `test_scripts/pg_toast/pg_schema_truncate_e2e.py` | 4/10 | **13/13** |
> | `test_scripts/mongo_failstop/mongo_apply_failure_e2e.py` | 4/8 | **8/8** |
>
> 「改造前」是把主源码 stash 掉、重新打包跑出来的真实基线，不是推算的。
> 单测：`CapPartialLineTest` 5/5、`PgUnchangedToastTest` 7/7、`UpdateColumnSubsetTypeAlignmentTest` 2/2、
> `CsfAssemblerTest` 7/7、`PgRelationMessageTest` 5/5、`TiCDCUnknownEventTest` 6/6。
> 回归：`xa_e2e` 12/12、`edge_e2e` 15/15、`./test.sh all` 全绿（引擎 241+62+248+162+8+14+5，agent 122，后端 82）。
> 新增错误码 E3025 / E3026 / E3027，三份目录已同步（`SyncErrorCodeCatalogTest` 通过）。

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

### 3. ✅已修 Oracle LogMiner 的 CSF 续行没处理，长语句被截在 4000 字节

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

**已修**：查询补上 `CSF` 列，拼接逻辑抽成 `CsfAssembler`（单测 7/7，含 4000 字节三段拼回原文）。
批边界上还挂着半条语句时整条不下发、位点也不前进（`currentScn` 只在写出事件后推进），
下一批从同一 SCN 重新读到完整的一串。

**验证到什么程度（说明白）**：拼接逻辑有单测；改后的查询在真实 Oracle 上验证过能解析
（`CSF` 列存在，整条 SQL 只报 ORA-01306「需先 START_LOGMNR」而非列名/语法错）。
**没有**跑通端到端的 Oracle CSF 同步 —— 那需要归档模式 + 补充日志的完整环境。

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

### 7. ✅已修 Mongo：单事件应用失败后 resume token 照常前进

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

**已修**：位点只推进到"最后一条**处理完**的事件"的 token（不再用 cursor 当前的 token ——
那个已经跑到失败事件之后了）；应用失败即 fail-stop 退出，与 MySQL/PG 链路语义一致。

**实测基线**：目标端建一个源端没有的唯一索引，第二条文档必然写失败。改造前进程继续跑，
`_id=2` **永久丢失**——去掉冲突约束再重启也拿不回来（4/8）；改造后进程退出，
去掉约束重启即补齐（8/8）。判据 `test_scripts/mongo_failstop/mongo_apply_failure_e2e.py`。

### 8. ✅已修 PG 复制槽在重连路径上会被**重建**，中间 WAL 静默丢失

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

**已修**：只有在"还没有任何要保护的位点"时才允许建槽；已经读到过位点却发现槽没了，
一律 E3006 停机（复制线程单独捕获该异常并停止，不再无限重连）。
**实测基线**：运行中 `pg_drop_replication_slot` 之后再写数据 —— 改造前 capture 默默建了新槽继续跑
（中间的变更就此消失），改造后 5 秒内写出 E3006 且不再自建槽。

### 9. ✅已修 PG 保留期巡检只在有数据流动时才会执行

`checkRetentionQuietly()` 的唯一调用点在 `processWalMessage` 末尾（`PostgresWalCapture.java:654`）。
它要防的场景恰恰是"槽没了 → 收不到数据"，那时 `processWalMessage` 一次都不会被调用，
巡检自然一次都不跑。等于告警在最需要它的时候必然缺席。

**已修**：巡检挪进复制线程的主循环（方法自身按间隔节流），有没有数据都照跑；
另外补一条数据面告警 E3027 —— 流中断超过 `capture.stream.down.report.ms`（默认 5 分钟）
仍未恢复就上报，流恢复后**按原文比对**撤销自己写的那条（error_status 是三个进程共用的文件，
不能顺手抹掉别人写的真错误）。

配套问题：`AbstractCapture` 的 `capture_liveness` 是**进程级**心跳（独立线程，只看 `running`，
`AbstractCapture.java:59-82`）。PG/Oracle/Mongo 三个 capture 都不写数据面心跳
（`grep -c heartbeat`：MySQL 45、TiCDC 5、PG/Oracle/Mongo **0**）。
WAL 线程死了或反复重连失败时，进程还在、活性文件照刷，看门狗判健康。

### 10. ✅已修 PG TRUNCATE 被丢弃

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

**已修**：解析 `'T'` 消息，下发成一条 DDL 语句走既有的 DDL 通道（库名/表名映射、方言翻译都在那条路上）。
`default:` 分支改成打 WARN 点名，不再当作没这回事。
一条语句 TRUNCATE 多张表是 PG 语法，MySQL 目标端只支持单表 —— 那种情况下目标端会明确报错，
而不是像改造前那样悄悄什么都不做。
**实测基线**：源端 TRUNCATE 后，改造前目标端仍有 1 行、改造后为 0 行。

### 11. ✅已修 PG 的列元数据缓存永不失效——权威信息就在手里却丢掉了

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

**已修**：capture 完整解析 Relation 消息（列名、类型 OID、键标志），按 relationId 缓存，
**每收到一条新的 Relation 消息即整体替换**（这就是失效机制），并顺手作废回查源库那条老路径的缓存；
类型 OID 译成与 `information_schema.data_type` 一致的写法（内置常见类型免查，其余回查
`format_type` 一次 —— OID 与类型的对应关系不随表结构变化，这份缓存可以一直留着）。
列类型随事件下发（`column_types:{…}`），extract 优先用事件自带的列名/类型，与当前定义不一致时
打日志并以事件自带的为准。顺带修掉一个隐藏错位：`readCString` 之后按 `String.length()`（字符数）
前进，非 ASCII 的库表名/列名会让其后每个字段都错位 —— 已改成按字节走的游标。

**实测基线要说准**：链路积压期间 `DROP COLUMN` 之后，改造前**不是**静默写坏，而是
**任务直接停摆**（E3004 `column "doomed" of relation "drift" does not exist`，
删列后的行一条都过不去）；改造后正常同步。我在第一版报告里把它归到"静默"是推断，
实测更正如上。真正静默的变体需要"过期列名恰好在目标端都存在"（例如目标端结构未同步改动），
本轮没有为它单独造判据。

### 12. ✅已修 TiCDC：解析失败与未知类型都是"记一行日志然后丢"

`TiCDCExtractor.java:99`　canal-json 解析失败 → `logger.warn(... 跳过)` → `return null`
`TiCDCExtractor.java:111`　未知事件类型 → `logger.debug("忽略未知 TiCDC 事件类型")` → `return null`

黑名单式放行 + DEBUG 级日志。MySQL 链路已经改成白名单 + `extract.unknown.event.policy=FAIL_STOP`，
TiCDC 链路没跟上。

**已修**：三处（字段数不足 / JSON 解析失败 / 未知事件类型）统一走 `failUnusableEvent`，
复用 MySQL 链路的 E3020 通道与同一个开关。单测 `TiCDCUnknownEventTest` 6/6。
**注意这是契约变更**：原有的 `TiCDCExtractorTest.malformedRecords` 断言的正是"安全跳过"，
已改为断言停机 —— 那条断言本身就是在为静默丢数据背书。

---

## 附：`.cap` 的 completed 标记（已修）

`.cap` 的"处理完成"标记实际上**永远不会被置位**：`processFileIncremental` 在
`totalLinesInFile <= progress.linesRead` 时提前返回，而只要有新行，文件字节数必然也变了
→ `fileStoppedGrowing` 恒为 false → `stableCheckCount` 永远到不了 3。

实测佐证：`files/*/thl_output/.extract_progress` 共约 100 个真实任务，
**每一条记录的 completed 字段都是 false**，包括早就轮转走、几天没动过的文件。后果：

- `cleanupCompletedCapFiles()` 从未生效，`.cap` 无限堆积；
- 每轮扫描（默认 3s）对**每个**未完成的 `.cap` 调一次 `countLines()` 整文件重读，
  重读量随保留数据线性增长，总开销是平方级。

**已修**：判据换成"已读完 + 不是最后被修改的那个 + 且已静默 `extract.cap.settle.ms`（默认 30s）"。

**这里差点自己埋一个雷**：第一版用的是"文件名不是最大的那个"。文件名是
`binlog_<时间戳>_<序号>.cap`，而 capture 重启后序号从 `0000` 重来 —— 同一秒内轮转 + 重启
就会产出一个名字比现存文件**更小的活跃文件**，按名字排序会把它当成旧文件，读完即标完成，
之后 capture 追加的内容再也不会被抽取。改成按**最后修改时间**判定并加一个静默期。

判据（`pg_schema_truncate_e2e.py` 里，把每文件事件数压到 4 逼出轮转）：
已读完的旧文件标 `true`、最新的那个仍是 `false`，且被清理到只剩保留份数。

## 收尾

12 项 + 附录全部修完。仍然留着的口子，明说：

- **Oracle 只验到查询与拼接逻辑**，没有端到端跑通 CSF 同步（需要归档模式 + 补充日志的完整环境）。
- **`.cap` 仍是裸文本行**，只是读取端不再消费半行。彻底的做法是像 THL 那样分帧，改动更大。
- **PG schema 漂移的"静默"变体没有判据**：现有判据覆盖的是"漂移导致链路停摆"，
  真正无声写坏需要"过期列名在目标端恰好都存在"的构造。
- Oracle/Mongo 仍无数据面心跳（本轮只给 PG 补了 E3027）；Oracle 的 `SCN >` 在批中途崩溃时
  会跳过同 SCN 的剩余行，这一条在第一版报告里没有单列，留作下一轮。

### 判据脚手架的坑（本轮实测撞到的，写下一个判据前先看）

- **光把值造大不会触发未变更 TOAST**：PG 推到行外存储之前会先压缩，`repeat('X',40000)`
  压完只剩几十字节、老老实实待在行内，pgoutput 照发完整值，判据全绿但什么也没测到。
  必须 `ALTER COLUMN … SET STORAGE EXTERNAL`（或用不可压缩内容），
  并**断言 TOAST 附属表真的有字节**再往下跑。
- **判据目录要整个删重建**：只删 `.cap`/`.thl` 而留下位点/进度文件，本轮就会从上一轮的
  LSN 续传，看到的是上一轮的数据。另外 macOS 上 `os.remove` 删不掉目录项，用 `shutil.rmtree`。
- **辅助函数里别写死列名**：`row_of` 按 `vals` 的列写死，换张表就 SQL 报错，而 `wait_until`
  把异常当成"还没同步到"一路等到超时 —— 判据永远失败且看不出原因（`xalib.rows_of` 栽过同一跤）。
  改用 `row_to_json` 与表结构解耦。
- **单测里改 `-pl <module>` 不带 `-am`** 会拿本地仓库里那份旧的 migration-common，
  新加的类"不存在"。同一条老规矩：改了 common 就要重新构建依赖。
- 沿用 [[lob-test-suite]] 的老规矩：子进程 stdout 必须持续排空；判据跑 fat jar，
  只 `compile` 不 `package` 等于跑旧代码。

---

## 附二：增量延迟指标不可信（2026-08-12 修）

上面 12 项修完后被问到一个好问题：Oracle/Mongo 没有数据面心跳，那它们的增量延迟准不准？
把整条计算链读完，答案是**四条链路各有各的不准**，而且其中一条不准也波及 MySQL。

延迟的算法是 `rtoMs = increment 本地 now − event.getSourceTstamp()`
（`ContinuousIncrementMain`），所以准不准全看 `sourceTstamp` 是谁的时钟。

| 源 | 改造前的 sourceTstamp | 问题 |
|---|---|---|
| MySQL / TiDB | 源库时钟（心跳表绕一圈回来），有 `clockOffsetMs` 校正 | 相对可信 |
| PG | **capture 读到消息那一刻的本机时钟** | 「源库提交→capture 读到」整段不计入 |
| Oracle | 源库时钟（`V$LOGMNR_CONTENTS.TIMESTAMP`），无偏移校正，列类型是 `DATE`=秒级 | 偏差 = 两机时钟差 |
| Mongo 同步 | 无任何延迟指标 | 面板 RTO/RPO 是空的 |
| Mongo 订阅 | `clusterTime`（源集群时钟） | 口径对，无偏移校正 |

**最要命的一条对所有源都成立**：extract 空闲时注入的兜底心跳用**本机时钟**
（`ContinuousExtractMain.writeHeartbeatIfNeeded`），而它的触发条件是"1 秒内没有事件流入" ——
这个条件在"源库真的空闲"和"capture 卡死了"两种情况下都成立。于是 capture 一死，
extract 每秒造一条心跳，increment 算出 `now − now ≈ 0` 写进 rto_metric，
**面板显示延迟极低，实际一条数据都没在流动**。

代码里其实意识到过一半：`AbstractTaskExecutor` 的注释写着"若只监控增量端 rto_metric，
上游 capture 冻结后 rto_metric 仍在推进"，所以**看门狗**改用了每进程一个活性文件 ——
但**面板上显示的那个延迟数字**没跟着改。

### 修法

1. **事件带源端时钟**：新增 `SourceClockOffset`（往返取中点测偏移，把源端时间戳折算到本机时钟域，
   下游的减法就只剩真实链路耗时）。PG 改用 BEGIN 消息里的**提交时间戳**；Oracle 折算 LogMiner
   TIMESTAMP；Mongo 同步用 `wallTime`/`clusterTime` 并**补写 rto_metric**（此前完全没有）。
   偏移超过 5 秒打 WARN 点名 NTP。
2. **合成心跳不冒充源端时钟**：打 `synthetic_heartbeat` 标记，increment 见到只推进位点与活性、
   不刷 rto_metric；capture 在源库空闲时改由自己打一条**源端时钟**心跳（PG 用 `clock_timestamp()`，
   Oracle 用 `SYSTIMESTAMP`），空闲期的延迟数字因此是真的在量链路耗时。
   agent 侧对**过期**的 rto_metric 按无数据处理 —— "量不出来"和"延迟很低"必须能区分。
3. **负延迟不再静默丢弃**：旧实现 `if (rtoMs >= 0)` 把负样本全扔掉，本机时钟偏快时
   rto_metric 从此停在旧值上。改成限流 WARN 点名时钟未对齐 + 按 0 上报。

### 判据额外揪出来的一个设计问题

第一版判据跑出 rto=1038ms（期望 ≥9000ms），查 `.cap` 发现事件时间戳本身是对的，
是**每 2 秒一次的空闲心跳把 12 秒的真实样本覆盖掉了**：心跳只覆盖 capture→apply 一段，
数据事件覆盖全程，有数据流动时必须以数据事件为准。现在心跳在数据事件上报后的静默期内不抢话。

判据 `test_scripts/pg_toast/pg_latency_e2e.py`：基线 8/11 → 11/11
（关键两项：12 秒积压 969ms→13145ms；capture 死后指标从"每几秒刷成 ≈0"变成"停止刷新并陈旧"）。
单测 `SourceClockOffsetTest` 7/7、`MetricStalenessTest` 5/5。

### 仍未覆盖

MySQL 的 `.cap` 事件时间戳仍是**未折算**的源库时钟（它的 RPO 有校正、RTO 没有），
本轮没动它以免影响既有的 MySQL 判据；同机部署时偏移≈0，影响有限。
Oracle 的秒级精度是 LogMiner 固有限制，量不出亚秒延迟。
