# 分库分表汇聚 / 拆分 与聚合路由 —— 设计方案

日期：2026-08-03
状态：设计已定稿，分 5 批实施

---

## 一、目标与对标口径

DTS 类产品（阿里/腾讯 DTS、NineData、Tapdata、ShardingSphere-Scaling）在"分库分表"这一块的通用能力集是三件事：

| 能力 | 语义 | 场景 |
|---|---|---|
| 分库分表汇聚 | N 个源表 → 1 个目标表（N:1） | 128 张 `order_xxx` 分表汇入数仓单表 |
| 拆分 | 1 个源表 → N 个目标库/表（1:N） | 单体库 `order` 打散成 8 库 × 16 表 |
| 聚合路由 | 规则引擎：匹配器 + 目标模板 + 行级分片函数 | 前两者的统一底座 |

### 现状差距

- `schema.mapping.table.*` 是**单值 Map**（`SchemaMappingConfig#mapTable`）。多个 key 指向同一 value 在结构上就是 N:1，
  所以"汇聚看起来已经能跑"——但它是假的：主键会撞、DDL 会重复应用 N 次、断点续传会互删数据。
- 拆分**结构上不可能**：一个 key 只能有一个 value；且行级路由要 shard key 参与计算，静态 properties 映射表达不了。
- `fanout`（`FanoutDispatcherService`）是**广播**（同一条 SQL 发给所有目标），不是路由。UI 上必须与拆分明确区分，
  否则用户当分片用会写出 N 份全量数据。

---

## 二、已定决策

| # | 决策 | 说明 |
|---|---|---|
| D1 | 汇聚全量**整体改走幂等 upsert 装载** | 不再用"未完成即 TRUNCATE 重搬"（该逻辑在汇聚下会清掉其他源已搬完的数据） |
| D2 | **跨实例汇聚首版就做** | 一个逻辑任务 = N 条子管线，复用 DR_SHADOW 的隐藏子任务模式 |
| D3 | 拆分的**目标表由我们预建** | 按模板自动 CREATE N 张（库/表），不要求用户提前建好 |

### D1 的派生决策（必须一起成立）

- **D1-a 主键策略默认 `COMPOSITE_SOURCE`**：目标表主键 = 源主键 + 来源标识列。
  upsert 的冲突目标是目标表主键/唯一键；若沿用源 PK（`KEEP`），两个分片的同 PK 行会互相覆盖 → 静默丢数据。
  `KEEP` 仅在用户显式声明"源主键全局唯一"（雪花 ID / 全局序列）时开放，UI 上给二次确认。
- **D1-b 汇聚全量装载通道降级为 BATCH**：PG 二进制 COPY 与 Oracle direct-path 都没有 upsert 语义。
  `JdbcBulkChannels#open` 增加 `upsert` 判定，MERGE 模式下 COPY/DIRECT_PATH → BATCH 并告警。
  代价：汇聚全量吞吐低于 1:1 链路。（P2 可做 COPY 进临时表 + MERGE 落表的两段式，首版不做。）
- **D1-c 续传语义翻转**：MERGE 表在 `DataMigration#resetIfIncompleteProgress` 中**不再清表**，
  直接按进度续搬——因为 upsert 幂等，重复搬运只是覆盖同值。1:1 与 SPLIT 链路维持原有清表逻辑不变。

---

## 三、核心设计：统一路由层

一条铁律：**路由只有一个真相源**。capture 的 `migration.included.tables`、全量建表清单、增量 DML 目标、
DDL 改写、校验对比的表配对，全部由 `RoutingConfig` 展开生成，不再各自解析 properties。

新增 `migration-common` 的 `com.migration.common.route`：

```
RoutingConfig.loadFromProperties(props)      // 与 ColumnProcessingConfig 同构
  └─ TableRouter#route(srcDb, srcTable, rowAccessor) → List<RouteTarget>
       ├─ IdentityRouter   // 现状 1:1，零回归，默认
       ├─ MergeRouter      // N:1，不看行数据
       └─ SplitRouter      // 1:N，按 shard key 逐行算
```

`RouteTarget = { nodeId, targetDb, targetTable, shardNo }`；`nodeId` 为空表示"任务默认目标实例"。

对四条链路的意义：**汇聚 / 拆分 / 1:1 是同一抽象的三个特例**，链路里只有一处 `router.route(...)`，
不再是每条链路各写一遍 if-else。

---

## 四、配置键

走**新键空间 `route.*`**，不复用 `schema.mapping.table.*`。理由：老键是单值语义，且全链路已有 4 处按单值假设写死，
混用会让 1:1 任务的行为不可推理。1:1 任务不下发任何 `route.*`，行为完全不变。

```properties
route.mode = NONE | MERGE | SPLIT              # 默认 NONE = 现状

# —— 汇聚 N:1 ——
route.merge.1.match        = shard_db_*.order_*        # 通配；regex: 前缀可选
route.merge.1.target       = dw.order_all
route.merge.1.pk.strategy  = COMPOSITE_SOURCE | KEEP   # 默认 COMPOSITE_SOURCE
route.merge.1.tag.columns  = _src_node,_src_db,_src_table
route.merge.1.ddl.policy   = FIRST_WINS | SKIP | MANUAL

# —— 拆分 1:N ——
route.split.1.match        = app.order
route.split.1.shard.key    = user_id
route.split.1.algo         = HASH_MOD | RANGE | LIST | DATE_FORMAT
route.split.1.count        = 16
route.split.1.target.db    = dw_${shard/2}             # 分库模板
route.split.1.target.table = order_${shard}            # 分表模板
route.split.1.target.group = g1                        # 跨实例目标组（可空 = 单实例）
route.split.1.unrouted     = BROADCAST | DEADLETTER | ERROR
route.split.1.range        = 0:1000,1000:5000,...      # RANGE 专用
route.split.1.list         = CN:0,US:1,JP:2            # LIST 专用
route.split.1.date.format  = yyyyMM                    # DATE_FORMAT 专用（此时 count 无意义）

# —— 目标实例组（跨实例拆分） ——
route.node.g1.0.host / .port / .database / .username / .password
```

模板变量只允许 `${shard}` 与 `${shard/N}`、`${shard%N}`（整数运算），不引脚本引擎——
避免把任意表达式执行面引进数据面。

---

## 五、全量链路

`TableInfo` 从"单个 targetTableName"改为持有 `List<RouteTarget>`：

- **MERGE**：N 个源表 → 同一目标通道。
  - INSERT 语句生成为 upsert：MySQL `ON DUPLICATE KEY UPDATE`，PG `ON CONFLICT (pk) DO UPDATE`。
  - 来源标识列由建表期注入（复用 `SchemaMigration#appendExtraColumnsToCreateSql` 的机制，
    但值 per-source 而非常量 → 扩展为路由注入列，全量 INSERT 逐行带值，增量同理）。
  - 建表只建一次：首个源表建表，其余源表走结构一致性预检（列集/类型/字符集差异 → error 阻断；
    仅 comment/索引差异 → warning 放行）。接入现有 `schemaPrecheck` 门禁。
  - 装载通道按 D1-b 降级 BATCH；续传按 D1-c 不清表。
- **SPLIT**：读到的每一行算 shard → 分桶 → 每个目标分片一条独立 `JdbcBulkChannel`。
  - 目标表按 D3 预建：源表 `SHOW CREATE TABLE` → `renameTableInCreateSql` 改名 N 次 → 逐分片建；
    分库时先 `ensureDatabaseExists`。**建表期剥掉 AUTO_INCREMENT**（各分片自增会撞），强制沿用源主键值。
  - 单表 PK 分片并行（`migrateTableDataSharded`）与路由分桶正交，可叠加；此时 `exclusiveWriter=false`。

## 六、增量链路

- `TypedDmlConverter` / `THLToSqlConverter` 产出 `List<RoutedDml>`（DML 带 targetNode）。
- `ContinuousIncrementMain` 目前持**单个** `targetConnection` → 引入连接路由（nodeId → Connection），
  每目标独立事务；checkpoint 取所有目标的**低水位**，复用并行应用那套低水位 + fail-stop 机制。
- SPLIT 的行级语义（首版必须全部覆盖）：
  1. **UPDATE 改了 shard key** → 跨分片搬迁：旧分片 `DELETE`（按前镜像算）+ 新分片 `INSERT`（按后镜像算）。
     MySQL binlog 自带前镜像；PG 必须 `REPLICA IDENTITY FULL`，预检卡住。
  2. **DELETE** 只有前镜像 → 一律用前镜像算 shard。
  3. **行内无 shard key / 无法定位** → 按 `unrouted` 策略，默认 BROADCAST。
  4. **一致性档位**：单实例分库分表的所有分片在同一连接上，事务性不受影响，
     `TRANSACTIONAL` 照常可选（实施后修正的结论，原方案一刀切禁掉是过度保守）。
     真正无法原子的是<b>跨实例</b>拆分——那一档目前直接拒绝，将来放开时再与档位互斥。
- MERGE 的增量：多源写同一目标表，DML 必须带来源标识列的值（否则 upsert 冲突目标不完整）。

## 七、DDL 策略

- **MERGE**：N 个源的同构 DDL 会重复打到同一目标表 → 按"归一化语句指纹 + 目标表当前状态"去重（`FIRST_WINS`）。
  `DROP TABLE` / `TRUNCATE` 默认降级为 SKIP + 告警（一个源的清表不该毁掉汇聚表）。
- **SPLIT**：同一条 DDL 经 `DdlIdentifierRewriter` 改写 N 次广播到各分片；任一分片失败 → fail-stop。
- 两者都沿用现有 `schema.ddl.apply.policy` 的 AUTO_APPLY/SKIP/MANUAL 总开关。

## 八、跨实例汇聚编排（D2）

一个逻辑任务 = N 条子管线，每条有独立的 config.properties / thl 目录 / 位点 / capture 进程。
**不新造调度模型**，复用 `DR_SHADOW` 的隐藏子任务模式（`WorkflowService` 建影子任务、列表过滤掉）：

- 父任务 `taskType=MERGE`，持 `route.*` 规则与目标连接；
- 每个源实例一个子任务 `taskType=MERGE_LEG`，对上层隐藏；
- 父任务聚合：进度 = Σ 子任务进度（按行数加权）、延迟 = max(子任务延迟)、状态 = 最差子状态；
- 任一 leg FAILED → 父任务 FAILED；恢复/续跑按 leg 粒度。
- 来源标识列的 `_src_node` 取子任务的实例标识，保证跨实例同名库表不冲突。

## 九、校验对比

`ValidationTaskService` 的表配对改为路由感知：

- MERGE：`Σ 源表行数 vs 目标表行数`；逐源明细按来源标识列切片比对；修复逻辑同样带来源列条件。
- SPLIT：`源表行数 vs Σ 分片行数`；抽样行按 shard 函数直接定位到目标分片，不做全分片扫描。

## 十、观测与 UI

- 向导新增"路由"页签（与"列处理"并列，第 3 步内）：模式选择 → 规则列表 → 目标预览（展开后的实际库表清单）。
- 任务详情：源→目标矩阵视图；SPLIT 展示**每分片行数分布**（暴露热点分片）；MERGE 展示每源进度/延迟。
- 指标：`route_hit{shard}`、`route_unrouted_total`、`route_cross_shard_move_total`（shard key 变更搬迁次数）。
- 死信记录带 shard 标签。

## 十一、边界与降级矩阵（首版）

| 维度 | 支持 | 不支持（明确写进文档与 UI） |
|---|---|---|
| 引擎对 | mysql→mysql、pg→pg | 异构库对、mongo/es/redis、订阅链路 |
| 汇聚 | 单实例多库多表 + 跨实例多 leg | 汇聚叠加列处理（首版互斥） |
| 拆分 | 单源 → 同实例分库/分表 + 跨实例目标组 | 跨分片事务原子性、分片扩缩容重分布 |
| 模式 | 全量 + 增量 | 灾备/倒换叠加路由 |

`route.split.*.count` 变更 = 需要数据重分布，首版不支持在线扩缩容：改 count 必须重建任务，UI 锁死已启动任务的该字段。

## 十二、分批实施

| 批次 | 内容 | 验收 |
|---|---|---|
| B1 ✅ | 路由内核：`common/route` 全量类 + 模板/分片算法 + 单测 | 38 个单测通过，零外部依赖 |
| B2 ✅ | 汇聚全量：多目标 TableInfo、upsert 装载、来源列注入、续传语义翻转、结构校验 | 单测 13 个 + 真库 E2E 13/13（`test_scripts/sharding/mysql_merge_full_e2e.py`） |
| B3a ✅ | 汇聚增量（引擎侧）：converter 路由 + 来源标识 WHERE、文本回退 fail-stop、DDL 去重与破坏性 DDL 拦截 | 单测 8 个 + 真库增量 E2E 11/11（`test_scripts/sharding/mysql_merge_increment_e2e.py`） |
| B3b ✅ | 跨实例 leg 编排：父任务 + 隐藏 MERGE_LEG 子任务、进度/状态聚合 | 与 B5 一起做，真实 API E2E 14/14 |
| B4a ✅ | 拆分全量：目标表预建（剥 AUTO_INCREMENT）、per-shard 装载通道、未路由策略、崩溃续传清全部分片 | 单测 7 个 + 真库 E2E 11/11（`test_scripts/sharding/mysql_split_full_e2e.py`，含 SIGKILL 中断续传） |
| B4b ✅ | 拆分增量：行级路由、shard key 变更的跨分片搬迁、DELETE 用前镜像、DDL 广播到全部分片 | 单测 8 个 + 真库增量 E2E 13/13（`test_scripts/sharding/mysql_split_increment_e2e.py`） |
| B5 ✅ | backend 字段/校验/下发 + 向导路由页签 + `test_scripts/sharding/` E2E 套件 | 单测 16 个 + API E2E 14/14；路由感知对比与分片指标见"未做" |

## 十二之二、实施记录（B1/B2 落地后补充）

写代码时冒出来、方案阶段没写进去的几条：

1. **进度 key 会串**。`ProgressManager` 以表名为 key，汇聚下 `shard_1.order_001` 与 `shard_2.order_001`
   共用一条进度记录，续传直接错位。改为 `TableInfo#getProgressKey()`：<b>只有汇聚</b>才带源库名前缀，
   1:1 任务的 key 不变，已有任务的续传状态不失效。
2. **结构校验必须 fail-stop**。`SchemaMigration#migrateAllTables` 原本吞掉单表建表异常继续跑
   （为了 FK 建表顺序）。汇聚表沿用这个行为的后果是"某个来源整列丢失但任务报完成"，
   因此汇聚表的结构异常改为直接抛出终止任务；非汇聚表行为不变。
3. **目标库可能与任务默认目标库不同**，而目标连接是绑库的。全量侧新增按目标库分组
   （`Main#migrateTablesRouted`），未配路由时只有一组，走的仍是原来那一次调用。
4. **来源实例标识**由 `route.node.id` 下发（跨实例汇聚的 leg 标识），未下发时退回源实例
   `host:port`——跨实例同名库表只能靠它区分。
5. **增量的文本回退是条暗雷**。类型化管道不适用时（旧 THL、缺元数据）会回退文本路径，
   而文本路径的 UPDATE/DELETE 只按源主键定位——落到汇聚表上就是改错/删错其它来源的行。
   汇聚表的行事件因此改为 fail-stop（`TypedDmlConverter#requiresTypedPipeline` + E3013），
   宁可停任务也不静默改坏数据。
6. **DDL 去重按"改写后的语句"做指纹**：N 个源表的同一条 ALTER 改写到汇聚目标表后完全相同，
   指纹去重即 FIRST_WINS，不需要额外的来源计数。指纹集是有界 LRU（512），长跑任务不会涨爆。
7. **破坏性 DDL 必须拦住**：一个分表的 `DROP TABLE` / `TRUNCATE` / `RENAME` 若被应用到汇聚表，
   会连同其它几十个来源的数据一起毁掉，一律 skip + 告警。
8. **拆分的多目标写入做成了一条"通道"**（`ShardedJdbcBulkChannel` 实现 `JdbcBulkChannel`），
   对上层仍是 add/isFull/flush 那套接口——全量的分页、断点、重连、进度循环一行没改，
   只是"写哪张表"从固定值变成了按行计算。这比在搬运循环里插 if-else 少一个数量级的回归面。
9. **崩溃续传要清的是全部分片**：一张源表散在 N 张分片表里，只清其中一张就等于让另外 N-1 张
   带着半截数据按 lastMigratedId 续扫，跳过的区间是永久丢失的行。E2E 用 SIGKILL 真打断验证。
   注意"只比总行数"验不出这个 bug（重复主键会被跳过），能验出来的是<b>缺行</b>。
10. **全量拆分要求分片可枚举**：目标表要预建、每片要有独立写通道，`DATE_FORMAT` 枚举不出来，
    全量阶段直接拒绝而不是把整表搬到某一片上。
11. **分片哈希必须跨"值的表示形式"稳定**（E2E 抓到的真 bug）：同一个 `user_id=5`，
    全量链路拿到的是 `Long 5`，增量类型化值里是字符串 `"5"`。按 Java 类型分流的话
    前者走整型取模落 1 号片、后者走 CRC32 落 2 号片——**同一行在两个分片里各存一份**，
    不报任何错，只有对数时才发现。`SplitRule#hashOf` 现在先把值归一成整数
    （Number / BigDecimal 零标度 / 数字字符串），归一不了才走 CRC32。
12. **单实例分库分表不牺牲事务性**：所有分片在同一个目标实例上，一条连接写限定名即可，
    因此增量的事务边界与位点机制原样可用，`TRANSACTIONAL` 一致性档位依然成立。
    需要低水位 checkpoint 的是<b>跨实例</b>拆分——那一档目前在启动时直接拒绝。

## 十二之三、B3b + B5 实施记录

1. **backend 不依赖引擎工程**（两边独立构建），所以路由配置有<b>两道校验</b>：
   backend 侧 `RouteConfigValidator` 只挡"UI 能填错的那些"（缺字段、枚举写错、模板不含分片号占位、
   实例标识重复），让用户在点保存时就看到错；引擎侧 `RoutingConfig` 仍是最终权威，
   agent 下发时用它兜底再校验一次（`RouteConfigExpander`）。
2. **leg 的连接串必须是 URI 格式**（`mysql://user:pass@host:port/db`）：
   agent 的 ConnectionStringParser 只认这个，派生 leg 时拼成 JSON 会直接
   "Invalid connection string format"（E2E 抓到）。
3. **跨实例并发建表的竞态**（E2E 抓到）：N 条 leg 是各自独立的进程，"查表不存在 → CREATE"
   之间必然有窗口，输的那条拿到 `Table already exists` 会让整条通道失败。
   汇聚建表现在把 already exists 当作"表已存在"处理，转去做结构一致性校验。
   B2 是单进程所以没暴露这个问题——跨实例才必然踩。
4. **Flyway 会把 SQL 注释里的 `${...}` 当成自己的占位符**：迁移脚本里不能写分片模板的字面形式。

### 未做（留给后续）
- 校验对比的路由感知（`ValidationTaskService` 仍按 1:1 配对；汇聚/拆分任务的对比结果不可信）
- 分片命中分布指标与监控页展示
- 跨实例<b>拆分</b>（全量/增量都在启动时直接拒绝）

## 十三、风险清单

1. **1:1 零回归**是硬要求：`route.mode=NONE` 时所有新代码路径必须短路（`IdentityRouter` 直接返回原库表）。
2. 汇聚 upsert 依赖目标表有可用的冲突键；无主键表在 MERGE 下无法幂等 → 预检阶段直接拒绝（error）。
3. SPLIT 的 shard key 必须非空且不可为 NULL；NULL 值行按 `unrouted` 策略处理，默认广播会造成重复 → UI 提示改用 DEADLETTER。
4. 跨实例 leg 的位点独立，父任务的"整体 RPO"只能取 max，不存在全局一致点——文档里要讲清楚，别让用户按单点位理解。
