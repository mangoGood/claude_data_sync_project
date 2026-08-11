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
     真正无法原子的是<b>跨实例</b>拆分：写入分布在多个实例上，只能靠"应用幂等 + 全部提交成功才推进位点"
     保证不丢，因此与 `TRANSACTIONAL` 档位互斥（启动即拒）。
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
| 引擎对 | mysql/pg 任意组合（含异构）、mongodb→mongodb、mysql→elasticsearch | Redis、Oracle、TiDB 源、订阅链路 |
| 汇聚 | 单实例多库多表 + 跨实例多 leg + 叠加列处理 | — |
| 拆分 | 单源 → 同实例分库/分表 + 跨实例目标组（跨实例仅最终一致） | 跨实例的事务原子性、分片扩缩容重分布 |
| 模式 | 全量 + 增量 | 灾备/倒换叠加路由 |

`route.split.*.count` 变更 = 需要数据重分布，首版不支持在线扩缩容：改 count 必须重建任务，UI 锁死已启动任务的该字段。

上表随实现推进已多次放宽（异构、mongo/es、叠加列处理都已支持），边界与实现是否一致由
`test_scripts/sharding/api_route_guard_e2e.py` 盯着。拦截的由来见「十二之五」。

## 十二、分批实施

| 批次 | 内容 | 验收 |
|---|---|---|
| B1 ✅ | 路由内核：`common/route` 全量类 + 模板/分片算法 + 单测 | 38 个单测通过，零外部依赖 |
| B2 ✅ | 汇聚全量：多目标 TableInfo、upsert 装载、来源列注入、续传语义翻转、结构校验 | 单测 13 个 + 真库 E2E 13/13（`test_scripts/sharding/mysql_merge_full_e2e.py`） |
| B3a ✅ | 汇聚增量（引擎侧）：converter 路由 + 来源标识 WHERE、文本回退 fail-stop、DDL 去重与破坏性 DDL 拦截 | 单测 8 个 + 真库增量 E2E 11/11（`test_scripts/sharding/mysql_merge_increment_e2e.py`） |
| B3b ✅ | 跨实例 leg 编排：父任务 + 隐藏 MERGE_LEG 子任务、进度/状态聚合 | 与 B5 一起做，真实 API E2E 14/14 |
| B4a ✅ | 拆分全量：目标表预建（剥 AUTO_INCREMENT）、per-shard 装载通道、未路由策略、崩溃续传清全部分片 | 单测 7 个 + 真库 E2E 11/11（`test_scripts/sharding/mysql_split_full_e2e.py`，含 SIGKILL 中断续传） |
| B4b ✅ | 拆分增量：行级路由、shard key 变更的跨分片搬迁、DELETE 用前镜像、DDL 广播到全部分片 | 单测 8 个 + 真库增量 E2E 13/13（`test_scripts/sharding/mysql_split_increment_e2e.py`） |
| B5 ✅ | backend 字段/校验/下发 + 向导路由页签 + 路由感知对比 + 分片分布指标 + E2E 套件 | 单测 26 个 + API E2E 14/14 与 8/8 |
| 跨实例拆分 ✅ | 分片表建到各自实例、全量按落点选连接、增量按 DML 的实例标识挑连接、与事务一致档位互斥 | 真库双实例 E2E 11/11 |

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

## 十二之四、收尾三项（全部完成）

1. **校验对比的路由感知**：汇聚按来源标识列切片统计目标行数（每个源表对到合并表里属于它的那部分），
   拆分把全部分片行数加起来。`RouteCompareSupport` 是引擎侧 ShardTemplate/TablePattern 的镜像实现
   （backend 不依赖引擎工程），用例与引擎侧一一对应防漂移。
   <b>内容对比对路由任务仍然拒绝</b>——逐行比字段在"合并表多了来源列 / 一张源表散在 N 张表"下没有可靠口径。
   来源实例标识的兜底规则必须与引擎一致（`route.node.id` ＞ 源实例 host:port），否则过滤条件一行都命中不了。
2. **分片命中分布指标**：增量应用侧按落点计数，5 秒限流写
   `files/<task>/binlog_output/route_metric`；agent `/api/route-metrics/{taskId}` 读取，
   backend 代理，任务详情新增「分片分布」页签（含热点分片告警：最大落点超均值 2 倍即提示）。
   汇聚模式下 key 记的是<b>来源</b>而不是目标——目标只有一张表，记目标看不出哪个分库偏斜。
3. **跨实例拆分**：分片表按 `route.node.*` 建到各自实例；全量按落点选连接写入，
   增量给每条 DML 打上目标实例标识、应用侧按标识挑连接。
   跨连接提交<b>不是原子的</b>：主连接与各实例连接依次提交，靠"应用幂等 + 全部提交成功才推进位点"
   保证不丢，因此与 `TRANSACTIONAL` 档位互斥（启动即拒），单实例分库分表不受影响。

## 十二之五、适用范围拦截（2026-08-04）

「十一」的边界矩阵此前只是文档承诺。实际行为是：

| 组合 | 此前的真实行为 |
|---|---|
| mongo / es / redis | 三个引擎模块里 `route.` 引用数为 **0**，agent 却无条件展开路由配置 → 任务安静地按 1:1 跑完，**用户以为在汇聚** |
| 叠加列处理 | 全量侧列处理规则的 key 取自<b>连接上的库名</b>（`DataMigration#getColumnNames` 等三处），汇聚跨多个源库时只有一个库能命中，其余**静默失效**；而增量侧源库名取自 binlog 事件是对的 → 同一张表全量放行、增量过滤，跑得越久差得越多 |
| 异构库对 | 汇聚的来源标识列由同构建表分支追加，异构分支生成建表语句后直接 return → 目标表没有这些列，跑到写数据才报 `Unknown column` |
| Oracle / TiDB | Oracle 目标没有 upsert 方言；TiDB 增量走 TiCDC canal-json，路由改写没验证过 |
| 灾备 / 订阅 | 订阅引擎不读 `route.*`（同"静默"类）；灾备复用增量引擎，路由**会生效但从未验证** |

拦在三层，少一层都能绕过去：

1. **引擎**（`RoutingConfig#loadFromProperties`）：库类型白名单 + 列处理互斥，进 `errors` → `router()` fail-stop。
   这层挡的是绕过接口直接改 `config.properties` 的情况。
   库类型**缺失时不判**——单测与直驱 E2E 不下发 `source.db.type`，把"没声明"当"不支持"会误伤。
2. **agent**（`ConfigService`）：把 `RouteConfigExpander.expand` 从库类型之<b>前</b>挪到之<b>后</b>。
   展开后本来就用引擎解析器兜底校验，但排在前面时 props 里还没有 `source.db.type`/`column.*`，
   上面那两道校验会全部落空——这行位置就是这一层的全部实现。
3. **backend**（`RouteConfigValidator#assertApplicable`）：库类型 / 任务类型 / 是否配了列处理。
   在**保存路由、改任务配置、启动任务**三处都调：配置可以分多次改，只在保存路由那一刻判，
   用户先存路由再回第 3 步加列处理就绕过去了。`MERGE_LEG` 子任务必须放行（它天生带父任务的路由配置）。
4. UI：路由页签与列处理三个页签互相禁用并给出原因；引擎对不支持时整个路由页签隐藏并把残留配置清回 NONE。
   老任务可能两者都有（本次改动之前存的），这时两边都不禁用、只给红字提示，否则用户哪个也删不掉。

验收：单测 +25（引擎 9 / backend 9 / agent 1，含"NONE 模式下完全不判"的零回归用例），
真实 API E2E `test_scripts/sharding/api_route_guard_e2e.py` 14/14。
回归：汇聚全量 13/13、拆分全量 11/11、拆分增量 16/16、跨实例汇聚后端链路 14/14。

## 十二之六、路由任务的内容对比（2026-08-05）

行数对比早就路由感知了，内容对比一直直接拒——现在补上，两阶段都要改。

**阶段 1：整表校验和 → 可合成的行级摘要。**
`CHECKSUM TABLE` / `md5(string_agg(...))` 在路由下根本没法用：汇聚的目标表混着别的来源的行还多了
标识列，拆分的"目标表"是 N 张。换成 `COUNT(*)` + `SUM(每行哈希)`——对顺序不敏感、可跨表相加，
于是汇聚能"源表 vs 目标切片"、拆分能"源表 vs Σ 各分片"。
每行哈希是 `CRC32(CONCAT_WS(0x01, 各列))`（PG 用 md5 前 8 位转 bigint），
列值一律转文本、NULL 换哨兵——`CONCAT_WS` 会跳过 NULL，不换的话 `(a,NULL,b)` 和 `(a,b,NULL)` 拼出来一样。

**别用 `BIT_XOR`**：重复行成对抵消，正好盖住"同一行在两个分片里各留一份"这类 bug。

**摘要一致 ≠ 拆分没问题。** 摘要对顺序不敏感正是它能相加的原因，也意味着一行从这片挪到那片，
count 与 sum 分毫不差。所以拆分任务的摘要一致<b>不给"一致"的结论</b>，强制走阶段 2 逐行核落点
（`NEEDS_ROW_SCAN`）。这条是 E2E 造"只有错片一个问题"的用例时暴露的——第一版就是靠摘要短路，
那个用例直接漏判。

**阶段 2：目标窗口必须由源端 chunk 的主键上界卡住。**
拆分下 N 张分片表各取 1000 行，并起来覆盖的主键区间比源端 chunk 宽得多，直接归并会把
"下一个 chunk 才轮到的源行"对应的目标行全判成目标端多余。改成先取源端 chunk、拿到它的主键上界，
再去各片取 `(lastPk, chunkMax]` 区间。源端扫完后另跑一次尾部扫描捡目标端残留行。

**目标行按主键建索引，而不是做 k 路归并。** 同一个主键可能在多张分片表里各有一份
（跨分片搬迁只插没删就是这个形态），归并流做不出"有几份、分别在哪片"的判断。

**正确分片按源行的分片键算，不是按目标行自己的值算。** 陈行带的是旧分片键，
它待的正是"按它自己的旧值该待的那一片"，按目标行自己的值算永远合规——什么也抓不到。

**新增差异类型 `WRONG_SHARD`**：行在目标端、内容也对，只是落在错误的分片上。
行数对比与 1:1 内容对比都发现不了（总行数一样、每行都在）。一行只报一条差异：
对的片上没有这份时，这条差异同时带上源行，修复就是一次搬迁（错片删 + 对片补）；
对的片上已有正确一份时不带源行，修复只删多余的那份。拆成 `WRONG_SHARD` + `SOURCE_ONLY` 两条
会让一个问题看起来像两个，差异条数也虚高一倍。

**修复与复核同样要路由感知**：汇聚的 INSERT 要带来源标识列、DELETE 的 WHERE 要带上它们
（PG 的 `ON CONFLICT` 冲突目标也要写全，汇聚目标表的主键是 源主键 + 标识列）；
拆分按源行的分片键选片写入、按差异记录的实际分片删除；复核的行数也要按落点算，
否则修得再对也永远显示没收敛。

**跨实例拆分不支持内容对比**：对比只有一条目标连接，够不着别的实例，标为 `UNSUPPORTED` 并说明原因
（报一堆假差异比不报更糟）。顺带记一笔：路由 JSON 里根本没有目标实例组的 host/port 字段，
跨实例拆分目前只能靠手写 config.properties，产品链路到不了。

验收：单测 +6（分片函数镜像：整数/字符串/BigDecimal/Double 表示形式必须落同一片、
RANGE 左闭右开、LIST 大小写回退、跨实例标记），
真实 API E2E `api_route_content_compare_e2e.py` 30/30。
回归：1:1 内容对比冒烟 1/1、路由行数对比 8/8。

## 十二之七、汇聚叠加列处理（2026-08-05）

两者从"互斥"改成"可叠加"。真正要改的是四处，其中两处是<b>静默</b>的：

1. **CUSTOM 附加列的值被烤死**。它的值是 `输入值@源库@源表`，此前由建表 DEFAULT 承载；
   而合并表只由<b>第一个</b>来源建出来，其余来源的行会全部带上第一个来源的库表名——
   而"标识来源"正是这个列存在的理由。改成汇聚下<b>逐行注值</b>（建表不带 DEFAULT），
   全量与增量两侧的列序必须严格一致：`源列 → 附加列 → 来源标识列`。
   CREATE_TIME/UPDATE_TIME 与来源无关，仍走 DEFAULT。
2. **结构一致性校验拿源列名去比**。配了列名映射后目标表建的是映射后的名字，
   第二个来源起会把每一个映射列都报成"缺失"，报错和真实原因完全对不上。
   改成比对映射后的名字，并把附加列也算进"目标应有列"。
   顺带定死一条口径：<b>汇入同一张目标表的各源表，列名映射结果必须一致</b>，不一致即 fail-stop。
3. **upsert 的冲突键要用映射后的列名**，否则 PG 的 `ON CONFLICT` 直接报列不存在。
4. 列处理规则的源库 key 改成<b>逐表取</b> `TableInfo#getSourceDatabase`。
   （多库任务是一个源库一条通道、连接上的库名恰好就是那个源库，所以旧写法在现有流程下并不出错；
   改过来是为了不依赖"每个源库各有一条通道"这个外部前提。）

### 顺手抓到的两个既有缺陷

- **`verifyMergeCompatibility` 的 catalog 没给**，`getColumns(null, ...)` 在 MySQL 上会把
  <b>所有库里</b>同名表的列并起来返回。别的库里恰好有张同名表，这道 fail-stop 校验就静默通过——
  实测拿到了 5 个库的列并集。已改为显式传目标库。
- **逐行写入失败不影响退出码**。`migrateAllData` 只把失败行数记了个数，进程照样退出 0、任务报"完成"。
  实测汇聚下某个来源列名对不上，那一整个来源的行<b>全部写入失败</b>，任务仍然成功。
  `continueOnError=false` 的语义就是"有失败就别装作成功"，已改成抛。

验收：真库 E2E `mysql_merge_column_processing_e2e.py` 12/12（三个分库配<b>不同</b>的过滤阈值，
断言各源保留行数各不相同；自定义附加列按各自来源取值；映射不一致 fail-stop 且点名到列）。

## 十二之八、异构库对与 mongo / es / redis（2026-08-05）

### 关系库异构（mysql↔pg）

两处真实缺口，补上即可，不需要重构：

- 汇聚的来源标识列此前只在<b>同构</b>建表分支追加，异构分支生成 DDL 后直接 return →
  目标表没有这几列，跑到写数据才报 `Unknown column '_src_db'`。
  翻译器产出的语句形态与同构一致（`CREATE TABLE x (\n 列...,\n PRIMARY KEY (..)\n)`），
  同一套 `MergeDdlRewriter` 改写对两边都成立。
- 拆分预建分片表读的是<b>源端</b> CREATE TABLE 文本，异构下方言不同 → 改走翻译器产出目标方言 DDL。
  异构翻译器本身就不产出自增属性，`stripAutoIncrement` 对它是空操作。

PG 的既有约束照旧：一条连接跨不了库，分片库名模板对 PG 没有意义，分片表落在任务目标库的 schema 下。

验收：`mysql2pg_route_e2e.py` 10/10（汇聚的标识列进了 PG 建表语句、复合主键、幂等重跑；
拆分由翻译器预建 4 张分片表、落点与分片键一致）。

### Mongo（集合级）与 ES（索引级）

规则模型完全共用 `RoutingConfig`——"库.表"读作"库.集合"或"库.表→索引"，匹配式/分片算法/模板一个字不改。
差别只在<b>落点长什么样</b>与<b>主键怎么防撞</b>，收敛到新的 `DocumentRouter`（只算名字与标识，不碰驱动 API）。

- **汇聚必须换文档标识**：`_id` 改成 `<来源标识>|<原_id>`。沿用原 `_id` 的话，
  两个来源里 `_id` 相同的文档 upsert 会互相覆盖——不报错，只少数据，与关系库的复合主键是同一件事。
- **Mongo 的 change stream 没有前镜像**，这逼出两条与 ES 不同的做法：
  DELETE 事件只带 `_id`、算不出分片键 → 按 `_id` <b>广播删</b>到每一片（删不存在的 `_id` 是 no-op，
  按某一片猜着删会漏删、留下幽灵文档）；UPDATE 改了分片键时旧落点算不出 →
  先把该 `_id` 从<b>其余各片</b>删掉再写新落点，等价于一次搬迁。
- **ES 的 binlog 带整行前镜像**，UPDATE/DELETE 的旧落点算得准，因此是"旧片精确删 + 新片写"。
- **Redis 明确不支持**：没有表的概念，所谓汇聚/拆分只能是 key 前缀命名空间的合并或分裂，
  与分库分表不是一回事，硬做只会误导。

验收：`mongo_route_e2e.py` 12/12、`es_route_e2e.py` 11/11（两边都覆盖全量落点、
增量 INSERT、改分片键的跨分片搬迁、DELETE 不留残留）。

### 仍未做
- 拆分的<b>在线扩缩容</b>（改分片数要重分布数据，目前必须重建任务）
- 跨实例拆分的内容对比（对比连不上第二个实例）
- Oracle 的路由（幂等 upsert 要 `MERGE INTO`，没实现）与 TiDB 源（增量走 TiCDC，未验证）
- mongo/es 链路的路由感知<b>对比</b>（内容对比目前只覆盖关系库）

## 十三、风险清单

1. **1:1 零回归**是硬要求：`route.mode=NONE` 时所有新代码路径必须短路（`IdentityRouter` 直接返回原库表）。
2. 汇聚 upsert 依赖目标表有可用的冲突键；无主键表在 MERGE 下无法幂等 → 预检阶段直接拒绝（error）。
3. SPLIT 的 shard key 必须非空且不可为 NULL；NULL 值行按 `unrouted` 策略处理，默认广播会造成重复 → UI 提示改用 DEADLETTER。
4. 跨实例 leg 的位点独立，父任务的"整体 RPO"只能取 max，不存在全局一致点——文档里要讲清楚，别让用户按单点位理解。
