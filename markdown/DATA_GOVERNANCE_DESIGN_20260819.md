# 数据治理 设计方案（F-06）

> 状态：**设计，未实现**。粒度到"改哪个类、判据怎么写"。

## 1. 当前有什么、缺什么

审查结论是"治理面偏薄"。拆开看，四件事里已经有一件半：

| 能力 | 现状 |
|---|---|
| 脱敏 | ⚠️ **只在订阅侧**有；同步链路（mysql→mysql 等）没有 |
| 字段级血缘 | ❌ |
| Schema 演进审批 | ❌ DDL 有 `AUTO / SKIP / MANUAL` 三档，但 MANUAL 只是"停下来等人",没有审批流 |
| 数据分级与标签 | ❌ |

好消息是**血缘所需的元数据大部分已经在手上**——这决定了实施顺序（见 §5）。

## 2. 字段级血缘：为什么它是第一优先

血缘不是新采集一套元数据，而是把**平台已经掌握、但只用在别处的信息**串起来：

| 已有的东西 | 在哪 | 血缘要用它做什么 |
|---|---|---|
| 库/表/列的源→目标映射 | `schema.mapping.db.*` / `schema.mapping.table.*`、`syncObjects` | 表级与库级的边 |
| 列处理规则（增删改名/类型转换/过滤） | 列处理特性（向导第 3 步）、`MongoDocumentProcessor` | **列级**的边与变换算子 |
| 分库分表路由（汇聚/拆分） | `route.mode` / `route.split.*` / `route.merge.*` | 多对一、一对多的边 |
| 表结构与其历史 | `schema-timeline` 的版本链 | 边的**有效时间区间**——列改过名，血缘要能说清哪段时间叫什么 |
| DDL 解析 | ANTLR + `DdlIdentifierRewriter` | 新增列/改名时自动更新边 |

也就是说：**血缘的采集不需要新的探针，只需要一个把这些配置归一成图的转换层。**
这跟从零做血缘（要解析所有 SQL、要 hook 每条写入）完全是两个量级。

### 2.1 模型

有向图，节点是"字段的一个版本"，边是"变换"。

```
节点  (connectionId, db, table, column, validFrom, validTo)
边    src节点 → dst节点 + 算子 + 产生它的 taskId
算子  IDENTITY | RENAME | CAST(from,to) | DEFAULT(expr) | FILTER(pred)
      | ROUTE_SPLIT(shardKey,algo) | ROUTE_MERGE | MASK(rule) | DROP
```

`validFrom/validTo` 用 **seqno** 而不是墙上时间：与 `schema-timeline` 同一把尺子，
才能回答"2026-08-01 那天这一列从哪来"这种问题（对齐到位点再换算时间）。

### 2.2 落库

```sql
-- V22__lineage.sql
CREATE TABLE lineage_node (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  connection_id VARCHAR(64) NOT NULL,
  db_name VARCHAR(128) NOT NULL,
  table_name VARCHAR(128) NOT NULL,
  column_name VARCHAR(128) NOT NULL,
  valid_from_seqno BIGINT NOT NULL,
  valid_to_seqno BIGINT DEFAULT NULL,   -- NULL = 当前有效
  UNIQUE KEY uk_node (connection_id, db_name, table_name, column_name, valid_from_seqno),
  INDEX idx_lookup (db_name, table_name, column_name)
);
CREATE TABLE lineage_edge (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  src_node_id BIGINT NOT NULL,
  dst_node_id BIGINT NOT NULL,
  operator VARCHAR(32) NOT NULL,
  operator_detail JSON DEFAULT NULL,
  workflow_id VARCHAR(36) NOT NULL,
  INDEX idx_src (src_node_id), INDEX idx_dst (dst_node_id),
  INDEX idx_workflow (workflow_id)
);
```

### 2.3 采集时机

**任务配置落库时同步生成，而不是运行期采集。** 理由：
血缘描述的是"数据会怎么流"，那是配置决定的；运行期采集只会把同一件事算很多遍，
还得处理"任务没跑过就没有血缘"的空洞。

增量 DDL 改变结构时（新增列、改名）由 `schema-timeline` 的版本推进触发一次增量更新——
这条路径已经有了，接上即可。

### 2.4 查询

- `GET /api/lineage/column?db=&table=&column=&direction=upstream|downstream&depth=`
- 影响面分析：改一个源列，下游哪些表/列会受影响（**这是运维最常用的那个问题**）
- 前端：以选中列为中心的有向图，边上标算子

## 3. 同步链路脱敏

订阅侧已有脱敏，同步侧没有。而"同步到测试环境要脱敏"是比订阅更常见的诉求。

**复用列处理那套管道**，不新起一条：列处理已经支持"附加列 / 过滤 / 类型转换"，
脱敏只是多一类算子。

| 规则 | 语义 |
|---|---|
| `MASK_ALL` | 整值替换为固定串 |
| `MASK_PARTIAL(keepPrefix,keepSuffix)` | 手机号/身份证式保留首尾 |
| `HASH(salt)` | 保留可连接性（同值同结果），不可逆 |
| `NULLIFY` | 置空 |
| `FAKE(type)` | 生成同型假数据（姓名/邮箱/地址） |

改动点：`ColumnProcessor` 加算子；全量与增量共用（**必须共用**——只在增量脱敏，
全量把原值搬过去了，等于没脱）。

判据要点：全量与增量两条路径的脱敏结果**必须一致**，否则同一行在不同阶段长得不一样。

## 4. 数据分级与 Schema 演进审批

这两块是流程而非技术，放在血缘之后：

- **分级**：给 `(db,table,column)` 打标签（`PUBLIC / INTERNAL / SENSITIVE / RESTRICTED`），
  可手工也可按规则（列名正则）。价值在于**与血缘联动**——
  "标了 RESTRICTED 的列流到了哪些目标"，以及"目标端的级别不能低于源端"这条策略校验。
- **演进审批**：把 DDL 的 `MANUAL` 档从"停下来等人"升级成"停下来 + 建审批单 + 通过后继续"。
  审批单落 `workflow` 表那套状态机即可，不需要新引擎。

## 5. 分阶段与理由

| 阶段 | 内容 | 为什么这个顺序 |
|---|---|---|
| 1 | 血缘模型 + 从任务配置生成 + 查询 API | 元数据已在手上，投入产出比最高；且后面几块都要靠它 |
| 2 | 血缘前端（影响面分析） | 让阶段 1 变成能用的东西 |
| 3 | 同步链路脱敏 | 独立价值，复用列处理管道 |
| 4 | 分级标签 + 与血缘联动的策略校验 | 依赖 1 |
| 5 | Schema 演进审批 | 依赖 4（"改动涉及 RESTRICTED 列必须审批"） |

**建议只承诺阶段 1~3。** 4~5 是流程性功能，形态高度依赖具体客户的合规要求，
先做出来大概率要返工。

## 6. 判据设计

- `test_scripts/lineage/lineage_e2e.py`
  - 建一个带**列处理 + 库表名映射 + 分片路由**的任务，断言生成的边覆盖每一列且算子正确
  - 增量期做一次 `RENAME COLUMN`，断言旧节点 `valid_to_seqno` 被闭合、新节点被开出
  - 影响面查询：改源列 → 断言返回的下游列集合与实际同步结果一致
- `test_scripts/masking/masking_e2e.py`
  - **全量与增量的脱敏结果必须逐行一致**（同一行经两条路径产出相同的值）
  - `HASH` 规则的可连接性：同源值在不同表产出相同结果

## 7. 与现有能力的冲突点（实施前须确认）

1. **脱敏与数据校验互斥**：目标端被脱敏后，内容对比必然报差异。
   要么校验感知脱敏规则、要么对脱敏列跳过校验——**必须先定这个语义**，
   否则用户会看到一堆"差异"而不知道是设计如此。
2. **脱敏与双向灾备互斥**：反向链路会把脱敏后的值写回源端。应当直接禁止组合。
3. **血缘与库级同步**：库级同步会自动纳入新表，血缘得跟着长——
   采集点要挂在"新表被纳入"那个事件上，而不是只在建任务时算一次。
