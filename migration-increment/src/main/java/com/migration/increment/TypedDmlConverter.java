package com.migration.increment;

import com.migration.thl.THLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 类型化值管道的 DML 生成器：基于 extractor 下发的 {@code rows_typed}/{@code rows_before_typed}
 * 类型化值，生成 <b>PreparedStatement 参数化 SQL</b>（? 占位 + 参数列表）。
 *
 * <p>取代文本管道（capture 序列化成字符串 → extractor 解析 tuple 字符串 → 拼 SQL 字面量）—
 * 本次会话修复的 #2/#3（MySQL bool/bit 字面量）、#5（Oracle 字符串引号）、#6（Oracle 日期 NLS 格式/空 CLOB）
 * 均源于这条文本管道某一环丢失类型/格式信息；参数绑定从机制上消除整类问题，无需逐个补丁。
 *
 * <p>启用矩阵（目标端 quote()/upsert 逻辑只按目标库类型分支、与源库类型无关，
 * 故同源同目标与异源同目标可复用同一套已验证正确的目标端处理）：
 * mysql→postgresql、oracle→postgresql、postgresql→mysql、mysql→mysql、postgresql→postgresql。
 * 尚未覆盖：任何以 oracle 为目标的链路，以及 mysql↔oracle 直连（源 oracle 的大写标识符
 * 落到 mysql 目标时需要专门的大小写折叠规则，且 Oracle 目标的幂等语义需要 MERGE 而非
 * ON DUPLICATE/ON CONFLICT，两者都还没有被验证过——即使在旧文本管道里也没有针对性处理，
 * 属于单独的设计工作，不在本次"根治文本拼接"范围内）；这些组合继续走文本路径。
 *
 * <p>任何条件不满足（旧 THL 文件、缺元数据、行列不齐、不支持的库对）返回 null，
 * 调用方回退文本路径——行为零风险；值经参数绑定进入目标库，
 * 引号/转义/字面量格式一类问题从机制上不存在。
 *
 * <p>幂等语义与文本路径一致：PG 目标 INSERT 带 ON CONFLICT (pk) DO NOTHING，
 * MySQL 目标 INSERT 带 ON DUPLICATE KEY UPDATE；UPDATE/DELETE 按主键
 * （缺主键时按整行前镜像）定位。标识符：PG 目标统一小写双引号（兼容 Oracle 大写源，
 * 对已是小写的同构 PG 源为幂等操作），MySQL 目标反引号保留大小写 + 目标库名限定。
 */
public class TypedDmlConverter {

    private static final Logger logger = LoggerFactory.getLogger(TypedDmlConverter.class);

    private final boolean enabled;
    private final boolean targetIsMysql;
    private final boolean guardWithBeforeImage;
    private final boolean sourceIsMysql;
    private final String targetDatabaseName;
    /** 表名映射（仅表级同步下发）："源库.源表" → 目标表名，来自 schema.mapping.table.* */
    private final Map<String, String> tableNameMapping = new java.util.HashMap<>();
    /** 小写回退索引：适配 MySQL 源 lower_case_table_names 不区分大小写（精确命中优先） */
    private final Map<String, String> tableNameMappingLower = new java.util.HashMap<>();
    /** 库名映射（schema.mapping.db.*）：源库 → 目标库。多库任务每个事件按自己的源库路由目标库 */
    private final Map<String, String> databaseMapping = new java.util.HashMap<>();
    private final Map<String, String> databaseMappingLower = new java.util.HashMap<>();
    /** 列处理（仅表级同步下发、mysql→mysql）：行过滤 + 列名映射；附加列由建表 DEFAULT 承载，DML 无需注值 */
    private final com.migration.config.ColumnProcessingConfig columnProcessing;
    /** 列处理是否生效（有配置且源/目标均为 MySQL） */
    private final boolean columnProcessingActive;
    /** 聚合路由：汇聚改写到合并表并带来源标识列；拆分按行路由到分片表 */
    private final com.migration.common.route.TableRouter router;
    private final boolean mergeActive;
    private final boolean splitActive;
    /** 本条管线的来源实例标识（route.node.id）：跨实例汇聚时用它区分同名库表 */
    private final String routeNodeId;
    /** 汇聚上下文缓存："源库.源表" → 落点（每事件都要用，别重复解析规则） */
    private final Map<String, MergeCtx> mergeCtxCache = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * 一张源表的汇聚落点。{@code tagValues} 是<b>有序</b>的来源标识列 → 值，
     * INSERT 按此序补在列尾，UPDATE/DELETE 的 WHERE 按此序追加条件。
     */
    private static final class MergeCtx {
        final String targetDb;
        final String targetTable;
        final java.util.LinkedHashMap<String, String> tagValues;
        final boolean compositePk;

        MergeCtx(String targetDb, String targetTable,
                 java.util.LinkedHashMap<String, String> tagValues, boolean compositePk) {
            this.targetDb = targetDb;
            this.targetTable = targetTable;
            this.tagValues = tagValues;
            this.compositePk = compositePk;
        }
    }

    public TypedDmlConverter(Properties props) {
        String source = props.getProperty("source.db.type", "mysql").toLowerCase();
        String target = props.getProperty("target.db.type", "mysql").toLowerCase();
        boolean switchOn = Boolean.parseBoolean(props.getProperty("increment.typed.pipeline.enabled", "true"));
        boolean pairSupported =
                ("mysql".equals(source) && "postgresql".equals(target))
                        || ("oracle".equals(source) && "postgresql".equals(target))
                        || ("postgresql".equals(source) && "mysql".equals(target))
                        || ("mysql".equals(source) && "mysql".equals(target))
                        || ("postgresql".equals(source) && "postgresql".equals(target));
        this.enabled = switchOn && pairSupported;
        this.targetIsMysql = "mysql".equals(target);
        // 双向冲突消解：UPDATE 的 WHERE 额外带上<b>整行前镜像</b>，"影响 0 行"即说明
        // 目标端这一行已被本端改过（并发写），交由 ConflictResolver 裁决。
        this.guardWithBeforeImage = Boolean.parseBoolean(
                props.getProperty("sync.bidi.conflict.before.image.guard", "false"));
        this.sourceIsMysql = "mysql".equals(source);
        this.targetDatabaseName = props.getProperty("target.db.database", "");

        // 表名映射：schema.mapping.table.<源库>.<源表>=<目标库>.<目标表>，DML 只需要表名部分
        String tableMappingPrefix = "schema.mapping.table.";
        // 库名映射：schema.mapping.db.<源库>=<目标库>（未映射的库按源库名原样路由）
        String dbMappingPrefix = "schema.mapping.db.";
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(tableMappingPrefix)) {
                String key = name.substring(tableMappingPrefix.length());
                String value = props.getProperty(name, "");
                String targetTable = value.contains(".") ? value.substring(value.indexOf('.') + 1) : value;
                if (!key.isEmpty() && !targetTable.isEmpty()) {
                    tableNameMapping.put(key, targetTable);
                    tableNameMappingLower.put(key.toLowerCase(), targetTable);
                }
            } else if (name.startsWith(dbMappingPrefix)) {
                String srcDb = name.substring(dbMappingPrefix.length());
                String tgtDb = props.getProperty(name, "");
                if (!srcDb.isEmpty() && !tgtDb.isEmpty()) {
                    databaseMapping.put(srcDb, tgtDb);
                    databaseMappingLower.put(srcDb.toLowerCase(), tgtDb);
                }
            }
        }
        this.columnProcessing = com.migration.config.ColumnProcessingConfig.loadFromProperties(props);
        // 列处理（行过滤/列名映射）在同引擎链路生效：mysql→mysql / pg→pg。类型化管道对两者
        // 均携带 rows_typed，rowExcluded/mapColumns 与库类型无关。
        this.columnProcessingActive = !columnProcessing.isEmpty()
                && (("mysql".equals(source) && "mysql".equals(target))
                    || ("postgresql".equals(source) && "postgresql".equals(target)));

        // 聚合路由：汇聚下 DML 要改写到合并目标表并带来源标识列。放在最后初始化——
        // 默认目标库解析复用本类既有的 per-db 映射（mapTargetDatabase），依赖上面那些字段。
        com.migration.common.route.RoutingConfig routing =
                com.migration.common.route.RoutingConfig.loadFromProperties(props);
        this.router = routing.router(this::mapTargetDatabase);
        this.mergeActive = routing.getMode() == com.migration.common.route.RoutingConfig.Mode.MERGE;
        this.splitActive = routing.getMode() == com.migration.common.route.RoutingConfig.Mode.SPLIT;
        this.routeNodeId = props.getProperty("route.node.id", "");
        // 跨实例拆分的增量写入要跨连接，事务与位点都要另做一套（低水位 checkpoint），
        // 首版不支持——与其让它半通不通地跑，不如在启动时就说清楚
        if (splitActive) {
            for (com.migration.common.route.SplitRule rule : routing.getSplitRules()) {
                if (rule.getNodeGroup() != null) {
                    throw new IllegalStateException("拆分规则 " + rule.getId()
                            + " 配了跨实例目标组，增量的跨实例写入尚未实现（单实例分库分表可用）");
                }
            }
        }

        logger.info("TypedDmlConverter enabled={} (source={}, target={}, switch={}, tableMappings={}, columnProcessing={}, merge={})",
                enabled, source, target, switchOn, tableNameMapping.size(), columnProcessingActive, mergeActive);
    }

    /**
     * 该事件是否必须走类型化管道。汇聚表的 DML 一旦回退文本路径就会<b>写错行</b>——
     * 文本路径不带来源标识列，UPDATE/DELETE 的 WHERE 只有源主键，会命中同一张汇聚表里
     * 其它来源的同主键行。调用方据此 fail-stop，而不是让它悄悄改坏别的来源的数据。
     */
    public boolean requiresTypedPipeline(THLEvent event) {
        if ((!mergeActive && !splitActive) || event == null || event.getMetadata() == null) {
            return false;
        }
        Map<String, Object> metadata = event.getMetadata();
        String eventType = (String) metadata.get("event_type");
        if (eventType == null || !isRowEvent(eventType)) {
            return false;
        }
        String srcDb = (String) metadata.getOrDefault("database_name", "");
        String srcTable = (String) metadata.getOrDefault("table_name", "");
        return !srcTable.isEmpty() && router.matches(srcDb, srcTable);
    }

    private static boolean isRowEvent(String eventType) {
        switch (eventType) {
            case "INSERT": case "WRITE_ROWS": case "EXT_WRITE_ROWS":
            case "UPDATE": case "UPDATE_ROWS": case "EXT_UPDATE_ROWS":
            case "DELETE": case "DELETE_ROWS": case "EXT_DELETE_ROWS":
                return true;
            default:
                return false;
        }
    }

    /** 该源表的汇聚落点；未命中规则返回 null（走原 1:1 路径）。 */
    private MergeCtx mergeCtxOf(String srcDb, String srcTable) {
        if (!mergeActive || srcTable == null || srcTable.isEmpty()) {
            return null;
        }
        String key = srcDb + "." + srcTable;
        MergeCtx cached = mergeCtxCache.get(key);
        if (cached != null) {
            return cached;
        }
        if (!router.matches(srcDb, srcTable)) {
            return null;
        }
        com.migration.common.route.RouteTarget target = router.allTargets(srcDb, srcTable).get(0);
        com.migration.common.route.MergeRule rule =
                ((com.migration.common.route.MergeRouter) router).find(srcDb, srcTable);
        java.util.LinkedHashMap<String, String> tags = new java.util.LinkedHashMap<>();
        for (String col : rule.getTagColumns()) {
            String value = rule.tagValue(col, routeNodeId, srcDb, srcTable);
            if (value != null) {
                tags.put(col, value);
            }
        }
        MergeCtx ctx = new MergeCtx(target.getDatabase(), target.getTable(), tags,
                rule.getPkStrategy() == com.migration.common.route.MergeRule.PkStrategy.COMPOSITE_SOURCE);
        mergeCtxCache.put(key, ctx);
        logger.info("汇聚路由（增量）: {}.{} -> {}.{}，来源标识 {}",
                srcDb, srcTable, ctx.targetDb, ctx.targetTable, tags);
        return ctx;
    }

    /** 该源表是否走拆分路由。 */
    private boolean isSplit(String srcDb, String srcTable) {
        return splitActive && router.matches(srcDb, srcTable);
    }

    /** 分片落点的目标表引用（MySQL 带库名限定——同实例跨库写限定名即可；PG 只用表名）。 */
    private String splitRef(com.migration.common.route.RouteTarget target) {
        if (!targetIsMysql || target.getDatabase() == null || target.getDatabase().isEmpty()) {
            return quote(target.getTable());
        }
        return quote(target.getDatabase()) + "." + quote(target.getTable());
    }

    /** 列名数组里找某一列的下标（大小写不敏感）；找不到返回 -1。 */
    private int indexOfColumn(String[] cols, String name) {
        if (cols == null || name == null) {
            return -1;
        }
        for (int i = 0; i < cols.length; i++) {
            if (cols[i] != null && cols[i].trim().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 按行算落点。分片键取不到值（列不在事件里 / 值为 NULL）时由规则的 unrouted 策略决定：
     * 广播到全部分片、返回空列表（调用方记死信）、或抛异常。
     */
    private List<com.migration.common.route.RouteTarget> routeRow(String srcDb, String srcTable,
                                                                  int shardKeyIndex, List<Object> row) {
        Object value = (shardKeyIndex >= 0 && row != null && shardKeyIndex < row.size())
                ? row.get(shardKeyIndex) : null;
        return router.route(srcDb, srcTable, value);
    }

    /** 两组落点是否完全相同（判断 UPDATE 是否跨分片）。 */
    private boolean sameTargets(List<com.migration.common.route.RouteTarget> a,
                                List<com.migration.common.route.RouteTarget> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).key().equals(b.get(i).key())) {
                return false;
            }
        }
        return true;
    }

    /**
     * 追加来源标识列的 WHERE 条件。<b>UPDATE/DELETE 必须调用</b>：汇聚表里源主键不唯一，
     * 只按源主键定位会命中其它来源的同主键行——改错、删错，且不会报任何错。
     */
    private void appendMergeTagWhere(StringBuilder sql, List<Object> params, MergeCtx ctx) {
        if (ctx == null) {
            return;
        }
        for (Map.Entry<String, String> tag : ctx.tagValues.entrySet()) {
            sql.append(" AND ").append(quote(tag.getKey())).append("=?");
            params.add(tag.getValue());
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * 尝试把事件转换为参数化 DML 列表；不适用时返回 null（回退文本路径）。
     */
    public List<ParameterizedDml> convert(THLEvent event) {
        if (!enabled) {
            return null;
        }
        Map<String, Object> metadata = event.getMetadata();
        String eventType = (String) metadata.get("event_type");
        if (eventType == null) {
            return null;
        }

        List<ArrayList<Object>> rowsTyped = castRows(metadata.get("rows_typed"));
        if (rowsTyped == null || rowsTyped.isEmpty()) {
            return null;
        }
        String srcTable = (String) metadata.getOrDefault("table_name", "");
        if (srcTable.isEmpty()) {
            return null;
        }
        String srcDb = (String) metadata.getOrDefault("database_name", "");
        String table = srcTable;
        // 表名映射：key 用源库名（metadata 的 database_name 是源库），仅表级同步配置。
        // 精确命中优先，小写回退（适配 MySQL 源 lower_case_table_names 不区分大小写）。
        if (!tableNameMapping.isEmpty()) {
            String key = srcDb + "." + srcTable;
            String mapped = tableNameMapping.get(key);
            if (mapped == null) {
                mapped = tableNameMappingLower.get(key.toLowerCase());
            }
            if (mapped != null) {
                table = mapped;
            }
        }
        // 汇聚：命中规则的源表一律落到合并后的目标表（优先级高于表名映射——
        // 两者不会同时配置，路由规则是更具体的那个）
        MergeCtx mergeCtx = mergeCtxOf(srcDb, srcTable);
        if (mergeCtx != null) {
            table = mergeCtx.targetTable;
        }

        switch (eventType) {
            case "INSERT":
            case "WRITE_ROWS":
            case "EXT_WRITE_ROWS":
                return convertInsert(metadata, srcDb, srcTable, table, rowsTyped);
            case "UPDATE":
            case "UPDATE_ROWS":
            case "EXT_UPDATE_ROWS":
                List<ArrayList<Object>> beforeTyped = castRows(metadata.get("rows_before_typed"));
                if (beforeTyped == null) {
                    // 无前镜像（如 PG REPLICA IDENTITY DEFAULT 只带 PK，不带完整 old-tuple）：
                    // 退化为用 after 值做 WHERE，与文本路径 THLToSqlConverter 的兜底行为一致
                    // （要求以主键定位，PK 值不随本次 UPDATE 变化——绝大多数场景成立）。
                    beforeTyped = rowsTyped;
                } else if (beforeTyped.size() != rowsTyped.size()) {
                    return null;
                }
                return convertUpdate(metadata, srcDb, srcTable, table, rowsTyped, beforeTyped);
            case "DELETE":
            case "DELETE_ROWS":
            case "EXT_DELETE_ROWS":
                return convertDelete(metadata, srcDb, srcTable, table, rowsTyped);
            default:
                return null; // DDL/QUERY/心跳等走文本路径
        }
    }

    /** 列过滤是否将该行排除（列处理未生效时恒 false）。 */
    private boolean rowExcluded(String srcDb, String srcTable, String[] cols, List<Object> values) {
        return columnProcessingActive && columnProcessing.rowExcluded(srcDb, srcTable, cols, values);
    }

    /** 列名映射：生成 SQL 用的目标列名数组（无映射时返回原数组）。 */
    private String[] mapColumns(String srcDb, String srcTable, String[] cols) {
        if (!columnProcessingActive || cols == null) {
            return cols;
        }
        Map<String, String> mapping = columnProcessing.getColumnMapping(srcDb, srcTable);
        if (mapping.isEmpty()) {
            return cols;
        }
        String[] mapped = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            mapped[i] = columnProcessing.mapColumn(srcDb, srcTable, cols[i].trim());
        }
        return mapped;
    }

    @SuppressWarnings("unchecked")
    private List<ArrayList<Object>> castRows(Object o) {
        return (o instanceof List) ? (List<ArrayList<Object>>) o : null;
    }

    private String[] columns(Map<String, Object> metadata, String preferredKey) {
        String preferred = preferredKey != null ? (String) metadata.get(preferredKey) : null;
        String s = (preferred != null && !preferred.isEmpty()) ? preferred : (String) metadata.get("column_names");
        if (s == null || s.isEmpty()) {
            return null;
        }
        return s.split("\\s*,\\s*");
    }

    /** 目标端标识符：PG 统一小写双引号（与全量建表一致）；MySQL 反引号保留大小写。 */
    private String quote(String identifier) {
        if (targetIsMysql) {
            return "`" + identifier + "`";
        }
        return "\"" + identifier.toLowerCase() + "\"";
    }

    /** 目标表引用：MySQL 目标带库名限定（按事件源库做 per-db 解析），PG 目标裸表名（schema 由连接串决定）。 */
    private String tableRef(Map<String, Object> metadata, String table) {
        return tableRef(metadata, table, null);
    }

    /**
     * 目标表引用。汇聚落点直接用规则里的目标库（不再按源库解析），
     * PG 目标仍是裸表名——PG 一条连接跨不了库，汇聚目标库必须等于任务目标库。
     */
    private String tableRef(Map<String, Object> metadata, String table, MergeCtx ctx) {
        if (!targetIsMysql) {
            return quote(table);
        }
        if (ctx != null) {
            return ctx.targetDb == null || ctx.targetDb.isEmpty()
                    ? quote(table) : quote(ctx.targetDb) + "." + quote(table);
        }
        String srcDb = (String) metadata.getOrDefault("database_name", "");
        String db = mapTargetDatabase(srcDb);
        return db.isEmpty() ? quote(table) : quote(db) + "." + quote(table);
    }

    /**
     * 目标库解析（仅 mysql 目标调用）：
     * <ul>
     *   <li>mysql→mysql（同名字空间）：库名映射命中用映射值，未命中保留事件源库名——
     *       多库任务各库独立路由，修复此前用单一 target.db.database 覆盖一切导致的多库串写；
     *       单库改名场景由 ConfigService 保证 schema.mapping.db.* 必然写入，映射照常命中。</li>
     *   <li>异构源（pg/oracle→mysql）：事件的 database_name 是源端 schema（如 pg 的 "public"），
     *       不是可路由的库名，保持旧行为整体落到 target.db.database。</li>
     * </ul>
     * 事件缺源库名时回退 target.db.database 兜底。精确命中优先，小写回退。
     */
    private String mapTargetDatabase(String srcDb) {
        String fallback = targetDatabaseName != null ? targetDatabaseName : "";
        if (srcDb == null || srcDb.isEmpty()) {
            return fallback;
        }
        if (!sourceIsMysql) {
            return fallback.isEmpty() ? srcDb : fallback;
        }
        String mapped = databaseMapping.get(srcDb);
        if (mapped == null) {
            mapped = databaseMappingLower.get(srcDb.toLowerCase());
        }
        return mapped != null ? mapped : srcDb;
    }

    private List<ParameterizedDml> convertInsert(Map<String, Object> metadata, String srcDb, String srcTable,
                                                 String table, List<ArrayList<Object>> rows) {
        String[] cols = columns(metadata, "insert_column_names");
        if (cols == null) {
            return null;
        }
        if (isSplit(srcDb, srcTable)) {
            return convertInsertSplit(metadata, srcDb, srcTable, cols, rows);
        }
        MergeCtx ctx = mergeCtxOf(srcDb, srcTable);
        String insertSql = buildInsertSql(metadata, srcDb, srcTable, table, cols);

        List<ParameterizedDml> out = new ArrayList<>();
        for (ArrayList<Object> row : rows) {
            if (row.size() != cols.length) {
                return null; // 列/值数量不齐：整事件回退
            }
            // 列过滤：命中条件的行不同步（返回空列表 = 事件已处理、无 DML，不回退文本路径）
            if (rowExcluded(srcDb, srcTable, cols, row)) {
                logger.debug("列过滤跳过 INSERT 行: {}.{}", srcDb, srcTable);
                continue;
            }
            List<Object> params = row;
            if (ctx != null) {
                params = new ArrayList<>(row);
                params.addAll(ctx.tagValues.values());
            }
            out.add(new ParameterizedDml(insertSql, params, table,
                    mergeRowKey(rowKeyOf(cols, row, pkSet(metadata)), ctx), "INSERT"));
        }
        return out;
    }

    /** 拆分 INSERT：每行按分片键路由到对应分片表。 */
    private List<ParameterizedDml> convertInsertSplit(Map<String, Object> metadata, String srcDb,
                                                      String srcTable, String[] cols,
                                                      List<ArrayList<Object>> rows) {
        String shardKey = router.shardKeyColumn(srcDb, srcTable);
        int keyIdx = indexOfColumn(cols, shardKey);
        String[] sqlCols = mapColumns(srcDb, srcTable, cols);
        java.util.Set<String> pks = pkSet(metadata);

        List<ParameterizedDml> out = new ArrayList<>();
        for (ArrayList<Object> row : rows) {
            if (row.size() != cols.length) {
                return null;
            }
            if (rowExcluded(srcDb, srcTable, cols, row)) {
                continue;
            }
            List<com.migration.common.route.RouteTarget> targets = routeRow(srcDb, srcTable, keyIdx, row);
            if (targets.isEmpty()) {
                logger.warn("表 {}.{} 的 INSERT 行算不出分片（分片键 {}），按死信策略未应用",
                        srcDb, srcTable, shardKey);
                continue;
            }
            for (com.migration.common.route.RouteTarget target : targets) {
                out.add(new ParameterizedDml(
                        buildInsertSqlForRef(metadata, splitRef(target), sqlCols), row,
                        target.getTable(), rowKeyOf(cols, row, pks), "INSERT"));
            }
        }
        return out;
    }

    /** 拆分 DELETE：按<b>前镜像</b>的分片键定位（DELETE 事件只有前镜像）。 */
    private List<ParameterizedDml> convertDeleteSplit(Map<String, Object> metadata, String srcDb,
                                                      String srcTable, String[] cols,
                                                      List<ArrayList<Object>> rows) {
        String shardKey = router.shardKeyColumn(srcDb, srcTable);
        int keyIdx = indexOfColumn(cols, shardKey);
        String[] sqlCols = mapColumns(srcDb, srcTable, cols);
        java.util.Set<String> pks = pkSet(metadata);

        List<ParameterizedDml> out = new ArrayList<>();
        for (ArrayList<Object> row : rows) {
            if (row.size() != cols.length) {
                return null;
            }
            if (rowExcluded(srcDb, srcTable, cols, row)) {
                continue;
            }
            List<com.migration.common.route.RouteTarget> targets = routeRow(srcDb, srcTable, keyIdx, row);
            if (targets.isEmpty()) {
                logger.warn("表 {}.{} 的 DELETE 行算不出分片，按死信策略未应用", srcDb, srcTable);
                continue;
            }
            for (com.migration.common.route.RouteTarget target : targets) {
                StringBuilder sql = new StringBuilder("DELETE FROM ").append(splitRef(target));
                List<Object> params = new ArrayList<>();
                if (!appendWhere(sql, params, cols, sqlCols, row, pks)) {
                    return null;
                }
                out.add(new ParameterizedDml(sql.toString(), params, target.getTable(),
                        rowKeyOf(cols, row, pks), "DELETE"));
            }
        }
        return out;
    }

    /**
     * 拆分 UPDATE：前后镜像各自算分片。
     *
     * <p>分片键<b>被改掉</b>时这一行要跨分片搬迁——旧分片 DELETE（按前镜像定位）+
     * 新分片 INSERT（后镜像整行）。只发 UPDATE 的话，旧分片留着一条陈行、新分片一条都没有，
     * 而且两边都不报错。
     */
    private List<ParameterizedDml> convertUpdateSplit(Map<String, Object> metadata, String srcDb,
                                                      String srcTable, String[] setCols, String[] whereCols,
                                                      List<ArrayList<Object>> afterRows,
                                                      List<ArrayList<Object>> beforeRows) {
        String shardKey = router.shardKeyColumn(srcDb, srcTable);
        int afterIdx = indexOfColumn(setCols, shardKey);
        int beforeIdx = indexOfColumn(whereCols, shardKey);
        String[] sqlSetCols = mapColumns(srcDb, srcTable, setCols);
        String[] sqlWhereCols = mapColumns(srcDb, srcTable, whereCols);
        java.util.Set<String> pks = pkSet(metadata);

        List<ParameterizedDml> out = new ArrayList<>();
        for (int r = 0; r < afterRows.size(); r++) {
            ArrayList<Object> after = afterRows.get(r);
            ArrayList<Object> before = beforeRows.get(r);
            if (after.size() != setCols.length || before.size() != whereCols.length) {
                return null;
            }
            List<com.migration.common.route.RouteTarget> oldTargets =
                    routeRow(srcDb, srcTable, beforeIdx, before);
            List<com.migration.common.route.RouteTarget> newTargets =
                    routeRow(srcDb, srcTable, afterIdx, after);
            if (oldTargets.isEmpty() && newTargets.isEmpty()) {
                logger.warn("表 {}.{} 的 UPDATE 行前后镜像都算不出分片，按死信策略未应用", srcDb, srcTable);
                continue;
            }

            if (!oldTargets.isEmpty() && sameTargets(oldTargets, newTargets)) {
                for (com.migration.common.route.RouteTarget target : newTargets) {
                    StringBuilder sql = new StringBuilder("UPDATE ").append(splitRef(target)).append(" SET ");
                    List<Object> params = new ArrayList<>();
                    for (int i = 0; i < sqlSetCols.length; i++) {
                        if (i > 0) sql.append(", ");
                        sql.append(quote(sqlSetCols[i])).append("=?");
                        params.add(after.get(i));
                    }
                    if (!appendWhere(sql, params, whereCols, sqlWhereCols, before, pks)) {
                        return null;
                    }
                    out.add(new ParameterizedDml(sql.toString(), params, target.getTable(),
                            rowKeyOf(whereCols, before, pks), "UPDATE"));
                }
                continue;
            }

            // 跨分片搬迁：先把旧分片那一行删掉，再往新分片插整行
            logger.info("表 {}.{} 分片键变更，跨分片搬迁: {} -> {}", srcDb, srcTable, oldTargets, newTargets);
            for (com.migration.common.route.RouteTarget target : oldTargets) {
                StringBuilder del = new StringBuilder("DELETE FROM ").append(splitRef(target));
                List<Object> params = new ArrayList<>();
                if (!appendWhere(del, params, whereCols, sqlWhereCols, before, pks)) {
                    return null;
                }
                out.add(new ParameterizedDml(del.toString(), params, target.getTable(),
                        rowKeyOf(whereCols, before, pks), "DELETE"));
            }
            for (com.migration.common.route.RouteTarget target : newTargets) {
                out.add(new ParameterizedDml(
                        buildInsertSqlForRef(metadata, splitRef(target), sqlSetCols), after,
                        target.getTable(), rowKeyOf(setCols, after, pks), "INSERT"));
            }
        }
        return out;
    }

    /**
     * 汇聚下的行标识：拼上来源标识，否则双向冲突消解会把不同来源的同主键行当成同一行。
     * 未汇聚时原样返回。
     */
    private String mergeRowKey(String rowKey, MergeCtx ctx) {
        if (ctx == null || rowKey == null) {
            return rowKey;
        }
        StringBuilder key = new StringBuilder(rowKey);
        for (String v : ctx.tagValues.values()) {
            key.append('').append(v);
        }
        return key.toString();
    }

    /** 生成 INSERT（含幂等子句）SQL：列名经列名映射改写；供 INSERT 与 UPDATE 升级插入共用。 */
    private String buildInsertSql(Map<String, Object> metadata, String srcDb, String srcTable,
                                  String table, String[] cols) {
        MergeCtx ctx = mergeCtxOf(srcDb, srcTable);
        String[] sqlCols = mapColumns(srcDb, srcTable, cols);
        if (ctx != null) {
            // 汇聚：来源标识列补在列尾，顺序与 convertInsert 的补值顺序一致
            String[] withTags = java.util.Arrays.copyOf(sqlCols, sqlCols.length + ctx.tagValues.size());
            int i = sqlCols.length;
            for (String tag : ctx.tagValues.keySet()) {
                withTags[i++] = tag;
            }
            sqlCols = withTags;
        }
        return buildInsertSqlForRef(metadata, tableRef(metadata, table, ctx), sqlCols, ctx);
    }

    /** 目标表引用已定时的 INSERT 生成（拆分按行给出分片表引用；汇聚/1:1 由上面那个入口算出引用）。 */
    private String buildInsertSqlForRef(Map<String, Object> metadata, String ref, String[] sqlCols) {
        return buildInsertSqlForRef(metadata, ref, sqlCols, null);
    }

    private String buildInsertSqlForRef(Map<String, Object> metadata, String ref,
                                        String[] sqlCols, MergeCtx ctx) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(ref).append(" (");
        StringBuilder ph = new StringBuilder();
        for (int i = 0; i < sqlCols.length; i++) {
            if (i > 0) { sql.append(", "); ph.append(", "); }
            sql.append(quote(sqlCols[i]));
            ph.append("?");
        }
        sql.append(") VALUES (").append(ph).append(")");

        if (targetIsMysql) {
            // 与文本路径一致的幂等语义：重复主键按新值覆盖
            sql.append(" ON DUPLICATE KEY UPDATE ");
            for (int i = 0; i < sqlCols.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(quote(sqlCols[i])).append(" = VALUES(").append(quote(sqlCols[i])).append(")");
            }
        } else {
            String pkStr = (String) metadata.get("primary_keys");
            if (pkStr != null && !pkStr.isEmpty()) {
                sql.append(" ON CONFLICT (");
                String[] pks = pkStr.split("\\s*,\\s*");
                for (int i = 0; i < pks.length; i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(quote(pks[i]));
                }
                // 汇聚且复合主键：冲突目标必须带上来源标识列，否则不同来源的同主键行
                // 会被判成冲突而 DO NOTHING 掉（这一行就此丢失）
                if (ctx != null && ctx.compositePk) {
                    for (String tag : ctx.tagValues.keySet()) {
                        sql.append(", ").append(quote(tag));
                    }
                }
                sql.append(") DO NOTHING");
            }
        }
        return sql.toString();
    }

    private List<ParameterizedDml> convertUpdate(Map<String, Object> metadata, String srcDb, String srcTable,
                                                 String table,
                                                 List<ArrayList<Object>> afterRows,
                                                 List<ArrayList<Object>> beforeRows) {
        String[] setCols = columns(metadata, "update_column_names");
        String[] whereCols = columns(metadata, "update_before_column_names");
        if (setCols == null || whereCols == null) {
            return null;
        }
        if (isSplit(srcDb, srcTable)) {
            return convertUpdateSplit(metadata, srcDb, srcTable, setCols, whereCols, afterRows, beforeRows);
        }
        java.util.Set<String> pks = pkSet(metadata);
        String[] sqlSetCols = mapColumns(srcDb, srcTable, setCols);
        String[] sqlWhereCols = mapColumns(srcDb, srcTable, whereCols);
        MergeCtx ctx = mergeCtxOf(srcDb, srcTable);

        List<ParameterizedDml> out = new ArrayList<>();
        for (int r = 0; r < afterRows.size(); r++) {
            ArrayList<Object> after = afterRows.get(r);
            ArrayList<Object> before = beforeRows.get(r);
            if (after.size() != setCols.length || before.size() != whereCols.length) {
                return null;
            }

            // 列过滤下的 UPDATE 语义：按前/后镜像分别判定，保证目标端与"过滤后的源"一致。
            // 后镜像命中过滤 → 该行不应再存在于目标端 → 转 DELETE；
            // 前镜像命中而后镜像未命中 → 该行此前未同步到目标端 → 升级为幂等 INSERT；
            // 两侧均命中 → 目标端本就没有该行 → 跳过。
            if (columnProcessingActive) {
                boolean beforeExcluded = rowExcluded(srcDb, srcTable, whereCols, before);
                boolean afterExcluded = rowExcluded(srcDb, srcTable, setCols, after);
                if (afterExcluded) {
                    if (!beforeExcluded) {
                        StringBuilder del = new StringBuilder("DELETE FROM ").append(tableRef(metadata, table, ctx));
                        List<Object> delParams = new ArrayList<>();
                        if (!appendWhere(del, delParams, whereCols, sqlWhereCols, before, pks)) {
                            return null;
                        }
                        appendMergeTagWhere(del, delParams, ctx);
                        out.add(new ParameterizedDml(del.toString(), delParams, table,
                                mergeRowKey(rowKeyOf(whereCols, before, pks), ctx), "DELETE"));
                        logger.debug("列过滤将 UPDATE 转为 DELETE: {}.{}", srcDb, srcTable);
                    }
                    continue;
                }
                if (beforeExcluded) {
                    String insertSql = buildInsertSql(metadata, srcDb, srcTable, table, setCols);
                    List<Object> insParams = after;
                    if (ctx != null) {
                        insParams = new ArrayList<>(after);
                        insParams.addAll(ctx.tagValues.values());
                    }
                    out.add(new ParameterizedDml(insertSql, insParams, table,
                            mergeRowKey(rowKeyOf(setCols, after, pks), ctx), "INSERT"));
                    logger.debug("列过滤将 UPDATE 转为 INSERT: {}.{}", srcDb, srcTable);
                    continue;
                }
            }

            StringBuilder sql = new StringBuilder("UPDATE ").append(tableRef(metadata, table, ctx)).append(" SET ");
            List<Object> params = new ArrayList<>(after.size() + before.size());
            for (int i = 0; i < sqlSetCols.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(quote(sqlSetCols[i])).append("=?");
                params.add(after.get(i));
            }
            // 主键定位版（冲突裁决判"来的一方赢"时用它强制覆盖）
            StringBuilder pkOnly = new StringBuilder(sql);
            List<Object> pkOnlyParams = new ArrayList<>(params);
            if (!appendWhere(pkOnly, pkOnlyParams, whereCols, sqlWhereCols, before, pks)) {
                return null;
            }
            // 汇聚：源主键在合并表里不唯一，两种 WHERE 都必须补来源标识列
            appendMergeTagWhere(pkOnly, pkOnlyParams, ctx);
            if (guardWithBeforeImage) {
                if (!appendBeforeImageWhere(sql, params, whereCols, sqlWhereCols, before, pks)) {
                    return null;
                }
                appendMergeTagWhere(sql, params, ctx);
            } else {
                sql = pkOnly;
                params = pkOnlyParams;
            }
            ParameterizedDml dml = new ParameterizedDml(sql.toString(), params, table,
                    mergeRowKey(rowKeyOf(whereCols, before, pks), ctx), "UPDATE");
            if (guardWithBeforeImage) {
                dml.withOverride(pkOnly.toString(), pkOnlyParams);
            }
            out.add(dml);
        }
        return out;
    }

    private List<ParameterizedDml> convertDelete(Map<String, Object> metadata, String srcDb, String srcTable,
                                                 String table, List<ArrayList<Object>> rows) {
        String[] cols = columns(metadata, null);
        if (cols == null) {
            return null;
        }
        if (isSplit(srcDb, srcTable)) {
            return convertDeleteSplit(metadata, srcDb, srcTable, cols, rows);
        }
        java.util.Set<String> pks = pkSet(metadata);
        String[] sqlCols = mapColumns(srcDb, srcTable, cols);
        MergeCtx ctx = mergeCtxOf(srcDb, srcTable);

        List<ParameterizedDml> out = new ArrayList<>();
        for (ArrayList<Object> row : rows) {
            if (row.size() != cols.length) {
                return null;
            }
            // 列过滤：命中条件的行本就未同步到目标端，DELETE 直接跳过
            if (rowExcluded(srcDb, srcTable, cols, row)) {
                logger.debug("列过滤跳过 DELETE 行: {}.{}", srcDb, srcTable);
                continue;
            }
            StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableRef(metadata, table, ctx));
            List<Object> params = new ArrayList<>();
            if (!appendWhere(sql, params, cols, sqlCols, row, pks)) {
                return null;
            }
            // 汇聚：不带来源标识的 DELETE 会连同其它来源的同主键行一起删掉
            appendMergeTagWhere(sql, params, ctx);
            out.add(new ParameterizedDml(sql.toString(), params, table,
                    mergeRowKey(rowKeyOf(cols, row, pks), ctx), "DELETE"));
        }
        return out;
    }

    /**
     * 行标识：主键列值按源列序拼接。双向冲突消解（P1-4）用它在旁路表里定位"同一行"，
     * 因此必须在两个方向上算出<b>同一个</b>字符串——所以用源列名顺序而不是目标列名顺序。
     * 无主键返回 null：这类表天然无法做行级冲突消解，调用方直接放行。
     */
    private String rowKeyOf(String[] cols, List<Object> values, java.util.Set<String> pks) {
        if (pks.isEmpty() || cols == null || values == null) {
            return null;
        }
        StringBuilder key = new StringBuilder();
        boolean any = false;
        for (int i = 0; i < cols.length && i < values.size(); i++) {
            if (!pks.contains(cols[i].trim().toLowerCase())) {
                continue;
            }
            if (any) key.append('\u0001');
            key.append(values.get(i));
            any = true;
        }
        return any ? key.toString() : null;
    }

    private java.util.Set<String> pkSet(Map<String, Object> metadata) {
        java.util.Set<String> pks = new java.util.HashSet<>();
        String pkStr = (String) metadata.get("primary_keys");
        if (pkStr != null && !pkStr.isEmpty()) {
            for (String pk : pkStr.split(",")) {
                pks.add(pk.trim().toLowerCase());
            }
        }
        return pks;
    }

    /**
     * 主键 + <b>整行前镜像</b>的 WHERE：双向并发写的检测手段。
     * 目标端那一行若已被本端改动过，这条 UPDATE 就会"影响 0 行"，从而被识别成冲突。
     * NULL 用 NULL 安全等价（MySQL {@code <=>} / PG {@code IS NOT DISTINCT FROM}），
     * 否则 NULL=NULL 恒为 UNKNOWN，含 NULL 的行会被误判成冲突。
     */
    private boolean appendBeforeImageWhere(StringBuilder sql, List<Object> params,
                                           String[] cols, String[] sqlCols, List<Object> values,
                                           java.util.Set<String> pks) {
        sql.append(" WHERE ");
        boolean first = true;
        String nullSafeEq = targetIsMysql ? "<=>" : "IS NOT DISTINCT FROM";
        for (int i = 0; i < cols.length; i++) {
            if (!first) sql.append(" AND ");
            first = false;
            Object v = values.get(i);
            boolean isPk = pks.contains(cols[i].trim().toLowerCase());
            if (v == null) {
                sql.append(quote(sqlCols[i])).append(" IS NULL");
            } else if (isPk) {
                sql.append(quote(sqlCols[i])).append("=?");
                params.add(v);
            } else {
                sql.append(quote(sqlCols[i])).append(' ').append(nullSafeEq).append(" ?");
                params.add(v);
            }
        }
        return !first;
    }

    /**
     * 生成 WHERE 子句：有主键时按主键列（参数绑定），无主键时按整行前镜像；
     * NULL 值输出 IS NULL。返回 false 表示无法生成有效条件（回退文本路径）。
     *
     * @param cols    源列名（用于主键匹配——metadata 的 primary_keys 是源列名）
     * @param sqlCols SQL 输出用列名（列名映射后的目标列名；无映射时与 cols 相同）
     */
    private boolean appendWhere(StringBuilder sql, List<Object> params,
                                String[] cols, String[] sqlCols, List<Object> values, java.util.Set<String> pks) {
        sql.append(" WHERE ");
        boolean first = true;
        boolean usePk = !pks.isEmpty();
        for (int i = 0; i < cols.length; i++) {
            if (usePk && !pks.contains(cols[i].trim().toLowerCase())) {
                continue;
            }
            if (!first) sql.append(" AND ");
            first = false;
            Object v = values.get(i);
            if (v == null) {
                sql.append(quote(sqlCols[i])).append(" IS NULL");
            } else {
                sql.append(quote(sqlCols[i])).append("=?");
                params.add(v);
            }
        }
        return !first; // 至少要有一个条件
    }
}
