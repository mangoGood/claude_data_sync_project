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
        this.columnProcessingActive = !columnProcessing.isEmpty()
                && "mysql".equals(source) && "mysql".equals(target);

        logger.info("TypedDmlConverter enabled={} (source={}, target={}, switch={}, tableMappings={}, columnProcessing={})",
                enabled, source, target, switchOn, tableNameMapping.size(), columnProcessingActive);
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
        if (targetIsMysql) {
            String srcDb = (String) metadata.getOrDefault("database_name", "");
            String db = mapTargetDatabase(srcDb);
            return db.isEmpty() ? quote(table) : quote(db) + "." + quote(table);
        }
        return quote(table);
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
            out.add(new ParameterizedDml(insertSql, row));
        }
        return out;
    }

    /** 生成 INSERT（含幂等子句）SQL：列名经列名映射改写；供 INSERT 与 UPDATE 升级插入共用。 */
    private String buildInsertSql(Map<String, Object> metadata, String srcDb, String srcTable,
                                  String table, String[] cols) {
        String[] sqlCols = mapColumns(srcDb, srcTable, cols);
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableRef(metadata, table)).append(" (");
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
        java.util.Set<String> pks = pkSet(metadata);
        String[] sqlSetCols = mapColumns(srcDb, srcTable, setCols);
        String[] sqlWhereCols = mapColumns(srcDb, srcTable, whereCols);

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
                        StringBuilder del = new StringBuilder("DELETE FROM ").append(tableRef(metadata, table));
                        List<Object> delParams = new ArrayList<>();
                        if (!appendWhere(del, delParams, whereCols, sqlWhereCols, before, pks)) {
                            return null;
                        }
                        out.add(new ParameterizedDml(del.toString(), delParams));
                        logger.debug("列过滤将 UPDATE 转为 DELETE: {}.{}", srcDb, srcTable);
                    }
                    continue;
                }
                if (beforeExcluded) {
                    String insertSql = buildInsertSql(metadata, srcDb, srcTable, table, setCols);
                    out.add(new ParameterizedDml(insertSql, after));
                    logger.debug("列过滤将 UPDATE 转为 INSERT: {}.{}", srcDb, srcTable);
                    continue;
                }
            }

            StringBuilder sql = new StringBuilder("UPDATE ").append(tableRef(metadata, table)).append(" SET ");
            List<Object> params = new ArrayList<>(after.size() + before.size());
            for (int i = 0; i < sqlSetCols.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(quote(sqlSetCols[i])).append("=?");
                params.add(after.get(i));
            }
            if (!appendWhere(sql, params, whereCols, sqlWhereCols, before, pks)) {
                return null;
            }
            out.add(new ParameterizedDml(sql.toString(), params));
        }
        return out;
    }

    private List<ParameterizedDml> convertDelete(Map<String, Object> metadata, String srcDb, String srcTable,
                                                 String table, List<ArrayList<Object>> rows) {
        String[] cols = columns(metadata, null);
        if (cols == null) {
            return null;
        }
        java.util.Set<String> pks = pkSet(metadata);
        String[] sqlCols = mapColumns(srcDb, srcTable, cols);

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
            StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableRef(metadata, table));
            List<Object> params = new ArrayList<>();
            if (!appendWhere(sql, params, cols, sqlCols, row, pks)) {
                return null;
            }
            out.add(new ParameterizedDml(sql.toString(), params));
        }
        return out;
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
