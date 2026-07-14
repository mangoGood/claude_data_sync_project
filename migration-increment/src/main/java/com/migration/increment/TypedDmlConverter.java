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
    private final String targetDatabaseName;

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
        this.targetDatabaseName = props.getProperty("target.db.database", "");
        logger.info("TypedDmlConverter enabled={} (source={}, target={}, switch={})",
                enabled, source, target, switchOn);
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
        String table = (String) metadata.getOrDefault("table_name", "");
        if (table.isEmpty()) {
            return null;
        }

        switch (eventType) {
            case "INSERT":
            case "WRITE_ROWS":
            case "EXT_WRITE_ROWS":
                return convertInsert(metadata, table, rowsTyped);
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
                return convertUpdate(metadata, table, rowsTyped, beforeTyped);
            case "DELETE":
            case "DELETE_ROWS":
            case "EXT_DELETE_ROWS":
                return convertDelete(metadata, table, rowsTyped);
            default:
                return null; // DDL/QUERY/心跳等走文本路径
        }
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

    /** 目标表引用：MySQL 目标带库名限定（目标库名优先），PG 目标裸表名（schema 由连接串决定）。 */
    private String tableRef(Map<String, Object> metadata, String table) {
        if (targetIsMysql) {
            String db = (targetDatabaseName != null && !targetDatabaseName.isEmpty())
                    ? targetDatabaseName
                    : (String) metadata.getOrDefault("database_name", "");
            return db.isEmpty() ? quote(table) : quote(db) + "." + quote(table);
        }
        return quote(table);
    }

    private List<ParameterizedDml> convertInsert(Map<String, Object> metadata, String table,
                                                 List<ArrayList<Object>> rows) {
        String[] cols = columns(metadata, "insert_column_names");
        if (cols == null) {
            return null;
        }
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableRef(metadata, table)).append(" (");
        StringBuilder ph = new StringBuilder();
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) { sql.append(", "); ph.append(", "); }
            sql.append(quote(cols[i]));
            ph.append("?");
        }
        sql.append(") VALUES (").append(ph).append(")");

        if (targetIsMysql) {
            // 与文本路径一致的幂等语义：重复主键按新值覆盖
            sql.append(" ON DUPLICATE KEY UPDATE ");
            for (int i = 0; i < cols.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(quote(cols[i])).append(" = VALUES(").append(quote(cols[i])).append(")");
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

        List<ParameterizedDml> out = new ArrayList<>();
        for (ArrayList<Object> row : rows) {
            if (row.size() != cols.length) {
                return null; // 列/值数量不齐：整事件回退
            }
            out.add(new ParameterizedDml(sql.toString(), row));
        }
        return out;
    }

    private List<ParameterizedDml> convertUpdate(Map<String, Object> metadata, String table,
                                                 List<ArrayList<Object>> afterRows,
                                                 List<ArrayList<Object>> beforeRows) {
        String[] setCols = columns(metadata, "update_column_names");
        String[] whereCols = columns(metadata, "update_before_column_names");
        if (setCols == null || whereCols == null) {
            return null;
        }
        java.util.Set<String> pks = pkSet(metadata);

        List<ParameterizedDml> out = new ArrayList<>();
        for (int r = 0; r < afterRows.size(); r++) {
            ArrayList<Object> after = afterRows.get(r);
            ArrayList<Object> before = beforeRows.get(r);
            if (after.size() != setCols.length || before.size() != whereCols.length) {
                return null;
            }
            StringBuilder sql = new StringBuilder("UPDATE ").append(tableRef(metadata, table)).append(" SET ");
            List<Object> params = new ArrayList<>(after.size() + before.size());
            for (int i = 0; i < setCols.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(quote(setCols[i])).append("=?");
                params.add(after.get(i));
            }
            if (!appendWhere(sql, params, whereCols, before, pks)) {
                return null;
            }
            out.add(new ParameterizedDml(sql.toString(), params));
        }
        return out;
    }

    private List<ParameterizedDml> convertDelete(Map<String, Object> metadata, String table,
                                                 List<ArrayList<Object>> rows) {
        String[] cols = columns(metadata, null);
        if (cols == null) {
            return null;
        }
        java.util.Set<String> pks = pkSet(metadata);

        List<ParameterizedDml> out = new ArrayList<>();
        for (ArrayList<Object> row : rows) {
            if (row.size() != cols.length) {
                return null;
            }
            StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableRef(metadata, table));
            List<Object> params = new ArrayList<>();
            if (!appendWhere(sql, params, cols, row, pks)) {
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
     */
    private boolean appendWhere(StringBuilder sql, List<Object> params,
                                String[] cols, List<Object> values, java.util.Set<String> pks) {
        sql.append(" WHERE ");
        boolean first = true;
        boolean usePk = !pks.isEmpty();
        for (int i = 0; i < cols.length; i++) {
            if (usePk && !pks.contains(cols[i].toLowerCase())) {
                continue;
            }
            if (!first) sql.append(" AND ");
            first = false;
            Object v = values.get(i);
            if (v == null) {
                sql.append(quote(cols[i])).append(" IS NULL");
            } else {
                sql.append(quote(cols[i])).append("=?");
                params.add(v);
            }
        }
        return !first; // 至少要有一个条件
    }
}
