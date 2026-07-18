package com.migration.config;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 列处理配置（仅表级同步下发，目前仅 mysql→mysql 链路消费）。
 *
 * <p>三种能力，键均以 "源库.源表" 定位（与 schema.mapping.table.* 的 key 约定一致，
 * 提供小写回退适配 MySQL 源 lower_case_table_names 不区分大小写）：
 * <ul>
 *   <li>列过滤：{@code column.filter.<源库>.<源表>} = {@code 列|op|值[;列|op|值...]}
 *       —— 命中任一条件的行<b>不同步</b>（排除语义）；op ∈ {@code < <= > >= = !=}，
 *       仅支持整数/bit/浮点定点/日期时间类型的列（由 UI 层约束）。</li>
 *   <li>列名映射：{@code column.mapping.<源库>.<源表>} = {@code 源列:目标列[,源列:目标列...]}
 *       —— 只改列名不改类型。</li>
 *   <li>附加列：{@code column.extra.<源库>.<源表>} = {@code 列名:CREATE_TIME | 列名:UPDATE_TIME | 列名:CUSTOM:输入值}
 *       逗号分隔 —— 由全量建表期以 DEFAULT 子句落地（CREATE_TIME=首次写入时间，
 *       UPDATE_TIME=目标端最近更新时间，CUSTOM=常量 "输入值@源库@源表"），DML 无需逐条注值。</li>
 * </ul>
 */
public class ColumnProcessingConfig {

    public static final String FILTER_PREFIX = "column.filter.";
    public static final String MAPPING_PREFIX = "column.mapping.";
    public static final String EXTRA_PREFIX = "column.extra.";

    /** 附加列类型 */
    public enum ExtraColumnKind { CREATE_TIME, UPDATE_TIME, CUSTOM }

    /** 附加列定义：列名 + 类型 +（CUSTOM 时的）输入值 */
    public static class ExtraColumn {
        public final String name;
        public final ExtraColumnKind kind;
        /** 仅 CUSTOM 有效：用户输入值（最终落库值 = 输入值@源库@源表） */
        public final String customValue;

        public ExtraColumn(String name, ExtraColumnKind kind, String customValue) {
            this.name = name;
            this.kind = kind;
            this.customValue = customValue;
        }

        /**
         * 生成 MySQL 建表列定义（不含前导逗号）。
         * CREATE_TIME/UPDATE_TIME 用 DATETIME 默认值语义，CUSTOM 为常量 DEFAULT。
         */
        public String toMysqlColumnDef(String sourceDb, String sourceTable) {
            switch (kind) {
                case CREATE_TIME:
                    return "`" + name + "` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '首次同步到目标库的时间'";
                case UPDATE_TIME:
                    return "`" + name + "` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '目标库最近更新时间'";
                default:
                    String v = (customValue == null ? "" : customValue) + "@" + sourceDb + "@" + sourceTable;
                    return "`" + name + "` VARCHAR(512) DEFAULT '" + v.replace("'", "''") + "' COMMENT '来源标识列'";
            }
        }
    }

    /** 列过滤条件：命中即排除该行 */
    public static class FilterCondition {
        public final String column;
        /** < <= > >= = != */
        public final String op;
        public final String value;

        public FilterCondition(String column, String op, String value) {
            this.column = column;
            this.op = op;
            this.value = value;
        }

        /**
         * 生成 SQL "保留行" 谓词（供全量 SELECT 使用）：排除命中条件的行，
         * 列值为 NULL 的行保留（NULL 不可能命中任何比较条件）。
         */
        public String toKeepSql(java.util.function.Function<String, String> quoter) {
            String col = quoter.apply(column);
            String literal = isNumeric(value) ? value : "'" + value.replace("'", "''") + "'";
            return "(NOT (" + col + " " + op + " " + literal + ") OR " + col + " IS NULL)";
        }

        /** Java 侧判定（供增量类型化管道使用）：该行是否命中排除条件。NULL/不可比 = 不命中。 */
        public boolean excludes(Object rowValue) {
            Integer cmp = compare(rowValue, value);
            if (cmp == null) {
                return false;
            }
            switch (op) {
                case "<":  return cmp < 0;
                case "<=": return cmp <= 0;
                case ">":  return cmp > 0;
                case ">=": return cmp >= 0;
                case "=":  return cmp == 0;
                case "!=": return cmp != 0;
                default:   return false;
            }
        }
    }

    /** "源库.源表" → 条件列表 */
    private final Map<String, List<FilterCondition>> filters = new LinkedHashMap<>();
    /** "源库.源表" → (源列 → 目标列) */
    private final Map<String, Map<String, String>> mappings = new LinkedHashMap<>();
    /** "源库.源表" → 附加列列表 */
    private final Map<String, List<ExtraColumn>> extras = new LinkedHashMap<>();
    /** 小写回退索引（key 小写化） */
    private final Map<String, List<FilterCondition>> filtersLower = new LinkedHashMap<>();
    private final Map<String, Map<String, String>> mappingsLower = new LinkedHashMap<>();
    private final Map<String, List<ExtraColumn>> extrasLower = new LinkedHashMap<>();

    public static ColumnProcessingConfig loadFromProperties(Properties props) {
        ColumnProcessingConfig config = new ColumnProcessingConfig();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(FILTER_PREFIX)) {
                String key = name.substring(FILTER_PREFIX.length());
                List<FilterCondition> list = parseFilters(props.getProperty(name, ""));
                if (!key.isEmpty() && !list.isEmpty()) {
                    config.filters.put(key, list);
                    config.filtersLower.put(key.toLowerCase(), list);
                }
            } else if (name.startsWith(MAPPING_PREFIX)) {
                String key = name.substring(MAPPING_PREFIX.length());
                Map<String, String> map = parseMapping(props.getProperty(name, ""));
                if (!key.isEmpty() && !map.isEmpty()) {
                    config.mappings.put(key, map);
                    config.mappingsLower.put(key.toLowerCase(), map);
                }
            } else if (name.startsWith(EXTRA_PREFIX)) {
                String key = name.substring(EXTRA_PREFIX.length());
                List<ExtraColumn> list = parseExtras(props.getProperty(name, ""));
                if (!key.isEmpty() && !list.isEmpty()) {
                    config.extras.put(key, list);
                    config.extrasLower.put(key.toLowerCase(), list);
                }
            }
        }
        return config;
    }

    private static List<FilterCondition> parseFilters(String raw) {
        List<FilterCondition> list = new ArrayList<>();
        for (String part : raw.split(";")) {
            String[] f = part.split("\\|", 3);
            if (f.length == 3 && !f[0].trim().isEmpty() && isValidOp(f[1].trim()) && !f[2].trim().isEmpty()) {
                list.add(new FilterCondition(f[0].trim(), f[1].trim(), f[2].trim()));
            }
        }
        return list;
    }

    private static Map<String, String> parseMapping(String raw) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String part : raw.split(",")) {
            int idx = part.indexOf(':');
            if (idx > 0 && idx < part.length() - 1) {
                String src = part.substring(0, idx).trim();
                String tgt = part.substring(idx + 1).trim();
                if (!src.isEmpty() && !tgt.isEmpty() && !src.equals(tgt)) {
                    map.put(src, tgt);
                }
            }
        }
        return map;
    }

    private static List<ExtraColumn> parseExtras(String raw) {
        List<ExtraColumn> list = new ArrayList<>();
        for (String part : raw.split(",")) {
            String[] f = part.split(":", 3);
            if (f.length < 2 || f[0].trim().isEmpty()) {
                continue;
            }
            try {
                ExtraColumnKind kind = ExtraColumnKind.valueOf(f[1].trim().toUpperCase());
                String custom = (kind == ExtraColumnKind.CUSTOM && f.length == 3) ? f[2].trim() : null;
                if (kind == ExtraColumnKind.CUSTOM && (custom == null || custom.isEmpty())) {
                    continue;
                }
                list.add(new ExtraColumn(f[0].trim(), kind, custom));
            } catch (IllegalArgumentException ignore) {
                // 未知类型，跳过
            }
        }
        return list;
    }

    private static boolean isValidOp(String op) {
        return "<".equals(op) || "<=".equals(op) || ">".equals(op)
                || ">=".equals(op) || "=".equals(op) || "!=".equals(op);
    }

    private <T> T lookup(Map<String, T> exact, Map<String, T> lower, String db, String table) {
        String key = db + "." + table;
        T hit = exact.get(key);
        if (hit != null) {
            return hit;
        }
        return lower.get(key.toLowerCase());
    }

    /** 该表的列过滤条件；无配置返回空列表。 */
    public List<FilterCondition> getFilters(String db, String table) {
        List<FilterCondition> list = lookup(filters, filtersLower, db, table);
        return list != null ? list : Collections.emptyList();
    }

    /** 该表的列名映射（源列 → 目标列）；无配置返回空 map。 */
    public Map<String, String> getColumnMapping(String db, String table) {
        Map<String, String> map = lookup(mappings, mappingsLower, db, table);
        return map != null ? map : Collections.emptyMap();
    }

    /** 该表的附加列定义；无配置返回空列表。 */
    public List<ExtraColumn> getExtraColumns(String db, String table) {
        List<ExtraColumn> list = lookup(extras, extrasLower, db, table);
        return list != null ? list : Collections.emptyList();
    }

    /** 映射单个列名；未配置返回原列名（大小写回退查找源列名）。 */
    public String mapColumn(String db, String table, String column) {
        Map<String, String> map = getColumnMapping(db, table);
        if (map.isEmpty() || column == null) {
            return column;
        }
        String tgt = map.get(column);
        if (tgt != null) {
            return tgt;
        }
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (e.getKey().equalsIgnoreCase(column)) {
                return e.getValue();
            }
        }
        return column;
    }

    /** 该表是否配置了任意列处理（过滤/映射/附加列）。 */
    public boolean hasProcessing(String db, String table) {
        return !getFilters(db, table).isEmpty()
                || !getColumnMapping(db, table).isEmpty()
                || !getExtraColumns(db, table).isEmpty();
    }

    /** 是否存在任何列处理配置（无配置时调用方可整体短路）。 */
    public boolean isEmpty() {
        return filters.isEmpty() && mappings.isEmpty() && extras.isEmpty();
    }

    /**
     * 生成全量 SELECT 的 "保留行" WHERE 片段（各条件 AND 连接，命中任一过滤条件的行被排除）；
     * 无过滤配置返回 null。
     */
    public String buildKeepClause(String db, String table, java.util.function.Function<String, String> quoter) {
        List<FilterCondition> list = getFilters(db, table);
        if (list.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (FilterCondition c : list) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            sb.append(c.toKeepSql(quoter));
        }
        return sb.toString();
    }

    /**
     * 判定一行是否应被排除（命中任一过滤条件）。列名大小写不敏感匹配；
     * 行中找不到过滤列或值不可比时不排除（宁可多同步不可丢数据）。
     *
     * @param columnNames 行的列名数组
     * @param values      对应值列表
     */
    public boolean rowExcluded(String db, String table, String[] columnNames, List<Object> values) {
        List<FilterCondition> list = getFilters(db, table);
        if (list.isEmpty() || columnNames == null || values == null) {
            return false;
        }
        for (FilterCondition c : list) {
            for (int i = 0; i < columnNames.length && i < values.size(); i++) {
                if (columnNames[i] != null && columnNames[i].trim().equalsIgnoreCase(c.column)) {
                    if (c.excludes(values.get(i))) {
                        return true;
                    }
                    break;
                }
            }
        }
        return false;
    }

    private static final DateTimeFormatter[] DATETIME_FORMATS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
    };

    private static boolean isNumeric(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        try {
            new BigDecimal(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 比较行值与条件值：返回负/零/正表示行值 &lt;/=/&gt; 条件值；类型不可比返回 null。
     * 支持数值（BigDecimal 语义）、bit（Boolean/byte[] 按无符号整数值）与日期时间
     * （DATETIME/DATE/TIME 常用格式）。
     */
    static Integer compare(Object rowValue, String condValue) {
        if (rowValue == null || condValue == null) {
            return null;
        }
        // bit 列：bit(1) 常以 Boolean 出现（tinyInt1isBit/驱动差异），bit(n) 为 byte[]（大端），
        // 均折算为无符号整数值后按数值比较
        if (rowValue instanceof Boolean) {
            rowValue = ((Boolean) rowValue) ? 1 : 0;
        } else if (rowValue instanceof byte[]) {
            byte[] bits = (byte[]) rowValue;
            if (bits.length == 0 || bits.length > 8) {
                return null;
            }
            long v = 0;
            for (byte b : bits) {
                v = (v << 8) | (b & 0xFF);
            }
            rowValue = v;
        } else if (rowValue instanceof java.util.BitSet) {
            java.util.BitSet bs = (java.util.BitSet) rowValue;
            if (bs.length() > 63) {
                return null;
            }
            long v = 0;
            for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
                v |= 1L << i;
            }
            rowValue = v;
        }
        // 数值比较
        if (rowValue instanceof Number) {
            if (!isNumeric(condValue)) {
                return null;
            }
            BigDecimal left = (rowValue instanceof BigDecimal)
                    ? (BigDecimal) rowValue : new BigDecimal(rowValue.toString());
            return left.compareTo(new BigDecimal(condValue));
        }
        // 时间类型比较（binlog 类型化值可能是 java.sql/java.time 各种时间对象）
        LocalDateTime rowDt = toLocalDateTime(rowValue);
        if (rowDt != null) {
            LocalDateTime condDt = parseDateTime(condValue);
            return condDt == null ? null : Integer.valueOf(rowDt.compareTo(condDt));
        }
        LocalTime rowTime = toLocalTime(rowValue);
        if (rowTime != null) {
            try {
                return rowTime.compareTo(LocalTime.parse(condValue));
            } catch (Exception e) {
                return null;
            }
        }
        // 字符串承载的数值/时间（文本管道或驱动差异）
        String s = rowValue.toString().trim();
        if (isNumeric(s) && isNumeric(condValue)) {
            return new BigDecimal(s).compareTo(new BigDecimal(condValue));
        }
        LocalDateTime leftDt = parseDateTime(s);
        LocalDateTime rightDt = parseDateTime(condValue);
        if (leftDt != null && rightDt != null) {
            return leftDt.compareTo(rightDt);
        }
        return null;
    }

    private static LocalDateTime toLocalDateTime(Object v) {
        if (v instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) v).toLocalDateTime();
        }
        if (v instanceof java.sql.Date) {
            return ((java.sql.Date) v).toLocalDate().atStartOfDay();
        }
        if (v instanceof java.util.Date) {
            return LocalDateTime.ofInstant(((java.util.Date) v).toInstant(), java.time.ZoneId.systemDefault());
        }
        if (v instanceof LocalDateTime) {
            return (LocalDateTime) v;
        }
        if (v instanceof LocalDate) {
            return ((LocalDate) v).atStartOfDay();
        }
        return null;
    }

    private static LocalTime toLocalTime(Object v) {
        if (v instanceof java.sql.Time) {
            return ((java.sql.Time) v).toLocalTime();
        }
        if (v instanceof LocalTime) {
            return (LocalTime) v;
        }
        return null;
    }

    private static LocalDateTime parseDateTime(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }
        for (DateTimeFormatter f : DATETIME_FORMATS) {
            try {
                return LocalDateTime.parse(s, f);
            } catch (Exception ignore) {
                // 尝试下一个格式
            }
        }
        try {
            return LocalDate.parse(s).atStartOfDay();
        } catch (Exception ignore) {
            return null;
        }
    }
}
