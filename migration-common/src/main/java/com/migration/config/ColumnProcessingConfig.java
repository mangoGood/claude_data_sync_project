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
 * 列处理配置（仅表级同步下发，由同构链路 mysql→mysql / postgresql→postgresql 消费）。
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
    /** 脱敏规则：{@code column.mask.<源库>.<源表>} = {@code 列:规则[:参数...][;列:规则...]} */
    public static final String MASK_PREFIX = "column.mask.";
    /** 脱敏盐（部署级）：同一盐下 HASH/FAKE 对同一输入恒产出同一结果。 */
    public static final String MASK_SALT_KEY = "column.mask.salt";

    /** 附加列类型 */
    public enum ExtraColumnKind { CREATE_TIME, UPDATE_TIME, CUSTOM }

    /** 脱敏规则类型。 */
    public enum MaskKind {
        /** 整值替换为固定串。 */
        MASK_ALL,
        /** 保留首 N / 尾 M 位，中间替换（手机号、身份证的常见形态）。 */
        MASK_PARTIAL,
        /** 不可逆哈希，但<b>保留可连接性</b>：同值恒同结果，跨表 JOIN 仍成立。 */
        HASH,
        /** 置空。列必须可空，否则目标端写入会失败。 */
        NULLIFY,
        /** 生成同型假数据（NAME / EMAIL / PHONE / ADDRESS / ID_CARD）。 */
        FAKE
    }

    /**
     * 一条脱敏规则。
     *
     * <p><b>所有规则都必须是确定性的</b>——同一输入恒产出同一输出。
     * 这不是美观要求：增量重放、断点续传、以及全量与增量对同一行的两次处理
     * 都依赖"再算一遍还是那个值"。若脱敏带随机性，幂等 upsert 会把每次重放
     * 都看成一次真实变更，目标端与源端将<b>永远对不齐</b>。
     */
    public static class MaskRule {
        public final String column;
        public final MaskKind kind;
        /** MASK_PARTIAL 的保留首位数；其余规则无意义。 */
        public final int keepPrefix;
        /** MASK_PARTIAL 的保留尾位数。 */
        public final int keepSuffix;
        /** FAKE 的类型（NAME/EMAIL/PHONE/ADDRESS/ID_CARD）；MASK_ALL 的替换串。 */
        public final String arg;

        public MaskRule(String column, MaskKind kind, int keepPrefix, int keepSuffix, String arg) {
            this.column = column;
            this.kind = kind;
            this.keepPrefix = Math.max(0, keepPrefix);
            this.keepSuffix = Math.max(0, keepSuffix);
            this.arg = arg;
        }
    }

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

        /** 按目标库方言生成建表列定义（不含前导逗号）。 */
        public String toColumnDef(String sourceDb, String sourceTable, boolean postgres) {
            return postgres ? toPostgresColumnDef(sourceDb, sourceTable)
                            : toMysqlColumnDef(sourceDb, sourceTable);
        }

        /**
         * CUSTOM 附加列的实际落库值：{@code 输入值@源库@源表}。
         *
         * <p>汇聚下这个值<b>必须逐行注</b>，不能靠建表 DEFAULT：合并表只由第一个来源建出来，
         * DEFAULT 里烤的是那一个来源的库表名，其余来源的行会全部带上错误的来源标识——
         * 而"标识来源"正是这个列存在的理由。
         */
        public String resolvedValue(String sourceDb, String sourceTable) {
            return (customValue == null ? "" : customValue) + "@" + sourceDb + "@" + sourceTable;
        }

        /**
         * 建表列定义，但<b>不带 DEFAULT</b>（值由 DML 逐行注）。
         * CREATE_TIME/UPDATE_TIME 与来源无关，仍走 DEFAULT，不受影响。
         */
        public String toColumnDefWithoutDefault(boolean postgres) {
            return postgres ? "\"" + name + "\" VARCHAR(512)"
                            : "`" + name + "` VARCHAR(512) COMMENT '来源标识列'";
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

        /**
         * 生成 PostgreSQL 建表列定义（不含前导逗号，双引号标识符）。
         * CREATE_TIME/UPDATE_TIME 用 TIMESTAMP DEFAULT CURRENT_TIMESTAMP；PG 无列级
         * ON UPDATE 语义，UPDATE_TIME 仅承载首次写入值（后续自动刷新需触发器，超出建表范围，
         * 增量 DML 不逐条注值——与 MySQL 路径一致按 DEFAULT 承载）。CUSTOM 为常量 DEFAULT。
         */
        public String toPostgresColumnDef(String sourceDb, String sourceTable) {
            switch (kind) {
                case CREATE_TIME:
                case UPDATE_TIME:
                    return "\"" + name + "\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                default:
                    String v = (customValue == null ? "" : customValue) + "@" + sourceDb + "@" + sourceTable;
                    return "\"" + name + "\" VARCHAR(512) DEFAULT '" + v.replace("'", "''") + "'";
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
    /** "源库.源表" → (列名小写 → 脱敏规则) */
    private final Map<String, Map<String, MaskRule>> masks = new LinkedHashMap<>();
    private final Map<String, Map<String, MaskRule>> masksLower = new LinkedHashMap<>();
    /** 脱敏盐。空盐也能工作，但同一份数据在不同部署会脱出相同结果——生产应当配。 */
    private String maskSalt = "";

    public static ColumnProcessingConfig loadFromProperties(Properties props) {
        ColumnProcessingConfig config = new ColumnProcessingConfig();
        config.maskSalt = props.getProperty(MASK_SALT_KEY, "");
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
            } else if (name.startsWith(MASK_PREFIX) && !name.equals(MASK_SALT_KEY)) {
                String key = name.substring(MASK_PREFIX.length());
                Map<String, MaskRule> map = parseMasks(props.getProperty(name, ""));
                if (!key.isEmpty() && !map.isEmpty()) {
                    config.masks.put(key, map);
                    config.masksLower.put(key.toLowerCase(), map);
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

    /**
     * 解析脱敏规则串：{@code 列:规则[:参数...]}，分号分隔。
     *
     * <pre>
     *   phone:MASK_PARTIAL:3:4     保留前 3 后 4
     *   id_card:MASK_ALL:******    整值替换
     *   email:HASH                 可连接的不可逆哈希
     *   remark:NULLIFY             置空
     *   name:FAKE:NAME             同型假数据
     * </pre>
     */
    private static Map<String, MaskRule> parseMasks(String raw) {
        Map<String, MaskRule> map = new LinkedHashMap<>();
        if (raw == null) {
            return map;
        }
        for (String part : raw.split(";")) {
            String[] f = part.split(":", 4);
            if (f.length < 2 || f[0].trim().isEmpty()) {
                continue;
            }
            String col = f[0].trim();
            MaskKind kind;
            try {
                kind = MaskKind.valueOf(f[1].trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                // 不认识的规则直接跳过而不是"当成不脱敏"——那样会静默把敏感数据原样搬过去。
                // 这里只能跳过（配置层没有抛的通道），但校验发生在控制面：
                // RouteConfigValidator 会在建任务时挡下非法规则。
                continue;
            }
            int keepPrefix = 0, keepSuffix = 0;
            String arg = null;
            if (kind == MaskKind.MASK_PARTIAL) {
                keepPrefix = parseIntSafe(f.length > 2 ? f[2] : "0", 0);
                keepSuffix = parseIntSafe(f.length > 3 ? f[3] : "0", 0);
            } else if (f.length > 2) {
                arg = f[2].trim();
            }
            map.put(col.toLowerCase(), new MaskRule(col, kind, keepPrefix, keepSuffix, arg));
        }
        return map;
    }

    private static int parseIntSafe(String s, int dflt) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return dflt;
        }
    }

    // ================================================================ 脱敏

    /** 该表是否配了脱敏。 */
    public boolean hasMask(String db, String table) {
        return !maskRules(db, table).isEmpty();
    }

    /** 该表被脱敏的列（小写）。内容对比要用它把这些列排除掉。 */
    public java.util.Set<String> maskedColumns(String db, String table) {
        return java.util.Collections.unmodifiableSet(maskRules(db, table).keySet());
    }

    /** 全部配了脱敏的 "库.表" key。 */
    public java.util.Set<String> maskedTables() {
        return java.util.Collections.unmodifiableSet(masks.keySet());
    }

    private Map<String, MaskRule> maskRules(String db, String table) {
        String key = db + "." + table;
        Map<String, MaskRule> m = masks.get(key);
        if (m == null) {
            m = masksLower.get(key.toLowerCase());
        }
        return m == null ? Collections.emptyMap() : m;
    }

    /**
     * 对一个列值应用脱敏；该列没配规则时<b>原样返回</b>。
     *
     * <p>null 一律保持 null——脱敏不该把"没有值"变成"有一个假值"，
     * 那会让下游分不清缺失与已脱敏。
     */
    public Object maskValue(String db, String table, String column, Object value) {
        if (column == null) {
            return value;
        }
        MaskRule rule = maskRules(db, table).get(column.trim().toLowerCase());
        if (rule == null || value == null) {
            return value;
        }
        String s = String.valueOf(value);
        switch (rule.kind) {
            case NULLIFY:
                return null;
            case MASK_ALL:
                return (rule.arg == null || rule.arg.isEmpty()) ? "******" : rule.arg;
            case MASK_PARTIAL:
                return maskPartial(s, rule.keepPrefix, rule.keepSuffix);
            case HASH:
                return hashHex(s);
            case FAKE:
                return fake(s, rule.arg);
            default:
                return value;
        }
    }

    /**
     * 保留首 N 尾 M，中间以 * 填充。
     *
     * <p>中间用<b>固定 6 个</b> * 而不是按原长度填：按原长度填会泄露原值的长度，
     * 而长度对身份证、手机号这类定长字段等于泄露了格式，对密码类字段更是直接的信息。
     */
    private static String maskPartial(String s, int keepPrefix, int keepSuffix) {
        if (keepPrefix + keepSuffix >= s.length()) {
            // 保留位数不小于原值长度：整值遮蔽，绝不能退化成"原样返回"
            return "******";
        }
        return s.substring(0, keepPrefix) + "******" + s.substring(s.length() - keepSuffix);
    }

    /** 可连接的不可逆哈希：同值恒同结果，跨表 JOIN 仍成立。 */
    private String hashHex(String s) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            md.update(maskSalt.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] h = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(32);
            for (int i = 0; i < 16; i++) {
                sb.append(String.format("%02x", h[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("脱敏哈希失败", e);
        }
    }

    private static final String[] FAKE_NAMES = {
            "张伟", "王芳", "李娜", "刘洋", "陈静", "杨勇", "赵敏", "黄磊", "周杰", "吴强"};
    private static final String[] FAKE_CITIES = {
            "北京市朝阳区", "上海市浦东新区", "广州市天河区", "深圳市南山区", "杭州市西湖区"};

    /**
     * 同型假数据。<b>由原值确定性导出</b>——同一个原值恒得到同一个假值，
     * 否则增量重放会把每次都当成一次真实变更。
     */
    private String fake(String s, String type) {
        int h = Math.abs(hashHex(s).hashCode());
        String t = type == null ? "" : type.trim().toUpperCase();
        switch (t) {
            case "NAME":
                return FAKE_NAMES[h % FAKE_NAMES.length];
            case "EMAIL":
                return "user" + (h % 1000000) + "@example.com";
            case "PHONE":
                return "138" + String.format("%08d", h % 100000000);
            case "ADDRESS":
                return FAKE_CITIES[h % FAKE_CITIES.length] + (h % 900 + 100) + "号";
            case "ID_CARD":
                return "11010119" + String.format("%02d", h % 90 + 10)
                        + String.format("%02d", h % 12 + 1) + String.format("%02d", h % 28 + 1)
                        + String.format("%04d", h % 10000);
            default:
                // 没指定类型：退化成哈希而不是原样返回——"不认识的类型"绝不能等于"不脱敏"
                return hashHex(s);
        }
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

    /**
     * 该表<b>需要逐行注值</b>的附加列（列名 → 值），即 CUSTOM 类型的那些。
     * 汇聚下由 DML 带值写入；1:1 与拆分下仍由建表 DEFAULT 承载，本方法不参与。
     */
    public java.util.LinkedHashMap<String, String> perRowExtraValues(String db, String table) {
        java.util.LinkedHashMap<String, String> values = new java.util.LinkedHashMap<>();
        for (ExtraColumn extra : getExtraColumns(db, table)) {
            if (extra.kind == ExtraColumnKind.CUSTOM) {
                values.put(extra.name, extra.resolvedValue(db, table));
            }
        }
        return values;
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
    /**
     * 是否没有任何列处理配置。
     *
     * <p><b>masks 必须算在内</b>：调用方用它决定"要不要走列处理这条路"，
     * 漏掉 masks 的后果是——只配了脱敏的任务被判定为"无列处理"，
     * 增量侧整条脱敏逻辑被跳过，<b>原值直接写进目标端</b>。
     * 而全量侧走的是另一条判断，照常脱敏——于是出现
     * "全量脱了、增量没脱"这种最难察觉的半脱敏状态。
     * 这个洞是脱敏的端到端判据抓出来的，单测碰不到（单测直接构造配置对象）。
     */
    public boolean isEmpty() {
        return filters.isEmpty() && mappings.isEmpty() && extras.isEmpty() && masks.isEmpty();
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
