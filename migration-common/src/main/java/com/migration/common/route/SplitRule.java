package com.migration.common.route;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 拆分规则（1:N）：一张源表按分片键路由到 N 个目标库/表。
 *
 * <p>分片标识由 {@link #resolveShard(Object)} 从<b>行内分片键的值</b>算出，再交给库/表模板渲染成落点。
 * 算不出分片（键值为 NULL、区间/枚举未覆盖、时间值无法解析）时返回 null，由调用方按
 * {@link UnroutedPolicy} 处置——默认广播，避免悄悄丢行。
 */
public final class SplitRule {

    /** 行内算不出分片时的处置 */
    public enum UnroutedPolicy {
        /** 广播到全部分片（默认；要求分片可枚举） */
        BROADCAST,
        /** 投递死信，人工裁决 */
        DEADLETTER,
        /** 直接失败，停任务 */
        ERROR
    }

    private final String id;
    private final TablePattern pattern;
    private final String shardKey;
    private final ShardAlgorithm algorithm;
    private final int count;
    private final ShardTemplate dbTemplate;
    private final ShardTemplate tableTemplate;
    private final String nodeGroup;
    private final ShardTemplate nodeTemplate;
    private final UnroutedPolicy unroutedPolicy;
    /** RANGE：左闭右开区间，下标即分片号 */
    private final List<long[]> ranges;
    /** LIST：枚举值 → 分片号 */
    private final Map<String, Integer> listMapping;
    /** DATE_FORMAT：输出格式 */
    private final DateTimeFormatter dateFormatter;

    SplitRule(String id, TablePattern pattern, String shardKey, ShardAlgorithm algorithm, int count,
              ShardTemplate dbTemplate, ShardTemplate tableTemplate,
              String nodeGroup, ShardTemplate nodeTemplate, UnroutedPolicy unroutedPolicy,
              List<long[]> ranges, Map<String, Integer> listMapping, DateTimeFormatter dateFormatter) {
        this.id = id;
        this.pattern = pattern;
        this.shardKey = shardKey;
        this.algorithm = algorithm;
        this.count = count;
        this.dbTemplate = dbTemplate;
        this.tableTemplate = tableTemplate;
        this.nodeGroup = nodeGroup;
        this.nodeTemplate = nodeTemplate;
        this.unroutedPolicy = unroutedPolicy == null ? UnroutedPolicy.BROADCAST : unroutedPolicy;
        this.ranges = ranges == null ? Collections.emptyList() : ranges;
        this.listMapping = listMapping == null ? Collections.emptyMap() : listMapping;
        this.dateFormatter = dateFormatter;
    }

    public String getId() {
        return id;
    }

    public TablePattern getPattern() {
        return pattern;
    }

    public String getShardKey() {
        return shardKey;
    }

    public ShardAlgorithm getAlgorithm() {
        return algorithm;
    }

    /** 分片数；DATE_FORMAT 下为 0（不可枚举）。 */
    public int getCount() {
        return count;
    }

    public String getNodeGroup() {
        return nodeGroup;
    }

    public UnroutedPolicy getUnroutedPolicy() {
        return unroutedPolicy;
    }

    public boolean isEnumerable() {
        return algorithm.isEnumerable() && count > 0;
    }

    /**
     * 由分片键的值算出分片标识；算不出返回 null（调用方按 {@link UnroutedPolicy} 处置）。
     */
    public ShardKey resolveShard(Object value) {
        if (value == null) {
            return null;
        }
        switch (algorithm) {
            case HASH_MOD:
                return ShardKey.ofIndex((int) Math.floorMod(hashOf(value), (long) count));
            case RANGE: {
                BigDecimal num = toNumber(value);
                if (num == null) {
                    return null;
                }
                for (int i = 0; i < ranges.size(); i++) {
                    long[] r = ranges.get(i);
                    if (num.compareTo(BigDecimal.valueOf(r[0])) >= 0
                            && num.compareTo(BigDecimal.valueOf(r[1])) < 0) {
                        return ShardKey.ofIndex(i);
                    }
                }
                return null;
            }
            case LIST: {
                String s = String.valueOf(value);
                Integer idx = listMapping.get(s);
                if (idx == null) {
                    // 大小写回退，与全链路的标识符匹配口径一致
                    for (Map.Entry<String, Integer> e : listMapping.entrySet()) {
                        if (e.getKey().equalsIgnoreCase(s)) {
                            idx = e.getValue();
                            break;
                        }
                    }
                }
                return idx == null ? null : ShardKey.ofIndex(idx);
            }
            case DATE_FORMAT: {
                LocalDateTime dt = toDateTime(value);
                return dt == null ? null : ShardKey.ofToken(dateFormatter.format(dt));
            }
            default:
                return null;
        }
    }

    /**
     * 把分片标识渲染成落点。
     *
     * @param defaultTargetDb 任务默认目标库（库模板缺省时用它）
     * @param sourceTable     源表名（表模板缺省时用它）
     * @param nodeCount       目标实例组的节点数；&le; 1 表示单实例
     */
    public RouteTarget toTarget(ShardKey key, String defaultTargetDb, String sourceTable, int nodeCount) {
        String db = dbTemplate != null ? dbTemplate.render(key) : defaultTargetDb;
        String table = tableTemplate != null ? tableTemplate.render(key) : sourceTable;
        String nodeId = null;
        if (nodeGroup != null && nodeCount > 0) {
            int ordinal = resolveNodeOrdinal(key, nodeCount);
            nodeId = nodeGroup + "#" + ordinal;
        }
        return RouteTarget.sharded(nodeId, db, table, key.index());
    }

    /**
     * 分片 → 目标实例序号。显式模板优先；缺省时按<b>连续块</b>划分
     * （{@code ordinal = shard * nodeCount / count}）——"8 库 × 16 表 = 128 片，
     * 每 16 片一个实例"是分库分表最常见的排布，取模式排布会把相邻分片打散到不同实例，
     * 与业务侧的容量规划对不上。
     */
    private int resolveNodeOrdinal(ShardKey key, int nodeCount) {
        if (nodeTemplate != null) {
            try {
                int ordinal = Integer.parseInt(nodeTemplate.render(key).trim());
                return Math.floorMod(ordinal, nodeCount);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        if (!key.isNumeric() || count <= 0) {
            return 0;
        }
        return Math.min(nodeCount - 1, (int) ((long) key.index() * nodeCount / count));
    }

    /** 全部分片落点（预建目标表、BROADCAST 未路由行用）；不可枚举时返回空列表。 */
    public List<RouteTarget> allTargets(String defaultTargetDb, String sourceTable, int nodeCount) {
        if (!isEnumerable()) {
            return Collections.emptyList();
        }
        List<RouteTarget> targets = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            targets.add(toTarget(ShardKey.ofIndex(i), defaultTargetDb, sourceTable, nodeCount));
        }
        return targets;
    }

    // ---------- 值归一 ----------

    /**
     * 哈希取模的值归一：<b>凡是整数一律按数值取模</b>（{@code user_id % 16} 与业务侧分片口径一致），
     * 其余走 CRC32(UTF-8)——跨 JVM 稳定，不能用 String#hashCode（其值虽也稳定，
     * 但对短数字串分布极差，会把分片压到少数几个上）。
     *
     * <p><b>"整数" 必须跨表示形式统一</b>：同一个值在全量链路是 {@code Long 5}，
     * 在增量链路的类型化值里可能是字符串 {@code "5"} 或 {@code BigDecimal 5.00}。
     * 若按 Java 类型分流，同一行会被全量和增量算到<b>两个不同的分片</b>上，
     * 于是两个分片里各留一份——这类不一致不会报错，只会在对数时才发现。
     */
    static long hashOf(Object value) {
        Long integral = asIntegral(value);
        if (integral != null) {
            return integral;
        }
        byte[] bytes = value instanceof byte[]
                ? (byte[]) value
                : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length);
        return crc.getValue();
    }

    /** 尽力把值解读成整数；解读不了返回 null（走 CRC32）。 */
    private static Long asIntegral(Object value) {
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        if (value instanceof java.math.BigInteger) {
            return ((java.math.BigInteger) value).longValue();
        }
        if (value instanceof BigDecimal) {
            BigDecimal d = (BigDecimal) value;
            return d.stripTrailingZeros().scale() <= 0 ? d.longValue() : null;
        }
        if (value instanceof Float || value instanceof Double) {
            double d = ((Number) value).doubleValue();
            return d == Math.rint(d) && !Double.isInfinite(d) ? (long) d : null;
        }
        if (value instanceof CharSequence) {
            String s = value.toString().trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return new BigDecimal(s).stripTrailingZeros().scale() <= 0
                        ? new BigDecimal(s).longValue() : null;
            } catch (NumberFormatException e) {
                return null;   // 真·字符串分片键，走 CRC32
            }
        }
        return null;
    }

    static BigDecimal toNumber(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        try {
            return new BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static LocalDateTime toDateTime(Object value) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay();
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate().atStartOfDay();
        }
        if (value instanceof java.util.Date) {
            return LocalDateTime.ofInstant(((java.util.Date) value).toInstant(), ZoneId.systemDefault());
        }
        if (value instanceof Instant) {
            return LocalDateTime.ofInstant((Instant) value, ZoneId.systemDefault());
        }
        String s = String.valueOf(value).trim();
        if (s.isEmpty()) {
            return null;
        }
        String normalized = s.length() > 10 && s.charAt(10) == ' ' ? s.replace(' ', 'T') : s;
        try {
            return LocalDateTime.parse(normalized);
        } catch (Exception ignored) {
            // 继续尝试纯日期
        }
        try {
            return LocalDate.parse(normalized.length() > 10 ? normalized.substring(0, 10) : normalized)
                    .atStartOfDay();
        } catch (Exception ignored) {
            return null;
        }
    }

    /** 解析 {@code lo:hi,lo:hi} 形式的区间表；非法条目直接抛，由加载期收集成校验错误。 */
    static List<long[]> parseRanges(String spec) {
        List<long[]> result = new ArrayList<>();
        for (String part : spec.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) {
                continue;
            }
            int colon = p.indexOf(':');
            if (colon <= 0 || colon == p.length() - 1) {
                throw new IllegalArgumentException("区间格式应为 lo:hi，实际: " + p);
            }
            long lo = Long.parseLong(p.substring(0, colon).trim());
            long hi = Long.parseLong(p.substring(colon + 1).trim());
            if (hi <= lo) {
                throw new IllegalArgumentException("区间右界必须大于左界（左闭右开）: " + p);
            }
            result.add(new long[]{lo, hi});
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("区间表为空");
        }
        return result;
    }

    /** 解析 {@code 值:分片号,值:分片号} 形式的枚举表。 */
    static Map<String, Integer> parseList(String spec) {
        Map<String, Integer> mapping = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) {
                continue;
            }
            int colon = p.lastIndexOf(':');
            if (colon <= 0 || colon == p.length() - 1) {
                throw new IllegalArgumentException("枚举格式应为 值:分片号，实际: " + p);
            }
            int shard = Integer.parseInt(p.substring(colon + 1).trim());
            if (shard < 0) {
                throw new IllegalArgumentException("分片号不能为负: " + p);
            }
            mapping.put(p.substring(0, colon).trim(), shard);
        }
        if (mapping.isEmpty()) {
            throw new IllegalArgumentException("枚举表为空");
        }
        return mapping;
    }

    @Override
    public String toString() {
        return "split[" + id + "] " + pattern + " by " + shardKey + "/" + algorithm
                + " count=" + count + " -> "
                + (dbTemplate == null ? "<默认库>" : dbTemplate) + "."
                + (tableTemplate == null ? "<源表名>" : tableTemplate)
                + (nodeGroup == null ? "" : " @" + nodeGroup)
                + " unrouted=" + unroutedPolicy;
    }
}
