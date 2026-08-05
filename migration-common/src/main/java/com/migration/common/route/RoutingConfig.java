package com.migration.common.route;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * 聚合路由配置：分库分表<b>汇聚</b>（N:1）与<b>拆分</b>（1:N）的统一规则模型。
 *
 * <p>键约定（与 {@code column.filter.*} / {@code schema.mapping.*} 同风格，由 agent 下发到 config.properties）：
 * <pre>
 * route.mode = NONE | MERGE | SPLIT                     # 默认 NONE = 现状 1:1，全链路零回归
 *
 * # 汇聚
 * route.merge.1.match        = shard_db_*.order_*        # 通配；regex: 前缀走正则
 * route.merge.1.target       = dw.order_all              # 目标库可省（用任务默认目标库）
 * route.merge.1.pk.strategy  = COMPOSITE_SOURCE | KEEP   # 默认 COMPOSITE_SOURCE
 * route.merge.1.tag.columns  = _src_node,_src_db,_src_table
 * route.merge.1.ddl.policy   = FIRST_WINS | SKIP | MANUAL
 *
 * # 拆分
 * route.split.1.match        = app.order
 * route.split.1.shard.key    = user_id
 * route.split.1.algo         = HASH_MOD | RANGE | LIST | DATE_FORMAT
 * route.split.1.count        = 16                        # HASH_MOD 必填；RANGE/LIST 由表长度推导
 * route.split.1.target.db    = dw_${shard/2}             # 省略 = 任务默认目标库
 * route.split.1.target.table = order_${shard}            # 省略 = 源表名
 * route.split.1.target.group = g1                        # 跨实例目标组，省略 = 单实例
 * route.split.1.target.node  = ${shard/2}                # 省略 = 按连续块划分
 * route.split.1.unrouted     = BROADCAST | DEADLETTER | ERROR
 * route.split.1.range        = 0:1000,1000:5000          # RANGE 专用，左闭右开
 * route.split.1.list         = CN:0,US:1,JP:2            # LIST 专用
 * route.split.1.date.format  = yyyyMM                    # DATE_FORMAT 专用
 *
 * # 目标实例组
 * route.node.g1.0.host / .port / .database / .username / .password
 * </pre>
 *
 * <p><b>非法配置一律 fail-stop</b>：{@link #isValid()} 为 false 时 {@link #router} 直接抛异常，
 * 而不是丢掉坏规则继续跑——路由错了就是数据写错地方，比任务起不来严重得多。
 */
public final class RoutingConfig {

    private static final Logger logger = LoggerFactory.getLogger(RoutingConfig.class);

    public static final String KEY_MODE = "route.mode";
    public static final String PREFIX_MERGE = "route.merge.";
    public static final String PREFIX_SPLIT = "route.split.";
    public static final String PREFIX_NODE = "route.node.";

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");
    /** 已验证支持聚合路由的关系库引擎（含异构 mysql↔pg） */
    private static final java.util.Set<String> ROUTABLE_ENGINES = java.util.Set.of("mysql", "postgresql");
    /** 校验模板渲染结果用的样例时刻（DATE_FORMAT 规则没有数值分片号可渲染） */
    private static final LocalDateTime SAMPLE_TIME = LocalDateTime.of(2026, 8, 3, 0, 0);

    public enum Mode { NONE, MERGE, SPLIT }

    private final Mode mode;
    private final List<MergeRule> mergeRules;
    private final List<SplitRule> splitRules;
    private final Map<String, List<RouteNode>> nodeGroups;
    private final List<String> errors;
    private final List<String> warnings;

    private RoutingConfig(Mode mode, List<MergeRule> mergeRules, List<SplitRule> splitRules,
                          Map<String, List<RouteNode>> nodeGroups,
                          List<String> errors, List<String> warnings) {
        this.mode = mode;
        this.mergeRules = Collections.unmodifiableList(mergeRules);
        this.splitRules = Collections.unmodifiableList(splitRules);
        this.nodeGroups = Collections.unmodifiableMap(nodeGroups);
        this.errors = Collections.unmodifiableList(errors);
        this.warnings = Collections.unmodifiableList(warnings);
    }

    /** 未配置路由（1:1）。 */
    public static RoutingConfig none() {
        return new RoutingConfig(Mode.NONE, new ArrayList<>(), new ArrayList<>(),
                new LinkedHashMap<>(), new ArrayList<>(), new ArrayList<>());
    }

    public static RoutingConfig loadFromProperties(Properties props) {
        if (props == null) {
            return none();
        }
        Mode mode = parseMode(props.getProperty(KEY_MODE));
        if (mode == Mode.NONE) {
            return none();
        }

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        validateEngineSupport(props, errors);
        Map<String, List<RouteNode>> nodeGroups = parseNodeGroups(props, errors);

        List<MergeRule> mergeRules = new ArrayList<>();
        List<SplitRule> splitRules = new ArrayList<>();
        if (mode == Mode.MERGE) {
            for (Map.Entry<String, Map<String, String>> e : groupByRuleId(props, PREFIX_MERGE).entrySet()) {
                MergeRule rule = buildMergeRule(e.getKey(), e.getValue(), errors, warnings);
                if (rule != null) {
                    mergeRules.add(rule);
                }
            }
            if (mergeRules.isEmpty() && errors.isEmpty()) {
                errors.add("route.mode=MERGE 但没有任何 route.merge.<id>.* 规则");
            }
            if (!groupByRuleId(props, PREFIX_SPLIT).isEmpty()) {
                warnings.add("route.mode=MERGE，配置里的 route.split.* 规则被忽略");
            }
        } else {
            for (Map.Entry<String, Map<String, String>> e : groupByRuleId(props, PREFIX_SPLIT).entrySet()) {
                SplitRule rule = buildSplitRule(e.getKey(), e.getValue(), nodeGroups, errors, warnings);
                if (rule != null) {
                    splitRules.add(rule);
                }
            }
            if (splitRules.isEmpty() && errors.isEmpty()) {
                errors.add("route.mode=SPLIT 但没有任何 route.split.<id>.* 规则");
            }
            if (!groupByRuleId(props, PREFIX_MERGE).isEmpty()) {
                warnings.add("route.mode=SPLIT，配置里的 route.merge.* 规则被忽略");
            }
        }

        RoutingConfig config = new RoutingConfig(mode, mergeRules, splitRules, nodeGroups, errors, warnings);
        for (String w : warnings) {
            logger.warn("路由配置告警: {}", w);
        }
        for (String err : errors) {
            logger.error("路由配置错误: {}", err);
        }
        logger.info("路由配置加载完成: mode={}, merge规则={}, split规则={}, 实例组={}, 错误={}",
                mode, mergeRules.size(), splitRules.size(), nodeGroups.size(), errors.size());
        return config;
    }

    /**
     * 引擎对白名单校验：哪些库对的聚合路由已经实现并验证过。
     *
     * <p>拦在这里而不是"跑到哪算哪"，是因为不支持的链路各有各的错法，而且都不是好错法：
     * <ul>
     *   <li>Redis：没有表的概念，"汇聚/拆分"只能是 key 前缀命名空间的合并或分裂，不是分库分表。</li>
     *   <li>Oracle：幂等 upsert 只实现了 MySQL/PostgreSQL 两种方言（{@link UpsertSqlBuilder}），
     *       Oracle 要 MERGE INTO。</li>
     *   <li>TiDB 源：增量走 TiCDC canal-json，路由改写在该链路上没有验证过。</li>
     * </ul>
     *
     * <p>已支持的范围：关系库 mysql/pg 任意组合（含异构，来源标识列会追加到翻译器产出的目标方言
     * DDL 上、分片表也由翻译器生成）、mongodb→mongodb（集合级）、mysql→elasticsearch（索引级），
     * 后两者由 {@link DocumentRouter} 消费同一份规则。</p>
     *
     * <p><b>库类型缺失时不判</b>：单测与部分直驱场景不下发 {@code source.db.type}，
     * 把"没声明"当成"不支持"会误伤；生产链路由 agent 的 ConfigService 保证两个键一定有值。
     */
    private static void validateEngineSupport(Properties props, List<String> errors) {
        String source = lower(props.getProperty("source.db.type"));
        String target = lower(props.getProperty("target.db.type"));
        if (source.isEmpty() || target.isEmpty()) {
            return;
        }
        String pair = source + "→" + target;
        // TiDB 源在引擎侧被归一成 mysql（见 agent ConfigService），只能靠 flavor 认出来
        if ("tidb".equals(lower(props.getProperty("source.db.flavor")))) {
            errors.add("TiDB 源暂不支持聚合路由：增量走 TiCDC canal-json，路由改写未在该链路验证");
            return;
        }
        if ("redis".equals(source) || "redis".equals(target)) {
            errors.add("Redis 不支持聚合路由：Redis 没有表的概念，所谓汇聚/拆分只能是"
                    + " key 前缀命名空间的合并或分裂，与分库分表不是一回事");
            return;
        }
        // 关系库对：源与目标都在白名单里即可，异构（mysql↔pg）也放行——汇聚的来源标识列已能追加到
        // 翻译器产出的 DDL 上、拆分改走翻译器建分片表，两条链路的幂等 upsert 方言也都齐了。
        // Oracle 仍不行：upsert 要 MERGE INTO，没实现。
        boolean relational = ROUTABLE_ENGINES.contains(source) && ROUTABLE_ENGINES.contains(target);
        // 文档型：mongo↔mongo 走集合、mysql→ES 走索引，由各自引擎的 DocumentRouter 消费同一份规则
        boolean documentPair = ("mongodb".equals(source) && "mongodb".equals(target))
                || ("mysql".equals(source) && "elasticsearch".equals(target));
        if (!relational && !documentPair) {
            errors.add("库对 " + pair + " 暂不支持聚合路由：关系库只实现了 MySQL 与 PostgreSQL 方言的"
                    + "幂等 upsert，文档型只支持 mongodb→mongodb 与 mysql→elasticsearch");
        }
    }

    private static String lower(String raw) {
        return raw == null ? "" : raw.trim().toLowerCase();
    }

    private static Mode parseMode(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Mode.NONE;
        }
        try {
            return Mode.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("route.mode 取值非法（{}），按 NONE 处理", raw);
            return Mode.NONE;
        }
    }

    /**
     * 把 {@code <prefix><id>.<字段>} 形式的键按规则 id 分组。id 全为数字时按数值排序
     * （避免 "10" 排在 "2" 前面这种让人对不上号的规则顺序）。
     */
    private static Map<String, Map<String, String>> groupByRuleId(Properties props, String prefix) {
        Map<String, Map<String, String>> grouped = new TreeMap<>(RoutingConfig::compareRuleId);
        for (String name : props.stringPropertyNames()) {
            if (!name.startsWith(prefix)) {
                continue;
            }
            String rest = name.substring(prefix.length());
            int dot = rest.indexOf('.');
            if (dot <= 0 || dot == rest.length() - 1) {
                continue;
            }
            grouped.computeIfAbsent(rest.substring(0, dot), k -> new LinkedHashMap<>())
                    .put(rest.substring(dot + 1), props.getProperty(name, "").trim());
        }
        return grouped;
    }

    private static int compareRuleId(String a, String b) {
        boolean na = a.chars().allMatch(Character::isDigit);
        boolean nb = b.chars().allMatch(Character::isDigit);
        if (na && nb) {
            int cmp = Long.compare(Long.parseLong(a), Long.parseLong(b));
            return cmp != 0 ? cmp : a.compareTo(b);
        }
        return a.compareTo(b);
    }

    private static Map<String, List<RouteNode>> parseNodeGroups(Properties props, List<String> errors) {
        // group → ordinal → 字段
        Map<String, Map<Integer, Map<String, String>>> raw = new LinkedHashMap<>();
        for (String name : props.stringPropertyNames()) {
            if (!name.startsWith(PREFIX_NODE)) {
                continue;
            }
            String[] parts = name.substring(PREFIX_NODE.length()).split("\\.", 3);
            if (parts.length != 3) {
                continue;
            }
            int ordinal;
            try {
                ordinal = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                errors.add("目标实例组节点序号不是整数: " + name);
                continue;
            }
            raw.computeIfAbsent(parts[0], k -> new TreeMap<>())
                    .computeIfAbsent(ordinal, k -> new LinkedHashMap<>())
                    .put(parts[2], props.getProperty(name, "").trim());
        }

        Map<String, List<RouteNode>> groups = new LinkedHashMap<>();
        for (Map.Entry<String, Map<Integer, Map<String, String>>> g : raw.entrySet()) {
            List<RouteNode> nodes = new ArrayList<>();
            for (Map.Entry<Integer, Map<String, String>> n : g.getValue().entrySet()) {
                Map<String, String> f = n.getValue();
                String host = f.get("host");
                if (host == null || host.isEmpty()) {
                    errors.add("目标实例组 " + g.getKey() + " 节点 " + n.getKey() + " 缺少 host");
                    continue;
                }
                int port;
                try {
                    port = Integer.parseInt(f.getOrDefault("port", "0"));
                } catch (NumberFormatException e) {
                    errors.add("目标实例组 " + g.getKey() + " 节点 " + n.getKey() + " 的 port 不是整数");
                    continue;
                }
                if (port <= 0) {
                    errors.add("目标实例组 " + g.getKey() + " 节点 " + n.getKey() + " 缺少 port");
                    continue;
                }
                nodes.add(new RouteNode(g.getKey(), n.getKey(), host, port,
                        f.get("database"), f.get("username"), f.get("password")));
            }
            if (!nodes.isEmpty()) {
                groups.put(g.getKey(), Collections.unmodifiableList(nodes));
            }
        }
        return groups;
    }

    private static MergeRule buildMergeRule(String id, Map<String, String> fields,
                                            List<String> errors, List<String> warnings) {
        String where = "route.merge." + id;
        TablePattern pattern = compilePattern(fields.get("match"), where, errors);
        String target = fields.get("target");
        if (target == null || target.isEmpty()) {
            errors.add(where + ".target 未配置（汇聚目标表必填）");
            return null;
        }
        String targetDb = null;
        String targetTable = target;
        int dot = target.indexOf('.');
        if (dot > 0 && dot < target.length() - 1) {
            targetDb = target.substring(0, dot);
            targetTable = target.substring(dot + 1);
        }
        if (!isIdentifier(targetTable) || (targetDb != null && !isIdentifier(targetDb))) {
            errors.add(where + ".target 不是合法的 [库.]表 标识符: " + target);
            return null;
        }

        MergeRule.PkStrategy pk = MergeRule.PkStrategy.COMPOSITE_SOURCE;
        String pkRaw = fields.get("pk.strategy");
        if (pkRaw != null && !pkRaw.isEmpty()) {
            try {
                pk = MergeRule.PkStrategy.valueOf(pkRaw.toUpperCase());
            } catch (IllegalArgumentException e) {
                errors.add(where + ".pk.strategy 取值非法: " + pkRaw);
                return null;
            }
        }

        List<String> tagColumns = null;
        String tagRaw = fields.get("tag.columns");
        if (tagRaw != null) {
            tagColumns = new ArrayList<>();
            for (String t : tagRaw.split(",")) {
                String col = t.trim();
                if (col.isEmpty()) {
                    continue;
                }
                if (!MergeRule.DEFAULT_TAG_COLUMNS.contains(col)) {
                    errors.add(where + ".tag.columns 含不支持的来源标识列 " + col
                            + "（支持: " + MergeRule.DEFAULT_TAG_COLUMNS + "）");
                    return null;
                }
                tagColumns.add(col);
            }
        }
        if (pk == MergeRule.PkStrategy.COMPOSITE_SOURCE && tagColumns != null && tagColumns.isEmpty()) {
            errors.add(where + " 的主键策略为 COMPOSITE_SOURCE，但来源标识列为空——"
                    + "复合主键无从构造，多源同主键行会互相覆盖");
            return null;
        }
        if (pk == MergeRule.PkStrategy.KEEP) {
            warnings.add(where + " 使用 KEEP 主键策略：要求源主键跨所有来源全局唯一，"
                    + "否则幂等装载会让同主键行互相覆盖");
        }

        MergeRule.DdlPolicy ddl = MergeRule.DdlPolicy.FIRST_WINS;
        String ddlRaw = fields.get("ddl.policy");
        if (ddlRaw != null && !ddlRaw.isEmpty()) {
            try {
                ddl = MergeRule.DdlPolicy.valueOf(ddlRaw.toUpperCase());
            } catch (IllegalArgumentException e) {
                errors.add(where + ".ddl.policy 取值非法: " + ddlRaw);
                return null;
            }
        }
        return pattern == null ? null
                : new MergeRule(id, pattern, targetDb, targetTable, pk, tagColumns, ddl);
    }

    private static SplitRule buildSplitRule(String id, Map<String, String> fields,
                                            Map<String, List<RouteNode>> nodeGroups,
                                            List<String> errors, List<String> warnings) {
        String where = "route.split." + id;
        TablePattern pattern = compilePattern(fields.get("match"), where, errors);
        String shardKey = fields.get("shard.key");
        if (shardKey == null || shardKey.isEmpty()) {
            errors.add(where + ".shard.key 未配置（拆分必须指定分片键）");
            return null;
        }
        ShardAlgorithm algo = ShardAlgorithm.parse(fields.get("algo"), null);
        if (algo == null) {
            errors.add(where + ".algo 取值非法或缺失（HASH_MOD|RANGE|LIST|DATE_FORMAT）: "
                    + fields.get("algo"));
            return null;
        }

        int count = 0;
        List<long[]> ranges = null;
        Map<String, Integer> listMapping = null;
        DateTimeFormatter dateFormatter = null;
        try {
            switch (algo) {
                case HASH_MOD:
                    count = Integer.parseInt(fields.getOrDefault("count", "0"));
                    if (count <= 0) {
                        errors.add(where + ".count 必须为正整数（HASH_MOD 分片数）");
                        return null;
                    }
                    break;
                case RANGE:
                    ranges = SplitRule.parseRanges(require(fields, "range", where));
                    count = ranges.size();
                    break;
                case LIST:
                    listMapping = SplitRule.parseList(require(fields, "list", where));
                    count = listMapping.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
                    break;
                case DATE_FORMAT:
                default:
                    dateFormatter = DateTimeFormatter.ofPattern(require(fields, "date.format", where));
                    count = 0;   // 时间分片不可枚举
                    break;
            }
        } catch (RuntimeException e) {
            errors.add(where + " 的分片参数非法: " + e.getMessage());
            return null;
        }

        ShardTemplate dbTemplate = compileTemplate(fields.get("target.db"), where + ".target.db", errors);
        ShardTemplate tableTemplate = compileTemplate(fields.get("target.table"), where + ".target.table", errors);
        ShardTemplate nodeTemplate = compileTemplate(fields.get("target.node"), where + ".target.node", errors);

        String nodeGroup = emptyToNull(fields.get("target.group"));
        if (nodeGroup != null && !nodeGroups.containsKey(nodeGroup)) {
            errors.add(where + ".target.group 引用了未配置的目标实例组: " + nodeGroup
                    + "（需配 route.node." + nodeGroup + ".<序号>.host/port）");
            return null;
        }
        // 库名、表名、实例三者至少有一个随分片变化，否则"拆分"会把所有分片写进同一张表
        boolean varies = (dbTemplate != null && dbTemplate.hasPlaceholder())
                || (tableTemplate != null && tableTemplate.hasPlaceholder())
                || nodeGroup != null;
        if (!varies) {
            errors.add(where + " 的库名/表名都不随 ${shard} 变化且未配目标实例组："
                    + "所有分片会落到同一张表，这不是拆分");
            return null;
        }
        if (!algo.isEnumerable()) {
            if (requiresNumeric(dbTemplate) || requiresNumeric(tableTemplate) || requiresNumeric(nodeTemplate)) {
                errors.add(where + " 使用 DATE_FORMAT 分片（分片标识是时间串），"
                        + "模板不能用 ${shard/N} / ${shard%N} 运算");
                return null;
            }
            if (nodeGroup != null && nodeTemplate == null) {
                errors.add(where + " 使用 DATE_FORMAT 分片且配了目标实例组，"
                        + "必须显式配 target.node 指定落到哪个实例（无法按分片号推导）");
                return null;
            }
        }

        SplitRule.UnroutedPolicy unrouted = SplitRule.UnroutedPolicy.BROADCAST;
        String unroutedRaw = fields.get("unrouted");
        if (unroutedRaw != null && !unroutedRaw.isEmpty()) {
            try {
                unrouted = SplitRule.UnroutedPolicy.valueOf(unroutedRaw.toUpperCase());
            } catch (IllegalArgumentException e) {
                errors.add(where + ".unrouted 取值非法: " + unroutedRaw);
                return null;
            }
        }
        if (unrouted == SplitRule.UnroutedPolicy.BROADCAST && !algo.isEnumerable()) {
            // 分片不可枚举就没法广播，降级投递死信而不是静默丢行
            warnings.add(where + " 使用 DATE_FORMAT 分片，无法广播未路由行，未路由策略降级为 DEADLETTER");
            unrouted = SplitRule.UnroutedPolicy.DEADLETTER;
        }

        if (pattern == null) {
            return null;
        }
        SplitRule rule = new SplitRule(id, pattern, shardKey, algo, count, dbTemplate, tableTemplate,
                nodeGroup, nodeTemplate, unrouted, ranges, listMapping, dateFormatter);

        // 渲染样例分片，确保模板产出的是合法标识符（把"建表时才发现库名非法"提前到加载期）
        ShardKey sample = algo.isEnumerable() ? ShardKey.ofIndex(0)
                : ShardKey.ofToken(dateFormatter.format(SAMPLE_TIME));
        if (dbTemplate != null && !isIdentifier(dbTemplate.render(sample))) {
            errors.add(where + ".target.db 渲染出的库名不是合法标识符: " + dbTemplate.render(sample));
            return null;
        }
        if (tableTemplate != null && !isIdentifier(tableTemplate.render(sample))) {
            errors.add(where + ".target.table 渲染出的表名不是合法标识符: " + tableTemplate.render(sample));
            return null;
        }
        return rule;
    }

    private static boolean requiresNumeric(ShardTemplate template) {
        return template != null && template.requiresNumericShard();
    }

    private static String require(Map<String, String> fields, String key, String where) {
        String v = fields.get(key);
        if (v == null || v.isEmpty()) {
            throw new IllegalArgumentException(where + "." + key + " 未配置");
        }
        return v;
    }

    private static TablePattern compilePattern(String spec, String where, List<String> errors) {
        if (spec == null || spec.isEmpty()) {
            errors.add(where + ".match 未配置");
            return null;
        }
        try {
            return TablePattern.compile(spec);
        } catch (IllegalArgumentException e) {
            errors.add(where + ".match 非法: " + e.getMessage());
            return null;
        }
    }

    private static ShardTemplate compileTemplate(String raw, String where, List<String> errors) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        try {
            return ShardTemplate.compile(raw);
        } catch (IllegalArgumentException e) {
            errors.add(where + " 非法: " + e.getMessage());
            return null;
        }
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    private static boolean isIdentifier(String s) {
        return s != null && IDENTIFIER.matcher(s).matches();
    }

    // ---------- 对外 ----------

    public Mode getMode() {
        return mode;
    }

    /** 未配置路由（1:1）。 */
    public boolean isEmpty() {
        return mode == Mode.NONE;
    }

    public boolean isValid() {
        return errors.isEmpty();
    }

    public List<String> getErrors() {
        return errors;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public List<MergeRule> getMergeRules() {
        return mergeRules;
    }

    public List<SplitRule> getSplitRules() {
        return splitRules;
    }

    public Map<String, List<RouteNode>> getNodeGroups() {
        return nodeGroups;
    }

    public List<RouteNode> getNodes(String groupId) {
        return nodeGroups.getOrDefault(groupId, Collections.emptyList());
    }

    /** 按节点 id（{@code 组#序号}）取节点；不存在返回 null。 */
    public RouteNode getNode(String nodeId) {
        if (nodeId == null) {
            return null;
        }
        int hash = nodeId.indexOf('#');
        if (hash <= 0) {
            return null;
        }
        for (RouteNode node : getNodes(nodeId.substring(0, hash))) {
            if (node.id().equals(nodeId)) {
                return node;
            }
        }
        return null;
    }

    /** 目标库名与源库同名的默认解析（单库任务/测试用）。 */
    public TableRouter router() {
        return router(Function.identity());
    }

    /**
     * 构造路由器。
     *
     * @param defaultTargetDb 源库 → 任务默认目标库的解析（多库任务传
     *                        {@code MigrationConfig::getTargetDatabaseFor}，让未写目标库的规则
     *                        沿用既有的库名映射，不在路由层重复实现一遍）
     * @throws IllegalStateException 配置有校验错误——路由错了就是数据写错地方，必须 fail-stop
     */
    public TableRouter router(Function<String, String> defaultTargetDb) {
        if (!isValid()) {
            throw new IllegalStateException("路由配置非法，拒绝启动: " + String.join("; ", errors));
        }
        Function<String, String> resolver = defaultTargetDb == null ? Function.identity() : defaultTargetDb;
        switch (mode) {
            case MERGE:
                return new MergeRouter(mergeRules, resolver);
            case SPLIT:
                return new SplitRouter(splitRules, resolver, nodeGroups);
            case NONE:
            default:
                return IdentityRouter.INSTANCE;
        }
    }

    @Override
    public String toString() {
        return "RoutingConfig{mode=" + mode + ", merge=" + mergeRules.size()
                + ", split=" + splitRules.size() + ", nodeGroups=" + nodeGroups.keySet()
                + ", errors=" + errors.size() + "}";
    }
}
