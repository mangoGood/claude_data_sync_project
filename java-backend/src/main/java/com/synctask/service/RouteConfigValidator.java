package com.synctask.service;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 聚合路由配置（{@code route_config} JSON）的结构校验。
 *
 * <p><b>这不是最终权威</b>：真正的解析与语义校验在引擎侧的
 * {@code com.migration.common.route.RoutingConfig}，任务启动时非法即 fail-stop。
 * backend 不依赖引擎工程（两边是独立构建），所以这里只挡"UI 能填错的那些"——
 * 缺字段、枚举写错、分片数非正、模板不含 {@code ${shard}}、标识符非法。
 * 目的是让用户在<b>点保存的时候</b>就看到错，而不是任务起不来再回来翻日志。
 */
public final class RouteConfigValidator {

    private static final Gson GSON = new Gson();
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");
    private static final List<String> PK_STRATEGIES = List.of("COMPOSITE_SOURCE", "KEEP");
    private static final List<String> DDL_POLICIES = List.of("FIRST_WINS", "SKIP", "MANUAL");
    private static final List<String> ALGORITHMS = List.of("HASH_MOD", "RANGE", "LIST", "DATE_FORMAT");
    private static final List<String> UNROUTED = List.of("BROADCAST", "DEADLETTER", "ERROR");
    /** 关系库侧支持聚合路由的引擎（含异构 mysql↔pg），与引擎侧 RoutingConfig 的白名单一致 */
    private static final List<String> ROUTABLE_ENGINES = List.of("mysql", "postgresql");
    /** 文档型引擎：路由由各自引擎实现（mongo 走集合、ES 走索引），不读 JDBC 侧的 route.* */
    private static final List<String> DOCUMENT_ENGINES = List.of("mongodb", "elasticsearch");
    private static final String[] COLUMN_PROCESSING_KEYS = {"columnFilter", "columnMapping", "extraColumns"};

    private RouteConfigValidator() {
    }

    /**
     * 校验并返回规范化后的 JSON；不合法时抛 {@link IllegalArgumentException}（消息直接给用户看）。
     *
     * @param json 前端提交的路由配置；空串/null 表示清除路由（回到 1:1）
     */
    @SuppressWarnings("unchecked")
    public static String validate(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        Map<String, Object> root;
        try {
            root = GSON.fromJson(json, Map.class);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("路由配置不是合法的 JSON: " + e.getMessage());
        }
        if (root == null) {
            return null;
        }
        String mode = str(root.get("mode"), "NONE").toUpperCase();
        if ("NONE".equals(mode)) {
            return null;
        }
        if (!"MERGE".equals(mode) && !"SPLIT".equals(mode)) {
            throw new IllegalArgumentException("路由模式只能是 NONE / MERGE / SPLIT，当前: " + mode);
        }

        List<String> errors = new ArrayList<>();
        if ("MERGE".equals(mode)) {
            List<Map<String, Object>> rules = (List<Map<String, Object>>) root.get("merge");
            if (rules == null || rules.isEmpty()) {
                errors.add("汇聚模式至少要有一条规则");
            } else {
                for (int i = 0; i < rules.size(); i++) {
                    validateMergeRule(rules.get(i), i + 1, errors);
                }
            }
            validateLegs((List<Map<String, Object>>) root.get("legs"), errors);
        } else {
            List<Map<String, Object>> rules = (List<Map<String, Object>>) root.get("split");
            if (rules == null || rules.isEmpty()) {
                errors.add("拆分模式至少要有一条规则");
            } else {
                for (int i = 0; i < rules.size(); i++) {
                    validateSplitRule(rules.get(i), i + 1, errors);
                }
            }
            if (root.get("legs") != null && !((List<?>) root.get("legs")).isEmpty()) {
                errors.add("拆分不支持跨实例源（legs 只对汇聚有意义）");
            }
        }

        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join("；", errors));
        }
        return GSON.toJson(root);
    }

    /**
     * 任务级适用性校验：这条任务的<b>库类型与任务类型</b>允不允许用聚合路由。
     *
     * <p>与 {@link #validate} 分开：那个只看路由 JSON 本身写得对不对，这个看它和任务其余配置
     * 兼不兼容——两边的入口不同（改路由、改同步对象、启动任务都要判），合在一起会漏。
     *
     * <p>口径必须与引擎侧 {@code RoutingConfig#validateEngineSupport} 一致；这里挡在保存/启动时，
     * 引擎那道挡的是绕过接口直接改 config.properties 的情况，两道都要有。
     *
     * @param syncObjects 任务的 syncObjects JSON（保留参数：调用方三处入口一致，后续判定要用）
     * @throws IllegalArgumentException 不允许时抛出，消息直接给用户看
     */
    public static void assertApplicable(String routeConfig, String sourceType, String targetType,
                                        String taskType, String syncObjects) {
        if (routeConfig == null || routeConfig.trim().isEmpty()) {
            return;
        }
        String source = lower(sourceType);
        String target = lower(targetType);
        String pair = source + "→" + target;
        if ("redis".equals(source) || "redis".equals(target)) {
            throw new IllegalArgumentException("Redis 不支持聚合路由：Redis 没有表的概念，"
                    + "所谓汇聚/拆分只能是 key 前缀命名空间的合并或分裂，与分库分表不是一回事");
        }
        if ("tidb".equals(source)) {
            throw new IllegalArgumentException("TiDB 源暂不支持聚合路由：增量走 TiCDC canal-json，"
                    + "路由改写未在该链路验证");
        }
        // Mongo / ES 的路由由各自引擎实现，与 JDBC 链路的规则同形（见 MongoSyncMain / ElasticSyncMain）
        boolean documentEngine = DOCUMENT_ENGINES.contains(source) || DOCUMENT_ENGINES.contains(target);
        if (!documentEngine && !source.isEmpty() && !target.isEmpty()
                && (!ROUTABLE_ENGINES.contains(source) || !ROUTABLE_ENGINES.contains(target))) {
            throw new IllegalArgumentException("库对 " + pair + " 暂不支持聚合路由："
                    + "幂等 upsert 装载只实现了 MySQL 与 PostgreSQL 方言");
        }
        // MERGE_LEG 是跨实例汇聚派生出的隐藏子任务，天然带父任务的路由配置，必须放行
        if (taskType != null && !"SYNC".equals(taskType) && !"MERGE_LEG".equals(taskType)) {
            throw new IllegalArgumentException("聚合路由目前只支持实时同步任务，"
                    + "灾备/订阅任务暂不支持（当前任务类型: " + taskType + "）");
        }
    }

    /**
     * 任务是否配置了列处理（syncObjects 任一库 entry 携带非空的
     * columnFilter / columnMapping / extraColumns）。
     */
    @SuppressWarnings("unchecked")
    public static boolean hasColumnProcessing(String syncObjectsJson) {
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            return false;
        }
        try {
            Map<String, Object> raw = GSON.fromJson(syncObjectsJson, Map.class);
            if (raw == null) {
                return false;
            }
            for (Object value : raw.values()) {
                if (!(value instanceof Map)) {
                    continue;
                }
                Map<?, ?> entry = (Map<?, ?>) value;
                for (String key : COLUMN_PROCESSING_KEYS) {
                    Object cp = entry.get(key);
                    if (cp instanceof Map && !((Map<?, ?>) cp).isEmpty()) {
                        return true;
                    }
                }
            }
        } catch (RuntimeException e) {
            return false;
        }
        return false;
    }

    private static String lower(String raw) {
        return raw == null ? "" : raw.trim().toLowerCase();
    }

    private static void validateMergeRule(Map<String, Object> rule, int index, List<String> errors) {
        String where = "汇聚规则 " + index;
        String match = str(rule.get("match"), "");
        if (match.isEmpty()) {
            errors.add(where + " 缺少匹配式（如 shard_db_*.order_*）");
        } else if (!match.toLowerCase().startsWith("regex:") && match.indexOf('.') <= 0) {
            errors.add(where + " 的匹配式必须写成 <库>.<表>");
        }
        String target = str(rule.get("target"), "");
        if (target.isEmpty()) {
            errors.add(where + " 缺少目标表");
        } else {
            String table = target.contains(".") ? target.substring(target.indexOf('.') + 1) : target;
            String db = target.contains(".") ? target.substring(0, target.indexOf('.')) : null;
            if (!isIdentifier(table) || (db != null && !isIdentifier(db))) {
                errors.add(where + " 的目标表不是合法的 [库.]表 标识符: " + target);
            }
        }
        checkEnum(rule.get("pkStrategy"), PK_STRATEGIES, where + " 的主键策略", errors);
        checkEnum(rule.get("ddlPolicy"), DDL_POLICIES, where + " 的 DDL 策略", errors);
    }

    private static void validateSplitRule(Map<String, Object> rule, int index, List<String> errors) {
        String where = "拆分规则 " + index;
        if (str(rule.get("match"), "").isEmpty()) {
            errors.add(where + " 缺少匹配式");
        }
        if (str(rule.get("shardKey"), "").isEmpty()) {
            errors.add(where + " 缺少分片键");
        }
        String algo = str(rule.get("algo"), "").toUpperCase();
        if (!ALGORITHMS.contains(algo)) {
            errors.add(where + " 的分片算法只能是 " + ALGORITHMS + "，当前: " + algo);
            return;
        }
        if ("HASH_MOD".equals(algo)) {
            int count = intOf(rule.get("count"));
            if (count <= 0) {
                errors.add(where + " 的分片数必须为正整数");
            }
        } else if ("RANGE".equals(algo) && str(rule.get("range"), "").isEmpty()) {
            errors.add(where + " 使用 RANGE 分片，必须给出区间表（如 0:1000,1000:5000）");
        } else if ("LIST".equals(algo) && str(rule.get("list"), "").isEmpty()) {
            errors.add(where + " 使用 LIST 分片，必须给出枚举表（如 CN:0,US:1）");
        } else if ("DATE_FORMAT".equals(algo)) {
            if (str(rule.get("dateFormat"), "").isEmpty()) {
                errors.add(where + " 使用 DATE_FORMAT 分片，必须给出时间格式（如 yyyyMM）");
            }
            errors.add(where + " 使用 DATE_FORMAT 分片，分片不可枚举，全量阶段无法预建目标表，暂不支持");
        }

        String targetDb = str(rule.get("targetDb"), "");
        String targetTable = str(rule.get("targetTable"), "");
        if (targetDb.isEmpty() && targetTable.isEmpty()) {
            errors.add(where + " 至少要配目标库模板或目标表模板");
        }
        if (!targetDb.contains("${shard") && !targetTable.contains("${shard")) {
            errors.add(where + " 的库名/表名都不随 ${shard} 变化，所有分片会落到同一张表");
        }
        checkEnum(rule.get("unrouted"), UNROUTED, where + " 的未路由策略", errors);
    }

    private static void validateLegs(List<Map<String, Object>> legs, List<String> errors) {
        if (legs == null || legs.isEmpty()) {
            return;
        }
        List<String> seen = new ArrayList<>();
        for (int i = 0; i < legs.size(); i++) {
            Map<String, Object> leg = legs.get(i);
            String where = "跨实例源 " + (i + 1);
            String nodeId = str(leg.get("nodeId"), "");
            if (nodeId.isEmpty()) {
                errors.add(where + " 缺少实例标识（nodeId，会写进来源标识列）");
            } else if (seen.contains(nodeId)) {
                errors.add("跨实例源的实例标识重复: " + nodeId + "（来源列靠它区分同名库表）");
            } else {
                seen.add(nodeId);
            }
            if (str(leg.get("host"), "").isEmpty()) {
                errors.add(where + " 缺少 host");
            }
            if (intOf(leg.get("port")) <= 0) {
                errors.add(where + " 缺少 port");
            }
        }
    }

    private static void checkEnum(Object value, List<String> allowed, String what, List<String> errors) {
        String s = str(value, "");
        if (!s.isEmpty() && !allowed.contains(s.toUpperCase())) {
            errors.add(what + "只能是 " + allowed + "，当前: " + s);
        }
    }

    private static boolean isIdentifier(String s) {
        return s != null && IDENTIFIER.matcher(s).matches();
    }

    private static String str(Object o, String def) {
        return o == null ? def : String.valueOf(o).trim();
    }

    private static int intOf(Object o) {
        if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        try {
            return o == null ? 0 : (int) Double.parseDouble(String.valueOf(o).trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
