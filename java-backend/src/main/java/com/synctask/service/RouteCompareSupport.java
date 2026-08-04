package com.synctask.service;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 行数对比的路由感知：把一张源表在<b>路由之后</b>的落点解析出来，让对比按真实落点统计。
 *
 * <p>没有它的话，汇聚任务会拿源表 {@code shard_db_1.order_001} 去目标端找同名表（根本不存在），
 * 拆分任务会拿 {@code app.orders} 去找同名表（数据其实散在 8 张分片表里）——
 * 两种情况的对比结果都是"目标端 0 行"，看着像同步全丢了。
 *
 * <p>模板渲染（{@code orders_${shard}}）在这里是<b>镜像实现</b>：权威定义在引擎侧的
 * {@code com.migration.common.route.ShardTemplate}，backend 不依赖引擎工程，
 * 所以按同一套语法重写一遍，只用于对比时枚举分片表名。语法只有三种形态，
 * 单测与引擎侧用例一一对应。
 */
public final class RouteCompareSupport {

    private static final Gson GSON = new Gson();
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{shard(?:([/%])(\\d+))?}");

    /** 汇聚落点：目标表 + 按来源标识列的过滤条件（对比时只统计本源表贡献的那部分行）。 */
    public static final class MergeTarget {
        public final String database;
        public final String table;
        /** 来源标识列 → 值；空 map 表示目标表没有来源列（KEEP 策略且用户没配） */
        public final Map<String, String> tagFilters;

        MergeTarget(String database, String table, Map<String, String> tagFilters) {
            this.database = database;
            this.table = table;
            this.tagFilters = tagFilters;
        }
    }

    /** 拆分落点：全部分片表（对比时把各片行数加起来）。 */
    public static final class SplitTargets {
        public final List<String[]> shards = new ArrayList<>();   // [db, table]
    }

    private RouteCompareSupport() {
    }

    /** 任务是否配置了路由（配置了就走路由感知对比）。 */
    @SuppressWarnings("unchecked")
    public static String modeOf(String routeConfigJson) {
        if (routeConfigJson == null || routeConfigJson.trim().isEmpty()) {
            return "NONE";
        }
        try {
            Map<String, Object> root = GSON.fromJson(routeConfigJson, Map.class);
            if (root == null) {
                return "NONE";
            }
            String mode = root.get("mode") == null ? "NONE" : String.valueOf(root.get("mode")).toUpperCase();
            return mode;
        } catch (RuntimeException e) {
            return "NONE";
        }
    }

    /**
     * 解析汇聚落点。
     *
     * @param nodeId 本任务的来源实例标识：路由配置里写了就用它，否则用源实例 {@code host:port}
     *               —— 与引擎侧 {@code MigrationConfig#getRouteNodeId} 的兜底规则保持一致，
     *               否则过滤条件会与实际写入的值对不上。
     * @return 命中规则时返回落点；未命中返回 null（该表按 1:1 对比）
     */
    @SuppressWarnings("unchecked")
    public static MergeTarget mergeTargetOf(String routeConfigJson, String sourceDb, String sourceTable,
                                            String defaultTargetDb, String nodeId) {
        Map<String, Object> root = parse(routeConfigJson);
        if (root == null || !"MERGE".equals(modeOf(routeConfigJson))) {
            return null;
        }
        List<Map<String, Object>> rules = (List<Map<String, Object>>) root.get("merge");
        if (rules == null) {
            return null;
        }
        for (Map<String, Object> rule : rules) {
            String match = str(rule.get("match"));
            if (!matches(match, sourceDb, sourceTable)) {
                continue;
            }
            String target = str(rule.get("target"));
            String db = defaultTargetDb;
            String table = target;
            int dot = target.indexOf('.');
            if (dot > 0 && dot < target.length() - 1) {
                db = target.substring(0, dot);
                table = target.substring(dot + 1);
            }
            Map<String, String> filters = new LinkedHashMap<>();
            List<String> tagColumns = (List<String>) rule.get("tagColumns");
            if (tagColumns == null || tagColumns.isEmpty()) {
                tagColumns = List.of("_src_node", "_src_db", "_src_table");
            }
            for (String col : tagColumns) {
                switch (col) {
                    case "_src_node":
                        if (nodeId != null && !nodeId.isEmpty()) {
                            filters.put(col, nodeId);
                        }
                        break;
                    case "_src_db":
                        filters.put(col, sourceDb);
                        break;
                    case "_src_table":
                        filters.put(col, sourceTable);
                        break;
                    default:
                        break;
                }
            }
            return new MergeTarget(db, table, filters);
        }
        return null;
    }

    /**
     * 解析拆分落点（枚举全部分片表）。
     *
     * @return 命中规则时返回全部分片；未命中或分片不可枚举返回 null
     */
    @SuppressWarnings("unchecked")
    public static SplitTargets splitTargetsOf(String routeConfigJson, String sourceDb, String sourceTable,
                                              String defaultTargetDb) {
        Map<String, Object> root = parse(routeConfigJson);
        if (root == null || !"SPLIT".equals(modeOf(routeConfigJson))) {
            return null;
        }
        List<Map<String, Object>> rules = (List<Map<String, Object>>) root.get("split");
        if (rules == null) {
            return null;
        }
        for (Map<String, Object> rule : rules) {
            if (!matches(str(rule.get("match")), sourceDb, sourceTable)) {
                continue;
            }
            int count = shardCount(rule);
            if (count <= 0) {
                return null;   // DATE_FORMAT 之类不可枚举的分片，对比退回 1:1（会被标为不支持）
            }
            String dbTemplate = str(rule.get("targetDb"));
            String tableTemplate = str(rule.get("targetTable"));
            SplitTargets targets = new SplitTargets();
            for (int shard = 0; shard < count; shard++) {
                String db = dbTemplate.isEmpty() ? defaultTargetDb : render(dbTemplate, shard);
                String table = tableTemplate.isEmpty() ? sourceTable : render(tableTemplate, shard);
                targets.shards.add(new String[]{db, table});
            }
            return targets;
        }
        return null;
    }

    /** 分片数：HASH_MOD 直接给，RANGE/LIST 由表长度推导（与引擎侧口径一致）。 */
    private static int shardCount(Map<String, Object> rule) {
        String algo = str(rule.get("algo")).toUpperCase();
        if ("RANGE".equals(algo)) {
            return countCsv(str(rule.get("range")));
        }
        if ("LIST".equals(algo)) {
            int max = -1;
            for (String part : str(rule.get("list")).split(",")) {
                int colon = part.lastIndexOf(':');
                if (colon > 0) {
                    try {
                        max = Math.max(max, Integer.parseInt(part.substring(colon + 1).trim()));
                    } catch (NumberFormatException ignored) {
                        // 非法条目由保存时的校验挡住，这里忽略
                    }
                }
            }
            return max + 1;
        }
        if ("HASH_MOD".equals(algo)) {
            Object count = rule.get("count");
            if (count instanceof Number) {
                return ((Number) count).intValue();
            }
            try {
                return Integer.parseInt(str(count));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;   // DATE_FORMAT：不可枚举
    }

    private static int countCsv(String csv) {
        int n = 0;
        for (String part : csv.split(",")) {
            if (!part.trim().isEmpty()) {
                n++;
            }
        }
        return n;
    }

    /** 渲染 {@code ${shard}} / {@code ${shard/N}} / {@code ${shard%N}}。 */
    static String render(String template, int shard) {
        java.util.regex.Matcher m = PLACEHOLDER.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String op = m.group(1);
            String operand = m.group(2);
            int value = shard;
            if (op != null && operand != null) {
                int n = Integer.parseInt(operand);
                if (n > 0) {
                    value = "/".equals(op) ? shard / n : shard % n;
                }
            }
            m.appendReplacement(out, String.valueOf(value));
        }
        m.appendTail(out);
        return out.toString();
    }

    /** 通配匹配（与引擎侧 TablePattern 同语义：{@code *} 任意串、{@code ?} 单字符、{@code regex:} 前缀走正则）。 */
    static boolean matches(String spec, String db, String table) {
        if (spec == null || spec.isEmpty() || db == null || table == null) {
            return false;
        }
        String full = db + "." + table;
        String regex;
        if (spec.regionMatches(true, 0, "regex:", 0, 6)) {
            regex = spec.substring(6).trim();
        } else {
            StringBuilder sb = new StringBuilder();
            for (char c : spec.toCharArray()) {
                if (c == '*') {
                    sb.append(".*");
                } else if (c == '?') {
                    sb.append('.');
                } else {
                    if ("\\.[]{}()+-^$|".indexOf(c) >= 0) {
                        sb.append('\\');
                    }
                    sb.append(c);
                }
            }
            regex = sb.toString();
        }
        try {
            return Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(full).matches();
        } catch (RuntimeException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            return GSON.fromJson(json, Map.class);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o).trim();
    }
}
