package com.migration.agent.service;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 把后端下发的路由配置 JSON 展开成引擎读的 {@code route.*} 属性。
 *
 * <p>展开后立刻用引擎自己的 {@code RoutingConfig} 校验一遍——同一套解析器，
 * 配置错了在<b>下发时</b>就报出来，而不是等全量跑到一半才发现表名拼错。
 *
 * <p>JSON 形状（与前端向导的"路由"页签一一对应）：
 * <pre>
 * {"mode":"MERGE","nodeId":"inst-b",
 *  "merge":[{"match":"shard_db_*.order_*","target":"dw.order_all",
 *            "pkStrategy":"COMPOSITE_SOURCE","tagColumns":["_src_db","_src_table"],
 *            "ddlPolicy":"FIRST_WINS"}]}
 * {"mode":"SPLIT",
 *  "split":[{"match":"app.orders","shardKey":"user_id","algo":"HASH_MOD","count":16,
 *            "targetDb":"dw_${shard/2}","targetTable":"orders_${shard}",
 *            "unrouted":"BROADCAST","range":"...","list":"...","dateFormat":"yyyyMM"}]}
 * </pre>
 */
public final class RouteConfigExpander {

    private static final Logger logger = LoggerFactory.getLogger(RouteConfigExpander.class);
    private static final Gson GSON = new Gson();

    private RouteConfigExpander() {
    }

    /**
     * 展开路由配置。传空即清除既有 {@code route.*}（任务改回 1:1 时不能留下失效规则）。
     *
     * @param routeNodeId 本条管线的来源实例标识；空则不下发（引擎用源实例地址兜底）
     * @throws IllegalArgumentException 配置非法（消息里带引擎给出的具体原因）
     */
    @SuppressWarnings("unchecked")
    public static void expand(Properties props, String routeConfigJson, String routeNodeId) {
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith("route.")) {
                props.remove(name);
            }
        }
        if (routeConfigJson == null || routeConfigJson.trim().isEmpty()) {
            return;
        }
        Map<String, Object> root;
        try {
            root = GSON.fromJson(routeConfigJson, Map.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("路由配置不是合法 JSON: " + e.getMessage());
        }
        if (root == null) {
            return;
        }
        String mode = str(root.get("mode"), "NONE").toUpperCase();
        if ("NONE".equals(mode)) {
            return;
        }
        props.setProperty("route.mode", mode);

        String nodeId = routeNodeId != null && !routeNodeId.trim().isEmpty()
                ? routeNodeId.trim() : str(root.get("nodeId"), "");
        if (!nodeId.isEmpty()) {
            props.setProperty("route.node.id", nodeId);
        }

        if ("MERGE".equals(mode)) {
            List<Map<String, Object>> rules = (List<Map<String, Object>>) root.get("merge");
            int i = 1;
            for (Map<String, Object> rule : nullSafe(rules)) {
                String prefix = "route.merge." + i++ + ".";
                put(props, prefix + "match", rule.get("match"));
                put(props, prefix + "target", rule.get("target"));
                put(props, prefix + "pk.strategy", rule.get("pkStrategy"));
                put(props, prefix + "ddl.policy", rule.get("ddlPolicy"));
                Object tags = rule.get("tagColumns");
                if (tags instanceof List && !((List<?>) tags).isEmpty()) {
                    props.setProperty(prefix + "tag.columns", String.join(",",
                            ((List<?>) tags).stream().map(String::valueOf).toList()));
                }
            }
        } else if ("SPLIT".equals(mode)) {
            List<Map<String, Object>> rules = (List<Map<String, Object>>) root.get("split");
            int i = 1;
            for (Map<String, Object> rule : nullSafe(rules)) {
                String prefix = "route.split." + i++ + ".";
                put(props, prefix + "match", rule.get("match"));
                put(props, prefix + "shard.key", rule.get("shardKey"));
                put(props, prefix + "algo", rule.get("algo"));
                putInt(props, prefix + "count", rule.get("count"));
                put(props, prefix + "target.db", rule.get("targetDb"));
                put(props, prefix + "target.table", rule.get("targetTable"));
                put(props, prefix + "target.group", rule.get("targetGroup"));
                put(props, prefix + "target.node", rule.get("targetNode"));
                put(props, prefix + "unrouted", rule.get("unrouted"));
                put(props, prefix + "range", rule.get("range"));
                put(props, prefix + "list", rule.get("list"));
                put(props, prefix + "date.format", rule.get("dateFormat"));
            }
        }

        // 用引擎自己的解析器兜底校验：两边共用同一套语义，不做第二套实现
        com.migration.common.route.RoutingConfig parsed =
                com.migration.common.route.RoutingConfig.loadFromProperties(props);
        if (!parsed.isValid()) {
            throw new IllegalArgumentException("路由配置非法: " + String.join("; ", parsed.getErrors()));
        }
        for (String warning : parsed.getWarnings()) {
            logger.warn("路由配置告警: {}", warning);
        }
        logger.info("路由配置已下发: mode={}, 汇聚规则={}, 拆分规则={}, nodeId={}",
                mode, parsed.getMergeRules().size(), parsed.getSplitRules().size(),
                nodeId.isEmpty() ? "<源实例地址兜底>" : nodeId);
    }

    private static <T> List<T> nullSafe(List<T> list) {
        return list == null ? List.of() : list;
    }

    private static void put(Properties props, String key, Object value) {
        String s = str(value, "");
        if (!s.isEmpty()) {
            props.setProperty(key, s);
        }
    }

    private static void putInt(Properties props, String key, Object value) {
        if (value instanceof Number) {
            props.setProperty(key, String.valueOf(((Number) value).intValue()));
        } else {
            put(props, key, value);
        }
    }

    private static String str(Object o, String def) {
        return o == null ? def : String.valueOf(o).trim();
    }
}
