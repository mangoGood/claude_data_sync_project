package com.migration.common.route;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

/**
 * 文档型引擎（MongoDB 集合 / Elasticsearch 索引）的聚合路由。
 *
 * <p>规则模型与关系库完全共用 {@link RoutingConfig}——"库.表"在这里读作"库.集合"或"库.表→索引"，
 * 汇聚/拆分的匹配式、分片算法、模板语法一个字都不用改。差别只在<b>落点长什么样</b>
 * 以及<b>主键怎么防撞</b>：
 *
 * <ul>
 *   <li><b>汇聚</b>：N 个集合/索引并成一个。文档的 {@code _id} 必须重新构造成
 *       {@code <来源标识>|<原 _id>}——不然两个来源里 {@code _id} 相同的文档会互相覆盖，
 *       与关系库那边"必须用复合主键"是同一件事，也是同一种静默丢数据。
 *       同时把来源标识写成文档字段，便于按来源筛选与对数。</li>
 *   <li><b>拆分</b>：按文档里某个字段的值算分片，落到 {@code orders_0..N} 这样的集合/索引。
 *       分片函数直接复用 {@link SplitRule}，与关系库同口径——同一个 user_id 在两条链路上
 *       必须算到同一片，否则同一份数据在两个引擎里的分布对不上。</li>
 * </ul>
 *
 * <p>本类只做<b>名字与标识的计算</b>，不碰任何驱动 API：Mongo 与 ES 的写入方式差得远，
 * 但"这条文档该去哪、_id 该叫什么"是同一套逻辑，放在一处才不会两边漂。
 */
public final class DocumentRouter {

    /** 来源标识在文档里的字段名（与关系库的 _src_* 列同名，便于对照） */
    public static final String FIELD_SRC_NODE = "_src_node";
    public static final String FIELD_SRC_DB = "_src_db";
    public static final String FIELD_SRC_TABLE = "_src_table";

    private final RoutingConfig config;
    private final String nodeId;

    private DocumentRouter(RoutingConfig config, String nodeId) {
        this.config = config;
        this.nodeId = nodeId == null ? "" : nodeId;
    }

    /** 从 config.properties 载入；未配置路由时 {@link #isActive()} 为 false，调用方走原 1:1 路径。 */
    public static DocumentRouter fromProperties(Properties props) {
        RoutingConfig config = RoutingConfig.loadFromProperties(props);
        if (!config.isValid()) {
            throw new IllegalStateException("路由配置非法: " + String.join("; ", config.getErrors()));
        }
        return new DocumentRouter(config, props.getProperty("route.node.id", ""));
    }

    public boolean isActive() {
        return config.getMode() != RoutingConfig.Mode.NONE;
    }

    public boolean isMerge() {
        return config.getMode() == RoutingConfig.Mode.MERGE;
    }

    public boolean isSplit() {
        return config.getMode() == RoutingConfig.Mode.SPLIT;
    }

    /** 该集合/表是否命中路由规则；未命中的按原 1:1 路径处理。 */
    public boolean matches(String db, String name) {
        if (!isActive()) {
            return false;
        }
        return isMerge() ? findMerge(db, name) != null : findSplit(db, name) != null;
    }

    /** 汇聚落点：目标库 + 目标集合/索引；未命中返回 null。 */
    public Target mergeTarget(String db, String name, String defaultTargetDb) {
        MergeRule rule = findMerge(db, name);
        if (rule == null) {
            return null;
        }
        String targetDb = rule.getTargetDb() != null && !rule.getTargetDb().isEmpty()
                ? rule.getTargetDb() : defaultTargetDb;
        return new Target(targetDb, rule.getTargetTable());
    }

    /** 汇聚的来源标识字段（有序）。 */
    public LinkedHashMap<String, String> mergeTags(String db, String name) {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        MergeRule rule = findMerge(db, name);
        if (rule == null) {
            return tags;
        }
        for (String col : rule.getTagColumns()) {
            String value = rule.tagValue(col, nodeId, db, name);
            if (value != null) {
                tags.put(col, value);
            }
        }
        return tags;
    }

    /**
     * 汇聚后的文档标识：{@code <来源标识各段用 | 连接>|<原 _id>}。
     *
     * <p>不能沿用原 {@code _id}：两个来源里 {@code _id} 相同的文档会 upsert 互相覆盖，
     * 数据只会少、不会报错。前缀放在前面而不是后面，是为了让同一来源的文档在
     * {@code _id} 上天然聚簇（范围扫描/对数都按来源切片）。
     */
    public String mergedId(String db, String name, Object originalId) {
        StringBuilder sb = new StringBuilder();
        for (String value : mergeTags(db, name).values()) {
            sb.append(value).append('|');
        }
        return sb.append(String.valueOf(originalId)).toString();
    }

    /** 拆分：全部分片落点（预建索引/集合、广播删除时用）；未命中或不可枚举返回空列表。 */
    public List<Target> allShards(String db, String name, String defaultTargetDb) {
        SplitRule rule = findSplit(db, name);
        List<Target> targets = new ArrayList<>();
        if (rule == null) {
            return targets;
        }
        for (RouteTarget t : rule.allTargets(defaultTargetDb, name, 0)) {
            targets.add(new Target(t.getDatabase(), t.getTable()));
        }
        return targets;
    }

    /** 拆分的分片键字段名；未命中返回 null。 */
    public String shardKeyField(String db, String name) {
        SplitRule rule = findSplit(db, name);
        return rule == null ? null : rule.getShardKey();
    }

    /**
     * 拆分落点：按分片键的值算。值为 null / 算不出分片时返回 null，
     * 调用方按未路由策略处理（默认广播到每一片）。
     */
    public Target shardOf(String db, String name, Object shardKeyValue, String defaultTargetDb) {
        SplitRule rule = findSplit(db, name);
        if (rule == null) {
            return null;
        }
        ShardKey key = rule.resolveShard(shardKeyValue);
        if (key == null) {
            return null;
        }
        RouteTarget t = rule.toTarget(key, defaultTargetDb, name, 0);
        return new Target(t.getDatabase(), t.getTable());
    }

    /** 未路由行的处置策略（分片键为空/算不出分片时）。 */
    public SplitRule.UnroutedPolicy unroutedPolicy(String db, String name) {
        SplitRule rule = findSplit(db, name);
        return rule == null ? SplitRule.UnroutedPolicy.BROADCAST : rule.getUnroutedPolicy();
    }

    private static boolean matches(TablePattern pattern, String db, String name) {
        return pattern.matches(db, name) || pattern.matchesIgnoreCase(db, name);
    }

    private MergeRule findMerge(String db, String name) {
        if (!isMerge()) {
            return null;
        }
        for (MergeRule rule : config.getMergeRules()) {
            if (matches(rule.getPattern(), db, name)) {
                return rule;
            }
        }
        return null;
    }

    private SplitRule findSplit(String db, String name) {
        if (!isSplit()) {
            return null;
        }
        for (SplitRule rule : config.getSplitRules()) {
            if (matches(rule.getPattern(), db, name)) {
                return rule;
            }
        }
        return null;
    }

    /** 一个落点：目标库 + 目标集合/索引名。 */
    public static final class Target {
        private final String database;
        private final String name;

        Target(String database, String name) {
            this.database = database;
            this.name = name;
        }

        public String getDatabase() {
            return database;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return database + "." + name;
        }
    }
}
