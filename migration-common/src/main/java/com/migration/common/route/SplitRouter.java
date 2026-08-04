package com.migration.common.route;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 拆分路由（1:N）：按行内分片键的值把每一行路由到对应的目标库/表。
 *
 * <p>算不出分片时按规则的 {@link SplitRule.UnroutedPolicy} 处置：广播到全部分片（默认）、
 * 返回空列表交调用方投递死信、或直接抛 {@link UnroutedRowException} 停任务。
 * <b>没有"静默丢弃"这个选项</b>——那正是分片链路最容易丢数据的地方。
 */
public final class SplitRouter implements TableRouter {

    private final List<SplitRule> rules;
    private final Function<String, String> defaultTargetDb;
    private final Map<String, List<RouteNode>> nodeGroups;

    SplitRouter(List<SplitRule> rules, Function<String, String> defaultTargetDb,
                Map<String, List<RouteNode>> nodeGroups) {
        this.rules = rules;
        this.defaultTargetDb = defaultTargetDb;
        this.nodeGroups = nodeGroups;
    }

    @Override
    public RoutingConfig.Mode mode() {
        return RoutingConfig.Mode.SPLIT;
    }

    @Override
    public boolean isIdentity() {
        return false;
    }

    @Override
    public boolean matches(String sourceDb, String sourceTable) {
        return find(sourceDb, sourceTable) != null;
    }

    @Override
    public String shardKeyColumn(String sourceDb, String sourceTable) {
        SplitRule rule = find(sourceDb, sourceTable);
        return rule == null ? null : rule.getShardKey();
    }

    @Override
    public List<RouteTarget> allTargets(String sourceDb, String sourceTable) {
        SplitRule rule = find(sourceDb, sourceTable);
        if (rule == null) {
            return Collections.singletonList(RouteTarget.identity(sourceDb, sourceTable));
        }
        return rule.allTargets(defaultTargetDb.apply(sourceDb), sourceTable, nodeCount(rule));
    }

    @Override
    public List<RouteTarget> route(String sourceDb, String sourceTable, Object shardKeyValue) {
        SplitRule rule = find(sourceDb, sourceTable);
        if (rule == null) {
            return Collections.singletonList(RouteTarget.identity(sourceDb, sourceTable));
        }
        ShardKey key = rule.resolveShard(shardKeyValue);
        if (key == null) {
            switch (rule.getUnroutedPolicy()) {
                case ERROR:
                    throw new UnroutedRowException(sourceDb, sourceTable, rule.getShardKey(), shardKeyValue);
                case DEADLETTER:
                    return Collections.emptyList();
                case BROADCAST:
                default:
                    // 不可枚举的算法（DATE_FORMAT）在加载期已降级为 DEADLETTER，这里恒可枚举
                    return rule.allTargets(defaultTargetDb.apply(sourceDb), sourceTable, nodeCount(rule));
            }
        }
        return Collections.singletonList(
                rule.toTarget(key, defaultTargetDb.apply(sourceDb), sourceTable, nodeCount(rule)));
    }

    /** 命中的规则；未命中返回 null。精确大小写优先，全部落空后按忽略大小写再扫一遍。 */
    public SplitRule find(String sourceDb, String sourceTable) {
        for (SplitRule rule : rules) {
            if (rule.getPattern().matches(sourceDb, sourceTable)) {
                return rule;
            }
        }
        for (SplitRule rule : rules) {
            if (rule.getPattern().matchesIgnoreCase(sourceDb, sourceTable)) {
                return rule;
            }
        }
        return null;
    }

    private int nodeCount(SplitRule rule) {
        if (rule.getNodeGroup() == null) {
            return 0;
        }
        List<RouteNode> nodes = nodeGroups.get(rule.getNodeGroup());
        return nodes == null ? 0 : nodes.size();
    }

    public List<SplitRule> getRules() {
        return rules;
    }
}
