package com.migration.common.route;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * 汇聚路由（N:1）：命中规则的源表全部落到规则指定的那张目标表，与行数据无关。
 */
public final class MergeRouter implements TableRouter {

    private final List<MergeRule> rules;
    /** 源库 → 任务默认目标库（规则未写目标库时用它，多库任务按各自映射解析） */
    private final Function<String, String> defaultTargetDb;

    MergeRouter(List<MergeRule> rules, Function<String, String> defaultTargetDb) {
        this.rules = rules;
        this.defaultTargetDb = defaultTargetDb;
    }

    @Override
    public RoutingConfig.Mode mode() {
        return RoutingConfig.Mode.MERGE;
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
        return null;   // 汇聚不做行级路由
    }

    @Override
    public List<RouteTarget> allTargets(String sourceDb, String sourceTable) {
        MergeRule rule = find(sourceDb, sourceTable);
        if (rule == null) {
            return Collections.singletonList(RouteTarget.identity(sourceDb, sourceTable));
        }
        return Collections.singletonList(rule.target(defaultTargetDb.apply(sourceDb)));
    }

    @Override
    public List<RouteTarget> route(String sourceDb, String sourceTable, Object shardKeyValue) {
        return allTargets(sourceDb, sourceTable);
    }

    /** 命中的规则；未命中返回 null。精确大小写优先，全部落空后按忽略大小写再扫一遍。 */
    public MergeRule find(String sourceDb, String sourceTable) {
        for (MergeRule rule : rules) {
            if (rule.getPattern().matches(sourceDb, sourceTable)) {
                return rule;
            }
        }
        for (MergeRule rule : rules) {
            if (rule.getPattern().matchesIgnoreCase(sourceDb, sourceTable)) {
                return rule;
            }
        }
        return null;
    }

    public List<MergeRule> getRules() {
        return rules;
    }
}
