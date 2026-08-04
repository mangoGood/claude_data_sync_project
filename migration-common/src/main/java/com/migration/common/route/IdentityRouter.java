package com.migration.common.route;

import java.util.Collections;
import java.util.List;

/**
 * 恒等路由：{@code route.mode=NONE}（默认）时使用，所有表原样返回 identity 落点。
 *
 * <p>存在的意义是让四条链路只有一条代码路径——不必在每个调用点写
 * "配了路由就走 A、没配就走 B"，未配置路由的任务由本实现自然退回原行为。
 */
public final class IdentityRouter implements TableRouter {

    public static final IdentityRouter INSTANCE = new IdentityRouter();

    private IdentityRouter() {
    }

    @Override
    public RoutingConfig.Mode mode() {
        return RoutingConfig.Mode.NONE;
    }

    @Override
    public boolean isIdentity() {
        return true;
    }

    @Override
    public boolean matches(String sourceDb, String sourceTable) {
        return false;
    }

    @Override
    public String shardKeyColumn(String sourceDb, String sourceTable) {
        return null;
    }

    @Override
    public List<RouteTarget> allTargets(String sourceDb, String sourceTable) {
        return Collections.singletonList(RouteTarget.identity(sourceDb, sourceTable));
    }

    @Override
    public List<RouteTarget> route(String sourceDb, String sourceTable, Object shardKeyValue) {
        return allTargets(sourceDb, sourceTable);
    }
}
