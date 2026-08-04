package com.migration.common.route;

import java.util.List;

/**
 * 表级路由：把"源库.源表（+ 行内分片键的值）"解析成一个或多个落点。
 *
 * <p>全量、增量、DDL、校验对比四条链路共用这一个接口——汇聚、拆分、1:1 是它的三个实现，
 * 链路里只有一处 {@code router.route(...)}，不再各写一遍库表名解析。
 *
 * <p>未命中任何规则的表返回 {@link RouteTarget#identity}，调用方走原有 1:1 路径，
 * 因此 {@code route.mode=NONE}（默认）时全链路行为与接入路由前完全一致。
 */
public interface TableRouter {

    RoutingConfig.Mode mode();

    /** 整个 router 是否恒等（{@code route.mode=NONE}）：调用方可据此整体短路。 */
    boolean isIdentity();

    /** 该源表是否命中路由规则。 */
    boolean matches(String sourceDb, String sourceTable);

    /**
     * 该源表的分片键列名；null 表示无需行级路由（1:1 与汇聚恒为 null）。
     * 调用方取到该列的值后传给 {@link #route}。
     */
    String shardKeyColumn(String sourceDb, String sourceTable);

    /**
     * 静态展开该源表的全部落点：预建目标表、DDL 广播、校验对比配对用。
     * 未命中规则时返回单元素的 identity 落点。
     */
    List<RouteTarget> allTargets(String sourceDb, String sourceTable);

    /**
     * 解析一行的落点。
     *
     * @param shardKeyValue 分片键的值；无需行级路由时传 null
     * @return 落点列表；空列表表示这一行被判为"未路由且策略为投递死信"，调用方不得静默丢弃
     * @throws UnroutedRowException 未路由且策略为 ERROR
     */
    List<RouteTarget> route(String sourceDb, String sourceTable, Object shardKeyValue);
}
