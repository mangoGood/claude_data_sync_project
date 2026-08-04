package com.migration.common.route;

import java.util.Objects;

/**
 * 一条路由的落点：目标实例 + 目标库 + 目标表（+ 分片号）。
 *
 * <p><b>identity 落点</b>是零回归的关键：源表未命中任何路由规则时返回 {@link #identity(String, String)}，
 * 调用方据此走<b>原有的 1:1 链路</b>（{@code schema.mapping.db.*} / {@code schema.mapping.table.*} /
 * {@code target.db.database} 那一套），而不是由路由层去重新实现一遍库表名解析——
 * 两套解析并存必然会在某个边角（大小写回退、库级同步、DR 通道）分叉。
 */
public final class RouteTarget {

    /** 目标实例组内的节点 id（形如 {@code g1#3}）；null = 任务配置里的默认目标实例 */
    private final String nodeId;
    private final String database;
    private final String table;
    /** 分片号；-1 = 非分片落点（汇聚 / identity） */
    private final int shardNo;
    private final boolean identity;

    private RouteTarget(String nodeId, String database, String table, int shardNo, boolean identity) {
        this.nodeId = nodeId;
        this.database = database;
        this.table = table;
        this.shardNo = shardNo;
        this.identity = identity;
    }

    /** 未命中任何规则：调用方按原 1:1 路径处理，路由层不干预。 */
    public static RouteTarget identity(String sourceDb, String sourceTable) {
        return new RouteTarget(null, sourceDb, sourceTable, -1, true);
    }

    /** 汇聚落点（不分片）。 */
    public static RouteTarget merged(String nodeId, String database, String table) {
        return new RouteTarget(nodeId, database, table, -1, false);
    }

    /** 拆分落点（带分片号）。 */
    public static RouteTarget sharded(String nodeId, String database, String table, int shardNo) {
        return new RouteTarget(nodeId, database, table, shardNo, false);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public int getShardNo() {
        return shardNo;
    }

    public boolean isIdentity() {
        return identity;
    }

    /** 落点的唯一标识：用于按目标去重（汇聚的 DDL 去重、拆分的连接复用）。 */
    public String key() {
        return (nodeId == null ? "" : nodeId) + "/" + database + "." + table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RouteTarget)) return false;
        RouteTarget that = (RouteTarget) o;
        return shardNo == that.shardNo
                && identity == that.identity
                && Objects.equals(nodeId, that.nodeId)
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, database, table, shardNo, identity);
    }

    @Override
    public String toString() {
        if (identity) {
            return "identity(" + database + "." + table + ")";
        }
        return key() + (shardNo >= 0 ? "#" + shardNo : "");
    }
}
