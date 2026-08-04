package com.migration.common.route;

/**
 * 目标实例组里的一个节点（跨实例拆分/汇聚用）。
 *
 * <p>连接串由 {@code route.node.<组名>.<序号>.*} 下发；{@code database} 可空——
 * 库名由拆分规则的库模板决定，节点只承载"连到哪个实例"。
 */
public final class RouteNode {

    private final String groupId;
    private final int ordinal;
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;

    public RouteNode(String groupId, int ordinal, String host, int port,
                     String database, String username, String password) {
        this.groupId = groupId;
        this.ordinal = ordinal;
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    /** 节点 id：{@code <组名>#<序号>}，即 {@link RouteTarget#getNodeId()}。 */
    public String id() {
        return groupId + "#" + ordinal;
    }

    public String getGroupId() {
        return groupId;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return id() + "(" + host + ":" + port + ")";
    }
}
