package com.migration.common.bidi;

/**
 * 双向同步 / 环路防护（active-active 双活）的共享常量与配置键。
 *
 * <p>原理（事务打标防回环）：apply 端在每个应用事务里先写一行 origin 标记到 {@link #MARKER_TABLE}，
 * 该标记随 DML 一起原子提交进目标库 binlog；capture 端读到带标记的事务即判定为"复制而来"，
 * 跳过其数据事件、不再回传，从而打断 A→B→A 的无限回环。本地应用写入没有标记，正常传播。
 *
 * <p>顺序保证：apply 把标记写在事务的第一条语句，故 binlog 里标记行事件先于该事务的业务 DML 出现，
 * capture 可用"前向单遍"游标（读到标记即置位、遇事务边界复位）过滤，无需缓冲整个事务。
 *
 * <p>适用范围：2 节点 active-active（双活）。N 节点全互联需要 origin 集合而非单标记，属后续扩展。
 */
public final class BidiConstants {

    private BidiConstants() {}

    /** 环路防护标记表名（各库统一）。capture 与 apply 靠此名约定。 */
    public static final String MARKER_TABLE = "__sync_origin";

    /** 是否启用双向同步/环路防护（默认 false，不影响单向同步）。 */
    public static final String KEY_ENABLED = "sync.bidirectional.enabled";

    /** 本节点标识：apply 端写入标记的 origin 值，用于观测/排障；capture 端"有标记即跳过"不依赖其取值。 */
    public static final String KEY_NODE_ID = "sync.node.id";

    /** 标记表固定主键（单行滚动更新，每个应用事务都产生一次行事件）。 */
    public static final int MARKER_ROW_ID = 1;

    public static boolean isEnabled(java.util.Properties props) {
        return Boolean.parseBoolean(props.getProperty(KEY_ENABLED, "false"));
    }

    public static String nodeId(java.util.Properties props) {
        return props.getProperty(KEY_NODE_ID, "unknown");
    }
}
