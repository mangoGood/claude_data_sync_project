package com.migration.increment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 双向同步的写写冲突消解（CDR，P1-4）。
 *
 * <p>此前双向只有防回环（{@code __sync_origin} 事务打标），<b>没有冲突策略</b>：
 * 两端同时改同一行时，两条通道各自 {@code ON DUPLICATE KEY UPDATE}，
 * 最终收敛结果取决于"谁后到"而不是业务时间序——两端很可能<b>各自留下不同的值</b>，
 * 而且永远不会自己收敛。
 *
 * <p>做法是旁路表 {@code __sync_rowmeta} 记录每行"最后一次写入的来源节点与源事件时间戳"，
 * 与业务 DML 在<b>同一个目标事务</b>里更新（所以它和数据永远一致，崩溃也不会错位）：
 *
 * <pre>
 *   来一条对 (表, 行) 的写入(node=N, ts=T)：
 *     旁路表没有记录             → 应用，并记下 (N, T)
 *     T &gt; 已记录 ts             → 应用（正常的后续修改）
 *     T &lt; 已记录 ts 且节点不同   → <b>冲突</b>，按策略裁决
 *     T = 已记录 ts 且节点不同   → <b>冲突</b>，按策略裁决（同一毫秒的并发写）
 *     T ≤ 已记录 ts 且节点相同   → 重放/乱序，直接跳过（幂等）
 * </pre>
 *
 * <p>策略（{@code sync.bidi.conflict.policy}）：
 * <ul>
 *   <li>{@code LWW_SOURCE_TS}（默认）——源事件时间戳大的赢；完全相等时按节点 id 字典序，
 *       保证<b>两个方向独立算出同一个赢家</b>，否则两端会收敛到不同的值；</li>
 *   <li>{@code NODE_PRIORITY}——{@code sync.bidi.primary.node} 指定的节点恒赢；</li>
 *   <li>{@code ERROR}——检测到冲突即 fail-stop 交人工，用于"绝不允许自动丢写"的场景。</li>
 * </ul>
 *
 * <p>时钟：LWW 用的是<b>源库事件时间戳</b>（binlog/WAL 里的事件时间），不是 agent 本地时间，
 * 因此依赖两端数据库时钟同步——与所有 LWW 方案的前提一致，偏差超过业务写入间隔就可能选错赢家。
 */
public class ConflictResolver {
    private static final Logger logger = LoggerFactory.getLogger(ConflictResolver.class);

    public static final String KEY_POLICY = "sync.bidi.conflict.policy";
    public static final String KEY_PRIMARY_NODE = "sync.bidi.primary.node";
    public static final String TABLE = "__sync_rowmeta";

    public enum Policy { LWW_SOURCE_TS, NODE_PRIORITY, ERROR, NONE }

    /** 裁决结果：应用 / 跳过（本次写入是输家） / 报错停止。 */
    public enum Decision { APPLY, SKIP, FAIL }

    private final Policy policy;
    private final String nodeId;
    /** 本端（目标库所在节点）标识：用于"两端各自算、结果必须一致"的确定性裁决。 */
    private final String localNodeId;
    private final String primaryNode;
    private final boolean isPostgresql;
    private final String taskId;

    private final AtomicLong conflicts = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private boolean tableReady = false;

    public ConflictResolver(Properties props, String taskId, String nodeId, boolean isPostgresql) {
        this.taskId = taskId;
        this.nodeId = nodeId != null ? nodeId : "unknown";
        this.localNodeId = props.getProperty("sync.local.node.id", "local");
        this.isPostgresql = isPostgresql;
        this.primaryNode = props.getProperty(KEY_PRIMARY_NODE, "").trim();
        Policy p;
        try {
            p = Policy.valueOf(props.getProperty(KEY_POLICY, "LWW_SOURCE_TS").trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("未知的冲突消解策略 {}，回退 LWW_SOURCE_TS", props.getProperty(KEY_POLICY));
            p = Policy.LWW_SOURCE_TS;
        }
        if (p == Policy.NODE_PRIORITY && primaryNode.isEmpty()) {
            logger.warn("NODE_PRIORITY 策略未配置 {}，回退 LWW_SOURCE_TS", KEY_PRIMARY_NODE);
            p = Policy.LWW_SOURCE_TS;
        }
        this.policy = p;
        logger.info("双向冲突消解已启用: policy={} 对端节点={} 本端节点={} primaryNode={}",
                policy, this.nodeId, this.localNodeId, primaryNode.isEmpty() ? "-" : primaryNode);
    }

    public boolean isActive() {
        return policy != Policy.NONE;
    }

    public Policy getPolicy() {
        return policy;
    }

    public long getConflictCount() {
        return conflicts.get();
    }

    public long getSkippedCount() {
        return skipped.get();
    }

    /** 建旁路表（幂等）。与业务表同库，便于和业务 DML 进同一个事务。 */
    public void ensureTable(Connection conn) {
        if (tableReady) return;
        String ddl = isPostgresql
                ? "CREATE TABLE IF NOT EXISTS \"" + TABLE + "\" ("
                    + "tbl VARCHAR(255) NOT NULL, row_key VARCHAR(512) NOT NULL, "
                    + "last_node VARCHAR(64), last_src_ts BIGINT, updated_at BIGINT, "
                    + "PRIMARY KEY (tbl, row_key))"
                : "CREATE TABLE IF NOT EXISTS `" + TABLE + "` ("
                    + "tbl VARCHAR(255) NOT NULL, row_key VARCHAR(512) NOT NULL, "
                    + "last_node VARCHAR(64), last_src_ts BIGINT, updated_at BIGINT, "
                    + "PRIMARY KEY (tbl, row_key))";
        try (Statement st = conn.createStatement()) {
            st.execute(ddl);
            tableReady = true;
            logger.info("冲突消解旁路表就绪: {}", TABLE);
        } catch (SQLException e) {
            logger.warn("创建冲突消解旁路表失败（冲突消解降级为不裁决）: {}", e.getMessage());
        }
    }

    /**
     * 裁决一条 DML 是否应该应用。
     *
     * @param sourceTs 源事件时间戳（毫秒）；为 0 表示事件没带时间戳，此时不做裁决直接应用
     */
    public Decision decide(Connection conn, String table, String rowKey, long sourceTs) {
        if (!isActive() || table == null || rowKey == null || sourceTs <= 0 || !tableReady) {
            return Decision.APPLY;
        }
        String sql = isPostgresql
                ? "SELECT last_node, last_src_ts FROM \"" + TABLE + "\" WHERE tbl=? AND row_key=?"
                : "SELECT last_node, last_src_ts FROM `" + TABLE + "` WHERE tbl=? AND row_key=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            ps.setString(2, truncateKey(rowKey));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Decision.APPLY;      // 这一行还没人写过
                }
                String lastNode = rs.getString(1);
                long lastTs = rs.getLong(2);
                boolean sameNode = nodeId.equals(lastNode);
                if (sourceTs > lastTs) {
                    return Decision.APPLY;      // 更新的写入，正常推进
                }
                if (sameNode) {
                    // 同一节点的旧/同时刻事件：resume 重放或同毫秒连续写，幂等跳过即可，不算冲突
                    return sourceTs == lastTs ? Decision.APPLY : Decision.SKIP;
                }
                return resolveConflict(table, rowKey, lastNode, lastTs, sourceTs);
            }
        } catch (SQLException e) {
            // 裁决查询失败不该阻断同步：退回"照常应用"，与未启用 CDR 时行为一致
            logger.warn("冲突裁决查询失败（按应用处理）: {}", e.getMessage());
            return Decision.APPLY;
        }
    }

    private Decision resolveConflict(String table, String rowKey, String lastNode, long lastTs, long sourceTs) {
        conflicts.incrementAndGet();
        boolean win;
        switch (policy) {
            case NODE_PRIORITY:
                win = nodeId.equals(primaryNode);
                break;
            case ERROR:
                recordConflict(table, rowKey, lastNode, lastTs, sourceTs, "ERROR", "fail-stop");
                return Decision.FAIL;
            case LWW_SOURCE_TS:
            default:
                // 到这里必然 sourceTs <= lastTs：时间戳相等才有翻盘机会，
                // 用节点 id 字典序做平局裁决——两个方向各自算，结果必须一致，否则两端收敛到不同值
                win = sourceTs == lastTs && nodeId.compareTo(lastNode) > 0;
                break;
        }
        recordConflict(table, rowKey, lastNode, lastTs, sourceTs, policy.name(), win ? "incoming" : "stored");
        if (!win) {
            skipped.incrementAndGet();
            return Decision.SKIP;
        }
        return Decision.APPLY;
    }

    /**
     * 前镜像不匹配时的裁决——这才是"两端同时改同一行"真正走的那条路。
     *
     * <p>为什么不能只看旁路表：旁路表记的是<b>对端写入</b>，本端自己的本地写入没人记。
     * 首次并发时两边旁路表都是空的，各自"照常应用"，结果就是<b>值互换</b>——
     * A 变成 B 的值、B 变成 A 的值，两端永远不一致，而且看起来还像同步成功了。
     * 所以真正的信号是「目标行的当前值 ≠ 事件的前镜像」：说明这行在本端被别人改过。
     *
     * <p>裁决必须<b>对称</b>：两个方向是两个独立进程，各算各的，规则必须让它们选出同一个赢家。
     * NODE_PRIORITY 看主端；LWW 优先比源事件时间戳（旁路表里有对端时刻时），
     * 无从比较时退化为节点 id 字典序——不看时间但一定收敛，比"谁后到谁覆盖"强。
     */
    public Decision decideOnMismatch(Connection conn, String table, String rowKey, long sourceTs) {
        if (!isActive()) {
            return Decision.APPLY;
        }
        conflicts.incrementAndGet();
        long storedTs = 0;
        String storedNode = null;
        if (tableReady && table != null && rowKey != null) {
            String sql = isPostgresql
                    ? "SELECT last_node, last_src_ts FROM \"" + TABLE + "\" WHERE tbl=? AND row_key=?"
                    : "SELECT last_node, last_src_ts FROM `" + TABLE + "` WHERE tbl=? AND row_key=?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, table);
                ps.setString(2, truncateKey(rowKey));
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        storedNode = rs.getString(1);
                        storedTs = rs.getLong(2);
                    }
                }
            } catch (SQLException e) {
                logger.debug("读取行元数据失败（按无记录处理）: {}", e.getMessage());
            }
        }

        boolean win;
        switch (policy) {
            case NODE_PRIORITY:
                win = nodeId.equals(primaryNode);
                break;
            case ERROR:
                recordConflict(table, rowKey, storedNode, storedTs, sourceTs, "ERROR", "fail-stop");
                return Decision.FAIL;
            case LWW_SOURCE_TS:
            default:
                if (storedNode != null && !storedNode.equals(nodeId) && storedTs > 0) {
                    win = sourceTs > storedTs
                            || (sourceTs == storedTs && nodeId.compareTo(storedNode) > 0);
                } else {
                    // 本端最后一次写入的时刻无从知晓（本地写入没人记）→ 用节点序保证两端一致收敛
                    win = nodeId.compareTo(localNodeId) > 0;
                }
                break;
        }
        recordConflict(table, rowKey, storedNode != null ? storedNode : localNodeId,
                storedTs, sourceTs, policy.name(), win ? "incoming" : "local");
        if (!win) {
            skipped.incrementAndGet();
            return Decision.SKIP;
        }
        return Decision.APPLY;
    }

    /** 应用成功后在<b>同一事务</b>里记下这一行的最后写入者。 */
    public void record(Connection conn, String table, String rowKey, long sourceTs) throws SQLException {
        if (!isActive() || table == null || rowKey == null || sourceTs <= 0 || !tableReady) {
            return;
        }
        String sql = isPostgresql
                ? "INSERT INTO \"" + TABLE + "\" (tbl, row_key, last_node, last_src_ts, updated_at) VALUES (?,?,?,?,?) "
                    + "ON CONFLICT (tbl, row_key) DO UPDATE SET last_node=EXCLUDED.last_node, "
                    + "last_src_ts=EXCLUDED.last_src_ts, updated_at=EXCLUDED.updated_at"
                : "INSERT INTO `" + TABLE + "` (tbl, row_key, last_node, last_src_ts, updated_at) VALUES (?,?,?,?,?) "
                    + "ON DUPLICATE KEY UPDATE last_node=VALUES(last_node), "
                    + "last_src_ts=VALUES(last_src_ts), updated_at=VALUES(updated_at)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            ps.setString(2, truncateKey(rowKey));
            ps.setString(3, nodeId);
            ps.setLong(4, sourceTs);
            ps.setLong(5, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    /** 冲突落 {@code files/<taskId>/conflict.jsonl}，供 UI 复用死信页面展示。 */
    private void recordConflict(String table, String rowKey, String lastNode, long lastTs,
                                long sourceTs, String policyName, String winner) {
        String line = "{\"ts\":" + System.currentTimeMillis()
                + ",\"table\":\"" + jsonEscape(table) + "\""
                + ",\"rowKey\":\"" + jsonEscape(truncateKey(rowKey)) + "\""
                + ",\"incomingNode\":\"" + jsonEscape(nodeId) + "\""
                + ",\"incomingTs\":" + sourceTs
                + ",\"storedNode\":\"" + jsonEscape(lastNode) + "\""
                + ",\"storedTs\":" + lastTs
                + ",\"policy\":\"" + policyName + "\""
                + ",\"winner\":\"" + winner + "\"}";
        logger.warn("双向写写冲突: table={} row={} 本端({},{}) vs 已记录({},{}) → 采用 {}",
                table, rowKey, nodeId, sourceTs, lastNode, lastTs, winner);
        File f = new File("./files/" + taskId + "/conflict.jsonl");
        try {
            if (f.getParentFile() != null) f.getParentFile().mkdirs();
            try (PrintWriter pw = new PrintWriter(new FileWriter(f, true))) {
                pw.println(line);
            }
        } catch (IOException e) {
            logger.warn("写冲突记录失败: {}", e.getMessage());
        }
    }

    /** 旁路表主键长度有限，超长行键截断（截断只影响极端宽主键的定位精度，不影响正确性方向）。 */
    private static String truncateKey(String key) {
        return key.length() <= 512 ? key : key.substring(0, 512);
    }

    private static String jsonEscape(String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
            }
        }
        return out.toString();
    }
}
