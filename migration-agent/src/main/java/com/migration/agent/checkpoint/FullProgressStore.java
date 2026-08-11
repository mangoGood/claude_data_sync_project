package com.migration.agent.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 全量表级断点的上卷与回灌。
 *
 * <p>位点中心化那一批只覆盖了<b>增量</b>：接管方能从中心库拿回 binlog/LSN/SCN 继续增量，
 * 但全量的表级断点还在 {@code files/<taskId>/migration_progress.mv.db} 这个本机 H2 里。
 * 换台机器接管时那个文件不存在，于是<b>已经搬完的表也要从头再搬一遍</b>。
 * 正确性没问题（重搬是幂等的），但 RTO 是"整个全量的时长"——
 * 全量提到 38K 行/秒之后，一张 10 亿行的表仍要 ~7 小时。
 *
 * <p>做法与位点一致：<b>引擎侧一个字都不改</b>，agent 读/写它的 H2。
 * 上卷只读已落盘的行，回灌在拉起子进程之前把行写回本地 H2。
 */
public class FullProgressStore {

    private static final Logger logger = LoggerFactory.getLogger(FullProgressStore.class);

    /** 与 {@code ProgressDatabase} 保持一致的 H2 路径与参数（AUTO_SERVER 让 agent 与子进程能同时开）。 */
    private static String localUrl(String taskId) {
        return "jdbc:h2:./files/" + taskId + "/migration_progress;MODE=MySQL;AUTO_SERVER=TRUE";
    }

    private static boolean localExists(String taskId) {
        return new File("files/" + taskId + "/migration_progress.mv.db").isFile();
    }

    public static class Row {
        public String tableKey;
        public String status;
        public long totalRows;
        public long migratedRows;
        public Long lastMigratedId;
    }

    private final String metaUrl;
    private final String metaUser;
    private final String metaPassword;

    public FullProgressStore(String metaUrl, String metaUser, String metaPassword) {
        this.metaUrl = metaUrl;
        this.metaUser = metaUser;
        this.metaPassword = metaPassword;
    }

    // ==================== 上卷 ====================

    /**
     * 把本地 H2 的表级进度上卷到中心库。<b>只读不写本地</b>，也绝不建库：
     * 本地文件还不存在就说明这个任务还没跑过全量，没什么可上卷的。
     *
     * @return 上卷的行数；失败返回 -1（best-effort，绝不影响任务）
     */
    public int upload(String taskId, String agentId) {
        if (!localExists(taskId)) {
            return 0;
        }
        List<Row> rows = readLocal(taskId);
        if (rows.isEmpty()) {
            return 0;
        }
        try (Connection conn = DriverManager.getConnection(metaUrl, metaUser, metaPassword);
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO task_full_progress (task_id, table_key, status, total_rows, "
                             + "migrated_rows, last_migrated_id, agent_id, updated_at) "
                             + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                             + "ON DUPLICATE KEY UPDATE status=VALUES(status), total_rows=VALUES(total_rows), "
                             + "migrated_rows=VALUES(migrated_rows), last_migrated_id=VALUES(last_migrated_id), "
                             + "agent_id=VALUES(agent_id), updated_at=VALUES(updated_at)")) {
            for (Row r : rows) {
                ps.setString(1, taskId);
                ps.setString(2, r.tableKey);
                ps.setString(3, r.status);
                ps.setLong(4, r.totalRows);
                ps.setLong(5, r.migratedRows);
                if (r.lastMigratedId == null) {
                    ps.setNull(6, java.sql.Types.BIGINT);
                } else {
                    ps.setLong(6, r.lastMigratedId);
                }
                ps.setString(7, agentId);
                // 时间戳一律 JVM 侧绑定（元数据库容器是 UTC，用 SQL NOW() 会差 8 小时）
                ps.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
                ps.addBatch();
            }
            ps.executeBatch();
            return rows.size();
        } catch (Exception e) {
            logger.debug("[{}] 全量进度上卷失败（不影响任务）: {}", taskId, e.getMessage());
            return -1;
        }
    }

    // ==================== 回灌 ====================

    /**
     * 跨机接管：本地没有进度而中心库有，就把中心库的行写回本地 H2，
     * 让 {@code ProgressManager} 照常走"已完成的表跳过、未完成的表清表重搬"。
     *
     * <p><b>只在本地完全没有时回灌</b>：本地有就是同机重启，它比中心库新。
     *
     * @return 回灌的行数；0 表示无需回灌
     */
    public int hydrate(String taskId) {
        if (localExists(taskId)) {
            return 0;
        }
        List<Row> central = readCentral(taskId);
        if (central.isEmpty()) {
            return 0;
        }
        try {
            new File("files/" + taskId).mkdirs();
            try (Connection conn = DriverManager.getConnection(localUrl(taskId), "sa", "")) {
                try (Statement st = conn.createStatement()) {
                    // 表结构必须与 ProgressDatabase.createTables() 一致——子进程起来后会直接用它
                    st.execute("CREATE TABLE IF NOT EXISTS migration_progress ("
                            + "id INT AUTO_INCREMENT PRIMARY KEY, "
                            + "table_name VARCHAR(255) NOT NULL UNIQUE, "
                            + "total_rows BIGINT NOT NULL DEFAULT 0, "
                            + "migrated_rows BIGINT NOT NULL DEFAULT 0, "
                            + "last_migrated_id BIGINT DEFAULT NULL, "
                            + "status VARCHAR(50) NOT NULL DEFAULT 'PENDING', "
                            + "start_time TIMESTAMP NOT NULL, "
                            + "last_update_time TIMESTAMP NOT NULL, "
                            + "complete_time TIMESTAMP, "
                            + "error_message TEXT)");
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "MERGE INTO migration_progress (table_name, total_rows, migrated_rows, "
                                + "last_migrated_id, status, start_time, last_update_time) "
                                + "KEY(table_name) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                    Timestamp now = new Timestamp(System.currentTimeMillis());
                    for (Row r : central) {
                        ps.setString(1, r.tableKey);
                        ps.setLong(2, r.totalRows);
                        ps.setLong(3, r.migratedRows);
                        if (r.lastMigratedId == null) {
                            ps.setNull(4, java.sql.Types.BIGINT);
                        } else {
                            ps.setLong(4, r.lastMigratedId);
                        }
                        ps.setString(5, r.status);
                        ps.setTimestamp(6, now);
                        ps.setTimestamp(7, now);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
            logger.warn("[{}] 已回灌全量表级断点 {} 条：接管方不必把已搬完的表再搬一遍", taskId, central.size());
            return central.size();
        } catch (Exception e) {
            // 回灌不上不算致命：退回"整个全量重做"，正确性不受影响，只是慢
            logger.warn("[{}] 全量进度回灌失败，将退回整段重做（数据仍正确，只是更慢）: {}", taskId, e.getMessage());
            return 0;
        }
    }

    /** 任务重做全量 / 倒换时作废中心行——留着会让下次接管跳过其实需要重搬的表。 */
    public void clear(String taskId) {
        try (Connection conn = DriverManager.getConnection(metaUrl, metaUser, metaPassword);
             PreparedStatement ps = conn.prepareStatement(
                     "DELETE FROM task_full_progress WHERE task_id = ?")) {
            ps.setString(1, taskId);
            ps.executeUpdate();
        } catch (Exception e) {
            logger.debug("[{}] 清理中心全量进度失败: {}", taskId, e.getMessage());
        }
    }

    // ==================== 读 ====================

    private List<Row> readLocal(String taskId) {
        List<Row> out = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(localUrl(taskId), "sa", "");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT table_name, status, total_rows, migrated_rows, last_migrated_id FROM migration_progress")) {
            while (rs.next()) {
                out.add(readRow(rs, "table_name"));
            }
        } catch (Exception e) {
            logger.debug("[{}] 读本地全量进度失败: {}", taskId, e.getMessage());
        }
        return out;
    }

    private List<Row> readCentral(String taskId) {
        List<Row> out = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(metaUrl, metaUser, metaPassword);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT table_key, status, total_rows, migrated_rows, last_migrated_id "
                             + "FROM task_full_progress WHERE task_id = ?")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(readRow(rs, "table_key"));
                }
            }
        } catch (Exception e) {
            logger.debug("[{}] 读中心全量进度失败: {}", taskId, e.getMessage());
        }
        return out;
    }

    private Row readRow(ResultSet rs, String keyColumn) throws java.sql.SQLException {
        Row r = new Row();
        r.tableKey = rs.getString(keyColumn);
        r.status = rs.getString("status");
        r.totalRows = rs.getLong("total_rows");
        r.migratedRows = rs.getLong("migrated_rows");
        long lid = rs.getLong("last_migrated_id");
        r.lastMigratedId = rs.wasNull() ? null : lid;
        return r;
    }
}
