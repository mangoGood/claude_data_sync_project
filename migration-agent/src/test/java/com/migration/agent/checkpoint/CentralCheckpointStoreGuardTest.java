package com.migration.agent.checkpoint;

import com.migration.common.position.CheckpointRecord;
import com.migration.common.position.MonotonicKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 中心位点写入的两条守卫：fencing（低 epoch 拒绝）与单调（同 epoch 内不许回退）。
 *
 * <p>两条守卫都压在 SQL 里而不是应用层，因为它们要防的正是<b>并发</b>：网络分区下没死透的老 agent
 * 还在写自己的位点，而新主已经在中心库上推进。应用层"先查再写"在这中间会开一个窗口。
 *
 * <p>跑在内存 H2（MySQL 兼容模式）上，不连外部库。
 */
@DisplayName("中心位点写入守卫")
class CentralCheckpointStoreGuardTest {

    private String dbUrl;
    private CentralCheckpointStore store;
    private final String taskId = "unit-test-guard";

    @BeforeEach
    void setUp() throws SQLException {
        dbUrl = "jdbc:h2:mem:ckpt-guard-" + System.nanoTime() + ";MODE=MySQL;DB_CLOSE_DELAY=-1";
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE task_checkpoints (" +
                    "task_id VARCHAR(36) NOT NULL," +
                    "stage VARCHAR(16) NOT NULL," +
                    "stream_key VARCHAR(128) NOT NULL DEFAULT '-'," +
                    "engine VARCHAR(32) NOT NULL," +
                    "kind VARCHAR(32) NOT NULL," +
                    "payload CLOB NOT NULL," +
                    "monotonic_key BIGINT NOT NULL DEFAULT 0," +
                    "source_ts TIMESTAMP NULL," +
                    "agent_id VARCHAR(64) NOT NULL," +
                    "lease_epoch INT NOT NULL DEFAULT 0," +
                    "updated_at TIMESTAMP NOT NULL," +
                    "reset_at TIMESTAMP NULL," +
                    "PRIMARY KEY (task_id, stage, stream_key))");
            st.execute("CREATE TABLE task_checkpoint_history (" +
                    "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                    "task_id VARCHAR(36) NOT NULL, stage VARCHAR(16) NOT NULL," +
                    "stream_key VARCHAR(128) NOT NULL DEFAULT '-'," +
                    "engine VARCHAR(32) NOT NULL DEFAULT ''," +
                    "kind VARCHAR(32) NOT NULL DEFAULT ''," +
                    "payload CLOB NOT NULL, monotonic_key BIGINT NOT NULL DEFAULT 0," +
                    "source_ts TIMESTAMP NULL, recorded_at TIMESTAMP NOT NULL," +
                    "reason VARCHAR(32) NOT NULL, operator VARCHAR(64) NULL)");
        }
        store = new CentralCheckpointStore(dbUrl, "sa", "");
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement()) {
            st.execute("SHUTDOWN");
        }
    }

    private CheckpointRecord recordAt(String binlogFile, long position) {
        Properties payload = new Properties();
        payload.setProperty("binlog.file", binlogFile);
        payload.setProperty("binlog.position", String.valueOf(position));
        return new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "mysql",
                CheckpointRecord.Kind.BINLOG_FILE_POS, payload,
                MonotonicKey.ofBinlog(binlogFile, position), 0);
    }

    private long currentKey() throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT monotonic_key FROM task_checkpoints")) {
            return rs.next() ? rs.getLong(1) : -1;
        }
    }

    @Test
    @DisplayName("首次写入 INSERT，后续推进 ACCEPTED")
    void insertThenAdvance() throws SQLException {
        assertEquals(CentralCheckpointStore.WriteResult.INSERTED,
                store.upsert(recordAt("mysql-bin.000001", 100), "agent-a", 1));
        assertEquals(CentralCheckpointStore.WriteResult.ACCEPTED,
                store.upsert(recordAt("mysql-bin.000001", 200), "agent-a", 1));
        assertEquals(MonotonicKey.ofBinlog("mysql-bin.000001", 200), currentKey());
    }

    @Test
    @DisplayName("fencing：低 epoch 的老 agent 写不进来，中心位点纹丝不动")
    void lowerEpochIsFenced() throws SQLException {
        store.upsert(recordAt("mysql-bin.000002", 500), "agent-b", 8);
        long before = currentKey();

        assertEquals(CentralCheckpointStore.WriteResult.REJECTED,
                store.upsert(recordAt("mysql-bin.000002", 900), "agent-a", 7),
                "被抢占的老 agent（epoch 更低）必须写不进来");
        assertEquals(before, currentKey());
    }

    @Test
    @DisplayName("单调：同 epoch 内位点回退被拒")
    void backwardsPositionIsRejected() throws SQLException {
        store.upsert(recordAt("mysql-bin.000002", 900), "agent-a", 3);
        long before = currentKey();

        assertEquals(CentralCheckpointStore.WriteResult.REJECTED,
                store.upsert(recordAt("mysql-bin.000002", 100), "agent-a", 3));
        assertEquals(before, currentKey());
    }

    @Test
    @DisplayName("更高 epoch 无条件放行：新主回灌后位点可能偏旧，拦住它会让位点永久卡死")
    void higherEpochWinsEvenIfOlder() throws SQLException {
        store.upsert(recordAt("mysql-bin.000002", 900), "agent-a", 3);

        assertEquals(CentralCheckpointStore.WriteResult.ACCEPTED,
                store.upsert(recordAt("mysql-bin.000002", 400), "agent-b", 4),
                "偏旧只意味着多重放（安全方向），而拦住合法新主会让它再也写不进位点");
        assertEquals(MonotonicKey.ofBinlog("mysql-bin.000002", 400), currentKey());
    }

    @Test
    @DisplayName("位点没变时重复写入算接受，不能误报成被拒（否则会刷一堆假告警）")
    void unchangedWriteIsNotAReject() {
        store.upsert(recordAt("mysql-bin.000003", 77), "agent-a", 2);
        assertEquals(CentralCheckpointStore.WriteResult.ACCEPTED,
                store.upsert(recordAt("mysql-bin.000003", 77), "agent-a", 2));
    }

    @Test
    @DisplayName("monotonic_key=0（GTID 集这类不可比位点）时单调守卫降级为不校验")
    void uncomparableKindSkipsMonotonicGuard() {
        Properties payload = new Properties();
        payload.setProperty("mongo.checkpoint.json", "{\"resumeToken\":{\"_data\":\"82AA\"}}");
        CheckpointRecord token = new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "mongodb",
                CheckpointRecord.Kind.RESUME_TOKEN, payload, MonotonicKey.UNKNOWN, 0);

        assertEquals(CentralCheckpointStore.WriteResult.INSERTED, store.upsert(token, "agent-a", 1));
        assertEquals(CentralCheckpointStore.WriteResult.ACCEPTED, store.upsert(token, "agent-a", 1),
                "折不出可比标量的位点不能被单调守卫拦死");
    }

    @Test
    @DisplayName("作废：删之前先留一份 history，事后能对账")
    void deleteKeepsHistory() throws SQLException {
        store.upsert(recordAt("mysql-bin.000004", 4), "agent-a", 1);
        store.deleteTask(taskId, "FAILOVER");

        assertEquals(-1, currentKey(), "倒换后中心位点必须清掉，否则接管方会把旧源位点回灌回来");
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT reason FROM task_checkpoint_history WHERE task_id='" + taskId + "'")) {
            assertEquals(true, rs.next());
            assertEquals("FAILOVER", rs.getString(1));
        }
    }
}
