package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 位点历史 / 重置 / 降级读（B3）。
 *
 * <p>重置是全平台**唯一允许位点倒退**的入口，所以这里守三件事：跑着的任务不许重置、
 * 重置必须留审计、重置必须打 {@code reset_at}（否则 agent 一看"本地有位点"就走同机重启分支，
 * 后端改的中心库永远不会生效）。
 *
 * <p>跑在内存 H2（MySQL 兼容模式）上，不连外部库。
 */
@DisplayName("中心位点：历史、重置与降级读")
class CheckpointCentralServiceTest {

    private CheckpointCentralService service;
    private JdbcTemplate jdbc;
    private final String taskId = "unit-test-central";

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:ckpt-central-" + System.nanoTime() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE task_checkpoints (" +
                "task_id VARCHAR(36) NOT NULL, stage VARCHAR(16) NOT NULL," +
                "stream_key VARCHAR(128) NOT NULL DEFAULT '-', engine VARCHAR(32) NOT NULL," +
                "kind VARCHAR(32) NOT NULL, payload CLOB NOT NULL," +
                "monotonic_key BIGINT NOT NULL DEFAULT 0, source_ts TIMESTAMP NULL," +
                "agent_id VARCHAR(64) NOT NULL, lease_epoch INT NOT NULL DEFAULT 0," +
                "updated_at TIMESTAMP NOT NULL, reset_at TIMESTAMP NULL," +
                "PRIMARY KEY (task_id, stage, stream_key))");
        jdbc.execute("CREATE TABLE task_checkpoint_history (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, task_id VARCHAR(36) NOT NULL," +
                "stage VARCHAR(16) NOT NULL, stream_key VARCHAR(128) NOT NULL DEFAULT '-'," +
                "engine VARCHAR(32) NOT NULL DEFAULT '', kind VARCHAR(32) NOT NULL DEFAULT ''," +
                "payload CLOB NOT NULL, monotonic_key BIGINT NOT NULL DEFAULT 0," +
                "source_ts TIMESTAMP NULL, recorded_at TIMESTAMP NOT NULL," +
                "reason VARCHAR(32) NOT NULL, operator VARCHAR(64) NULL)");

        service = new CheckpointCentralService();
        ReflectionTestUtils.setField(service, "jdbcTemplate", jdbc);
        ReflectionTestUtils.setField(service, "historyRetentionHours", 72);
    }

    @AfterEach
    void tearDown() {
        jdbc.execute("SHUTDOWN");
    }

    private void seedCurrent(String payload, long key) {
        jdbc.update("INSERT INTO task_checkpoints " +
                        "(task_id, stage, stream_key, engine, kind, payload, monotonic_key, agent_id, lease_epoch, updated_at) " +
                        "VALUES (?,'CAPTURE','-','mysql','BINLOG_FILE_POS',?,?,'agent-a',3,?)",
                taskId, payload, key, new Timestamp(System.currentTimeMillis()));
    }

    private long seedHistory(String payload, long key, long recordedAt, String reason) {
        jdbc.update("INSERT INTO task_checkpoint_history " +
                        "(task_id, stage, stream_key, engine, kind, payload, monotonic_key, recorded_at, reason) " +
                        "VALUES (?,'CAPTURE','-','mysql','BINLOG_FILE_POS',?,?,?,?)",
                taskId, payload, key, new Timestamp(recordedAt), reason);
        return jdbc.queryForObject("SELECT MAX(id) FROM task_checkpoint_history", Long.class);
    }

    private Workflow workflow(WorkflowStatus status) {
        Workflow w = new Workflow();
        w.setId(taskId);
        w.setStatus(status);
        return w;
    }

    private Map<String, Object> target(String type, Object value) {
        Map<String, Object> t = new HashMap<>();
        t.put("type", type);
        t.put("value", value);
        return t;
    }

    @Test
    @DisplayName("跑着的任务不许重置位点：本地位点还在推进，改中心库纯属自欺欺人")
    void cannotResetRunningTask() {
        seedCurrent("binlog.file=mysql-bin.000009\nbinlog.position=900\n", 900);
        long hid = seedHistory("binlog.file=mysql-bin.000001\nbinlog.position=4\n", 4,
                System.currentTimeMillis() - 60_000, "SAMPLE");

        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
                service.reset(workflow(WorkflowStatus.INCREMENT_RUNNING), "CAPTURE", "-",
                        target("HISTORY_ID", hid), "alice"));
        assertTrue(e.getMessage().contains("只有已暂停"));
    }

    @Test
    @DisplayName("按 history id 重置：现值被改回去、打上 reset_at、并留 RESET 审计")
    void resetByHistoryId() {
        seedCurrent("binlog.file=mysql-bin.000009\nbinlog.position=900\n", 900);
        long hid = seedHistory("binlog.file=mysql-bin.000001\nbinlog.position=4\n", 4,
                System.currentTimeMillis() - 60_000, "SAMPLE");

        Map<String, Object> result = service.reset(workflow(WorkflowStatus.PAUSED), "CAPTURE", "-",
                target("HISTORY_ID", hid), "alice");
        assertTrue(String.valueOf(result.get("payload")).contains("mysql-bin.000001"));

        Map<String, Object> current = jdbc.queryForMap(
                "SELECT payload, monotonic_key, reset_at FROM task_checkpoints WHERE task_id=?", taskId);
        assertTrue(String.valueOf(current.get("payload")).contains("mysql-bin.000001"),
                "现值必须真的被改回去");
        assertEquals(4L, ((Number) current.get("monotonic_key")).longValue());
        assertNotNull(current.get("reset_at"),
                "不打 reset_at 的话 agent 会走同机重启分支，重置永远不生效");

        List<Map<String, Object>> audit = jdbc.queryForList(
                "SELECT operator FROM task_checkpoint_history WHERE reason='RESET'");
        assertEquals(1, audit.size());
        assertEquals("alice", audit.get(0).get("operator"));
    }

    @Test
    @DisplayName("按时间点重置：取该时刻或之前最近的一条采样")
    void resetByTimestampPicksNearestBefore() {
        long now = System.currentTimeMillis();
        seedCurrent("binlog.file=mysql-bin.000009\nbinlog.position=900\n", 900);
        seedHistory("binlog.file=mysql-bin.000001\nbinlog.position=4\n", 4, now - 600_000, "SAMPLE");
        seedHistory("binlog.file=mysql-bin.000005\nbinlog.position=50\n", 50, now - 300_000, "SAMPLE");
        seedHistory("binlog.file=mysql-bin.000008\nbinlog.position=80\n", 80, now - 10_000, "SAMPLE");

        Map<String, Object> result = service.reset(workflow(WorkflowStatus.FAILED), "CAPTURE", "-",
                target("TIMESTAMP", now - 200_000), "bob");
        assertTrue(String.valueOf(result.get("payload")).contains("mysql-bin.000005"),
                "应取目标时刻之前最近的采样，而不是之后的");
    }

    @Test
    @DisplayName("目标时刻之前没有任何采样：明确报错，不许静默挑一个")
    void resetWithoutUsableHistoryFails() {
        seedCurrent("binlog.file=mysql-bin.000009\nbinlog.position=900\n", 900);
        assertThrows(IllegalStateException.class, () ->
                service.reset(workflow(WorkflowStatus.PAUSED), "CAPTURE", "-",
                        target("TIMESTAMP", System.currentTimeMillis() - 999_000), "bob"));
    }

    @Test
    @DisplayName("中心表里还没有这一段时，重置要能把行补出来")
    void resetInsertsRowWhenMissing() {
        long hid = seedHistory("binlog.file=mysql-bin.000002\nbinlog.position=20\n", 20,
                System.currentTimeMillis() - 1000, "SAMPLE");
        service.reset(workflow(WorkflowStatus.PAUSED), "CAPTURE", "-", target("HISTORY_ID", hid), "carol");

        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM task_checkpoints WHERE task_id=?", Integer.class, taskId));
    }

    @Test
    @DisplayName("清理只删 SAMPLE：RESET/FAILOVER 是审计，很久以后才会被问起")
    void cleanupKeepsAuditRows() {
        long old = System.currentTimeMillis() - 100L * 3600_000L;
        seedHistory("a=1\n", 1, old, "SAMPLE");
        seedHistory("b=2\n", 2, old, "RESET");
        seedHistory("c=3\n", 3, old, "FAILOVER");

        service.cleanupHistory();

        List<Map<String, Object>> left = jdbc.queryForList(
                "SELECT reason FROM task_checkpoint_history ORDER BY reason");
        assertEquals(2, left.size());
        assertEquals("FAILOVER", left.get(0).get("reason"));
        assertEquals("RESET", left.get(1).get("reason"));
    }

    @Test
    @DisplayName("降级视图：agent 不可达时也要能看到位点，且必须自报是降级数据")
    void degradedViewIsExplicitAboutBeingStale() {
        seedCurrent("binlog.file=mysql-bin.000007\nbinlog.position=700\ngtid.set=uuid:1-9\n", 700);
        jdbc.update("INSERT INTO task_checkpoints " +
                        "(task_id, stage, stream_key, engine, kind, payload, monotonic_key, agent_id, lease_epoch, updated_at) " +
                        "VALUES (?,'APPLY','-','mysql','SEQNO',?,?,'agent-a',3,?)",
                taskId, "seqno=5000\nbinlog.file=mysql-bin.000007\nbinlog.position=690\n", 5000,
                new Timestamp(System.currentTimeMillis()));

        Map<String, Object> view = service.degradedVisualization(taskId, "connect timed out");

        assertEquals("central", view.get("source"));
        assertEquals(true, view.get("degraded"));
        assertEquals("connect timed out", view.get("degradedReason"));

        @SuppressWarnings("unchecked")
        Map<String, Object> binlog = (Map<String, Object>) view.get("binlog");
        assertEquals(true, binlog.get("available"));
        assertEquals("mysql-bin.000007", binlog.get("file"));
        assertEquals(700L, binlog.get("position"));
        assertEquals("uuid:1-9", binlog.get("gtid"));

        @SuppressWarnings("unchecked")
        Map<String, Object> checkpoint = (Map<String, Object>) view.get("checkpoint");
        assertEquals(true, checkpoint.get("available"));
        assertEquals(5000L, checkpoint.get("seqno"));
    }

    @Test
    @DisplayName("没有任何位点时降级视图也不能抛异常，只是 available=false")
    void degradedViewOnEmptyTable() {
        Map<String, Object> view = service.degradedVisualization(taskId, "agent down");
        @SuppressWarnings("unchecked")
        Map<String, Object> binlog = (Map<String, Object>) view.get("binlog");
        assertFalse((Boolean) binlog.get("available"));
        assertEquals("UNKNOWN", view.get("linkStatus"));
    }
}
