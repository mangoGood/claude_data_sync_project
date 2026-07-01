package com.migration.agent.service;

import com.migration.agent.model.TaskStateInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TaskStateService} 任务状态机持久化与流转单元测试。
 *
 * <p>验证任务状态在 H2 元数据库中的保存、读取、删除以及状态流转逻辑。
 * 使用内存 H2 数据库（通过子类重写 getConnection）避免文件 I/O 和 AUTO_SERVER 端口绑定问题。
 */
@DisplayName("TaskStateService 状态机持久化测试")
class TaskStateServiceTest {

    /** 测试用子类：使用内存 H2 数据库，避免 AUTO_SERVER 端口绑定和文件锁问题 */
    static class InMemoryTaskStateService extends TaskStateService {
        private static final String DB_URL = "jdbc:h2:mem:test-state;MODE=MySQL;DB_CLOSE_DELAY=-1";

        InMemoryTaskStateService() throws SQLException {
            // 不调用 super() 的 initializeDatabase，避免创建 files 目录
            // 初始化共享 schema（连接后立即关闭，DB_CLOSE_DELAY=-1 保证库不销毁）
            try (Connection init = DriverManager.getConnection(DB_URL, "sa", "");
                 Statement stmt = init.createStatement()) {
                stmt.execute(
                        "CREATE TABLE IF NOT EXISTS task_state (" +
                        "task_id VARCHAR(36) PRIMARY KEY, " +
                        "task_name VARCHAR(255), " +
                        "user_id BIGINT, " +
                        "migration_mode VARCHAR(50), " +
                        "source_connection VARCHAR(500), " +
                        "target_connection VARCHAR(500), " +
                        "source_type VARCHAR(20) DEFAULT 'mysql', " +
                        "target_type VARCHAR(20) DEFAULT 'mysql', " +
                        "status VARCHAR(50), " +
                        "progress INT DEFAULT 0, " +
                        "last_processed_table VARCHAR(255), " +
                        "last_updated BIGINT, " +
                        "created_at VARCHAR(100)" +
                        ")"
                );
            }
        }

        @Override
        protected Connection getConnection(String taskId) throws SQLException {
            // 每次返回新连接，允许父类 try-with-resources 关闭而不影响后续调用
            return DriverManager.getConnection(DB_URL, "sa", "");
        }
    }

    private InMemoryTaskStateService taskStateService;

    @BeforeEach
    void setUp() throws Exception {
        taskStateService = new InMemoryTaskStateService();
    }

    @AfterEach
    void tearDown() throws SQLException {
        // 清理内存表数据，避免测试间相互影响
        try (Connection cleanup = DriverManager.getConnection(
                InMemoryTaskStateService.DB_URL, "sa", "");
             Statement stmt = cleanup.createStatement()) {
            stmt.execute("DELETE FROM task_state");
        }
    }

    private TaskStateInfo buildState(String taskId, String status, int progress) {
        TaskStateInfo state = new TaskStateInfo(taskId);
        state.setTaskName("test-task-" + taskId);
        state.setUserId(1001L);
        state.setMigrationMode("fullAndIncre");
        state.setSourceConnection("mysql://root:pass@127.0.0.1:3306/src");
        state.setTargetConnection("mysql://root:pass@127.0.0.1:3306/dst");
        state.setSourceType("mysql");
        state.setTargetType("mysql");
        state.setStatus(status);
        state.setProgress(progress);
        state.setCreatedAt(LocalDateTime.now());
        return state;
    }

    @Test
    @DisplayName("保存任务状态后应能读取到对应记录")
    void savedStateShouldBeRetrievable() {
        TaskStateInfo state = buildState("task-001", "FULL_MIGRATING", 30);

        taskStateService.saveTaskState(state);

        TaskStateInfo loaded = taskStateService.getTaskState("task-001");
        assertNotNull(loaded);
        assertEquals("task-001", loaded.getTaskId());
        assertEquals("FULL_MIGRATING", loaded.getStatus());
        assertEquals(30, loaded.getProgress());
        assertEquals("fullAndIncre", loaded.getMigrationMode());
        assertEquals("mysql", loaded.getSourceType());
        assertEquals("mysql", loaded.getTargetType());
    }

    @Test
    @DisplayName("未保存的任务应返回 null")
    void unsavedTaskShouldReturnNull() {
        TaskStateInfo loaded = taskStateService.getTaskState("non-existent-task");
        assertNull(loaded);
    }

    @Test
    @DisplayName("状态流转：FULL_MIGRATING -> FULL_COMPLETED -> INCREMENT_RUNNING")
    void stateTransitionFullToIncrement() {
        String taskId = "task-002";

        // 初始状态：全量迁移中
        taskStateService.saveTaskState(buildState(taskId, "FULL_MIGRATING", 50));
        assertEquals("FULL_MIGRATING", taskStateService.getTaskState(taskId).getStatus());

        // 流转到：全量完成
        taskStateService.saveTaskState(buildState(taskId, "FULL_COMPLETED", 100));
        TaskStateInfo afterFull = taskStateService.getTaskState(taskId);
        assertEquals("FULL_COMPLETED", afterFull.getStatus());
        assertEquals(100, afterFull.getProgress());

        // 流转到：增量运行中
        taskStateService.saveTaskState(buildState(taskId, "INCREMENT_RUNNING", 100));
        TaskStateInfo afterIncrement = taskStateService.getTaskState(taskId);
        assertEquals("INCREMENT_RUNNING", afterIncrement.getStatus());
    }

    @Test
    @DisplayName("状态流转：INCREMENT_RUNNING -> SUBSCRIBE_RUNNING")
    void stateTransitionIncrementToSubscribe() {
        String taskId = "task-003";

        taskStateService.saveTaskState(buildState(taskId, "INCREMENT_RUNNING", 100));
        taskStateService.saveTaskState(buildState(taskId, "SUBSCRIBE_RUNNING", 100));

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals("SUBSCRIBE_RUNNING", loaded.getStatus());
    }

    @Test
    @DisplayName("状态流转：任意状态 -> FAILED")
    void stateTransitionToFailed() {
        String taskId = "task-004";

        taskStateService.saveTaskState(buildState(taskId, "FULL_MIGRATING", 60));
        taskStateService.saveTaskState(buildState(taskId, "FAILED", 0));

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals("FAILED", loaded.getStatus());
        assertEquals(0, loaded.getProgress());
    }

    @Test
    @DisplayName("状态流转：任意状态 -> PAUSED")
    void stateTransitionToPaused() {
        String taskId = "task-005";

        taskStateService.saveTaskState(buildState(taskId, "INCREMENT_RUNNING", 100));
        taskStateService.saveTaskState(buildState(taskId, "PAUSED", 100));

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals("PAUSED", loaded.getStatus());
    }

    @Test
    @DisplayName("状态流转：PAUSED -> INCREMENT_RUNNING (恢复)")
    void stateTransitionPausedToResumed() {
        String taskId = "task-006";

        taskStateService.saveTaskState(buildState(taskId, "PAUSED", 100));
        taskStateService.saveTaskState(buildState(taskId, "INCREMENT_RUNNING", 100));

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals("INCREMENT_RUNNING", loaded.getStatus());
    }

    @Test
    @DisplayName("状态流转：SWITCHING -> INCREMENT_RUNNING (主备倒换完成)")
    void stateTransitionSwitchingToIncrement() {
        String taskId = "task-007";

        taskStateService.saveTaskState(buildState(taskId, "SWITCHING", 100));
        taskStateService.saveTaskState(buildState(taskId, "INCREMENT_RUNNING", 100));

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals("INCREMENT_RUNNING", loaded.getStatus());
    }

    @Test
    @DisplayName("删除任务状态后应返回 null")
    void deletedTaskShouldReturnNull() {
        String taskId = "task-008";
        taskStateService.saveTaskState(buildState(taskId, "FULL_MIGRATING", 50));
        assertNotNull(taskStateService.getTaskState(taskId));

        taskStateService.deleteTaskState(taskId);

        assertNull(taskStateService.getTaskState(taskId));
    }

    @Test
    @DisplayName("多次保存同一任务应更新而非插入")
    void multipleSavesShouldUpdateNotInsert() {
        String taskId = "task-009";

        taskStateService.saveTaskState(buildState(taskId, "FULL_MIGRATING", 10));
        taskStateService.saveTaskState(buildState(taskId, "FULL_MIGRATING", 50));
        taskStateService.saveTaskState(buildState(taskId, "FULL_COMPLETED", 100));

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertNotNull(loaded);
        assertEquals("FULL_COMPLETED", loaded.getStatus());
        assertEquals(100, loaded.getProgress());
    }

    @Test
    @DisplayName("lastUpdated 时间戳应随保存更新")
    void lastUpdatedShouldBeRefreshed() throws InterruptedException {
        String taskId = "task-010";

        taskStateService.saveTaskState(buildState(taskId, "FULL_MIGRATING", 10));
        long firstUpdate = taskStateService.getTaskState(taskId).getLastUpdated();

        Thread.sleep(10);

        taskStateService.saveTaskState(buildState(taskId, "FULL_MIGRATING", 20));
        long secondUpdate = taskStateService.getTaskState(taskId).getLastUpdated();

        assertTrue(secondUpdate >= firstUpdate,
                "lastUpdated should be refreshed on subsequent saves");
    }

    @Test
    @DisplayName("PostgreSQL 源类型应被正确持久化")
    void postgresSourceTypeShouldBePersisted() {
        String taskId = "task-011";
        TaskStateInfo state = buildState(taskId, "FULL_MIGRATING", 0);
        state.setSourceType("postgresql");
        state.setTargetType("postgresql");

        taskStateService.saveTaskState(state);

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals("postgresql", loaded.getSourceType());
        assertEquals("postgresql", loaded.getTargetType());
    }

    @Test
    @DisplayName("null userId 应被持久化为 0")
    void nullUserIdShouldBePersistedAsZero() {
        String taskId = "task-012";
        TaskStateInfo state = buildState(taskId, "FULL_MIGRATING", 0);
        state.setUserId(null);

        taskStateService.saveTaskState(state);

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals(0L, loaded.getUserId());
    }

    @Test
    @DisplayName("null sourceType/targetType 应被持久化为 mysql 默认值")
    void nullDbTypesShouldDefaultToMysql() {
        String taskId = "task-013";
        TaskStateInfo state = buildState(taskId, "FULL_MIGRATING", 0);
        state.setSourceType(null);
        state.setTargetType(null);

        taskStateService.saveTaskState(state);

        TaskStateInfo loaded = taskStateService.getTaskState(taskId);
        assertEquals("mysql", loaded.getSourceType());
        assertEquals("mysql", loaded.getTargetType());
    }
}
