package com.migration.agent.service;

import com.migration.agent.model.RecoveryTask;
import com.migration.agent.model.TaskMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * agent 重启恢复任务时必须带上<b>目标库名</b>。
 *
 * <p>为什么要专门锁这一条：连接串通常不带库名（{@code mysql://user:pass@host:port}），
 * {@code ConfigService.updateConfig} 解析它时会把 {@code target.db.database} 覆盖成空，
 * 指望随后的 {@code taskMessage.getTargetDbName()} 再填回来。而 {@code RecoveryService}
 * 曾经既没在 SQL 里 select {@code target_db_name}、{@code RecoveryTask.toTaskMessage()}
 * 也没有把它塞进 TaskMessage —— 于是 agent 每次重启恢复后 {@code target.db.database} 为空，
 * increment 退化成用<b>源库名</b>限定 DML（{@code INSERT INTO `源库`.`表`}）：
 * 目标库一条数据都收不到，任务状态却一直是"同步中"。
 *
 * <p>这个缺陷在"子进程随 agent 一起死"之前被掩盖着——agent 崩溃后孤儿子进程带着正确的内存态
 * 继续同步，恢复出来的第二套进程写错库也看不出来（只是重复双写）。孤儿一收敛，它立刻变成静默丢数据。
 * 实测复现：agent SIGKILL 重启后写入的 500 行全部落回源库，目标停在旧行数。
 */
@DisplayName("恢复路径必须携带目标库名")
class RecoveryTargetDbNameTest {

    private final String taskId = "unit-test-recovery-" + System.nanoTime();

    @AfterEach
    void cleanup() {
        deleteRecursively(new File("files/" + taskId));
    }

    private void deleteRecursively(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) deleteRecursively(c);
            }
        }
        f.delete();
    }

    private RecoveryTask sampleRecoveryTask() {
        RecoveryTask task = new RecoveryTask();
        task.setTaskId(taskId);
        task.setTaskName("recovered-task");
        task.setUserId(1L);
        task.setSourceConnection("mysql://root:rootpassword@127.0.0.1:33306");
        task.setTargetConnection("mysql://root:rootpassword@127.0.0.1:33306");
        task.setMigrationMode("fullAndIncre");
        task.setStatus("INCREMENT_RUNNING");
        task.setSourceDbName("src_db");
        task.setTargetDbName("tgt_db");
        task.setSourceType("mysql");
        task.setTargetType("mysql");
        return task;
    }

    @Test
    @DisplayName("RecoveryTask → TaskMessage 会带上 targetDbName")
    void toTaskMessageCarriesTargetDbName() {
        TaskMessage msg = sampleRecoveryTask().toTaskMessage();
        assertEquals("src_db", msg.getSourceDbName());
        assertEquals("tgt_db", msg.getTargetDbName(),
                "恢复出来的 TaskMessage 必须带目标库名，否则增量会把 DML 写回源库");
    }

    @Test
    @DisplayName("恢复写出的 config.properties 里 target.db.database 不为空")
    void recoveredConfigHasNonEmptyTargetDatabase() throws Exception {
        ConfigService configService = new ConfigService(new AgentConfig());
        configService.updateConfig(sampleRecoveryTask().toTaskMessage());

        File configFile = new File("files/" + taskId + "/config.properties");
        assertTrue(configFile.exists(), "updateConfig 应写出 config.properties");

        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }
        String targetDb = props.getProperty("target.db.database");
        assertNotNull(targetDb);
        assertEquals("tgt_db", targetDb,
                "target.db.database 为空会让 increment 拿源库名限定 DML，目标库静默收不到数据");
    }
}
