package com.migration.agent.service;

import com.migration.agent.model.TaskMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用户在创建任务时选的一致性语义，必须原样落到<b>任务自己的</b> config.properties 里——
 * 引擎只认配置文件，这一步漏了，页面上选的东西就等于没选。
 *
 * <p>两条最容易错的：
 * <ul>
 *   <li>事务一致必须<b>连带把并发关掉</b>。多 worker 各自提交只能保住每个连通分量内部有序，
 *       "目标提交顺序 = 源事务提交顺序"这条就不成立了。</li>
 *   <li>老 backend 发来的消息没有这个字段，此时必须按<b>任务类型</b>取默认
 *       （订阅/灾备事务一致、同步最终一致），而不是一律落到某一边。</li>
 * </ul>
 */
@DisplayName("一致性语义 → 引擎参数")
class ConsistencyModeConfigTest {

    private final String taskId = "unit-test-consistency-" + System.nanoTime();

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

    private Properties writeConfig(String taskType, String consistencyMode) throws Exception {
        TaskMessage msg = new TaskMessage();
        msg.setTaskId(taskId);
        msg.setTaskName("consistency-task");
        msg.setUserId(1L);
        msg.setSourceConnection("mysql://root:rootpassword@127.0.0.1:33306");
        msg.setTargetConnection("mysql://root:rootpassword@127.0.0.1:33306");
        msg.setMigrationMode("fullAndIncre");
        msg.setSourceDbName("src_db");
        msg.setTargetDbName("tgt_db");
        msg.setSourceType("mysql");
        msg.setTargetType("mysql");
        msg.setTaskType(taskType);
        msg.setConsistencyMode(consistencyMode);

        new ConfigService(new AgentConfig()).updateConfig(msg);

        File configFile = new File("files/" + taskId + "/config.properties");
        assertTrue(configFile.exists(), "updateConfig 应写出 config.properties");
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }
        return props;
    }

    @Test
    @DisplayName("同步任务不带字段 → 最终一致：EVENT + 冲突矩阵并发（线程数/提交条数都有默认值）")
    void syncDefaultsToEventual() throws Exception {
        Properties p = writeConfig("SYNC", null);

        assertEquals("EVENTUAL", p.getProperty("sync.consistency.mode"));
        assertEquals("EVENT", p.getProperty("apply.transaction.mode"));
        assertEquals("ROW", p.getProperty("increment.apply.conflict.granularity"));
        assertTrue(Integer.parseInt(p.getProperty("increment.apply.parallelism")) > 1,
                "最终一致要并发应用，线程数必须 >1");
        assertTrue(Integer.parseInt(p.getProperty("increment.apply.commit.batch.sql")) > 1,
                "最终一致允许攒批提交，单次提交 SQL 上限必须 >1");
        assertTrue(Integer.parseInt(p.getProperty("increment.apply.batch.size")) > 0);
    }

    @Test
    @DisplayName("订阅任务不带字段 → 事务一致：TRANSACTION + 串行 + 事务标记 topic")
    void subscribeDefaultsToTransactional() throws Exception {
        Properties p = writeConfig("SUBSCRIBE", null);

        assertEquals("TRANSACTIONAL", p.getProperty("sync.consistency.mode"));
        assertEquals("TRANSACTION", p.getProperty("apply.transaction.mode"));
        assertEquals("1", p.getProperty("increment.apply.parallelism"),
                "事务一致要求目标提交顺序与源一致，必须串行");
        assertEquals("true", p.getProperty("subscribe.transaction.topic.enabled"),
                "下游要按源事务边界重组消息，事务标记 topic 必须打开");
    }

    @Test
    @DisplayName("灾备任务不带字段 → 事务一致")
    void drDefaultsToTransactional() throws Exception {
        Properties p = writeConfig("DR", null);
        assertEquals("TRANSACTIONAL", p.getProperty("sync.consistency.mode"));
        assertEquals("TRANSACTION", p.getProperty("apply.transaction.mode"));
        assertEquals("1", p.getProperty("increment.apply.parallelism"));
    }

    @Test
    @DisplayName("用户显式选事务一致的同步任务：同样串行按源事务提交")
    void syncCanOptIntoTransactional() throws Exception {
        Properties p = writeConfig("SYNC", "TRANSACTIONAL");
        assertEquals("TRANSACTIONAL", p.getProperty("sync.consistency.mode"));
        assertEquals("TRANSACTION", p.getProperty("apply.transaction.mode"));
        assertEquals("1", p.getProperty("increment.apply.parallelism"));
    }

    @Test
    @DisplayName("用户显式选最终一致的灾备任务：按冲突矩阵并发")
    void drCanOptIntoEventual() throws Exception {
        Properties p = writeConfig("DR", "EVENTUAL");
        assertEquals("EVENTUAL", p.getProperty("sync.consistency.mode"));
        assertEquals("EVENT", p.getProperty("apply.transaction.mode"));
        assertTrue(Integer.parseInt(p.getProperty("increment.apply.parallelism")) > 1);
    }

    @Test
    @DisplayName("非法取值回落到该任务类型的默认值，不静默乱跑")
    void illegalValueFallsBackToTypeDefault() throws Exception {
        Properties p = writeConfig("SYNC", "STRONG");
        assertEquals("EVENTUAL", p.getProperty("sync.consistency.mode"));
        assertEquals("EVENT", p.getProperty("apply.transaction.mode"));
    }
}
