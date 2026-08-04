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
 * 用户选的"全量装载通道 + 一致性快照"必须原样落到<b>任务自己的</b> config.properties 里。
 *
 * <p>这一步是整条链路里最容易断的地方：引擎（SQL 全量 / Mongo / ES / Redis）只认配置文件，
 * agent 不写这两组键，页面上选的档位就等于没选——升级前这些键<b>根本没人写</b>，
 * 用户只能吃引擎的编译期默认值，CONSISTENT 档位实际上不可达。
 *
 * <p>另一条：老 backend 发来的消息不带这些字段，此时必须按<b>源端</b>取默认（MySQL 只记位点，
 * 其余源端给真快照），而不是一律落到某一边。
 */
@DisplayName("全量装载/快照档位 → 引擎参数")
class FullLoadConfigTest {

    private final String taskId = "unit-test-fullload-" + System.nanoTime();

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

    private String connectionScheme(String sourceType) {
        switch (sourceType) {
            case "postgresql":
                return "postgresql";
            case "mongodb":
                return "mongodb";
            case "oracle":
                return "oracle";
            case "redis":
                return "redis";
            default:
                return "mysql";
        }
    }

    private Properties writeConfig(String sourceType, Boolean bulkEnabled, String bulkMode, String snapshotMode)
            throws Exception {
        TaskMessage msg = new TaskMessage();
        msg.setTaskId(taskId);
        msg.setTaskName("fullload-task");
        msg.setUserId(1L);
        // 连接串协议按源端类型给（TiDB 走 MySQL 协议）；本用例关心的是档位下发，不是连接解析
        msg.setSourceConnection(connectionScheme(sourceType) + "://root:rootpassword@127.0.0.1:33306");
        msg.setTargetConnection("mysql://root:rootpassword@127.0.0.1:33306");
        msg.setMigrationMode("fullAndIncre");
        msg.setSourceDbName("src_db");
        msg.setTargetDbName("tgt_db");
        msg.setSourceType(sourceType);
        msg.setTargetType("mysql");
        msg.setTaskType("SYNC");
        msg.setBulkLoadEnabled(bulkEnabled);
        msg.setBulkLoadMode(bulkMode);
        msg.setSnapshotMode(snapshotMode);

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
    @DisplayName("用户选的档位原样落到配置文件（这两组键升级前根本没人写）")
    void writesChosenModes() throws Exception {
        Properties p = writeConfig("postgresql", true, "COPY", "CONSISTENT");

        assertEquals("true", p.getProperty("migration.full.bulk.enabled"));
        assertEquals("COPY", p.getProperty("migration.full.bulk.mode"));
        assertEquals("CONSISTENT", p.getProperty("migration.full.snapshot.mode"));
    }

    @Test
    @DisplayName("关闭批量装载：开关必须落成 false，引擎据此退回逐条写")
    void writesDisabledFlag() throws Exception {
        Properties p = writeConfig("mysql", false, "AUTO", "NONE");
        assertEquals("false", p.getProperty("migration.full.bulk.enabled"));
        assertEquals("NONE", p.getProperty("migration.full.snapshot.mode"));
    }

    @Test
    @DisplayName("老消息不带字段 → 装载 AUTO + 快照按源端取默认")
    void defaultsPerSourceType() throws Exception {
        assertEquals("GTID_ONLY", writeConfig("mysql", null, null, null)
                .getProperty("migration.full.snapshot.mode"), "MySQL 真快照要全局读锁，默认只记位点");
        assertEquals("AUTO", writeConfig("mysql", null, null, null)
                .getProperty("migration.full.bulk.mode"));
        assertEquals("true", writeConfig("mysql", null, null, null)
                .getProperty("migration.full.bulk.enabled"));

        assertEquals("CONSISTENT", writeConfig("tidb", null, null, null)
                .getProperty("migration.full.snapshot.mode"), "TiDB 历史读不加锁");
        assertEquals("CONSISTENT", writeConfig("postgresql", null, null, null)
                .getProperty("migration.full.snapshot.mode"));
        assertEquals("CONSISTENT", writeConfig("oracle", null, null, null)
                .getProperty("migration.full.snapshot.mode"));
        assertEquals("CONSISTENT", writeConfig("mongodb", null, null, null)
                .getProperty("migration.full.snapshot.mode"));
    }

    @Test
    @DisplayName("非法档位回落默认值，不静默乱跑也不让任务起不来")
    void illegalValuesFallBack() throws Exception {
        Properties p = writeConfig("mysql", true, "auto", "STRONG");
        assertEquals("AUTO", p.getProperty("migration.full.bulk.mode"), "档位大小写不敏感");
        assertEquals("GTID_ONLY", p.getProperty("migration.full.snapshot.mode"));
    }

    @Test
    @DisplayName("默认档位规则与后端一致（两边各写一份，规则必须同源）")
    void defaultRuleMatchesBackend() {
        assertEquals("GTID_ONLY", ConfigService.defaultSnapshotMode("mysql"));
        assertEquals("GTID_ONLY", ConfigService.defaultSnapshotMode(null));
        assertEquals("CONSISTENT", ConfigService.defaultSnapshotMode("redis"));
        assertEquals("CONSISTENT", ConfigService.defaultSnapshotMode("TiDB"));
    }
}
