package com.migration.agent.service;

import com.migration.agent.AgentMain;
import com.migration.common.position.CapturePositionStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 主备倒换必须清掉 capture 的已落盘位点 —— 这是"位点持久化优先"的配套不变量。
 *
 * <p>倒换后源库换成了原目标实例，旧实例的 binlog 位点/GTID/LSN/SCN 在新源上毫无意义。
 * GTID 尤其致命：GTID 集里全是旧源 server_uuid 的事务，新源一条都不认识，服务端会按
 * "客户端缺这些事务"的语义从新源 binlog 的<b>最开头</b>重放整段历史（含历史 DDL 与全量导入），
 * 把新备库冲垮——这是实测踩过的坑。
 *
 * <p>清 {@code config.properties} 里的位点键是原有逻辑；改成"已落盘位点优先"之后，
 * {@code binlog_output/capture_position.properties} 成了新的位点载体，同样必须被清掉，
 * 否则倒换后 capture 会绕过被清空的 config 直接捡起旧源位点。本测试锁死这条链路。
 */
@DisplayName("倒换清理：capture 已落盘位点必须一并清掉")
class FailoverPositionCleanupTest {

    private final String taskId = "unit-test-failover-" + System.nanoTime();

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

    private File seedPersistedPosition() {
        String outputDir = "files/" + taskId + "/binlog_output";
        new File(outputDir).mkdirs();
        Properties pos = new Properties();
        pos.setProperty("binlog.file", "binlog.000042");
        pos.setProperty("binlog.position", "43310545");
        pos.setProperty("gtid.set", "8b2c1d3e-0000-0000-0000-000000000001:1-99999");
        CapturePositionStore.save(outputDir, pos, "seeded by test");
        File f = CapturePositionStore.fileIn(outputDir);
        assertTrue(f.exists(), "前置条件：位点文件应已写出");
        return f;
    }

    @Test
    @DisplayName("FailoverService.cleanFailoverFiles 清掉 capture_position.properties")
    void failoverServiceClearsPersistedPosition() throws Exception {
        File posFile = seedPersistedPosition();

        FailoverService svc = new FailoverService(null, null, null, null);
        Method m = FailoverService.class.getDeclaredMethod("cleanFailoverFiles", String.class);
        m.setAccessible(true);
        m.invoke(svc, taskId);

        assertFalse(posFile.exists(),
                "倒换后必须清掉已落盘位点，否则 capture 会拿旧源的 GTID/位点去连新源");
    }

    @Test
    @DisplayName("FailoverService.cleanFailoverFiles 也清掉统一位点载体")
    void failoverServiceClearsUnifiedCheckpoint() throws Exception {
        // 位点中心化之后，这条不变量的边界跟着扩了：本地清干净而统一载体留着，
        // 下一拍上卷就把旧源位点又送回中心库，接管方回灌后照样拿旧源的 GTID 去连新源。
        Properties payload = new Properties();
        payload.setProperty("binlog.file", "binlog.000042");
        payload.setProperty("binlog.position", "43310545");
        com.migration.common.position.LocalCheckpointStore.save(
                new com.migration.common.position.CheckpointRecord(taskId,
                        com.migration.common.position.CheckpointRecord.Stage.CAPTURE, "mysql",
                        com.migration.common.position.CheckpointRecord.Kind.BINLOG_FILE_POS,
                        payload, 1L, 0));
        assertTrue(new File("files/" + taskId + "/checkpoint/positions/capture.properties").isFile(),
                "前置条件：统一位点应已写出");

        FailoverService svc = new FailoverService(null, null, null, null);
        Method m = FailoverService.class.getDeclaredMethod("cleanFailoverFiles", String.class);
        m.setAccessible(true);
        m.invoke(svc, taskId);

        assertTrue(com.migration.common.position.LocalCheckpointStore.loadAll(taskId).isEmpty(),
                "倒换后统一位点必须一并清掉，否则会被重新上卷到中心库");
    }

    @Test
    @DisplayName("倒换清理的 config 位点键覆盖三种源类型的 capture 起始位点")
    void staleKeysCoverAllSourceTypes() {
        java.util.List<String> keys = Arrays.asList(AgentMain.STALE_POSITION_KEYS_ON_FAILOVER);
        for (String required : new String[]{
                "capture.binlog.file", "capture.binlog.position", "capture.gtid.set",
                "capture.wal.lsn", "capture.redo.scn"}) {
            assertTrue(keys.contains(required), "倒换必须清除 config 中的 " + required);
        }
    }
}
