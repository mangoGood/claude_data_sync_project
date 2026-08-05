package com.migration.agent.checkpoint;

import com.migration.common.position.CapturePositionStore;
import com.migration.common.position.CheckpointRecord;
import com.migration.common.position.LocalCheckpointStore;
import com.migration.common.position.MonotonicKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 回灌决策表：位点持续持久化要修的那个静默数据丢失，正面就锁在这里。
 *
 * <p>没有回灌时，跨机接管走的是 {@code AbstractTaskExecutor.initMysqlCheckpoint} 的
 * "loadCheckpoint() == null → 取源库此刻的位点"分支，崩溃到接管之间的变更被整段跳过，
 * 而且不报错、不告警、进度条一路 100%。
 */
@DisplayName("中心位点回灌决策")
class CheckpointHydratorTest {

    private final String taskId = "unit-test-hydrate-" + System.nanoTime();

    @AfterEach
    void cleanup() {
        deleteRecursively(new File("files/" + taskId));
        CheckpointHydrator.reset();   // 别把静态单例漏给同一 JVM 里的其它单测
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

    /** 不连库的中心存储替身：只回放给定的记录，或按需抛异常模拟元数据库不可达。 */
    private static class FakeCentralStore extends CentralCheckpointStore {
        private final List<CheckpointRecord> records;
        private final boolean unreachable;

        FakeCentralStore(List<CheckpointRecord> records, boolean unreachable) {
            super(null, null, null);
            this.records = records;
            this.unreachable = unreachable;
        }

        @Override
        public List<CheckpointRecord> loadAll(String taskId) throws SQLException {
            if (unreachable) {
                throw new SQLException("模拟元数据库不可达");
            }
            return new ArrayList<>(records);
        }

        @Override
        public WriteResult upsert(CheckpointRecord record, String agentId, int leaseEpoch) {
            records.add(record);
            return WriteResult.ACCEPTED;
        }

        @Override
        public int leaseEpoch(String taskId) {
            return 1;
        }
    }

    private CheckpointRecord captureRecord() {
        Properties payload = new Properties();
        payload.setProperty("binlog.file", "mysql-bin.000042");
        payload.setProperty("binlog.position", "43310545");
        payload.setProperty("gtid.set", "8b2c1d3e-0000-0000-0000-000000000001:1-99999");
        return new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "mysql",
                CheckpointRecord.Kind.BINLOG_FILE_POS, payload,
                MonotonicKey.ofBinlog("mysql-bin.000042", 43310545L), 0);
    }

    private CheckpointRecord applySeqnoRecord() {
        Properties payload = new Properties();
        payload.setProperty("seqno", "5000");
        return new CheckpointRecord(taskId, CheckpointRecord.Stage.APPLY, "mysql",
                CheckpointRecord.Kind.SEQNO, payload, 5000, 0);
    }

    private CheckpointHydrator hydratorFor(CentralCheckpointStore store, boolean failStop) {
        CheckpointHydrator.initialize(store, "agent-test", failStop);
        return CheckpointHydrator.getInstance();
    }

    @Test
    @DisplayName("本地已有位点 = 同机重启：一个字节都不动")
    void localPositionWins() {
        Properties pos = new Properties();
        pos.setProperty("binlog.file", "mysql-bin.000001");
        pos.setProperty("binlog.position", "4");
        CapturePositionStore.save("files/" + taskId + "/binlog_output", pos, "seeded");

        CheckpointHydrator hydrator = hydratorFor(
                new FakeCentralStore(new ArrayList<>(Collections.singletonList(captureRecord())), false), true);

        assertEquals(CheckpointHydrator.Result.NOT_NEEDED, hydrator.hydrate(taskId));
        assertEquals("4", CapturePositionStore.load("files/" + taskId + "/binlog_output")
                .getProperty("binlog.position"), "同机重启不得被中心位点覆盖");
    }

    @Test
    @DisplayName("本地无位点 + 中心有源端位点 = 跨机接管：回灌成老载体，capture 才读得到")
    void hydratesOntoLegacyCarrier() {
        CheckpointHydrator hydrator = hydratorFor(
                new FakeCentralStore(new ArrayList<>(Collections.singletonList(captureRecord())), false), true);

        assertEquals(CheckpointHydrator.Result.HYDRATED, hydrator.hydrate(taskId));

        Properties materialized = CapturePositionStore.load("files/" + taskId + "/binlog_output");
        assertEquals("mysql-bin.000042", materialized.getProperty("binlog.file"));
        assertEquals("43310545", materialized.getProperty("binlog.position"));
        assertEquals("8b2c1d3e-0000-0000-0000-000000000001:1-99999", materialized.getProperty("gtid.set"));
        assertTrue(new File("files/" + taskId + "/checkpoint/positions/capture.properties").isFile(),
                "统一载体也要落一份，下一拍上卷才有东西可读");
    }

    @Test
    @DisplayName("中心库只有 seqno：绝不能回灌，也不能放行——必须 FAILED")
    void seqnoIsNeverHydratable() {
        // seqno 是 thl_output/ 里的本机文件坐标。接管方 THL 目录是空的、会从 0 重新编号，
        // 把"已应用到 seqno=5000"灌过去，increment 会把新产出的 seqno<=5000 事件全部跳过——
        // 那是比重放严重得多的静默丢数据。
        CheckpointHydrator hydrator = hydratorFor(
                new FakeCentralStore(new ArrayList<>(Collections.singletonList(applySeqnoRecord())), false), true);

        assertEquals(CheckpointHydrator.Result.FAILED, hydrator.hydrate(taskId));
        assertTrue(CapturePositionStore.load("files/" + taskId + "/binlog_output").isEmpty(),
                "没有可用源端位点时，绝不能凭空造一个位点文件出来");
    }

    @Test
    @DisplayName("同时有 CAPTURE 与 APPLY：只灌 CAPTURE，APPLY 一律丢弃")
    void hydratesOnlySourceSideCoordinates() {
        CheckpointHydrator hydrator = hydratorFor(new FakeCentralStore(
                new ArrayList<>(Arrays.asList(captureRecord(), applySeqnoRecord())), false), true);

        assertEquals(CheckpointHydrator.Result.HYDRATED, hydrator.hydrate(taskId));
        assertTrue(new File("files/" + taskId + "/checkpoint/positions/capture.properties").isFile());
        assertTrue(LocalCheckpointStore.load(taskId, CheckpointRecord.Stage.APPLY) == null,
                "APPLY 的 seqno 位点不得落到接管方本地");
    }

    @Test
    @DisplayName("中心库确认没有记录 = 真·首启：放行，走原有取源库当前位点")
    void emptyCentralMeansFirstStart() {
        CheckpointHydrator hydrator = hydratorFor(new FakeCentralStore(new ArrayList<>(), false), true);
        assertEquals(CheckpointHydrator.Result.FIRST_START, hydrator.hydrate(taskId));
    }

    @Test
    @DisplayName("中心库读不到 = 判不出首启还是接管：fail-stop，不许猜")
    void unreachableCentralFailsStop() {
        CheckpointHydrator hydrator = hydratorFor(new FakeCentralStore(new ArrayList<>(), true), true);
        assertEquals(CheckpointHydrator.Result.FAILED, hydrator.hydrate(taskId),
                "猜成首启而实际是接管 = 静默丢一段数据，这是本批要消灭的那类故障");
    }

    @Test
    @DisplayName("fail.stop=false 时退回旧行为（仅排障用，代价是可能丢数据）")
    void failStopCanBeDisabledForTroubleshooting() {
        CheckpointHydrator hydrator = hydratorFor(new FakeCentralStore(new ArrayList<>(), true), false);
        assertEquals(CheckpointHydrator.Result.FIRST_START, hydrator.hydrate(taskId));
    }

    @Test
    @DisplayName("人工重置：本地有位点也要被强制覆盖，否则重置永远不生效")
    void resetOverridesLocalPosition() {
        Properties pos = new Properties();
        pos.setProperty("binlog.file", "mysql-bin.000099");
        pos.setProperty("binlog.position", "88888");
        CapturePositionStore.save("files/" + taskId + "/binlog_output", pos, "seeded");
        LocalCheckpointStore.save(new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE,
                CheckpointRecord.DEFAULT_STREAM_KEY, "mysql", CheckpointRecord.Kind.BINLOG_FILE_POS,
                pos, MonotonicKey.ofBinlog("mysql-bin.000099", 88888L), 0,
                System.currentTimeMillis(), 0L));

        // 后端把位点退回到更早的点并打上 reset_at
        Properties older = new Properties();
        older.setProperty("binlog.file", "mysql-bin.000042");
        older.setProperty("binlog.position", "4");
        CheckpointRecord reset = new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE,
                CheckpointRecord.DEFAULT_STREAM_KEY, "mysql", CheckpointRecord.Kind.BINLOG_FILE_POS,
                older, MonotonicKey.ofBinlog("mysql-bin.000042", 4L), 0,
                System.currentTimeMillis(), System.currentTimeMillis());

        CheckpointHydrator hydrator = hydratorFor(
                new FakeCentralStore(new ArrayList<>(Collections.singletonList(reset)), false), true);

        assertEquals(CheckpointHydrator.Result.RESET_APPLIED, hydrator.hydrate(taskId));
        assertEquals("mysql-bin.000042",
                CapturePositionStore.load("files/" + taskId + "/binlog_output").getProperty("binlog.file"),
                "重置的位点必须落到 capture 真正会读的老载体上");

        // 已应用过的重置不能反复覆盖：本地记下了同一个 resetAt，再启动就该是 NOT_NEEDED
        assertEquals(CheckpointHydrator.Result.NOT_NEEDED, hydrator.hydrate(taskId));
    }

    @Test
    @DisplayName("本地有位点而中心库读不到：按同机重启继续，不因中心库抖动挡住启动")
    void unreachableCentralWithLocalPositionStillStarts() {
        Properties pos = new Properties();
        pos.setProperty("binlog.file", "mysql-bin.000001");
        pos.setProperty("binlog.position", "4");
        CapturePositionStore.save("files/" + taskId + "/binlog_output", pos, "seeded");

        CheckpointHydrator hydrator = hydratorFor(new FakeCentralStore(new ArrayList<>(), true), true);
        assertEquals(CheckpointHydrator.Result.NOT_NEEDED, hydrator.hydrate(taskId));
    }

    @Test
    @DisplayName("首启位点立刻进中心库，不等上卷那一拍")
    void publishesInitialPositionImmediately() {
        List<CheckpointRecord> central = new ArrayList<>();
        CheckpointHydrator hydrator = hydratorFor(new FakeCentralStore(central, false), true);

        hydrator.publishInitialPosition(taskId, "mysql", "mysql-bin.000009", 1234L,
                "8b2c1d3e-0000-0000-0000-000000000001:1-10");

        assertEquals(1, central.size(), "首启后几秒内崩溃并被接管时，中心库里必须已经有这条位点");
        assertEquals("mysql-bin.000009", central.get(0).payloadValue("binlog.file"));
        assertEquals("mysql-bin.000009", LocalCheckpointStore.load(taskId, CheckpointRecord.Stage.CAPTURE)
                .payloadValue("binlog.file"));
    }
}
