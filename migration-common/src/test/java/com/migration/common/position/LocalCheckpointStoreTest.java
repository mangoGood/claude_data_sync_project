package com.migration.common.position;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 统一位点的本地载体：落盘、还原、节流，以及从老载体反推。
 *
 * <p>这份载体是 agent 上卷与接管回灌的唯一输入，所以它必须能<b>原样</b>round-trip
 * GTID 集（含 {@code :} {@code ,} {@code -}）和 Mongo resume token 的 JSON 文本——
 * 位点在这里失真一个字符，接管方就会拿着一个定位不到的位点去连源库。
 */
@DisplayName("本地统一位点存储")
class LocalCheckpointStoreTest {

    private final String taskId = "unit-test-ckpt-" + System.nanoTime();

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

    private CheckpointRecord mysqlRecord(String file, long pos, String gtid) {
        Properties payload = new Properties();
        payload.setProperty("binlog.file", file);
        payload.setProperty("binlog.position", String.valueOf(pos));
        if (gtid != null) {
            payload.setProperty("gtid.set", gtid);
        }
        return new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "mysql",
                CheckpointRecord.Kind.BINLOG_FILE_POS, payload,
                MonotonicKey.ofBinlog(file, pos), 1754000000000L);
    }

    @Test
    @DisplayName("落盘再读回：GTID 集一个字符都不能变")
    void roundTripKeepsGtidSetIntact() {
        String gtid = "8b2c1d3e-0000-0000-0000-000000000001:1-99999,"
                + "9c3d2e4f-0000-0000-0000-000000000002:1-5:7-12";
        assertTrue(LocalCheckpointStore.save(mysqlRecord("mysql-bin.000042", 43310545L, gtid)));

        CheckpointRecord back = LocalCheckpointStore.load(taskId, CheckpointRecord.Stage.CAPTURE);
        assertNotNull(back);
        assertEquals("mysql-bin.000042", back.payloadValue("binlog.file"));
        assertEquals("43310545", back.payloadValue("binlog.position"));
        assertEquals(gtid, back.payloadValue("gtid.set"));
        assertEquals(CheckpointRecord.Kind.BINLOG_FILE_POS, back.getKind());
        assertEquals(MonotonicKey.ofBinlog("mysql-bin.000042", 43310545L), back.getMonotonicKey());
        assertEquals(1754000000000L, back.getSourceTs());
    }

    @Test
    @DisplayName("payload 文本 round-trip：resume token 的 JSON 原样进出")
    void payloadTextRoundTripKeepsJson() {
        String json = "{\"resumeToken\": {\"_data\": \"826688A1B2000000012B0229\"}, \"sourceId\": \"rs0\"}";
        Properties payload = new Properties();
        payload.setProperty("mongo.checkpoint.json", json);
        CheckpointRecord record = new CheckpointRecord(taskId, CheckpointRecord.Stage.CAPTURE, "mongodb",
                CheckpointRecord.Kind.RESUME_TOKEN, payload, MonotonicKey.UNKNOWN, 0);

        Properties parsed = CheckpointRecord.parsePayload(record.payloadText());
        assertEquals(json, parsed.getProperty("mongo.checkpoint.json"));
    }

    @Test
    @DisplayName("payloadText 不含时间戳注释行：否则位点没变也会被当成变了、每拍都写库")
    void payloadTextIsStableAcrossCalls() throws Exception {
        CheckpointRecord record = mysqlRecord("mysql-bin.000001", 4L, null);
        String first = record.payloadText();
        Thread.sleep(5);
        assertEquals(first, record.payloadText());
        assertFalse(first.contains("#"), "payload 文本里不该出现注释行");
    }

    @Test
    @DisplayName("节流：间隔内不落盘，force 强制落盘")
    void throttleSkipsWithinInterval() {
        assertTrue(LocalCheckpointStore.save(mysqlRecord("mysql-bin.000001", 100L, null)));
        assertFalse(LocalCheckpointStore.saveThrottled(mysqlRecord("mysql-bin.000001", 200L, null), 60_000L, false),
                "节流窗口内不应落盘");
        assertTrue(LocalCheckpointStore.saveThrottled(mysqlRecord("mysql-bin.000001", 300L, null), 60_000L, true),
                "force=true 必须无视节流");
        assertEquals("300", LocalCheckpointStore.load(taskId, CheckpointRecord.Stage.CAPTURE)
                .payloadValue("binlog.position"));
    }

    @Test
    @DisplayName("loadAll 汇总多段位点；deleteAll 全清（倒换要用）")
    void loadAllAndDeleteAll() {
        LocalCheckpointStore.save(mysqlRecord("mysql-bin.000001", 4L, null));
        Properties applyPayload = new Properties();
        applyPayload.setProperty("seqno", "5000");
        LocalCheckpointStore.save(new CheckpointRecord(taskId, CheckpointRecord.Stage.APPLY, "mysql",
                CheckpointRecord.Kind.SEQNO, applyPayload, 5000, 0));

        List<CheckpointRecord> all = LocalCheckpointStore.loadAll(taskId);
        assertEquals(2, all.size());

        LocalCheckpointStore.deleteAll(taskId);
        assertTrue(LocalCheckpointStore.loadAll(taskId).isEmpty());
    }

    @Test
    @DisplayName("老载体反推：位点文件是自描述的，四种源各认各的键")
    void fromLegacyCapturePosition() {
        Properties mysql = new Properties();
        mysql.setProperty("binlog.file", "mysql-bin.000007");
        mysql.setProperty("binlog.position", "1234");
        assertEquals(CheckpointRecord.Kind.BINLOG_FILE_POS,
                LocalCheckpointStore.fromCapturePosition(taskId, mysql).getKind());

        Properties pg = new Properties();
        pg.setProperty("wal.lsn", "0/16B3748");
        CheckpointRecord pgRecord = LocalCheckpointStore.fromCapturePosition(taskId, pg);
        assertEquals(CheckpointRecord.Kind.LSN, pgRecord.getKind());
        assertEquals("postgresql", pgRecord.getEngine());

        Properties oracle = new Properties();
        oracle.setProperty("redo.scn", "8675309");
        assertEquals(CheckpointRecord.Kind.SCN,
                LocalCheckpointStore.fromCapturePosition(taskId, oracle).getKind());

        // TiCDC 也写 binlog.file/position，所以 ticdc.commit.ts 必须先判，否则会被认成 MySQL
        Properties ticdc = new Properties();
        ticdc.setProperty("binlog.file", "ticdc.000001");
        ticdc.setProperty("binlog.position", "449576929101021185");
        ticdc.setProperty("ticdc.commit.ts", "449576929101021185");
        CheckpointRecord ticdcRecord = LocalCheckpointStore.fromCapturePosition(taskId, ticdc);
        assertEquals(CheckpointRecord.Kind.TSO, ticdcRecord.getKind());
        assertEquals("tidb", ticdcRecord.getEngine());

        assertNull(LocalCheckpointStore.fromCapturePosition(taskId, new Properties()),
                "认不出任何位点键时必须返回 null，让调用方按无位点处理");
    }

    @Test
    @DisplayName("残缺记录一律当作没有位点：宁可多重放，也不能拿半个位点去定位")
    void brokenRecordIsTreatedAsAbsent() {
        Properties broken = new Properties();
        broken.setProperty("ckpt.task.id", taskId);
        broken.setProperty("ckpt.stage", "CAPTURE");
        // 少了 ckpt.kind
        assertNull(CheckpointRecord.fromProperties(broken));

        Properties unknownEnum = new Properties();
        unknownEnum.setProperty("ckpt.task.id", taskId);
        unknownEnum.setProperty("ckpt.stage", "SOME_FUTURE_STAGE");
        unknownEnum.setProperty("ckpt.kind", "BINLOG_FILE_POS");
        assertNull(CheckpointRecord.fromProperties(unknownEnum),
                "不认识的枚举值不能猜，猜错就是拿错位点续传");
    }
}
