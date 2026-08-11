package com.migration.extract;

import com.migration.common.txn.TxnMetadata;
import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 源库 XA 事务：<b>缓冲到源库提交点，才整段下发</b>。
 *
 * <p>MySQL 的 XA 事务在 binlog 里是拆成两段写的（实测 8.0.44）——行事件在
 * {@code XA PREPARE} 时刻就落盘，提交/回滚的决议要等到之后任意位置的
 * {@code XA COMMIT} / {@code XA ROLLBACK}，中间还会穿插任意多个其它事务。
 *
 * <p>改造前这里三处都是错的：行事件在 prepare 时刻就被应用（源库随后回滚就在目标库留下
 * <b>永久幻影行</b>）；{@code XA START} 这类控制语句被当成普通语句原样打到目标库，把应用连接
 * 卡进 XA ACTIVE 态导致任务永久停摆；而且整个 XA 事务在目标库还是逐行提交、没有原子性。
 */
@DisplayName("MySQL extract：源库 XA 事务缓冲到提交点再下发")
class XaTransactionSyncTest {

    private static final String XID_A = "X'6161',X'6262',1";
    private static final String XID_B = "X'6363',X'6464',1";

    private MySQLBinlogExtractor extractor;
    private Path spoolRoot;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        spoolRoot = tempDir;
        extractor = newExtractor(tempDir);
    }

    private MySQLBinlogExtractor newExtractor(Path outputDir) throws Exception {
        MySQLBinlogExtractor ex = new MySQLBinlogExtractor();

        Properties props = new Properties();
        props.setProperty("extract.input.dir", "binlog_output");
        props.setProperty("extract.output.dir", outputDir.toString());
        java.lang.reflect.Field propsField = MySQLBinlogExtractor.class.getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(ex, props);

        Connection h2Conn = DriverManager.getConnection(
                "jdbc:h2:mem:xa-sync-test;MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        ex.sourceConnection = h2Conn;

        XaTransactionBuffer buffer = new XaTransactionBuffer(props, outputDir.toString());
        buffer.recover();
        java.lang.reflect.Field bufferField = MySQLBinlogExtractor.class.getDeclaredField("xaBuffer");
        bufferField.setAccessible(true);
        bufferField.set(ex, buffer);

        return ex;
    }

    /** 喂一条 .cap 行，返回它<b>实际下发</b>的事件（含该行触发的整段 XA 重放）。 */
    private List<THLEvent> feed(String eventType, long position, String eventData) throws Exception {
        return feed(extractor, eventType, position, 1_700_000_000_000L, eventData);
    }

    private List<THLEvent> feed(MySQLBinlogExtractor ex, String eventType,
                                long position, long timestamp, String eventData) throws Exception {
        List<THLEvent> emitted = new ArrayList<>();
        String line = eventType + "\001mysql-bin.000001\001" + position + "\001" + timestamp + "\0011\001" + eventData;
        THLEvent direct = ex.doExtract(line.getBytes("UTF-8"));
        if (direct != null) {
            emitted.add(direct);
        }
        ex.drainXaReplay(emitted::add);
        return emitted;
    }

    private List<THLEvent> query(long position, String sql) throws Exception {
        return feed("QUERY", position, "QueryEventData{threadId=1, database='db', sql='" + sql + "'}");
    }

    private List<THLEvent> row(long position, int id) throws Exception {
        return feed("WRITE_ROWS", position,
                "WriteRowsEventData{tableId=1, includedColumns={0}, rows=[[" + id + "]]}");
    }

    private List<THLEvent> prepare(long position, boolean onePhase) throws Exception {
        return feed("XA_PREPARE", position,
                "XAPrepareEventData{onePhase=" + onePhase + ", formatID=1, gtridLength=2, bqualLength=2}");
    }

    private File[] spoolFiles() {
        File[] files = new File(spoolRoot.toFile(), "xa_spool").listFiles();
        return files == null ? new File[0] : files;
    }

    @Test
    @DisplayName("两阶段提交：prepare 段整段扣下，XA COMMIT 才作为一个普通事务下发")
    void twoPhaseCommitIsDeferredToCommitPoint() throws Exception {
        assertTrue(query(100, "XA START " + XID_A).isEmpty(), "XA START 不下发（目标端没有对应动作）");
        assertTrue(row(200, 1).isEmpty(), "prepare 段的行事件必须扣下，不能在源库提交前就应用");
        assertTrue(row(300, 2).isEmpty());
        assertTrue(query(400, "XA END " + XID_A).isEmpty());
        assertTrue(prepare(500, false).isEmpty(), "prepare 只是「数据已就绪」，不是提交");

        // prepare 与 commit 之间穿插的普通事务照常流过，不受未决分支影响
        assertEquals(1, query(600, "BEGIN").size());
        assertEquals(1, row(700, 99).size());
        assertEquals(1, feed("XID", 800, "XidEventData{xid=4242}").size());

        List<THLEvent> committed = query(900, "XA COMMIT " + XID_A);

        assertEquals(3, committed.size(), "两个行事件 + 一个收尾的 COMMIT 事件");
        String txId = TxnMetadata.txIdOf(committed.get(0).getMetadata());
        assertNotNull(txId, "整段必须带上同一个 tx_id，增量端才会把它当一个事务原子提交");
        for (THLEvent e : committed) {
            assertEquals(txId, TxnMetadata.txIdOf(e.getMetadata()));
        }
        assertFalse(TxnMetadata.isTxLast(committed.get(0).getMetadata()));
        assertFalse(TxnMetadata.isTxLast(committed.get(1).getMetadata()));

        THLEvent last = committed.get(2);
        assertTrue(TxnMetadata.isTxLast(last.getMetadata()), "末条带 tx_last，增量端看到它才提交");
        assertEquals("XID", last.getMetadata().get("event_type"),
                "收尾事件伪装成普通事务的 XID，下游不需要认识 XA");

        // 位点改写成 XA COMMIT 的位点：照搬 prepare 时刻的位点会让应用端位点倒退
        for (THLEvent e : committed) {
            assertEquals(900L, e.getMetadata().get("binlog_position"),
                    "重放事件的位点必须是提交点，否则应用端 checkpoint 会往回跳");
        }
        assertEquals("mysql-bin.000001:200", committed.get(0).getMetadata().get("xa_prepare_position"),
                "原始 prepare 位点保留备查");
        assertEquals("mysql-bin.000001:900#0", committed.get(0).getEventId());
        assertEquals("mysql-bin.000001:900#c", last.getEventId());

        assertEquals(0, spoolFiles().length, "分支下发后落盘文件必须删掉，不能在磁盘上泄漏");
    }

    @Test
    @DisplayName("两阶段回滚：整段丢弃，目标库不会留下幻影行")
    void twoPhaseRollbackEmitsNothing() throws Exception {
        query(100, "XA START " + XID_B);
        row(200, 1);
        row(300, 2);
        query(400, "XA END " + XID_B);
        prepare(500, false);

        assertEquals(1, spoolFiles().length, "已 prepare 的分支落盘等决议");

        List<THLEvent> rolledBack = query(900, "XA ROLLBACK " + XID_B);

        assertTrue(rolledBack.isEmpty(), "源库回滚的 XA 事务一个事件都不能下发");
        assertEquals(0, spoolFiles().length, "回滚后落盘文件立即删除");
    }

    @Test
    @DisplayName("一阶段提交（XA COMMIT … ONE PHASE）：prepare 事件本身就是提交点")
    void onePhaseCommitIsAppliedAtPrepareEvent() throws Exception {
        query(100, "XA START " + XID_A);
        row(200, 7);

        // 一阶段提交在 binlog 里就是一个 onePhase=true 的 XA_prepare 事件，没有后续的 XA COMMIT
        List<THLEvent> committed = prepare(300, true);

        assertEquals(2, committed.size(), "一个行事件 + 一个收尾的 COMMIT 事件");
        assertTrue(TxnMetadata.isTxLast(committed.get(1).getMetadata()));
        assertEquals(300L, committed.get(0).getMetadata().get("binlog_position"));
        assertEquals(0, spoolFiles().length);
    }

    @Test
    @DisplayName("跨 extract 重启：已 prepare 的分支从落盘文件恢复，决议到达时照常下发")
    void preparedBranchSurvivesRestart() throws Exception {
        query(100, "XA START " + XID_A);
        row(200, 11);
        row(300, 12);
        query(400, "XA END " + XID_A);
        prepare(500, false);
        assertEquals(1, spoolFiles().length);

        // 换一个 extractor 实例 = extract 进程重启（落盘目录不变）
        MySQLBinlogExtractor restarted = newExtractor(spoolRoot);

        List<THLEvent> committed = feed(restarted, "QUERY", 900, 1_700_000_000_000L,
                "QueryEventData{threadId=1, database='db', sql='XA COMMIT " + XID_A + "'}");

        assertEquals(3, committed.size(), "重启前 prepare 的分支，重启后仍要能完整下发");
        assertTrue(TxnMetadata.isTxLast(committed.get(2).getMetadata()));
        assertEquals(0, spoolFiles().length);
    }

    @Test
    @DisplayName("收集中的分支跨重启会被丢弃（靠 .cap 进度回退重收集），不会拼出半个事务")
    void halfCollectedBranchIsDroppedOnRestart() throws Exception {
        query(100, "XA START " + XID_A);
        row(200, 21);
        assertTrue(extractor.isXaBranchActive(), "分支收集中，此时 extract 不许把 .cap 读取进度落盘");

        MySQLBinlogExtractor restarted = newExtractor(spoolRoot);

        assertEquals(0, spoolFiles().length, "收集到一半的 .part 落盘文件重启时一律删除");
        assertFalse(restarted.isXaBranchActive());
    }

    @Test
    @DisplayName("被扣下的事件不消耗 seqno，THL 里不会留下永久空洞")
    void heldEventsDoNotBurnSeqno() throws Exception {
        long before = extractor.getCurrentSeqno();

        query(100, "XA START " + XID_A);
        row(200, 31);
        query(300, "XA END " + XID_A);
        prepare(400, false);

        assertEquals(before, extractor.getCurrentSeqno(),
                "扣下的行如果先占了 seqno，增量端按 seqno 连续推进就会卡在空洞上");

        List<THLEvent> committed = query(500, "XA COMMIT " + XID_A);
        assertEquals(before, committed.get(0).getSeqno(), "seqno 在真正下发的那一刻才分配");
    }

    @Test
    @DisplayName("未决分支不影响其它事务：中间穿插的普通事务照常实时下发")
    void unrelatedTransactionsFlowWhileBranchPending() throws Exception {
        query(100, "XA START " + XID_A);
        row(200, 41);
        query(300, "XA END " + XID_A);
        prepare(400, false);

        assertEquals(1, query(500, "BEGIN").size());
        assertEquals(1, row(600, 42).size());
        assertEquals(1, feed("XID", 700, "XidEventData{xid=9}").size());
    }
}
