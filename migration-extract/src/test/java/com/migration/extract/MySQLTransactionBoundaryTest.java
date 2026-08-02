package com.migration.extract;

import com.migration.common.txn.TxnMetadata;
import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MySQL 源事务边界下发：{@code BEGIN … 行事件 … XID} 里的每个事件都要带上同一个 {@code tx_id}，
 * XID 额外带 {@code tx_last}。
 *
 * <p>没有这些元数据，增量端只能一个事件一个目标事务地落库——源库一笔转账的两条 UPDATE
 * 会变成目标库两个独立事务，中间时刻读到的是"扣款已落、入账未落"的不平账。
 * 事务信息其实一路都在（XID 事件本来就透传到了 extract），只是从来没往下传。
 */
@DisplayName("MySQL extract 下发源事务边界")
class MySQLTransactionBoundaryTest {

    private MySQLBinlogExtractor extractor;
    private Method doExtractMethod;

    @BeforeEach
    void setUp() throws Exception {
        extractor = new MySQLBinlogExtractor();
        Properties props = new Properties();
        props.setProperty("extract.input.dir", "binlog_output");
        props.setProperty("extract.output.dir", "thl_output");
        java.lang.reflect.Field propsField = extractor.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(extractor, props);

        Connection h2Conn = DriverManager.getConnection(
                "jdbc:h2:mem:txn-boundary-test;MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        java.lang.reflect.Field connField = extractor.getClass().getDeclaredField("sourceConnection");
        connField.setAccessible(true);
        connField.set(extractor, h2Conn);

        doExtractMethod = extractor.getClass().getDeclaredMethod("doExtract", byte[].class);
        doExtractMethod.setAccessible(true);
    }

    private THLEvent extract(String eventType, long position, String eventData) throws Exception {
        String line = eventType + "\001mysql-bin.000001\001" + position + "\0011700000000000\0011\001" + eventData;
        return (THLEvent) doExtractMethod.invoke(extractor, line.getBytes("UTF-8"));
    }

    private THLEvent begin(long position) throws Exception {
        return extract("QUERY", position, "QueryEventData{threadId=1, database='db', sql='BEGIN'}");
    }

    private THLEvent row(long position) throws Exception {
        return extract("WRITE_ROWS", position,
                "WriteRowsEventData{tableId=1, includedColumns={0}, rows=[[1]]}");
    }

    private THLEvent xid(long position, long xidValue) throws Exception {
        return extract("XID", position, "XidEventData{xid=" + xidValue + "}");
    }

    @Test
    @DisplayName("BEGIN…行事件…XID 共享同一个 tx_id，XID 带 tx_last")
    void eventsInOneTransactionShareTxId() throws Exception {
        THLEvent b = begin(100);
        THLEvent r1 = row(200);
        THLEvent r2 = row(300);
        THLEvent x = xid(400, 777);

        String txId = TxnMetadata.txIdOf(b.getMetadata());
        assertNotNull(txId);
        assertEquals("mysql-bin.000001:100", txId, "事务标识取 BEGIN 的位点——XID 只在事务末尾才出现，"
                + "行事件在它之前就得打上标识");
        assertEquals(txId, TxnMetadata.txIdOf(r1.getMetadata()));
        assertEquals(txId, TxnMetadata.txIdOf(r2.getMetadata()));
        assertEquals(txId, TxnMetadata.txIdOf(x.getMetadata()));

        assertFalse(TxnMetadata.isTxLast(r1.getMetadata()));
        assertFalse(TxnMetadata.isTxLast(r2.getMetadata()));
        assertTrue(TxnMetadata.isTxLast(x.getMetadata()), "XID 是事务末条，增量端见到它才提交");
        assertEquals("777", x.getMetadata().get(TxnMetadata.TX_SOURCE_ID), "源库真实事务号供下游对账");
    }

    @Test
    @DisplayName("两个事务的 tx_id 不同，XID 之后不再串到下一个事务")
    void separateTransactionsGetSeparateIds() throws Exception {
        begin(100);
        THLEvent r1 = row(200);
        xid(300, 1);

        begin(400);
        THLEvent r2 = row(500);

        assertEquals("mysql-bin.000001:100", TxnMetadata.txIdOf(r1.getMetadata()));
        assertEquals("mysql-bin.000001:400", TxnMetadata.txIdOf(r2.getMetadata()));
    }

    @Test
    @DisplayName("DDL 是隐式提交的独立事务，不并入当前事务")
    void ddlDoesNotJoinCurrentTransaction() throws Exception {
        begin(100);
        THLEvent ddl = extract("QUERY", 200,
                "QueryEventData{threadId=1, database='db', sql='ALTER TABLE t ADD COLUMN c INT'}");
        THLEvent afterDdl = row(300);

        assertNull(TxnMetadata.txIdOf(ddl.getMetadata()), "DDL 不属于任何显式事务");
        assertNull(TxnMetadata.txIdOf(afterDdl.getMetadata()),
                "DDL 隐式提交后当前事务已结束，后续行事件不应还挂在旧 tx_id 上");
    }

    @Test
    @DisplayName("没见过 BEGIN 就收到 XID（断点续传落在事务中间）时该事件自成一事务")
    void xidWithoutBeginFallsBackToItsOwnPosition() throws Exception {
        THLEvent x = xid(900, 42);
        assertEquals("mysql-bin.000001:900", TxnMetadata.txIdOf(x.getMetadata()));
        assertTrue(TxnMetadata.isTxLast(x.getMetadata()));
    }

    @Test
    @DisplayName("非事务引擎用 QUERY 'COMMIT' 收尾，同样算事务末条")
    void queryCommitClosesTransaction() throws Exception {
        begin(100);
        THLEvent r = row(200);
        THLEvent c = extract("QUERY", 300, "QueryEventData{threadId=1, database='db', sql='COMMIT'}");

        assertEquals("mysql-bin.000001:100", TxnMetadata.txIdOf(r.getMetadata()));
        assertEquals("mysql-bin.000001:100", TxnMetadata.txIdOf(c.getMetadata()));
        assertTrue(TxnMetadata.isTxLast(c.getMetadata()));
    }
}
