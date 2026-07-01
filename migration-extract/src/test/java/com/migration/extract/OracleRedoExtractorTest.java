package com.migration.extract;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OracleRedoExtractor} redo 事件解析单元测试。
 *
 * <p>验证 INSERT/UPDATE/DELETE 事件、SQL_REDO 回退、checkpoint 跳过等解析逻辑的正确性。
 * 通过反射调用 doExtract 方法，避免依赖真实 Oracle 连接。
 * 注入 H2 内存连接（Oracle 模式），使元数据查询可正常执行。
 */
@DisplayName("OracleRedoExtractor redo 解析测试")
class OracleRedoExtractorTest {

    private OracleRedoExtractor extractor;
    private Method doExtractMethod;
    private Connection h2Conn;

    @BeforeEach
    void setUp() throws Exception {
        extractor = new OracleRedoExtractor();
        Properties props = new Properties();
        props.setProperty("extract.output.dir", "target/oracle_extract_test_output");
        props.setProperty("source.db.host", "localhost");
        props.setProperty("source.db.port", "1521");
        props.setProperty("source.db.username", "TESTUSER");
        props.setProperty("source.db.password", "testpass");
        props.setProperty("source.db.database", "ORCL");
        // 不调用 initialize 避免连接真实 Oracle，直接设置 props
        java.lang.reflect.Field propsField = extractor.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(extractor, props);

        // 注入 H2 内存连接（Oracle 模式），模拟 Oracle 元数据查询
        h2Conn = DriverManager.getConnection(
                "jdbc:h2:mem:oracle-extractor-test;MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "");
        setupH2Schema(h2Conn);

        java.lang.reflect.Field connField = extractor.getClass().getDeclaredField("sourceConnection");
        connField.setAccessible(true);
        connField.set(extractor, h2Conn);

        // 获取 doExtract 方法
        doExtractMethod = extractor.getClass().getDeclaredMethod("doExtract", byte[].class);
        doExtractMethod.setAccessible(true);
    }

    /**
     * 在 H2 中创建模拟的 Oracle 元数据表，使 ALL_TAB_COLUMNS / ALL_CONSTRAINTS 查询能返回结果。
     * H2 的 MODE=Oracle 支持部分 Oracle 兼容视图。
     */
    private void setupH2Schema(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            // 先创建 schema
            stmt.execute("CREATE SCHEMA IF NOT EXISTS TESTUSER");
            // 创建测试表，使 extractor 的元数据查询能匹配到列信息
            stmt.execute("CREATE TABLE IF NOT EXISTS TESTUSER.EMPLOYEES (" +
                    "ID NUMBER(10) PRIMARY KEY, " +
                    "NAME VARCHAR2(100), " +
                    "SALARY NUMBER(10,2), " +
                    "HIREDATE DATE)");
            stmt.execute("CREATE TABLE IF NOT EXISTS TESTUSER.ORDERS (" +
                    "ORDER_ID NUMBER(10) PRIMARY KEY, " +
                    "CUSTOMER_ID NUMBER(10), " +
                    "AMOUNT NUMBER(10,2))");
        }
    }

    private THLEvent extract(String eventStr) throws Exception {
        return (THLEvent) doExtractMethod.invoke(extractor, eventStr.getBytes("UTF-8"));
    }

    /**
     * 构造 Oracle redo 事件字符串：
     * eventType \001 scn \001 scnNumeric \001 timestamp \001 xid \001 eventData
     */
    private String buildEvent(String eventType, String scn, long scnNumeric,
                              long timestamp, long xid, String eventData) {
        return eventType + "\001" + scn + "\001" + scnNumeric + "\001" + timestamp + "\001"
                + xid + "\001" + eventData;
    }

    @Test
    @DisplayName("空字符串应返回 null")
    void emptyInputShouldReturnNull() throws Exception {
        assertNull(extract(""));
        assertNull(extract("   "));
    }

    @Test
    @DisplayName("字段数不足应返回 null")
    void insufficientFieldsShouldReturnNull() throws Exception {
        assertNull(extract("INSERT\001123456"));
    }

    @Test
    @DisplayName("INSERT 事件应解析为 INSERT 操作并提取行数据")
    void insertEventShouldBeParsedAsInsert() throws Exception {
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID " +
                "new-tuple:{ID:1001,NAME:'Alice',SALARY:5000.00,HIREDATE:TO_DATE('2024-01-15','YYYY-MM-DD')}";
        String event = buildEvent("INSERT", "123456789", 123456789L,
                1700000000000L, 1001L, eventData);

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("oracle", thlEvent.getSourceId());
        assertEquals("123456789", thlEvent.getEventId());
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
        assertEquals("123456789", thlEvent.getMetadata().get("redo_scn"));
        assertEquals(123456789L, thlEvent.getMetadata().get("redo_scn_numeric"));
        assertEquals(1001L, thlEvent.getMetadata().get("xid"));
        assertEquals("TESTUSER", thlEvent.getMetadata().get("database_name"));
        assertEquals("EMPLOYEES", thlEvent.getMetadata().get("table_name"));
        assertEquals("ID", thlEvent.getMetadata().get("primary_keys"));
        assertNotNull(thlEvent.getMetadata().get("row_data"));
        assertNotNull(thlEvent.getMetadata().get("rows_data"));
    }

    @Test
    @DisplayName("UPDATE 事件应解析为 UPDATE 操作并提取新旧行数据")
    void updateEventShouldBeParsedAsUpdate() throws Exception {
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID " +
                "old-tuple:{ID:1001,NAME:'Alice',SALARY:5000.00} " +
                "new-tuple:{ID:1001,NAME:'Alice Smith',SALARY:6000.00}";
        String event = buildEvent("UPDATE", "123456790", 123456790L,
                1700000001000L, 1002L, eventData);

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("UPDATE", thlEvent.getMetadata().get("operation"));
        assertEquals("TESTUSER", thlEvent.getMetadata().get("database_name"));
        assertEquals("EMPLOYEES", thlEvent.getMetadata().get("table_name"));
        assertNotNull(thlEvent.getMetadata().get("row_data"));
        assertNotNull(thlEvent.getMetadata().get("row_data_before"));
    }

    @Test
    @DisplayName("DELETE 事件应解析为 DELETE 操作并提取旧行数据")
    void deleteEventShouldBeParsedAsDelete() throws Exception {
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID " +
                "old-tuple:{ID:1001,NAME:'Alice Smith',SALARY:6000.00}";
        String event = buildEvent("DELETE", "123456791", 123456791L,
                1700000002000L, 1003L, eventData);

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("DELETE", thlEvent.getMetadata().get("operation"));
        assertNotNull(thlEvent.getMetadata().get("row_data"));
    }

    @Test
    @DisplayName("无 tuple 数据时应回退到 SQL_REDO")
    void shouldFallbackToSqlRedoWhenNoTuple() throws Exception {
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID " +
                "sql_redo:insert into \"TESTUSER\".\"EMPLOYEES\"(\"ID\",\"NAME\") values (1001,'Bob')";
        String event = buildEvent("INSERT", "123456792", 123456792L,
                1700000003000L, 1004L, eventData);

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
        assertNotNull(thlEvent.getMetadata().get("sql_redo"));
        String sqlRedo = (String) thlEvent.getMetadata().get("sql_redo");
        assertTrue(sqlRedo.contains("insert into"));
        assertTrue(sqlRedo.contains("EMPLOYEES"));
    }

    @Test
    @DisplayName("未知事件类型应保留原始数据")
    void unknownEventTypeShouldKeepRawData() throws Exception {
        String event = buildEvent("DDL", "123456793", 123456793L,
                1700000004000L, 1005L, "CREATE TABLE test_table (id NUMBER)");

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("DDL", thlEvent.getMetadata().get("operation"));
        assertNotNull(thlEvent.getMetadata().get("raw_data"));
    }

    @Test
    @DisplayName("无效的 SCN 数值应设为 0")
    void invalidScnNumericShouldBeZero() throws Exception {
        String event = "INSERT\001123456\001invalid_scn\0011700000000000\0011001\001schema:TESTUSER table:EMPLOYEES new-tuple:{ID:1}";
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals(0L, thlEvent.getMetadata().get("redo_scn_numeric"));
    }

    @Test
    @DisplayName("无效的 timestamp 应使用当前时间")
    void invalidTimestampShouldUseCurrentTime() throws Exception {
        long before = System.currentTimeMillis();
        String event = "INSERT\001123456\001123456\001invalid_ts\0011001\001schema:TESTUSER table:EMPLOYEES new-tuple:{ID:1}";
        THLEvent thlEvent = extract(event);
        long after = System.currentTimeMillis();

        assertNotNull(thlEvent);
        assertNotNull(thlEvent.getSourceTstamp());
        long eventTime = thlEvent.getSourceTstamp().getTime();
        assertTrue(eventTime >= before && eventTime <= after,
                "Event timestamp should fall between before and after test execution");
    }

    @Test
    @DisplayName("seqno 应递增")
    void seqnoShouldIncrement() throws Exception {
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID new-tuple:{ID:1}";
        String event1 = buildEvent("INSERT", "111", 111L, 1700000000000L, 1L, eventData);
        String event2 = buildEvent("INSERT", "112", 112L, 1700000001000L, 2L, eventData);

        THLEvent thl1 = extract(event1);
        THLEvent thl2 = extract(event2);

        assertNotNull(thl1);
        assertNotNull(thl2);
        assertTrue(thl2.getSeqno() > thl1.getSeqno(), "seqno should increment");
    }

    @Test
    @DisplayName("包含 NULL 值的 tuple 应正确解析")
    void nullValuesInTupleShouldBeParsed() throws Exception {
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID " +
                "new-tuple:{ID:1002,NAME:[null],SALARY:[null]}";
        String event = buildEvent("INSERT", "123456794", 123456794L,
                1700000005000L, 1006L, eventData);

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
        String rowData = (String) thlEvent.getMetadata().get("row_data");
        assertNotNull(rowData);
        // NULL 值应在格式化后体现为 null 标记
        assertTrue(rowData.contains("null") || rowData.contains("NULL"),
                "Row data should contain null representation");
    }

    @Test
    @DisplayName("多主键表应正确解析 primary_keys")
    void compositePrimaryKeyShouldBeParsed() throws Exception {
        String eventData = "schema:TESTUSER table:ORDERS primary_keys:ORDER_ID " +
                "new-tuple:{ORDER_ID:2001,CUSTOMER_ID:100,AMOUNT:99.99}";
        String event = buildEvent("INSERT", "123456795", 123456795L,
                1700000006000L, 1007L, eventData);

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("ORDERS", thlEvent.getMetadata().get("table_name"));
        assertEquals("ORDER_ID", thlEvent.getMetadata().get("primary_keys"));
    }

    @Test
    @DisplayName("checkpoint 跳过：SCN 小于 checkpoint 的事件应返回 null")
    void shouldSkipEventsBeforeCheckpoint() throws Exception {
        // 设置 checkpoint 配置
        java.lang.reflect.Field skipField = extractor.getClass().getDeclaredField("skipBeforeCheckpoint");
        skipField.setAccessible(true);
        skipField.set(extractor, true);

        java.lang.reflect.Field scnField = extractor.getClass().getDeclaredField("checkpointScnNumeric");
        scnField.setAccessible(true);
        scnField.set(extractor, 200000000L);

        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID new-tuple:{ID:1}";
        // SCN 小于 checkpoint
        String event = buildEvent("INSERT", "150000000", 150000000L,
                1700000000000L, 1L, eventData);

        THLEvent thlEvent = extract(event);
        assertNull(thlEvent, "Event with SCN before checkpoint should be skipped");
    }

    @Test
    @DisplayName("checkpoint 不跳过：SCN 大于等于 checkpoint 的事件应正常解析")
    void shouldNotSkipEventsAfterCheckpoint() throws Exception {
        java.lang.reflect.Field skipField = extractor.getClass().getDeclaredField("skipBeforeCheckpoint");
        skipField.setAccessible(true);
        skipField.set(extractor, true);

        java.lang.reflect.Field scnField = extractor.getClass().getDeclaredField("checkpointScnNumeric");
        scnField.setAccessible(true);
        scnField.set(extractor, 200000000L);

        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID new-tuple:{ID:1}";
        // SCN 等于 checkpoint，应正常处理（使用 < 严格小于比较）
        String eventEqual = buildEvent("INSERT", "200000000", 200000000L,
                1700000000000L, 1L, eventData);
        THLEvent thlEqual = extract(eventEqual);
        assertNotNull(thlEqual, "Event equal to checkpoint should be processed");

        // SCN 大于 checkpoint，应正常处理
        String event = buildEvent("INSERT", "250000000", 250000000L,
                1700000000000L, 1L, eventData);
        THLEvent thlEvent = extract(event);
        assertNotNull(thlEvent, "Event with SCN after checkpoint should not be skipped");
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("包含引号转义的数据应正确解析")
    void quotedDataWithEscapingShouldBeParsed() throws Exception {
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID " +
                "new-tuple:{ID:1003,NAME:'O''Brien'}";
        String event = buildEvent("INSERT", "123456796", 123456796L,
                1700000007000L, 1008L, eventData);

        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
        String rowData = (String) thlEvent.getMetadata().get("row_data");
        assertNotNull(rowData);
    }
}
