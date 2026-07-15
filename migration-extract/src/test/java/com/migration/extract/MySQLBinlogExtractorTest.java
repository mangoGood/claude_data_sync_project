package com.migration.extract;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MySQLBinlogExtractor} binlog 事件解析单元测试。
 *
 * <p>验证 binlog 行事件、QUERY 事件、心跳事件等解析逻辑的正确性。
 * 通过反射调用 doExtract 方法，避免依赖真实 MySQL 连接。
 * 注入 H2 内存连接，使 TABLE_MAP 事件的 schema 查询返回空结果。
 */
@DisplayName("MySQLBinlogExtractor binlog 解析测试")
class MySQLBinlogExtractorTest {

    private MySQLBinlogExtractor extractor;
    private Method doExtractMethod;

    @BeforeEach
    void setUp() throws Exception {
        extractor = new MySQLBinlogExtractor();
        Properties props = new Properties();
        props.setProperty("extract.input.dir", "binlog_output");
        props.setProperty("extract.output.dir", "thl_output");
        props.setProperty("source.db.host", "localhost");
        props.setProperty("source.db.port", "3306");
        props.setProperty("source.db.username", "root");
        props.setProperty("source.db.password", "");
        // 不调用 initialize 避免连接数据库，直接设置 props
        java.lang.reflect.Field propsField = extractor.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(extractor, props);

        // 注入 H2 内存连接，避免 TABLE_MAP 事件触发 schema 查询时 NPE。
        // H2 拥有 INFORMATION_SCHEMA.COLUMNS，查询返回空结果即可。
        Connection h2Conn = DriverManager.getConnection(
                "jdbc:h2:mem:extractor-test;MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        java.lang.reflect.Field connField = extractor.getClass().getDeclaredField("sourceConnection");
        connField.setAccessible(true);
        connField.set(extractor, h2Conn);

        // 获取 doExtract 方法
        doExtractMethod = extractor.getClass().getDeclaredMethod("doExtract", byte[].class);
        doExtractMethod.setAccessible(true);
    }

    private THLEvent extract(String eventStr) throws Exception {
        return (THLEvent) doExtractMethod.invoke(extractor, eventStr.getBytes("UTF-8"));
    }

    /** 构造 binlog 事件字符串：eventType \001 binlogFile \001 position \001 timestamp \001 serverId \001 eventData */
    private String buildEvent(String eventType, String binlogFile, long position, long timestamp,
                              long serverId, String eventData) {
        return eventType + "\001" + binlogFile + "\001" + position + "\001" + timestamp + "\001"
                + serverId + "\001" + eventData;
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
        assertNull(extract("WRITE_ROWS\001mysql-bin.000001"));
    }

    @Test
    @DisplayName("WRITE_ROWS 事件应解析为 INSERT 操作")
    void writeRowsEventShouldBeParsedAsInsert() throws Exception {
        String event = buildEvent("WRITE_ROWS", "mysql-bin.000001", 1234, 1700000000000L, 1,
                "WriteRowsEventData{tableId=1, includedColumns={0,1}, rows=[[1, \"test\"]]}");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("mysql", thlEvent.getSourceId());
        assertEquals("mysql-bin.000001:1234", thlEvent.getEventId());
        assertEquals("WRITE_ROWS", thlEvent.getMetadata().get("event_type"));
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
        assertEquals(1234L, thlEvent.getMetadata().get("binlog_position"));
        assertEquals(1L, thlEvent.getMetadata().get("server_id"));
    }

    @Test
    @DisplayName("EXT_WRITE_ROWS 事件应解析为 INSERT 操作")
    void extWriteRowsEventShouldBeParsedAsInsert() throws Exception {
        String event = buildEvent("EXT_WRITE_ROWS", "mysql-bin.000002", 5678, 1700000001000L, 2,
                "WriteRowsEventData{tableId=2, rows=[[10, \"data\"]]}");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("UPDATE_ROWS 事件应解析为 UPDATE 操作")
    void updateRowsEventShouldBeParsedAsUpdate() throws Exception {
        String event = buildEvent("UPDATE_ROWS", "mysql-bin.000003", 9012, 1700000002000L, 3,
                "UpdateRowsEventData{tableId=1, rows=[before=[1, \"old\"], after=[1, \"new\"]]}");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("UPDATE", thlEvent.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("DELETE_ROWS 事件应解析为 DELETE 操作")
    void deleteRowsEventShouldBeParsedAsDelete() throws Exception {
        String event = buildEvent("DELETE_ROWS", "mysql-bin.000004", 3456, 1700000003000L, 4,
                "DeleteRowsEventData{tableId=1, rows=[[1, \"test\"]]}");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("DELETE", thlEvent.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("QUERY 事件应解析 SQL 和 database_name")
    void queryEventShouldParseSqlAndDatabase() throws Exception {
        String eventData = "QueryEvent{database='test_db', sql='CREATE TABLE t1 (id INT PRIMARY KEY)'}";
        String event = buildEvent("QUERY", "mysql-bin.000005", 7890, 1700000004000L, 5, eventData);
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("QUERY", thlEvent.getMetadata().get("operation"));
        assertEquals("test_db", thlEvent.getMetadata().get("database_name"));
        String sql = (String) thlEvent.getMetadata().get("sql");
        assertNotNull(sql);
        assertTrue(sql.contains("CREATE TABLE"));
    }

    @Test
    @DisplayName("SYNC_HEARTBEAT 事件应设置心跳类型")
    void heartbeatEventShouldSetHeartbeatType() throws Exception {
        String event = buildEvent("SYNC_HEARTBEAT", "mysql-bin.000006", 9999, 1700000005000L, 6, "");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals(THLEvent.HEARTBEAT_EVENT, thlEvent.getType());
        assertEquals("HEARTBEAT", thlEvent.getMetadata().get("operation"));
        assertEquals(1700000005000L, thlEvent.getMetadata().get("source_db_timestamp"));
    }

    @Test
    @DisplayName("XID 事件应解析为 COMMIT 操作")
    void xidEventShouldBeParsedAsCommit() throws Exception {
        String event = buildEvent("XID", "mysql-bin.000007", 1000, 1700000006000L, 7, "XidEventData{xid=12345}");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("COMMIT", thlEvent.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("ROTATE 事件应解析为 ROTATE 操作")
    void rotateEventShouldBeParsedAsRotate() throws Exception {
        String event = buildEvent("ROTATE", "mysql-bin.000008", 2000, 1700000007000L, 8,
                "RotateEventData{binlogNext='mysql-bin.000009', binlogPosition=4}");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("ROTATE", thlEvent.getMetadata().get("operation"));
    }

    @Test
    @DisplayName("TABLE_MAP 事件应被解析（仅元数据，schema 查询需 DB 连接）")
    void tableMapEventShouldBeParsed() throws Exception {
        // TABLE_MAP 事件会触发 schema 查询（getTableColumns 等），需要 sourceConnection。
        // 此处仅验证事件类型元数据被正确设置；schema 查询在集成测试中覆盖。
        String event = buildEvent("TABLE_MAP", "mysql-bin.000009", 3000, 1700000008000L, 9,
                "TableMapEventData{tableId=1, database='test_db', table='users'}");
        THLEvent thlEvent = extract(event);

        assertNotNull(thlEvent);
        assertEquals("TABLE_MAP", thlEvent.getMetadata().get("event_type"));
    }

    @Test
    @DisplayName("无效的 binlog position 应返回 null")
    void invalidBinlogPositionShouldReturnNull() throws Exception {
        String event = "WRITE_ROWS\001mysql-bin.000001\001invalid_position\0011700000000000\0011\001data";
        assertNull(extract(event));
    }

    @Test
    @DisplayName("无效的 timestamp 应使用当前时间")
    void invalidTimestampShouldUseCurrentTime() throws Exception {
        long before = System.currentTimeMillis();
        String event = "WRITE_ROWS\001mysql-bin.000001\001100\001invalid_timestamp\0011\001data";
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
        String event1 = buildEvent("WRITE_ROWS", "mysql-bin.000001", 100, 1700000000000L, 1, "data1");
        String event2 = buildEvent("WRITE_ROWS", "mysql-bin.000001", 200, 1700000001000L, 1, "data2");

        THLEvent thl1 = extract(event1);
        THLEvent thl2 = extract(event2);

        assertNotNull(thl1);
        assertNotNull(thl2);
        assertTrue(thl2.getSeqno() > thl1.getSeqno(), "seqno should increment");
    }

    // ---- DDL 后列元数据缓存失效（防止 ALTER 新增列的值被静默丢弃）----

    /** 按 database.table 键缓存列元数据的五个缓存字段。 */
    private static final String[] TABLE_META_CACHES = {
            "tableSchemaCache", "tableColumnTypeCache", "tableColumnFullTypeCache",
            "enumSetValuesCache", "primaryKeyCache"
    };

    @SuppressWarnings("unchecked")
    private Map<String, Object> cache(String fieldName) throws Exception {
        Field f = MySQLBinlogExtractor.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return (Map<String, Object>) f.get(extractor);
    }

    /** 为某个 database.table 键在全部五个缓存中植入陈旧条目，模拟 DDL 前已缓存的旧列布局。 */
    private void seedStaleCaches(String key) throws Exception {
        cache("tableSchemaCache").put(key, new ArrayList<>(Arrays.asList("id")));
        cache("tableColumnTypeCache").put(key, new ArrayList<>(Arrays.asList("int")));
        cache("tableColumnFullTypeCache").put(key, new ArrayList<>(Arrays.asList("int")));
        cache("enumSetValuesCache").put(key, new HashMap<>());
        cache("primaryKeyCache").put(key, new ArrayList<>(Arrays.asList("id")));
    }

    private void assertNotCached(String key) throws Exception {
        for (String name : TABLE_META_CACHES) {
            assertFalse(cache(name).containsKey(key), name + " 应已失效 " + key + " 的缓存");
        }
    }

    private void assertCached(String key) throws Exception {
        for (String name : TABLE_META_CACHES) {
            assertTrue(cache(name).containsKey(key), name + " 不应清除 " + key + " 的缓存");
        }
    }

    @Test
    @DisplayName("ALTER TABLE 的 QUERY 事件应失效该表的列元数据缓存")
    void alterTableQueryShouldInvalidateColumnCaches() throws Exception {
        seedStaleCaches("test1.t5");
        // QueryEventData.toString() 形如 {..., database='<会话库>', sql='<DDL>'}
        String eventData = "QueryEventData{database='test2', "
                + "sql='ALTER TABLE test1.t5 ADD COLUMN name VARCHAR(30)'}";
        THLEvent thlEvent = extract(buildEvent("QUERY", "mysql-bin.000010", 100, 1700000009000L, 10, eventData));

        assertNotNull(thlEvent);
        assertEquals("QUERY", thlEvent.getMetadata().get("operation"));
        assertNotCached("test1.t5");
    }

    @Test
    @DisplayName("未限定库名的 ALTER 用会话库补全后失效缓存")
    void unqualifiedAlterUsesSessionDatabase() throws Exception {
        seedStaleCaches("test1.t5");
        String eventData = "QueryEventData{database='test1', sql='ALTER TABLE t5 ADD COLUMN name VARCHAR(30)'}";
        extract(buildEvent("QUERY", "mysql-bin.000011", 200, 1700000010000L, 11, eventData));

        assertNotCached("test1.t5");
    }

    @Test
    @DisplayName("RENAME TABLE 应同时失效旧表名与新表名的缓存")
    void renameTableQueryShouldInvalidateBothNames() throws Exception {
        seedStaleCaches("test1.t5");
        seedStaleCaches("test1.t6");
        String eventData = "QueryEventData{database='test1', sql='RENAME TABLE test1.t5 TO test1.t6'}";
        extract(buildEvent("QUERY", "mysql-bin.000012", 300, 1700000011000L, 12, eventData));

        assertNotCached("test1.t5");
        assertNotCached("test1.t6");
    }

    @Test
    @DisplayName("DROP TABLE 多表应逐个失效各自缓存")
    void dropMultipleTablesShouldInvalidateEach() throws Exception {
        seedStaleCaches("test1.t5");
        seedStaleCaches("test1.t6");
        String eventData = "QueryEventData{database='test1', sql='DROP TABLE IF EXISTS test1.t5, t6'}";
        extract(buildEvent("QUERY", "mysql-bin.000015", 600, 1700000014000L, 15, eventData));

        assertNotCached("test1.t5");
        assertNotCached("test1.t6");
    }

    @Test
    @DisplayName("非表级 DDL（CREATE DATABASE）不应清除列元数据缓存")
    void databaseDdlShouldNotInvalidateColumnCaches() throws Exception {
        seedStaleCaches("test1.t5");
        String eventData = "QueryEventData{database='test1', sql='CREATE DATABASE newdb'}";
        extract(buildEvent("QUERY", "mysql-bin.000013", 400, 1700000012000L, 13, eventData));

        assertCached("test1.t5");
    }

    @Test
    @DisplayName("ALTER 其它表不应误清除本表缓存")
    void alterDifferentTableShouldNotInvalidateOtherTable() throws Exception {
        seedStaleCaches("test1.t5");
        String eventData = "QueryEventData{database='test1', sql='ALTER TABLE test1.t9 ADD COLUMN c INT'}";
        extract(buildEvent("QUERY", "mysql-bin.000014", 500, 1700000013000L, 14, eventData));

        assertCached("test1.t5");
    }
}
