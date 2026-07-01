package com.migration.extract;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Oracle 数据订阅端到端测试。
 *
 * <p>模拟完整的 Oracle 数据订阅流程：
 * <ol>
 *   <li>模拟 LogMiner 输出的 redo 事件（capture 阶段产物）</li>
 *   <li>使用 {@link OracleRedoExtractor} 解析事件为 {@link THLEvent}（extract 阶段）</li>
 *   <li>验证 THLEvent 的元数据、操作类型、行数据等是否符合预期</li>
 * </ol>
 *
 * <p>使用 H2 内存数据库（Oracle 模式）模拟 Oracle 元数据查询，
 * 避免依赖真实 Oracle 实例。
 */
@DisplayName("Oracle 数据订阅端到端测试")
class OracleSubscriptionE2ETest {

    private OracleRedoExtractor extractor;
    private Method doExtractMethod;
    private Connection h2Conn;
    private static final String OUTPUT_DIR = "target/oracle_e2e_test_output";

    @BeforeEach
    void setUp() throws Exception {
        // 清理输出目录
        File outputDir = new File(OUTPUT_DIR);
        if (outputDir.exists()) {
            for (File f : outputDir.listFiles()) {
                f.delete();
            }
        }
        outputDir.mkdirs();

        // 初始化 extractor
        extractor = new OracleRedoExtractor();
        Properties props = new Properties();
        props.setProperty("extract.output.dir", OUTPUT_DIR);
        props.setProperty("source.db.host", "localhost");
        props.setProperty("source.db.port", "1521");
        props.setProperty("source.db.username", "TESTUSER");
        props.setProperty("source.db.password", "testpass");
        props.setProperty("source.db.database", "ORCL");

        Field propsField = extractor.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(extractor, props);

        // 注入 H2 内存连接（Oracle 模式），模拟 Oracle 元数据
        h2Conn = DriverManager.getConnection(
                "jdbc:h2:mem:oracle-e2e-test;MODE=Oracle;DB_CLOSE_DELAY=-1", "sa", "");
        setupOracleSchema(h2Conn);

        Field connField = extractor.getClass().getDeclaredField("sourceConnection");
        connField.setAccessible(true);
        connField.set(extractor, h2Conn);

        doExtractMethod = extractor.getClass().getDeclaredMethod("doExtract", byte[].class);
        doExtractMethod.setAccessible(true);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (h2Conn != null) {
            h2Conn.close();
        }
    }

    /**
     * 在 H2 中创建模拟的 Oracle 表结构，使 extractor 的元数据查询能返回列信息。
     */
    private void setupOracleSchema(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            // 先创建 schema
            stmt.execute("CREATE SCHEMA IF NOT EXISTS TESTUSER");
            // 模拟业务表：员工表
            stmt.execute("CREATE TABLE IF NOT EXISTS TESTUSER.EMPLOYEES (" +
                    "ID NUMBER(10) PRIMARY KEY, " +
                    "NAME VARCHAR2(100), " +
                    "EMAIL VARCHAR2(200), " +
                    "SALARY NUMBER(10,2), " +
                    "DEPARTMENT_ID NUMBER(10), " +
                    "HIREDATE DATE, " +
                    "IS_ACTIVE NUMBER(1))");

            // 模拟业务表：订单表
            stmt.execute("CREATE TABLE IF NOT EXISTS TESTUSER.ORDERS (" +
                    "ORDER_ID NUMBER(10) PRIMARY KEY, " +
                    "CUSTOMER_ID NUMBER(10), " +
                    "PRODUCT_NAME VARCHAR2(200), " +
                    "QUANTITY NUMBER(10), " +
                    "TOTAL_AMOUNT NUMBER(12,2), " +
                    "ORDER_DATE DATE, " +
                    "STATUS VARCHAR2(50))");

            // 模拟业务表：审计日志表（无主键）
            stmt.execute("CREATE TABLE IF NOT EXISTS TESTUSER.AUDIT_LOG (" +
                    "LOG_ID NUMBER(10), " +
                    "ACTION VARCHAR2(100), " +
                    "TIMESTAMP DATE)");
        }
    }

    private THLEvent extract(String eventStr) throws Exception {
        return (THLEvent) doExtractMethod.invoke(extractor, eventStr.getBytes("UTF-8"));
    }

    /**
     * 构造 Oracle redo 事件字符串（模拟 OracleRedoCapture 的输出格式）：
     * eventType \001 scn \001 scnNumeric \001 timestamp \001 xid \001 eventData
     */
    private String buildRedoEvent(String eventType, long scn, long timestamp,
                                  long xid, String eventData) {
        return eventType + "\001" + scn + "\001" + scn + "\001" + timestamp + "\001"
                + xid + "\001" + eventData;
    }

    /**
     * 模拟 capture 阶段生成的 INSERT 事件数据。
     */
    private String buildInsertEventData(String schema, String table, String primaryKeys, String tuple) {
        return "schema:" + schema + " table:" + table + " primary_keys:" + primaryKeys
                + " new-tuple:{" + tuple + "}";
    }

    /**
     * 模拟 capture 阶段生成的 UPDATE 事件数据。
     */
    private String buildUpdateEventData(String schema, String table, String primaryKeys,
                                        String oldTuple, String newTuple) {
        return "schema:" + schema + " table:" + table + " primary_keys:" + primaryKeys
                + " old-tuple:{" + oldTuple + "} new-tuple:{" + newTuple + "}";
    }

    /**
     * 模拟 capture 阶段生成的 DELETE 事件数据。
     */
    private String buildDeleteEventData(String schema, String table, String primaryKeys, String tuple) {
        return "schema:" + schema + " table:" + table + " primary_keys:" + primaryKeys
                + " old-tuple:{" + tuple + "}";
    }

    // ==================== 端到端流程测试 ====================

    @Test
    @DisplayName("E2E: INSERT 事件完整流程 - capture格式 → extract → THLEvent验证")
    void e2eInsertEventFullFlow() throws Exception {
        // 模拟 capture 阶段输出的事件
        String eventData = buildInsertEventData("TESTUSER", "EMPLOYEES", "ID",
                "ID:1001,NAME:'Alice',EMAIL:'alice@example.com',SALARY:5000.00,DEPARTMENT_ID:10,HIREDATE:TO_DATE('2024-01-15','YYYY-MM-DD'),IS_ACTIVE:1");
        String redoEvent = buildRedoEvent("INSERT", 1000001L, 1700000000000L, 5001L, eventData);

        // extract 阶段：解析事件
        THLEvent thlEvent = extract(redoEvent);

        // 验证 THLEvent
        assertNotNull(thlEvent);
        assertEquals("oracle", thlEvent.getSourceId());
        assertEquals("1000001", thlEvent.getEventId());
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
        assertEquals("TESTUSER", thlEvent.getMetadata().get("database_name"));
        assertEquals("EMPLOYEES", thlEvent.getMetadata().get("table_name"));
        assertEquals("ID", thlEvent.getMetadata().get("primary_keys"));
        assertEquals(1000001L, thlEvent.getMetadata().get("redo_scn_numeric"));
        assertEquals(5001L, thlEvent.getMetadata().get("xid"));

        // 验证行数据
        String rowData = (String) thlEvent.getMetadata().get("row_data");
        assertNotNull(rowData);
        assertTrue(rowData.contains("1001"), "Row data should contain ID value");
        assertTrue(rowData.contains("Alice"), "Row data should contain NAME value");

        // 验证 rows_data 列表
        @SuppressWarnings("unchecked")
        List<String> rowsData = (List<String>) thlEvent.getMetadata().get("rows_data");
        assertNotNull(rowsData);
        assertEquals(1, rowsData.size());
    }

    @Test
    @DisplayName("E2E: UPDATE 事件完整流程 - capture格式 → extract → THLEvent验证")
    void e2eUpdateEventFullFlow() throws Exception {
        String oldTuple = "ID:1001,NAME:'Alice',SALARY:5000.00";
        String newTuple = "ID:1001,NAME:'Alice Smith',SALARY:6000.00";
        String eventData = buildUpdateEventData("TESTUSER", "EMPLOYEES", "ID", oldTuple, newTuple);
        String redoEvent = buildRedoEvent("UPDATE", 1000002L, 1700000001000L, 5002L, eventData);

        THLEvent thlEvent = extract(redoEvent);

        assertNotNull(thlEvent);
        assertEquals("UPDATE", thlEvent.getMetadata().get("operation"));
        assertEquals("TESTUSER", thlEvent.getMetadata().get("database_name"));
        assertEquals("EMPLOYEES", thlEvent.getMetadata().get("table_name"));

        // 验证新值
        String newRowData = (String) thlEvent.getMetadata().get("row_data");
        assertNotNull(newRowData);
        assertTrue(newRowData.contains("Alice Smith"));
        assertTrue(newRowData.contains("6000.00"));

        // 验证旧值
        String oldRowData = (String) thlEvent.getMetadata().get("row_data_before");
        assertNotNull(oldRowData);
        assertTrue(oldRowData.contains("Alice"));
        assertTrue(oldRowData.contains("5000.00"));
    }

    @Test
    @DisplayName("E2E: DELETE 事件完整流程 - capture格式 → extract → THLEvent验证")
    void e2eDeleteEventFullFlow() throws Exception {
        String eventData = buildDeleteEventData("TESTUSER", "EMPLOYEES", "ID",
                "ID:1001,NAME:'Alice Smith',SALARY:6000.00");
        String redoEvent = buildRedoEvent("DELETE", 1000003L, 1700000002000L, 5003L, eventData);

        THLEvent thlEvent = extract(redoEvent);

        assertNotNull(thlEvent);
        assertEquals("DELETE", thlEvent.getMetadata().get("operation"));
        assertEquals("TESTUSER", thlEvent.getMetadata().get("database_name"));
        assertEquals("EMPLOYEES", thlEvent.getMetadata().get("table_name"));

        String rowData = (String) thlEvent.getMetadata().get("row_data");
        assertNotNull(rowData);
        assertTrue(rowData.contains("1001"));
        assertTrue(rowData.contains("Alice Smith"));
    }

    @Test
    @DisplayName("E2E: 多事件连续处理 - 模拟事务中的多个 DML 操作")
    void e2eMultipleEventsInTransaction() throws Exception {
        // 模拟一个事务中的多个操作
        List<THLEvent> events = new ArrayList<>();

        // 操作1: INSERT 员工
        String insertData = buildInsertEventData("TESTUSER", "EMPLOYEES", "ID",
                "ID:2001,NAME:'Bob',SALARY:4500.00");
        events.add(extract(buildRedoEvent("INSERT", 2000001L, 1700000003000L, 6001L, insertData)));

        // 操作2: INSERT 订单
        String insertOrder = buildInsertEventData("TESTUSER", "ORDERS", "ORDER_ID",
                "ORDER_ID:3001,CUSTOMER_ID:2001,PRODUCT_NAME:'Laptop',QUANTITY:1,TOTAL_AMOUNT:999.99,STATUS:'PENDING'");
        events.add(extract(buildRedoEvent("INSERT", 2000002L, 1700000003001L, 6001L, insertOrder)));

        // 操作3: UPDATE 订单状态
        String updateOrder = buildUpdateEventData("TESTUSER", "ORDERS", "ORDER_ID",
                "ORDER_ID:3001,STATUS:'PENDING'", "ORDER_ID:3001,STATUS:'SHIPPED'");
        events.add(extract(buildRedoEvent("UPDATE", 2000003L, 1700000003002L, 6001L, updateOrder)));

        // 操作4: DELETE 审计日志
        String deleteLog = buildDeleteEventData("TESTUSER", "AUDIT_LOG", "LOG_ID",
                "LOG_ID:999,ACTION:'OLD_ACTION'");
        events.add(extract(buildRedoEvent("DELETE", 2000004L, 1700000003003L, 6001L, deleteLog)));

        // 验证所有事件
        assertEquals(4, events.size());

        // 验证事件1: INSERT 员工
        assertEquals("INSERT", events.get(0).getMetadata().get("operation"));
        assertEquals("EMPLOYEES", events.get(0).getMetadata().get("table_name"));

        // 验证事件2: INSERT 订单
        assertEquals("INSERT", events.get(1).getMetadata().get("operation"));
        assertEquals("ORDERS", events.get(1).getMetadata().get("table_name"));

        // 验证事件3: UPDATE 订单
        assertEquals("UPDATE", events.get(2).getMetadata().get("operation"));
        assertEquals("ORDERS", events.get(2).getMetadata().get("table_name"));

        // 验证事件4: DELETE 审计日志
        assertEquals("DELETE", events.get(3).getMetadata().get("operation"));
        assertEquals("AUDIT_LOG", events.get(3).getMetadata().get("table_name"));

        // 验证 SCN 递增
        long scn1 = (long) events.get(0).getMetadata().get("redo_scn_numeric");
        long scn2 = (long) events.get(1).getMetadata().get("redo_scn_numeric");
        long scn3 = (long) events.get(2).getMetadata().get("redo_scn_numeric");
        long scn4 = (long) events.get(3).getMetadata().get("redo_scn_numeric");
        assertTrue(scn1 < scn2);
        assertTrue(scn2 < scn3);
        assertTrue(scn3 < scn4);

        // 验证 seqno 递增
        assertTrue(events.get(0).getSeqno() < events.get(1).getSeqno());
        assertTrue(events.get(1).getSeqno() < events.get(2).getSeqno());
        assertTrue(events.get(2).getSeqno() < events.get(3).getSeqno());

        // 验证所有事件属于同一事务（xid 相同）
        assertEquals(6001L, events.get(0).getMetadata().get("xid"));
        assertEquals(6001L, events.get(1).getMetadata().get("xid"));
        assertEquals(6001L, events.get(2).getMetadata().get("xid"));
        assertEquals(6001L, events.get(3).getMetadata().get("xid"));
    }

    @Test
    @DisplayName("E2E: SQL_REDO 回退场景 - 当 tuple 解析失败时使用 SQL_REDO")
    void e2eSqlRedoFallbackScenario() throws Exception {
        // 模拟 capture 阶段无法解析 tuple，回退到 sql_redo
        String eventData = "schema:TESTUSER table:EMPLOYEES primary_keys:ID " +
                "sql_redo:insert into \"TESTUSER\".\"EMPLOYEES\"(\"ID\",\"NAME\") values (5001,'Complex Data with, comma')";
        String redoEvent = buildRedoEvent("INSERT", 3000001L, 1700000004000L, 7001L, eventData);

        THLEvent thlEvent = extract(redoEvent);

        assertNotNull(thlEvent);
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));
        assertEquals("TESTUSER", thlEvent.getMetadata().get("database_name"));
        assertEquals("EMPLOYEES", thlEvent.getMetadata().get("table_name"));

        // 应包含 sql_redo 元数据
        String sqlRedo = (String) thlEvent.getMetadata().get("sql_redo");
        assertNotNull(sqlRedo);
        assertTrue(sqlRedo.contains("insert into"));
        assertTrue(sqlRedo.contains("EMPLOYEES"));
        assertTrue(sqlRedo.contains("5001"));
    }

    @Test
    @DisplayName("E2E: checkpoint 续传场景 - 跳过已处理的事件")
    void e2eCheckpointResumeScenario() throws Exception {
        // 设置 checkpoint，跳过 SCN < 4000000 的事件
        Field skipField = extractor.getClass().getDeclaredField("skipBeforeCheckpoint");
        skipField.setAccessible(true);
        skipField.set(extractor, true);

        Field scnField = extractor.getClass().getDeclaredField("checkpointScnNumeric");
        scnField.setAccessible(true);
        scnField.set(extractor, 4000000L);

        // 事件1: SCN 小于 checkpoint，应被跳过
        String eventData1 = buildInsertEventData("TESTUSER", "EMPLOYEES", "ID", "ID:1,NAME:'Old'");
        THLEvent skipped = extract(buildRedoEvent("INSERT", 3999999L, 1700000005000L, 8001L, eventData1));
        assertEquals(null, skipped, "Event before checkpoint should be skipped");

        // 事件2: SCN 等于 checkpoint，应正常处理（使用 < 严格小于比较）
        THLEvent equal = extract(buildRedoEvent("INSERT", 4000000L, 1700000005001L, 8002L, eventData1));
        assertNotNull(equal, "Event equal to checkpoint should be processed (strict less-than comparison)");

        // 事件3: SCN 大于 checkpoint，应正常处理
        String eventData2 = buildInsertEventData("TESTUSER", "EMPLOYEES", "ID", "ID:2,NAME:'New'");
        THLEvent processed = extract(buildRedoEvent("INSERT", 4000001L, 1700000005002L, 8003L, eventData2));
        assertNotNull(processed, "Event after checkpoint should be processed");
        assertEquals("INSERT", processed.getMetadata().get("operation"));
        assertEquals(4000001L, processed.getMetadata().get("redo_scn_numeric"));
    }

    @Test
    @DisplayName("E2E: 包含 NULL 值的数据处理")
    void e2eNullValueHandling() throws Exception {
        String eventData = buildInsertEventData("TESTUSER", "EMPLOYEES", "ID",
                "ID:6001,NAME:'Charlie',EMAIL:[null],SALARY:[null],DEPARTMENT_ID:20");
        String redoEvent = buildRedoEvent("INSERT", 5000001L, 1700000006000L, 9001L, eventData);

        THLEvent thlEvent = extract(redoEvent);

        assertNotNull(thlEvent);
        assertEquals("INSERT", thlEvent.getMetadata().get("operation"));

        String rowData = (String) thlEvent.getMetadata().get("row_data");
        assertNotNull(rowData);
        // NULL 值应在格式化后体现
        assertTrue(rowData.contains("null") || rowData.contains("NULL"),
                "Row data should contain null representation for NULL columns");
    }

    @Test
    @DisplayName("E2E: 模拟文件写入和读取流程")
    void e2eFileWriteReadFlow() throws Exception {
        // 模拟 capture 阶段将事件写入文件
        File captureFile = new File(OUTPUT_DIR, "oracle_redo_001.dat");
        StringBuilder fileContent = new StringBuilder();

        // 生成多个事件并写入文件
        for (int i = 1; i <= 5; i++) {
            String eventData = buildInsertEventData("TESTUSER", "EMPLOYEES", "ID",
                    "ID:" + (7000 + i) + ",NAME:'User" + i + "',SALARY:" + (4000 + i * 100) + ".00");
            String redoEvent = buildRedoEvent("INSERT", 6000000L + i, 1700000007000L + i, 10001L + i, eventData);
            fileContent.append(redoEvent).append("\n");
        }

        // 写入文件
        try (java.io.BufferedWriter writer = new java.io.BufferedWriter(new java.io.FileWriter(captureFile))) {
            writer.write(fileContent.toString());
        }

        // 读取文件并逐行解析（模拟 extract 阶段的文件读取）
        List<THLEvent> events = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(captureFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    THLEvent event = extract(line);
                    if (event != null) {
                        events.add(event);
                    }
                }
            }
        }

        // 验证所有事件都被正确解析
        assertEquals(5, events.size());

        for (int i = 0; i < 5; i++) {
            THLEvent event = events.get(i);
            assertNotNull(event);
            assertEquals("INSERT", event.getMetadata().get("operation"));
            assertEquals("EMPLOYEES", event.getMetadata().get("table_name"));
            assertEquals("TESTUSER", event.getMetadata().get("database_name"));
            assertEquals(6000001L + i, event.getMetadata().get("redo_scn_numeric"));
        }

        // 验证 seqno 递增
        for (int i = 1; i < 5; i++) {
            assertTrue(events.get(i).getSeqno() > events.get(i - 1).getSeqno(),
                    "seqno should be monotonically increasing");
        }
    }

    @Test
    @DisplayName("E2E: 混合 DML 操作的完整订阅场景")
    void e2eMixedDmlSubscriptionScenario() throws Exception {
        // 模拟一个完整的业务场景：
        // 1. 新员工入职（INSERT）
        // 2. 更新员工薪资（UPDATE）
        // 3. 新建订单（INSERT）
        // 4. 订单发货（UPDATE）
        // 5. 删除临时记录（DELETE）

        List<THLEvent> events = new ArrayList<>();

        // 1. 新员工入职
        events.add(extract(buildRedoEvent("INSERT", 7000001L, 1700000008000L, 11001L,
                buildInsertEventData("TESTUSER", "EMPLOYEES", "ID",
                        "ID:8001,NAME:'David',EMAIL:'david@example.com',SALARY:5500.00,DEPARTMENT_ID:10"))));

        // 2. 更新员工薪资
        events.add(extract(buildRedoEvent("UPDATE", 7000002L, 1700000008001L, 11002L,
                buildUpdateEventData("TESTUSER", "EMPLOYEES", "ID",
                        "ID:8001,SALARY:5500.00", "ID:8001,SALARY:6500.00"))));

        // 3. 新建订单
        events.add(extract(buildRedoEvent("INSERT", 7000003L, 1700000008002L, 11003L,
                buildInsertEventData("TESTUSER", "ORDERS", "ORDER_ID",
                        "ORDER_ID:9001,CUSTOMER_ID:8001,PRODUCT_NAME:'Workstation',QUANTITY:2,TOTAL_AMOUNT:2999.98,STATUS:'NEW'"))));

        // 4. 订单发货
        events.add(extract(buildRedoEvent("UPDATE", 7000004L, 1700000008003L, 11004L,
                buildUpdateEventData("TESTUSER", "ORDERS", "ORDER_ID",
                        "ORDER_ID:9001,STATUS:'NEW'", "ORDER_ID:9001,STATUS:'SHIPPED'"))));

        // 5. 删除临时记录
        events.add(extract(buildRedoEvent("DELETE", 7000005L, 1700000008004L, 11005L,
                buildDeleteEventData("TESTUSER", "AUDIT_LOG", "LOG_ID",
                        "LOG_ID:500,ACTION:'TEMP'"))));

        // 验证事件数量
        assertEquals(5, events.size());

        // 验证操作类型序列
        assertEquals("INSERT", events.get(0).getMetadata().get("operation"));
        assertEquals("UPDATE", events.get(1).getMetadata().get("operation"));
        assertEquals("INSERT", events.get(2).getMetadata().get("operation"));
        assertEquals("UPDATE", events.get(3).getMetadata().get("operation"));
        assertEquals("DELETE", events.get(4).getMetadata().get("operation"));

        // 验证表名
        assertEquals("EMPLOYEES", events.get(0).getMetadata().get("table_name"));
        assertEquals("EMPLOYEES", events.get(1).getMetadata().get("table_name"));
        assertEquals("ORDERS", events.get(2).getMetadata().get("table_name"));
        assertEquals("ORDERS", events.get(3).getMetadata().get("table_name"));
        assertEquals("AUDIT_LOG", events.get(4).getMetadata().get("table_name"));

        // 验证所有事件的 sourceId 为 oracle
        for (THLEvent event : events) {
            assertEquals("oracle", event.getSourceId());
        }

        // 验证 SCN 严格递增
        for (int i = 1; i < events.size(); i++) {
            long prevScn = (long) events.get(i - 1).getMetadata().get("redo_scn_numeric");
            long currScn = (long) events.get(i).getMetadata().get("redo_scn_numeric");
            assertTrue(prevScn < currScn, "SCN should be strictly increasing");
        }
    }
}
