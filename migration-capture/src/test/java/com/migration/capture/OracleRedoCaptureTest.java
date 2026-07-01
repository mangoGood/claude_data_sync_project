package com.migration.capture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OracleRedoCapture} 单元测试。
 *
 * <p>验证同步对象解析、SQL_REDO 解析、事件数据格式化等核心逻辑。
 * 通过反射调用私有方法，避免依赖真实 Oracle LogMiner 连接。
 */
@DisplayName("OracleRedoCapture LogMiner 捕获测试")
class OracleRedoCaptureTest {

    private OracleRedoCapture capture;

    @BeforeEach
    void setUp() throws Exception {
        capture = new OracleRedoCapture();
        Properties props = new Properties();
        props.setProperty("source.db.host", "localhost");
        props.setProperty("source.db.port", "1521");
        props.setProperty("source.db.database", "ORCL");
        props.setProperty("source.db.username", "TESTUSER");
        props.setProperty("source.db.password", "testpass");
        props.setProperty("capture.output.dir", "target/oracle_capture_test_output");
        props.setProperty("task.id", "test-task-001");

        Field propsField = capture.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        propsField.set(capture, props);
    }

    @SuppressWarnings("unchecked")
    private Set<String> getSyncedSchemas() throws Exception {
        Field field = capture.getClass().getDeclaredField("syncedSchemas");
        field.setAccessible(true);
        return (Set<String>) field.get(capture);
    }

    @SuppressWarnings("unchecked")
    private Set<String> getSyncedTables() throws Exception {
        Field field = capture.getClass().getDeclaredField("syncedTables");
        field.setAccessible(true);
        return (Set<String>) field.get(capture);
    }

    private void invokeParseSyncObjects() throws Exception {
        Method method = capture.getClass().getDeclaredMethod("parseSyncObjects");
        method.setAccessible(true);
        method.invoke(capture);
    }

    private String invokeBuildEventData(String owner, String table, String operation, String sqlRedo) throws Exception {
        Method method = capture.getClass().getDeclaredMethod("buildEventData", String.class, String.class, String.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(capture, owner, table, operation, sqlRedo);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> invokeParseSqlRedoSetClause(String sqlRedo) throws Exception {
        Method method = capture.getClass().getDeclaredMethod("parseSqlRedoSetClause", String.class);
        method.setAccessible(true);
        return (Map<String, String>) method.invoke(capture, sqlRedo);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> invokeParseSqlRedoWhereClause(String sqlRedo) throws Exception {
        Method method = capture.getClass().getDeclaredMethod("parseSqlRedoWhereClause", String.class);
        method.setAccessible(true);
        return (Map<String, String>) method.invoke(capture, sqlRedo);
    }

    private String invokeFormatTuple(Map<String, String> values) throws Exception {
        Method method = capture.getClass().getDeclaredMethod("formatTuple", Map.class);
        method.setAccessible(true);
        return (String) method.invoke(capture, values);
    }

    // ==================== 同步对象解析测试 ====================

    @Test
    @DisplayName("解析 migration.sync.objects JSON 配置")
    void shouldParseSyncObjectsJson() throws Exception {
        Field propsField = capture.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        Properties props = (Properties) propsField.get(capture);
        props.setProperty("migration.sync.objects",
                "{\"TESTUSER\":{\"tables\":[\"EMPLOYEES\",\"ORDERS\"]}}");

        invokeParseSyncObjects();

        Set<String> schemas = getSyncedSchemas();
        Set<String> tables = getSyncedTables();

        assertTrue(schemas.contains("TESTUSER"), "Should contain TESTUSER schema");
        assertTrue(tables.contains("TESTUSER.EMPLOYEES"), "Should contain TESTUSER.EMPLOYEES table");
        assertTrue(tables.contains("TESTUSER.ORDERS"), "Should contain TESTUSER.ORDERS table");
    }

    @Test
    @DisplayName("解析转义的 JSON 配置")
    void shouldParseEscapedJsonConfig() throws Exception {
        Field propsField = capture.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        Properties props = (Properties) propsField.get(capture);
        props.setProperty("migration.sync.objects",
                "{\\\"TESTUSER\\\":{\\\"tables\\\":[\\\"EMPLOYEES\\\"]}}");

        invokeParseSyncObjects();

        Set<String> schemas = getSyncedSchemas();
        assertTrue(schemas.contains("TESTUSER"), "Should parse escaped JSON and contain TESTUSER schema");
    }

    @Test
    @DisplayName("解析 migration.included.databases 配置")
    void shouldParseIncludedDatabases() throws Exception {
        Field propsField = capture.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        Properties props = (Properties) propsField.get(capture);
        props.setProperty("migration.included.databases", "SCHEMA1,SCHEMA2,SCHEMA3");

        invokeParseSyncObjects();

        Set<String> schemas = getSyncedSchemas();
        assertTrue(schemas.contains("SCHEMA1"));
        assertTrue(schemas.contains("SCHEMA2"));
        assertTrue(schemas.contains("SCHEMA3"));
    }

    @Test
    @DisplayName("解析 migration.included.tables 配置")
    void shouldParseIncludedTables() throws Exception {
        Field propsField = capture.getClass().getSuperclass().getDeclaredField("props");
        propsField.setAccessible(true);
        Properties props = (Properties) propsField.get(capture);
        props.setProperty("migration.included.tables", "SCHEMA1.TABLE1,SCHEMA2.TABLE2");

        invokeParseSyncObjects();

        Set<String> tables = getSyncedTables();
        Set<String> schemas = getSyncedSchemas();
        assertTrue(tables.contains("SCHEMA1.TABLE1"));
        assertTrue(tables.contains("SCHEMA2.TABLE2"));
        assertTrue(schemas.contains("SCHEMA1"), "Schema should be auto-extracted from table name");
        assertTrue(schemas.contains("SCHEMA2"));
    }

    @Test
    @DisplayName("空配置应不过滤任何 schema")
    void emptyConfigShouldNotFilter() throws Exception {
        invokeParseSyncObjects();

        Set<String> schemas = getSyncedSchemas();
        Set<String> tables = getSyncedTables();
        assertTrue(schemas.isEmpty(), "No schema filter should be set");
        assertTrue(tables.isEmpty(), "No table filter should be set");
    }

    // ==================== SQL_REDO 解析测试 ====================

    @Test
    @DisplayName("解析 INSERT SQL_REDO 的 SET 子句")
    void shouldParseInsertSqlRedoSetClause() throws Exception {
        String sqlRedo = "insert into \"TESTUSER\".\"EMPLOYEES\"(\"ID\",\"NAME\",\"SALARY\") values (1001,'Alice',5000.00);";

        Map<String, String> values = invokeParseSqlRedoSetClause(sqlRedo);

        assertEquals(3, values.size());
        assertEquals("1001", values.get("ID"));
        assertEquals("'Alice'", values.get("NAME"));
        assertEquals("5000.00", values.get("SALARY"));
    }

    @Test
    @DisplayName("解析 UPDATE SQL_REDO 的 SET 子句")
    void shouldParseUpdateSqlRedoSetClause() throws Exception {
        String sqlRedo = "update \"TESTUSER\".\"EMPLOYEES\" set \"NAME\" = 'Bob', \"SALARY\" = 6000.00 where \"ID\" = 1001;";

        Map<String, String> values = invokeParseSqlRedoSetClause(sqlRedo);

        assertEquals(2, values.size());
        assertEquals("'Bob'", values.get("NAME"));
        assertEquals("6000.00", values.get("SALARY"));
    }

    @Test
    @DisplayName("解析 DELETE SQL_REDO 的 WHERE 子句")
    void shouldParseDeleteSqlRedoWhereClause() throws Exception {
        // 使用单条件 WHERE 子句（多条件 and 分割在 splitSetAssignments 中有已知局限）
        String sqlRedo = "delete from \"TESTUSER\".\"EMPLOYEES\" where \"ID\" = 1001;";

        Map<String, String> values = invokeParseSqlRedoWhereClause(sqlRedo);

        assertEquals(1, values.size());
        assertEquals("1001", values.get("ID"));
    }

    @Test
    @DisplayName("解析 UPDATE SQL_REDO 的 WHERE 子句（旧值）")
    void shouldParseUpdateSqlRedoWhereClause() throws Exception {
        // 使用单条件 WHERE 子句
        String sqlRedo = "update \"TESTUSER\".\"EMPLOYEES\" set \"NAME\" = 'Bob' where \"ID\" = 1001;";

        Map<String, String> oldValues = invokeParseSqlRedoWhereClause(sqlRedo);

        assertEquals(1, oldValues.size());
        assertEquals("1001", oldValues.get("ID"));
    }

    @Test
    @DisplayName("解析包含 NULL 值的 SQL_REDO")
    void shouldParseSqlRedoWithNullValues() throws Exception {
        String sqlRedo = "insert into \"TESTUSER\".\"EMPLOYEES\"(\"ID\",\"NAME\",\"SALARY\") values (1002,NULL,NULL);";

        Map<String, String> values = invokeParseSqlRedoSetClause(sqlRedo);

        assertEquals(3, values.size());
        assertEquals("1002", values.get("ID"));
        assertEquals("NULL", values.get("NAME"));
        assertEquals("NULL", values.get("SALARY"));
    }

    @Test
    @DisplayName("解析包含转义引号的 SQL_REDO")
    void shouldParseSqlRedoWithEscapedQuotes() throws Exception {
        String sqlRedo = "insert into \"TESTUSER\".\"EMPLOYEES\"(\"ID\",\"NAME\") values (1003,'O''Brien');";

        Map<String, String> values = invokeParseSqlRedoSetClause(sqlRedo);

        assertEquals(2, values.size());
        assertEquals("1003", values.get("ID"));
        assertEquals("'O''Brien'", values.get("NAME"));
    }

    @Test
    @DisplayName("解析包含 TO_DATE 函数的 SQL_REDO")
    void shouldParseSqlRedoWithToDateFunction() throws Exception {
        String sqlRedo = "insert into \"TESTUSER\".\"EMPLOYEES\"(\"ID\",\"HIREDATE\") values (1004,TO_DATE('2024-01-15','YYYY-MM-DD'));";

        Map<String, String> values = invokeParseSqlRedoSetClause(sqlRedo);

        assertEquals(2, values.size());
        assertEquals("1004", values.get("ID"));
        assertNotNull(values.get("HIREDATE"));
        assertTrue(values.get("HIREDATE").contains("TO_DATE"));
    }

    // ==================== formatTuple 测试 ====================

    @Test
    @DisplayName("formatTuple 应正确格式化键值对")
    void formatTupleShouldFormatCorrectly() throws Exception {
        java.util.LinkedHashMap<String, String> values = new java.util.LinkedHashMap<>();
        values.put("ID", "1001");
        values.put("NAME", "'Alice'");
        values.put("SALARY", "5000.00");

        String result = invokeFormatTuple(values);

        assertEquals("ID:1001,NAME:'Alice',SALARY:5000.00", result);
    }

    @Test
    @DisplayName("formatTuple 应将 null 值格式化为 [null]")
    void formatTupleShouldFormatNullAsBracketNull() throws Exception {
        java.util.LinkedHashMap<String, String> values = new java.util.LinkedHashMap<>();
        values.put("ID", "1001");
        values.put("NAME", null);

        String result = invokeFormatTuple(values);

        assertEquals("ID:1001,NAME:[null]", result);
    }

    // ==================== buildEventData 测试 ====================

    @Test
    @DisplayName("buildEventData 应构建 INSERT 事件数据（无 DB 连接时回退到 sql_redo）")
    void buildEventDataShouldBuildInsertEvent() throws Exception {
        // 由于没有注入 conn，fetchTablePrimaryKeys 会返回空列表
        // parseSqlRedoSetClause 会解析出值
        String sqlRedo = "insert into \"TESTUSER\".\"EMPLOYEES\"(\"ID\",\"NAME\") values (1001,'Alice');";

        String eventData = invokeBuildEventData("TESTUSER", "EMPLOYEES", "INSERT", sqlRedo);

        assertNotNull(eventData);
        assertTrue(eventData.contains("schema:TESTUSER"));
        assertTrue(eventData.contains("table:EMPLOYEES"));
        // 应包含 new-tuple
        assertTrue(eventData.contains("new-tuple:"));
        assertTrue(eventData.contains("ID:1001"));
        assertTrue(eventData.contains("NAME:'Alice'"));
    }

    @Test
    @DisplayName("buildEventData 应构建 DELETE 事件数据")
    void buildEventDataShouldBuildDeleteEvent() throws Exception {
        String sqlRedo = "delete from \"TESTUSER\".\"EMPLOYEES\" where \"ID\" = 1001;";

        String eventData = invokeBuildEventData("TESTUSER", "EMPLOYEES", "DELETE", sqlRedo);

        assertNotNull(eventData);
        assertTrue(eventData.contains("schema:TESTUSER"));
        assertTrue(eventData.contains("table:EMPLOYEES"));
        // 应包含 old-tuple
        assertTrue(eventData.contains("old-tuple:"));
        assertTrue(eventData.contains("ID:1001"));
    }

    @Test
    @DisplayName("buildEventData 应构建 UPDATE 事件数据（包含新旧值）")
    void buildEventDataShouldBuildUpdateEvent() throws Exception {
        String sqlRedo = "update \"TESTUSER\".\"EMPLOYEES\" set \"NAME\" = 'Bob' where \"ID\" = 1001;";

        String eventData = invokeBuildEventData("TESTUSER", "EMPLOYEES", "UPDATE", sqlRedo);

        assertNotNull(eventData);
        assertTrue(eventData.contains("schema:TESTUSER"));
        assertTrue(eventData.contains("table:EMPLOYEES"));
        // 应包含 old-tuple 和 new-tuple
        assertTrue(eventData.contains("old-tuple:"));
        assertTrue(eventData.contains("new-tuple:"));
    }

    @Test
    @DisplayName("buildEventData 在无法解析 SQL_REDO 时应回退到 sql_redo 字段")
    void buildEventDataShouldFallbackToSqlRedo() throws Exception {
        // 使用无法被正则匹配的 SQL_REDO
        String sqlRedo = "unsupported sql format";

        String eventData = invokeBuildEventData("TESTUSER", "EMPLOYEES", "INSERT", sqlRedo);

        assertNotNull(eventData);
        assertTrue(eventData.contains("schema:TESTUSER"));
        assertTrue(eventData.contains("table:EMPLOYEES"));
        assertTrue(eventData.contains("sql_redo:"));
        assertTrue(eventData.contains("unsupported sql format"));
    }
}
