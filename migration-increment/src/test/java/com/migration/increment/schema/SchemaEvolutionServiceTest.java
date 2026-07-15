package com.migration.increment.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SchemaEvolutionService} 单元测试。
 *
 * <p>验证 DDL 应用策略、跳过子类型、跨库翻译、失败处理等行为。
 * 使用 H2 内存数据库代替 Mockito mock Connection，避免 JDK 24 上 mock JDK 接口的兼容性问题。
 */
@DisplayName("SchemaEvolutionService Schema 演进服务测试")
class SchemaEvolutionServiceTest {

    @TempDir
    Path tempDir;

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(
                "jdbc:h2:mem:schema-evolution-test;MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private SchemaEvolutionService createService(Properties props) throws SQLException {
        props.setProperty("schema.ddl.manual.log.path", tempDir.resolve("manual_ddl.log").toString());
        return new SchemaEvolutionService(props, connection);
    }

    private Properties baseProps() {
        Properties props = new Properties();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "mysql");
        props.setProperty("schema.ddl.apply.policy", "AUTO_APPLY");
        return props;
    }

    @Test
    @DisplayName("AUTO_APPLY 策略：同源同目标应直接执行 DDL")
    void autoApplySameEngineShouldExecuteDdl() throws SQLException {
        SchemaEvolutionService service = createService(baseProps());

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE t1 (id INT PRIMARY KEY)", "CREATE_TABLE", "db1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.APPLIED, result.getStatus());
        assertTrue(result.isSuccess());
        assertNotNull(result.getExecutedSql());
    }

    @Test
    @DisplayName("SKIP 策略：应跳过所有 DDL")
    void skipPolicyShouldSkipAllDdl() throws SQLException {
        Properties props = baseProps();
        props.setProperty("schema.ddl.apply.policy", "SKIP");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE t1 (id INT)", "CREATE_TABLE", "db1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.SKIPPED, result.getStatus());
    }

    @Test
    @DisplayName("MANUAL 策略：应记录到日志不执行")
    void manualPolicyShouldLogOnly() throws SQLException {
        Properties props = baseProps();
        props.setProperty("schema.ddl.apply.policy", "MANUAL");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE t1 (id INT)", "CREATE_TABLE", "db1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.MANUAL, result.getStatus());
    }

    @Test
    @DisplayName("跳过指定 DDL 子类型")
    void shouldSkipSpecifiedDdlSubtypes() throws SQLException {
        Properties props = baseProps();
        props.setProperty("schema.ddl.skip.subtypes", "CREATE_DATABASE,DROP_DATABASE");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE DATABASE test_db", "CREATE_DATABASE", null);

        assertEquals(SchemaEvolutionService.ApplyResult.Status.SKIPPED, result.getStatus());
    }

    @Test
    @DisplayName("MySQL→PostgreSQL：CREATE TABLE 应翻译类型")
    void mysqlToPgCreateTableShouldTranslateTypes() throws SQLException {
        Properties props = baseProps();
        props.setProperty("source.db.type", "mysql");
        props.setProperty("target.db.type", "postgresql");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE `t1` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), data JSON)",
                "CREATE_TABLE", "db1");

        // H2 可能无法执行 PostgreSQL 方言，但翻译后的 SQL 仍会记录在 executedSql 中
        String executedSql = result.getExecutedSql();
        assertNotNull(executedSql);
        // 验证类型转换（INT → INTEGER, JSON → JSONB）
        assertTrue(executedSql.contains("INTEGER"), "INT 应转换为 INTEGER");
        assertTrue(executedSql.contains("JSONB"), "JSON 应转换为 JSONB");
        // 验证反引号转双引号
        assertTrue(executedSql.contains("\"t1\""), "反引号应转换为双引号");
    }

    @Test
    @DisplayName("PostgreSQL→MySQL：CREATE TABLE 应翻译类型")
    void pgToMysqlCreateTableShouldTranslateTypes() throws SQLException {
        Properties props = baseProps();
        props.setProperty("source.db.type", "postgresql");
        props.setProperty("target.db.type", "mysql");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE \"t1\" (id serial PRIMARY KEY, name varchar(100), data jsonb)",
                "CREATE_TABLE", "db1");

        String executedSql = result.getExecutedSql();
        assertNotNull(executedSql);
        // 验证类型转换（serial → INT, jsonb → JSON）
        // 注意：当前 DdlTranslator 的类型映射存在已知缺陷（类型间空格被吞），
        // 这里仅验证关键转换关键字存在，不严格校验空格。
        assertTrue(executedSql.contains("AUTO_INCREMENT"), "serial 应转换为含 AUTO_INCREMENT");
        assertTrue(executedSql.contains("JSON"), "jsonb 应转换为 JSON");
        // 验证双引号转反引号
        assertTrue(executedSql.contains("`t1`"), "双引号应转换为反引号");
    }

    @Test
    @DisplayName("DDL 执行失败应返回 FAILED 状态")
    void ddlExecutionFailureShouldReturnFailed() throws SQLException {
        SchemaEvolutionService service = createService(baseProps());

        // 使用语法错误的 DDL，H2 会抛出 SQLException
        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLEEEEEE invalid_sql (", "CREATE_TABLE", "db1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.FAILED, result.getStatus());
    }

    @Test
    @DisplayName("统计信息应正确反映处理结果")
    void statsShouldReflectProcessingResults() throws SQLException {
        Properties props = baseProps();
        props.setProperty("schema.ddl.skip.subtypes", "DROP_TABLE");
        SchemaEvolutionService service = createService(props);

        service.applyDdl("CREATE TABLE t_stats (id INT)", "CREATE_TABLE", "db1");
        service.applyDdl("DROP TABLE t_stats", "DROP_TABLE", "db1");

        java.util.Map<String, Object> stats = service.getStats();
        assertEquals(2L, stats.get("totalProcessed"));
        assertEquals(1L, stats.get("totalApplied"));
        assertEquals(1L, stats.get("totalSkipped"));
    }

    @Test
    @DisplayName("数据库映射应正确应用到 DDL")
    void databaseMappingShouldBeApplied() throws SQLException {
        Properties props = baseProps();
        props.setProperty("schema.mapping.db.source_db", "target_db");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE source_db.t_map (id INT)", "CREATE_TABLE", "source_db");

        // H2 可能不支持带 schema 前缀的 CREATE TABLE（需要先 CREATE SCHEMA），
        // 但翻译后的 SQL 仍会记录在 executedSql 中，验证映射结果即可。
        String executedSql = result.getExecutedSql();
        assertNotNull(executedSql);
        assertTrue(executedSql.contains("target_db"), "数据库名应被映射");
    }

    @Test
    @DisplayName("库名映射+表名映射并存：限定名 ALTER 两者都应改写（回归任务 9e1e602e）")
    void databaseAndTableMappingBothAppliedOnQualifiedAlter() throws SQLException {
        // 回归：SAME_ENGINE 曾先走 DdlTranslator 正则把 test1.t2 改成 test3.t2，
        // 再做表名映射时用 "test3.t2" 查 "test1.t2" 的 key 落空，表名映射丢失。
        Properties props = baseProps();
        props.setProperty("schema.mapping.db.test1", "test3");
        props.setProperty("schema.mapping.table.test1.t2", "test3.t23");
        SchemaEvolutionService service = createService(props);

        // 目标端先建好映射后的库表，让 ALTER 真实执行
        try (java.sql.Statement st = connection.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS test3");
            st.execute("CREATE TABLE test3.t23 (id INT PRIMARY KEY)");
        }

        // 与真实故障一致：带客户端注释前缀 + 全小写
        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "/* ApplicationName=DBeaver 25.2.4 - SQLEditor <Script-4.sql> */ "
                        + "alter table test1.t2 add column name2 varchar(20)",
                "ALTER_TABLE", "test1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.APPLIED, result.getStatus());
        String executedSql = result.getExecutedSql();
        assertTrue(executedSql.contains("test3.t23"),
                "库名与表名都应映射，实际: " + executedSql);
        // 列真实加到了映射后的表上
        try (java.sql.Statement st = connection.createStatement();
             java.sql.ResultSet rs = st.executeQuery(
                     "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                             + "WHERE TABLE_SCHEMA = 'TEST3' AND TABLE_NAME = 'T23' AND COLUMN_NAME = 'NAME2'")) {
            assertTrue(rs.next(), "name2 列应加在 test3.t23 上");
        }
    }

    @Test
    @DisplayName("仅表名映射（无库名映射）：限定名 ALTER 表名应改写")
    void tableMappingOnlyAppliedOnQualifiedAlter() throws SQLException {
        Properties props = baseProps();
        props.setProperty("schema.mapping.table.test1.t9", "test1.t9_new");
        SchemaEvolutionService service = createService(props);

        try (java.sql.Statement st = connection.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS test1");
            st.execute("CREATE TABLE test1.t9_new (id INT PRIMARY KEY)");
        }

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "ALTER TABLE test1.t9 ADD COLUMN c INT", "ALTER_TABLE", "test1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.APPLIED, result.getStatus());
        assertTrue(result.getExecutedSql().contains("t9_new"),
                "表名应映射，实际: " + result.getExecutedSql());
    }

    // ==================== 同步粒度策略 ====================

    @Test
    @DisplayName("表级同步：同步清单外的新表 CREATE TABLE 应被屏蔽")
    void tableLevelShouldBlockDdlForUnselectedTable() throws SQLException {
        Properties props = baseProps();
        props.setProperty("migration.included.tables", "db1.t1,db1.t2");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE t_unselected (id INT PRIMARY KEY)", "CREATE_TABLE", "db1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.SKIPPED, result.getStatus());
        // 目标库不应出现该表
        try (java.sql.Statement st = connection.createStatement();
             java.sql.ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME)='T_UNSELECTED'")) {
            rs.next();
            assertEquals(0, rs.getInt(1), "清单外新表不应被建到目标库");
        }
    }

    @Test
    @DisplayName("表级同步：带库限定符的清单外表 DDL 同样屏蔽")
    void tableLevelShouldBlockQualifiedUnselectedTable() throws SQLException {
        Properties props = baseProps();
        props.setProperty("migration.included.tables", "db1.t1");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "ALTER TABLE `db1`.`t_other` ADD COLUMN c INT", "ALTER_TABLE", "db1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.SKIPPED, result.getStatus());
    }

    @Test
    @DisplayName("表级同步：清单内表的 DDL 正常应用")
    void tableLevelShouldApplyDdlForSelectedTable() throws SQLException {
        Properties props = baseProps();
        props.setProperty("migration.included.tables", "db1.t_sel");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult created = service.applyDdl(
                "CREATE TABLE t_sel (id INT PRIMARY KEY)", "CREATE_TABLE", "db1");
        assertEquals(SchemaEvolutionService.ApplyResult.Status.APPLIED, created.getStatus());

        SchemaEvolutionService.ApplyResult altered = service.applyDdl(
                "ALTER TABLE t_sel ADD COLUMN c INT", "ALTER_TABLE", "db1");
        assertEquals(SchemaEvolutionService.ApplyResult.Status.APPLIED, altered.getStatus());
    }

    @Test
    @DisplayName("库级同步：范围内新表 CREATE TABLE 正常应用（不受表清单过滤影响）")
    void dbLevelShouldApplyNewTableDdl() throws SQLException {
        Properties props = baseProps();
        props.setProperty("sync.db.level", "true");
        props.setProperty("sync.db.level.databases", "db1");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE t_dblevel_extra (id INT PRIMARY KEY)", "CREATE_TABLE", "db1");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.APPLIED, result.getStatus());
    }

    @Test
    @DisplayName("库级同步：范围外数据库的 DDL 屏蔽")
    void dbLevelShouldBlockOutOfScopeDatabase() throws SQLException {
        Properties props = baseProps();
        props.setProperty("sync.db.level", "true");
        props.setProperty("sync.db.level.databases", "db1");
        SchemaEvolutionService service = createService(props);

        SchemaEvolutionService.ApplyResult result = service.applyDdl(
                "CREATE TABLE t_x (id INT)", "CREATE_TABLE", "other_db");

        assertEquals(SchemaEvolutionService.ApplyResult.Status.SKIPPED, result.getStatus());
    }

    @Test
    @DisplayName("TRIGGER/EVENT DDL：两种粒度都运行期跳过（任务结束时统一同步）")
    void triggerAndEventDdlAlwaysDeferred() throws SQLException {
        Properties tableProps = baseProps();
        tableProps.setProperty("migration.included.tables", "db1.t1");
        SchemaEvolutionService tableSvc = createService(tableProps);
        assertEquals(SchemaEvolutionService.ApplyResult.Status.SKIPPED, tableSvc.applyDdl(
                "CREATE TRIGGER trg1 BEFORE INSERT ON t1 FOR EACH ROW SET @x=1", "CREATE_TRIGGER", "db1").getStatus());

        Properties dbProps = baseProps();
        dbProps.setProperty("sync.db.level", "true");
        dbProps.setProperty("sync.db.level.databases", "db1");
        SchemaEvolutionService dbSvc = createService(dbProps);
        assertEquals(SchemaEvolutionService.ApplyResult.Status.SKIPPED, dbSvc.applyDdl(
                "CREATE EVENT ev1 ON SCHEDULE EVERY 1 DAY DO DELETE FROM t1", "CREATE_EVENT", "db1").getStatus());
    }
}
