package com.migration.increment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SqlClassifier} SQL 分类与生成单元测试。
 *
 * <p>验证 DDL/DML/事务语句的分类、子类型识别、表名提取等逻辑。
 */
@DisplayName("SqlClassifier SQL 分类测试")
class SqlClassifierTest {

    private SqlClassifier classifier;

    @BeforeEach
    void setUp() {
        classifier = new SqlClassifier();
    }

    @Test
    @DisplayName("CREATE TABLE 应分类为 DDL/CREATE_TABLE")
    void createTableShouldBeClassifiedAsDdl() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.CREATE_TABLE, result.getDdlSubType());
        assertTrue(result.isNeedsDatabaseSelection());
        assertTrue(result.isParseSuccess());
    }

    @Test
    @DisplayName("CREATE TABLE IF NOT EXISTS 应正确分类")
    void createTableIfNotExistsShouldBeClassified() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "CREATE TABLE IF NOT EXISTS t1 (id INT)");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.CREATE_TABLE, result.getDdlSubType());
    }

    @Test
    @DisplayName("ALTER TABLE 应分类为 DDL/ALTER_TABLE")
    void alterTableShouldBeClassifiedAsDdl() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "ALTER TABLE t1 ADD COLUMN age INT");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.ALTER_TABLE, result.getDdlSubType());
    }

    @Test
    @DisplayName("DROP TABLE 应分类为 DDL/DROP_TABLE")
    void dropTableShouldBeClassifiedAsDdl() {
        SqlClassifier.ClassificationResult result = classifier.classify("DROP TABLE t1");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.DROP_TABLE, result.getDdlSubType());
    }

    @Test
    @DisplayName("TRUNCATE 应分类为 DDL/TRUNCATE")
    void truncateShouldBeClassifiedAsDdl() {
        SqlClassifier.ClassificationResult result = classifier.classify("TRUNCATE TABLE t1");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.TRUNCATE, result.getDdlSubType());
    }

    @Test
    @DisplayName("CREATE INDEX 应分类为 DDL/CREATE_INDEX")
    void createIndexShouldBeClassifiedAsDdl() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "CREATE INDEX idx_name ON t1 (name)");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.CREATE_INDEX, result.getDdlSubType());
    }

    @Test
    @DisplayName("CREATE DATABASE 应分类为 DDL/CREATE_DATABASE")
    void createDatabaseShouldBeClassifiedAsDdl() {
        SqlClassifier.ClassificationResult result = classifier.classify("CREATE DATABASE test_db");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.CREATE_DATABASE, result.getDdlSubType());
        assertFalse(result.isNeedsDatabaseSelection());
    }

    @Test
    @DisplayName("DROP DATABASE 应分类为 DDL/DROP_DATABASE")
    void dropDatabaseShouldBeClassifiedAsDdl() {
        SqlClassifier.ClassificationResult result = classifier.classify("DROP DATABASE test_db");

        assertTrue(result.isDdl());
        assertEquals(SqlClassifier.DdlSubType.DROP_DATABASE, result.getDdlSubType());
    }

    @Test
    @DisplayName("INSERT 应分类为 DML/INSERT")
    void insertShouldBeClassifiedAsDml() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "INSERT INTO t1 (id, name) VALUES (1, 'test')");

        assertTrue(result.isDml());
        assertEquals(SqlClassifier.DmlSubType.INSERT, result.getDmlSubType());
    }

    @Test
    @DisplayName("UPDATE 应分类为 DML/UPDATE")
    void updateShouldBeClassifiedAsDml() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "UPDATE t1 SET name = 'new' WHERE id = 1");

        assertTrue(result.isDml());
        assertEquals(SqlClassifier.DmlSubType.UPDATE, result.getDmlSubType());
    }

    @Test
    @DisplayName("DELETE 应分类为 DML/DELETE")
    void deleteShouldBeClassifiedAsDml() {
        SqlClassifier.ClassificationResult result = classifier.classify("DELETE FROM t1 WHERE id = 1");

        assertTrue(result.isDml());
        assertEquals(SqlClassifier.DmlSubType.DELETE, result.getDmlSubType());
    }

    @Test
    @DisplayName("BEGIN 应分类为 TRANSACTION/BEGIN")
    void beginShouldBeClassifiedAsTransaction() {
        SqlClassifier.ClassificationResult result = classifier.classify("BEGIN");

        assertTrue(result.isTransaction());
        assertEquals(SqlClassifier.TransactionSubType.BEGIN, result.getTransactionSubType());
    }

    @Test
    @DisplayName("COMMIT 应分类为 TRANSACTION/COMMIT")
    void commitShouldBeClassifiedAsTransaction() {
        SqlClassifier.ClassificationResult result = classifier.classify("COMMIT");

        assertTrue(result.isTransaction());
        assertEquals(SqlClassifier.TransactionSubType.COMMIT, result.getTransactionSubType());
    }

    @Test
    @DisplayName("ROLLBACK 应分类为 TRANSACTION/ROLLBACK")
    void rollbackShouldBeClassifiedAsTransaction() {
        SqlClassifier.ClassificationResult result = classifier.classify("ROLLBACK");

        assertTrue(result.isTransaction());
        assertEquals(SqlClassifier.TransactionSubType.ROLLBACK, result.getTransactionSubType());
    }

    @Test
    @DisplayName("USE 语句应分类为 USE")
    void useStatementShouldBeClassifiedAsUse() {
        SqlClassifier.ClassificationResult result = classifier.classify("USE test_db");

        assertTrue(result.isUse());
        assertEquals("test_db", result.getSchemaName());
    }

    @Test
    @DisplayName("带 schema 前缀的表名应正确提取")
    void tableWithSchemaPrefixShouldBeExtracted() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "CREATE TABLE mydb.t1 (id INT)");

        assertTrue(result.isDdl());
        assertEquals("mydb", result.getSchemaName());
        assertEquals("t1", result.getTableName());
        assertEquals("mydb.t1", result.getFullTableName());
    }

    @Test
    @DisplayName("空 SQL 应分类为 OTHER 且解析失败")
    void emptySqlShouldBeClassifiedAsOther() {
        SqlClassifier.ClassificationResult result = classifier.classify("");

        assertEquals(SqlClassifier.StatementType.OTHER, result.getStatementType());
        assertFalse(result.isParseSuccess());
    }

    @Test
    @DisplayName("null SQL 应分类为 OTHER 且解析失败")
    void nullSqlShouldBeClassifiedAsOther() {
        SqlClassifier.ClassificationResult result = classifier.classify(null);

        assertEquals(SqlClassifier.StatementType.OTHER, result.getStatementType());
        assertFalse(result.isParseSuccess());
    }

    /**
     * XA 控制语句必须被认出来，绝不能落进 OTHER。
     *
     * <p>OTHER 在 {@code THLToSqlConverter#generateQuerySql} 里是"不认识但照样执行"的兜底路径。
     * {@code XA START X'..',X'..',1} 走这条路会在目标端<b>执行成功</b>，把应用连接推进 XA ACTIVE 态，
     * 随后的 COMMIT 与 ROLLBACK 双双报 1399 XAER_RMFAIL，增量 fail-stop 且重试永远撞同一堵墙
     * （实测 MySQL 8.0.44）。正常链路上 extract 端已把这些语句消化掉，这里是第二道闸。
     */
    @Test
    @DisplayName("XA 控制语句应分类为 TRANSACTION/XA，不能落进 OTHER 兜底执行")
    void xaStatementsShouldBeClassifiedAsXaTransaction() {
        String[] xaStatements = {
                "XA START X'67747269645f41',X'627175616c5f41',1",
                "XA END X'67747269645f41',X'627175616c5f41',1",
                "XA PREPARE X'67747269645f41',X'627175616c5f41',1",
                "XA COMMIT X'67747269645f41',X'627175616c5f41',1",
                "XA COMMIT X'67747269645f43',X'627175616c5f43',1 ONE PHASE",
                "XA ROLLBACK X'67747269645f42',X'627175616c5f42',1",
                "xa start 'gtrid','bqual',1",
        };

        for (String sql : xaStatements) {
            SqlClassifier.ClassificationResult result = classifier.classify(sql);
            assertTrue(result.isTransaction(), "应分类为事务语句: " + sql);
            assertEquals(SqlClassifier.TransactionSubType.XA, result.getTransactionSubType(),
                    "应识别为 XA 子类型: " + sql);
            assertFalse(result.isNeedsDatabaseSelection(), "XA 语句与库无关: " + sql);
        }
    }

    @Test
    @DisplayName("普通 BEGIN/COMMIT 不受 XA 识别影响")
    void plainTransactionStatementsAreUnaffected() {
        assertEquals(SqlClassifier.TransactionSubType.BEGIN,
                classifier.classify("BEGIN").getTransactionSubType());
        assertEquals(SqlClassifier.TransactionSubType.COMMIT,
                classifier.classify("COMMIT").getTransactionSubType());
        assertEquals(SqlClassifier.TransactionSubType.ROLLBACK,
                classifier.classify("ROLLBACK").getTransactionSubType());
    }

    @Test
    @DisplayName("ClassificationResult toString 应包含关键信息")
    void classificationResultToStringShouldContainKeyInfo() {
        SqlClassifier.ClassificationResult result = classifier.classify(
                "CREATE TABLE t1 (id INT)");

        String str = result.toString();
        assertNotNull(str);
        assertTrue(str.contains("DDL"));
        assertTrue(str.contains("CREATE_TABLE"));
    }
}
