package com.migration.traffic;

import com.migration.traffic.capture.StatementClassifier;
import com.migration.traffic.model.StatementClass;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("语句分类：类别判定 / 预处理噪声 / 口令抹除")
class StatementClassifierTest {

    @Nested
    @DisplayName("基本类别")
    class Basic {
        @Test
        void select() {
            assertEquals(StatementClass.SELECT, StatementClassifier.classify("SELECT * FROM t"));
            assertEquals(StatementClass.SELECT, StatementClassifier.classify("show tables"));
            assertEquals(StatementClass.SELECT, StatementClassifier.classify("EXPLAIN SELECT 1"));
            assertEquals(StatementClass.SELECT, StatementClassifier.classify("(SELECT 1) UNION (SELECT 2)"));
        }

        @Test
        void dml() {
            assertEquals(StatementClass.DML, StatementClassifier.classify("INSERT INTO t VALUES(1)"));
            assertEquals(StatementClass.DML, StatementClassifier.classify("update t set a=1"));
            assertEquals(StatementClass.DML, StatementClassifier.classify("LOAD DATA INFILE 'x' INTO TABLE t"));
        }

        @Test
        void ddl() {
            assertEquals(StatementClass.DDL, StatementClassifier.classify("CREATE TABLE t(id INT)"));
            assertEquals(StatementClass.DDL, StatementClassifier.classify("ALTER TABLE t ADD c INT"));
            assertEquals(StatementClass.DDL, StatementClassifier.classify("TRUNCATE TABLE t"));
            assertEquals(StatementClass.DDL, StatementClassifier.classify("RENAME TABLE a TO b"));
        }

        @Test
        void tclAndSetAndUse() {
            assertEquals(StatementClass.TCL, StatementClassifier.classify("BEGIN"));
            assertEquals(StatementClass.TCL, StatementClassifier.classify("start transaction"));
            assertEquals(StatementClass.TCL, StatementClassifier.classify("COMMIT"));
            assertEquals(StatementClass.TCL, StatementClassifier.classify("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
            assertEquals(StatementClass.SET, StatementClassifier.classify("SET autocommit=0"));
            assertEquals(StatementClass.SET, StatementClassifier.classify("SET @a=7, @b='x'"));
            assertEquals(StatementClass.USE, StatementClassifier.classify("USE mydb"));
        }
    }

    @Nested
    @DisplayName("DCL 必须与 DDL 分开 —— 否则「不回放 DCL」这个安全开关直接失效")
    class Dcl {
        @Test
        void userAndRoleAreDclNotDdl() {
            assertEquals(StatementClass.DCL, StatementClassifier.classify("CREATE USER 'a'@'%'"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("DROP USER 'a'@'%'"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("ALTER USER 'a'@'%' IDENTIFIED BY 'x'"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("RENAME USER a TO b"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("CREATE ROLE r"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("GRANT SELECT ON d.* TO 'a'@'%'"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("REVOKE ALL ON *.* FROM 'a'@'%'"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("SET PASSWORD FOR 'a'@'%' = 'x'"));
            assertEquals(StatementClass.DCL, StatementClassifier.classify("FLUSH PRIVILEGES"));
        }

        @Test
        void nonPrivilegeFlushIsNotDcl() {
            assertEquals(StatementClass.OTHER, StatementClassifier.classify("FLUSH TABLES"));
        }
    }

    @Nested
    @DisplayName("前导注释必须剥掉 —— ORM/中间件普遍加 hint 前缀")
    class LeadingComments {
        @Test
        void blockComment() {
            assertEquals(StatementClass.DML,
                    StatementClassifier.classify("/* traceId=abc */ INSERT INTO t VALUES(1)"));
            assertEquals(StatementClass.SELECT,
                    StatementClassifier.classify("/*+ MAX_EXECUTION_TIME(1000) */ SELECT 1"));
        }

        @Test
        void lineComment() {
            assertEquals(StatementClass.SELECT, StatementClassifier.classify("-- note\nSELECT 1"));
            assertEquals(StatementClass.SELECT, StatementClassifier.classify("# note\nSELECT 1"));
        }

        @Test
        void unterminatedBlockCommentIsNotAStatement() {
            assertEquals(StatementClass.OTHER, StatementClassifier.classify("/* never closed"));
        }
    }

    @Nested
    @DisplayName("CTE：WITH 后带写操作算 DML")
    class Cte {
        @Test
        void withSelect() {
            assertEquals(StatementClass.SELECT,
                    StatementClassifier.classify("WITH c AS (SELECT 1) SELECT * FROM c"));
        }

        @Test
        void withDelete() {
            assertEquals(StatementClass.DML,
                    StatementClassifier.classify("WITH c AS (SELECT id FROM t) DELETE FROM t WHERE id IN (SELECT id FROM c)"));
        }
    }

    @Nested
    @DisplayName("预处理语句的噪声行必须整条丢弃")
    class PreparedNoise {
        @Test
        void prepareExecuteDeallocate() {
            // 实测 general_log 把 PREPARE 的文本抹成 "..."，照原样回放必然语法错
            assertTrue(StatementClassifier.isPreparedStatementNoise("PREPARE st FROM ..."));
            assertTrue(StatementClassifier.isPreparedStatementNoise("EXECUTE st USING @a"));
            assertTrue(StatementClassifier.isPreparedStatementNoise("DEALLOCATE PREPARE st"));
        }

        @Test
        void realStatementsAreNotNoise() {
            assertFalse(StatementClassifier.isPreparedStatementNoise("SELECT note FROM t WHERE id = 7"));
            assertFalse(StatementClassifier.isPreparedStatementNoise("DEALLOCATE something_else"));
        }
    }

    @Nested
    @DisplayName("口令抹除：MySQL 自己写的占位符，谁也拿不到原文")
    class Redaction {
        @Test
        void detectsSecretPlaceholder() {
            assertTrue(StatementClassifier.isRedacted("CREATE USER 'a'@'%' IDENTIFIED BY <secret>"));
            assertFalse(StatementClassifier.isRedacted("GRANT SELECT ON d.* TO 'a'@'%'"));
        }
    }

    @Nested
    @DisplayName("USE 目标库解析")
    class UseTarget {
        @Test
        void plainAndQuoted() {
            assertEquals("mydb", StatementClassifier.useTarget("USE mydb"));
            assertEquals("mydb", StatementClassifier.useTarget("use  mydb ;"));
            assertEquals("my db", StatementClassifier.useTarget("USE `my db`"));
            assertNull(StatementClassifier.useTarget("SELECT 1"));
        }
    }
}
