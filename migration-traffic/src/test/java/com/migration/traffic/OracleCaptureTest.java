package com.migration.traffic;

import com.migration.traffic.capture.OracleAuditTrafficSource;
import com.migration.traffic.capture.StatementClassifier;
import com.migration.traffic.capture.oracle.OracleBindParser;
import com.migration.traffic.capture.oracle.OraclePlaceholders;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Oracle 语句流捕获")
class OracleCaptureTest {

    @Nested
    @DisplayName("SQL_BINDS 解析")
    class Binds {

        @Test
        @DisplayName("按声明长度切片 —— 值里有空格和 # 时，按分隔符切必然错位")
        void slicesByDeclaredLength() {
            List<String> b = OracleBindParser.parse(" #1(1):2 #2(20):has space and # hash");
            assertEquals(2, b.size());
            assertEquals("2", b.get(0));
            assertEquals("has space and # hash", b.get(1), "值里的 # 与空格不能把清单切错");
        }

        @Test
        @DisplayName("#n(0): 是 NULL，不是空串")
        void zeroLengthIsNull() {
            List<String> b = OracleBindParser.parse(" #1(0): ");
            assertEquals(1, b.size());
            assertNull(b.get(0));
        }

        @Test
        @DisplayName("多字节值按字符长度切（4 字节 UTF-8 也算 1 个字符）")
        void multibyteLength() {
            List<String> b = OracleBindParser.parse(" #1(9):zh中文emoji");
            assertEquals("zh中文emoji", b.get(0));
        }

        @Test
        @DisplayName("没有绑定值返回 null（不是空列表）")
        void empty() {
            assertNull(OracleBindParser.parse(null));
            assertNull(OracleBindParser.parse(""));
        }
    }

    @Nested
    @DisplayName("占位符改写（:name → ?）")
    class Placeholders {

        @Test
        @DisplayName("按出现顺序改写")
        void basic() {
            OraclePlaceholders.Rewritten r = OraclePlaceholders.rewrite("INSERT INTO t VALUES (:b1, :b2)");
            assertEquals("INSERT INTO t VALUES (?, ?)", r.sql);
            assertEquals(Arrays.asList("B1", "B2"), r.names);
        }

        @Test
        @DisplayName("同名占位符重复引用：SQL_BINDS 只给一份值，两处都要绑上")
        void repeatedName() {
            OraclePlaceholders.Rewritten r = OraclePlaceholders.rewrite(
                    "SELECT * FROM t WHERE a = :x OR b = :x");
            assertEquals("SELECT * FROM t WHERE a = ? OR b = ?", r.sql);
            assertEquals(Arrays.asList("v", "v"), OraclePlaceholders.expand(r.names, List.of("v")));
        }

        @Test
        @DisplayName("字符串字面量与注释里的 :name 不是占位符")
        void insideLiteral() {
            OraclePlaceholders.Rewritten r = OraclePlaceholders.rewrite(
                    "SELECT ':b1' , :b1 FROM dual -- :b9");
            assertTrue(r.sql.contains("':b1'"));
            assertTrue(r.sql.contains("-- :b9"));
            assertEquals(1, r.names.size());
        }

        @Test
        @DisplayName("按位置展开：名字第一次出现的顺序就是 #1 #2 的顺序")
        void expandByPosition() {
            OraclePlaceholders.Rewritten r = OraclePlaceholders.rewrite("INSERT INTO t VALUES (:a, :b, :a)");
            assertEquals(Arrays.asList("A", "B", "A"),
                    OraclePlaceholders.expand(r.names, Arrays.asList("A", "B")));
        }
    }

    @Nested
    @DisplayName("审计文本的清洗")
    class TextCleanup {

        @Test
        @DisplayName("剥掉 CLOB 尾部的 NUL —— 不剥就是 ORA-00911，且录制文件里看不出来")
        void trimsTrailingNul() {
            assertEquals("COMMIT", OracleAuditTrafficSource.trimNul("COMMIT" + "\u0000"));
            assertEquals("SELECT 1 FROM dual",
                    OracleAuditTrafficSource.trimNul("SELECT 1 FROM dual" + "\u0000" + "\n "));
            assertEquals("SELECT 1", OracleAuditTrafficSource.trimNul("SELECT 1"));
            assertNull(OracleAuditTrafficSource.trimNul(null));
        }

        @Test
        @DisplayName("IDENTIFIED BY * 是 Oracle 自己抹的口令，标记为不可回放")
        void detectsRedactedPassword() {
            assertTrue(OracleAuditTrafficSource.isPasswordRedacted("CREATE USER u IDENTIFIED BY *"));
            assertTrue(OracleAuditTrafficSource.isPasswordRedacted("ALTER USER u IDENTIFIED BY  *"));
            assertFalse(OracleAuditTrafficSource.isPasswordRedacted("CREATE USER u IDENTIFIED BY realpass"));
            assertFalse(OracleAuditTrafficSource.isPasswordRedacted("SELECT * FROM t"));
        }
    }

    @Nested
    @DisplayName("还原语句的不对称规则")
    class Noaudit {

        @Test
        @DisplayName("BY 要原样带上；EXCEPT 反而不能带（ORA-46352）")
        void noauditScopeRules() {
            assertEquals("NOAUDIT POLICY P BY APP_USER",
                    OracleAuditTrafficSource.noauditFor("P", "BY APP_USER"));
            assertEquals("NOAUDIT POLICY P",
                    OracleAuditTrafficSource.noauditFor("P", "EXCEPT TRFCAP"),
                    "带 EXCEPT 会报 ORA-46352，随后 DROP 也失败，审计就一直开着");
            assertEquals("NOAUDIT POLICY P", OracleAuditTrafficSource.noauditFor("P", ""));
            assertEquals("NOAUDIT POLICY P", OracleAuditTrafficSource.noauditFor("P", null));
        }
    }

    @Nested
    @DisplayName("Oracle 语句分类")
    class Classify {

        @Test
        @DisplayName("BEGIN 是匿名 PL/SQL 块，不是事务开始 —— Oracle 根本没有 BEGIN 事务语句")
        void beginIsPlSqlBlock() {
            assertEquals(StatementClass.DML,
                    StatementClassifier.classify(TrafficEngine.ORACLE, "BEGIN :b1 := 2; END;"));
            assertEquals(StatementClass.TCL,
                    StatementClassifier.classify(TrafficEngine.MYSQL, "BEGIN"), "MySQL 的 BEGIN 仍然是事务开始");
        }

        @Test
        @DisplayName("ALTER SESSION 是会话状态，必须恒回放（NLS / CURRENT_SCHEMA 都靠它）")
        void alterSessionIsSessionState() {
            assertEquals(StatementClass.SET, StatementClassifier.classify(
                    TrafficEngine.ORACLE, "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'"));
        }

        @Test
        @DisplayName("MERGE 是 DML")
        void merge() {
            assertEquals(StatementClass.DML, StatementClassifier.classify(TrafficEngine.ORACLE,
                    "MERGE INTO t USING s ON (t.id=s.id) WHEN MATCHED THEN UPDATE SET t.v=s.v"));
        }
    }
}
