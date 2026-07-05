package com.migration.dialect;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * {@link SqlDialect} 方言矩阵单元测试：
 * 标识符引用、分页子句、驱动/端口/JDBC URL、按类型解析与回退。
 * 锁定历史 bug：MySQL 源分页曾误用 FETCH FIRST（MySQL 不支持）导致全量失败。
 */
@DisplayName("SqlDialect 方言矩阵")
class SqlDialectTest {

    @Test
    @DisplayName("forType 解析与回退：未知/空类型回退 MySQL，postgres 前缀识别为 PG")
    void forTypeDispatch() {
        assertInstanceOf(MySqlDialect.class, SqlDialect.forType("mysql"));
        assertInstanceOf(MySqlDialect.class, SqlDialect.forType("MySQL"));
        assertInstanceOf(PostgreSqlDialect.class, SqlDialect.forType("postgresql"));
        assertInstanceOf(PostgreSqlDialect.class, SqlDialect.forType("postgres"));
        assertInstanceOf(OracleDialect.class, SqlDialect.forType("oracle"));
        assertInstanceOf(OracleDialect.class, SqlDialect.forType("Oracle"));
        // 未知/空类型回退 MySQL
        assertInstanceOf(MySqlDialect.class, SqlDialect.forType(null));
        assertInstanceOf(MySqlDialect.class, SqlDialect.forType(""));
        assertInstanceOf(MySqlDialect.class, SqlDialect.forType("sqlserver"));
    }

    @Test
    @DisplayName("标识符引用：MySQL 反引号，PG/Oracle 双引号")
    void quoteIdentifier() {
        assertEquals("`t1`", SqlDialect.forType("mysql").quoteIdentifier("t1"));
        assertEquals("\"t1\"", SqlDialect.forType("postgresql").quoteIdentifier("t1"));
        assertEquals("\"T1\"", SqlDialect.forType("oracle").quoteIdentifier("T1"));
    }

    @Test
    @DisplayName("分页子句：MySQL 用 LIMIT，PG/Oracle 用 FETCH FIRST（回归：MySQL 不支持 FETCH FIRST）")
    void limitClause() {
        assertEquals("LIMIT 1000", SqlDialect.forType("mysql").limitClause(1000));
        assertEquals("FETCH FIRST 1000 ROWS ONLY", SqlDialect.forType("postgresql").limitClause(1000));
        assertEquals("FETCH FIRST 500 ROWS ONLY", SqlDialect.forType("oracle").limitClause(500));
    }

    @Test
    @DisplayName("引用字符/驱动类/默认端口")
    void connectionProfile() {
        SqlDialect my = SqlDialect.forType("mysql");
        SqlDialect pg = SqlDialect.forType("postgresql");
        SqlDialect ora = SqlDialect.forType("oracle");

        assertEquals("`", my.quoteChar());
        assertEquals("\"", pg.quoteChar());
        assertEquals("\"", ora.quoteChar());

        assertEquals("com.mysql.cj.jdbc.Driver", my.jdbcDriverClass());
        assertEquals("org.postgresql.Driver", pg.jdbcDriverClass());
        assertEquals("oracle.jdbc.OracleDriver", ora.jdbcDriverClass());

        assertEquals(3306, my.defaultPort());
        assertEquals(5432, pg.defaultPort());
        assertEquals(1521, ora.defaultPort());
    }

    @Test
    @DisplayName("JDBC URL 生成与历史 ConfigService 行为逐字节一致")
    void jdbcUrl() {
        assertEquals("jdbc:mysql://h:3306/db?useSSL=false&serverTimezone=UTC&characterEncoding=utf8",
                SqlDialect.forType("mysql").jdbcUrl("h", "3306", "db"));
        assertEquals("jdbc:postgresql://h:5432/db?currentSchema=public&stringtype=unspecified",
                SqlDialect.forType("postgresql").jdbcUrl("h", "5432", "db"));
        assertEquals("jdbc:oracle:thin:@h:1521/SVC",
                SqlDialect.forType("oracle").jdbcUrl("h", "1521", "SVC"));
    }

    @Test
    @DisplayName("getType 归一化协议名")
    void typeName() {
        assertEquals("mysql", SqlDialect.forType("MYSQL").getType());
        assertEquals("postgresql", SqlDialect.forType("postgres").getType());
        assertEquals("oracle", SqlDialect.forType("oracle").getType());
    }
}
