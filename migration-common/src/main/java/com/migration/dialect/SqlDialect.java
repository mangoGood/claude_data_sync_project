package com.migration.dialect;

/**
 * 数据库 SQL 方言抽象。
 *
 * <p>集中处理各数据库在 SQL 语法上的差异（标识符引用、分页等），取代散落在迁移代码里的
 * {@code isOracle()} / {@code isPostgresql()} 字符串分支，避免新增数据库类型时反复踩
 * “某条 SQL 在某个库上语法不对”的坑（例如 MySQL 不支持 {@code FETCH FIRST ... ROWS ONLY}）。
 */
public interface SqlDialect {

    /** 规范化的数据库类型：{@code "mysql"} / {@code "postgresql"} / {@code "oracle"}。 */
    String getType();

    /** 标识符（库名/表名/列名）引用：MySQL 用反引号，PostgreSQL/Oracle 用双引号。 */
    String quoteIdentifier(String identifier);

    /**
     * 键集分页（keyset pagination）时限制返回行数的子句，不含前导空格。
     * 例如 MySQL 返回 {@code "LIMIT 1000"}，Oracle/PostgreSQL 返回
     * {@code "FETCH FIRST 1000 ROWS ONLY"}。
     */
    String limitClause(int rows);

    /** 原始引用字符（不含被引用的标识符本身）：MySQL 反引号，PostgreSQL/Oracle 双引号。 */
    String quoteChar();

    /** JDBC 驱动类名。 */
    String jdbcDriverClass();

    /** 该数据库的默认端口。 */
    int defaultPort();

    /** 按主机/端口/库名构造常规 JDBC 连接串（含该库惯用的连接参数）。 */
    String jdbcUrl(String host, String port, String database);

    /** 按数据库类型字符串解析方言；为空或未知类型时回退到 MySQL。 */
    static SqlDialect forType(String dbType) {
        if (dbType != null) {
            String t = dbType.trim().toLowerCase();
            if (t.startsWith("postgres")) {
                return new PostgreSqlDialect();
            }
            if (t.startsWith("oracle")) {
                return new OracleDialect();
            }
        }
        return new MySqlDialect();
    }
}
