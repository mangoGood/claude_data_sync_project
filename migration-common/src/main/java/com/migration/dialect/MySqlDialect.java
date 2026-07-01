package com.migration.dialect;

/** MySQL 方言：反引号引用标识符，分页用 {@code LIMIT}。 */
public class MySqlDialect implements SqlDialect {

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String limitClause(int rows) {
        return "LIMIT " + rows;
    }

    @Override
    public String quoteChar() {
        return "`";
    }

    @Override
    public String jdbcDriverClass() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public int defaultPort() {
        return 3306;
    }

    @Override
    public String jdbcUrl(String host, String port, String database) {
        return "jdbc:mysql://" + host + ":" + port + "/" + database
                + "?useSSL=false&serverTimezone=UTC&characterEncoding=utf8";
    }
}
