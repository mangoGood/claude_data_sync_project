package com.migration.dialect;

/** PostgreSQL 方言：双引号引用标识符，分页用标准 {@code FETCH FIRST ... ROWS ONLY}。 */
public class PostgreSqlDialect implements SqlDialect {

    @Override
    public String getType() {
        return "postgresql";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String limitClause(int rows) {
        return "FETCH FIRST " + rows + " ROWS ONLY";
    }

    @Override
    public String quoteChar() {
        return "\"";
    }

    @Override
    public String jdbcDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public int defaultPort() {
        return 5432;
    }

    @Override
    public String jdbcUrl(String host, String port, String database) {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database
                + "?currentSchema=public&stringtype=unspecified";
    }
}
