package com.migration.dialect;

/** Oracle 方言：双引号引用标识符（保留原始大小写），分页用 {@code FETCH FIRST ... ROWS ONLY}（12c+）。 */
public class OracleDialect implements SqlDialect {

    @Override
    public String getType() {
        return "oracle";
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
        return "oracle.jdbc.OracleDriver";
    }

    @Override
    public int defaultPort() {
        return 1521;
    }

    @Override
    public String jdbcUrl(String host, String port, String database,
                          com.migration.common.ssl.SslMaterial ssl) {
        // Oracle 开 TLS 不是加参数：协议换成 TCPS、URL 结构整个变，端口通常也从 1521 改成 2484。
        // 信任材料走连接属性（SslMaterial#applyOracleProperties），thin URL 里没有查询串。
        return ssl.enabled()
                ? ssl.oracleTcpsUrl(host, Integer.parseInt(port), database)
                : jdbcUrl(host, port, database);
    }

    @Override
    public String jdbcUrl(String host, String port, String database) {
        return "jdbc:oracle:thin:@" + host + ":" + port + "/" + database;
    }
}
