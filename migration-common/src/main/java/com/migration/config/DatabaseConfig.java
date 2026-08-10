package com.migration.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class DatabaseConfig {
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private String dbType = "mysql";
    /**
     * 同一 dbType 下的具体实现（source.db.flavor / target.db.flavor）。目前只用于 TiDB：
     * TiDB 走 MySQL 协议、dbType 归一成 mysql（别去改那 89 处 mysql 分支），但一致性快照的
     * 手法完全不同——TiDB 是 MVCC 的 {@code AS OF TIMESTAMP} 无锁快照，不是 MySQL 的
     * FLUSH TABLES WITH READ LOCK。null/空 = 与 dbType 相同。
     */
    private String flavor;
    private String schema;
    /**
     * 追加到 JDBC URL 上的驱动参数（仅 MySQL / PostgreSQL；Oracle 的 thin URL 不带查询串）。
     * 目前用于全量写侧的批量语句重写（rewriteBatchedStatements / reWriteBatchedInserts），
     * 见 {@code JdbcBatchChannel}。按连接对象逐个设置，不影响其它链路的连接。
     */
    private final Map<String, String> extraJdbcOptions = new LinkedHashMap<>();

    public DatabaseConfig(String host, int port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public DatabaseConfig(String host, int port, String database, String username, String password, String dbType) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.dbType = dbType != null ? dbType : "mysql";
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType != null ? dbType : "mysql";
    }

    /** 具体实现（如 tidb）；未设置时回落到 dbType。 */
    public String getFlavor() {
        return flavor != null && !flavor.trim().isEmpty() ? flavor : dbType;
    }

    public void setFlavor(String flavor) {
        this.flavor = flavor;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    /** 追加一个 JDBC URL 参数（Oracle 忽略）。同名参数覆盖。 */
    public void setJdbcOption(String key, String value) {
        if (key != null && !key.isEmpty() && value != null) {
            extraJdbcOptions.put(key, value);
        }
    }

    /** 复制另一份配置上的 JDBC 参数（多库模式下派生 per-db 配置时用，否则参数会丢）。 */
    public void copyJdbcOptionsFrom(DatabaseConfig other) {
        if (other != null) {
            extraJdbcOptions.putAll(other.extraJdbcOptions);
        }
    }

    public Map<String, String> getJdbcOptions() {
        return java.util.Collections.unmodifiableMap(extraJdbcOptions);
    }

    private String withExtraOptions(String url) {
        if (extraJdbcOptions.isEmpty()) {
            return url;
        }
        StringBuilder sb = new StringBuilder(url);
        for (Map.Entry<String, String> e : extraJdbcOptions.entrySet()) {
            sb.append(url.contains("?") || sb.indexOf("?") >= 0 ? '&' : '?')
              .append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    /**
     * 传输层加密档位：{@code DISABLED（默认）| PREFERRED | REQUIRED | VERIFY_CA | VERIFY_IDENTITY}。
     *
     * <p>此前全仓 34 处硬编码 {@code useSSL=false}，且没有任何 SSL 配置项——
     * 平台把凭证 AES-GCM 落库加密、THL 文件也能加密，唯独<b>真正流动的业务数据在网络上是明文的</b>。
     * 金融政企的入网评审基本过不了这一关。默认仍是 DISABLED，行为与之前完全一致。
     */
    private String sslMode = "DISABLED";
    /** CA 证书/信任库路径（VERIFY_CA / VERIFY_IDENTITY 时用） */
    private String sslRootCert;

    public void setSslMode(String sslMode) {
        if (sslMode != null && !sslMode.trim().isEmpty()) {
            this.sslMode = sslMode.trim().toUpperCase();
        }
    }

    public String getSslMode() {
        return sslMode;
    }

    public void setSslRootCert(String sslRootCert) {
        this.sslRootCert = sslRootCert;
    }

    public boolean isSslEnabled() {
        return !"DISABLED".equalsIgnoreCase(sslMode);
    }

    /** MySQL 的 SSL 参数段（Connector/J 8 的 sslMode 取代了老的 useSSL/requireSSL 组合）。 */
    private String mysqlSslParams() {
        if (!isSslEnabled()) {
            return "useSSL=false";
        }
        StringBuilder sb = new StringBuilder("sslMode=").append(sslMode);
        if (sslRootCert != null && !sslRootCert.isEmpty()) {
            sb.append("&trustCertificateKeyStoreUrl=file:").append(sslRootCert);
        }
        return sb.toString();
    }

    /** PG 的 sslmode 取值与我们的档位一一对应。 */
    private String pgSslParams() {
        String mode;
        switch (sslMode) {
            case "PREFERRED":       mode = "prefer"; break;
            case "REQUIRED":        mode = "require"; break;
            case "VERIFY_CA":       mode = "verify-ca"; break;
            case "VERIFY_IDENTITY": mode = "verify-full"; break;
            default:                mode = "disable";
        }
        StringBuilder sb = new StringBuilder("sslmode=").append(mode);
        if (isSslEnabled() && sslRootCert != null && !sslRootCert.isEmpty()) {
            sb.append("&sslrootcert=").append(sslRootCert);
        }
        return sb.toString();
    }

    public String getJdbcUrl() {
        if ("postgresql".equals(dbType)) {
            String currentSchema = (schema != null && !schema.isEmpty()) ? schema : "public";
            return withExtraOptions(String.format(
                    "jdbc:postgresql://%s:%d/%s?currentSchema=%s&stringtype=unspecified&%s",
                    host, port, database, currentSchema, pgSslParams()));
        }
        if ("oracle".equals(dbType)) {
            // Oracle 使用 service name 方式连接: jdbc:oracle:thin:@host:port/service
            // database 字段存储服务名/SID
            String service = (database != null && !database.isEmpty()) ? database : "ORCL";
            return String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);
        }
        return withExtraOptions(String.format(
                "jdbc:mysql://%s:%d/%s?%s&serverTimezone=UTC&characterEncoding=utf8&autoReconnect=true&connectTimeout=30000&socketTimeout=0",
                host, port, database, mysqlSslParams()));
    }

    public String getJdbcDriverClass() {
        if ("postgresql".equals(dbType)) {
            return "org.postgresql.Driver";
        }
        if ("oracle".equals(dbType)) {
            return "oracle.jdbc.OracleDriver";
        }
        return "com.mysql.cj.jdbc.Driver";
    }

    public String getCreateDatabaseSql() {
        if ("postgresql".equals(dbType)) {
            return "CREATE DATABASE \"" + database + "\" WITH ENCODING 'UTF8'";
        }
        // Oracle 不支持通过 JDBC 创建"数据库"（即 PDB），此处返回空语句
        if ("oracle".equals(dbType)) {
            return "SELECT 1 FROM dual";
        }
        return "CREATE DATABASE IF NOT EXISTS `" + database + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
    }

    public String getRootJdbcUrl() {
        if ("postgresql".equals(dbType)) {
            return String.format("jdbc:postgresql://%s:%d/postgres?stringtype=unspecified", host, port);
        }
        if ("oracle".equals(dbType)) {
            String service = (database != null && !database.isEmpty()) ? database : "ORCL";
            return String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);
        }
        return String.format("jdbc:mysql://%s:%d/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8",
                           host, port);
    }

    public boolean isOracle() {
        return "oracle".equalsIgnoreCase(dbType);
    }

    @Override
    public String toString() {
        return String.format("DatabaseConfig{host='%s', port=%d, database='%s', username='%s', dbType='%s'}",
                           host, port, database, username, dbType);
    }
}
