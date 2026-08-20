package com.migration.config;

import com.migration.common.security.JdbcUrlSafety;
import com.migration.common.ssl.SslMaterial;

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
        this(host, port, database, username, password, null);
    }

    /**
     * host / port / database 在构造时就过闸门（{@link JdbcUrlSafety}）。
     *
     * <p>放在构造而不是 {@link #getJdbcUrl()}：这三段有四条拼装路径
     * （MySQL / PG / Oracle TCPS / root 连接），逐条加校验必然漏；而且构造期报错
     * 指向的是"任务配置里那个值不对"，连接期报错则会伪装成"目标库连不上"。
     *
     * <p>拦的是把驱动参数塞进 JDBC URL 那类注入——{@code allowLoadLocalInfile}
     * 能让恶意服务端读走本机任意文件，{@code socketFactory} / {@code autoDeserialize}
     * 直接是 RCE。
     */
    public DatabaseConfig(String host, int port, String database, String username, String password, String dbType) {
        this.host = JdbcUrlSafety.requireSafeHost(host);
        this.port = JdbcUrlSafety.requireSafePort(port);
        this.database = JdbcUrlSafety.requireSafeDatabase(database);
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

    /**
     * 追加一个 JDBC URL 参数（Oracle 忽略）。同名参数覆盖。
     *
     * <p><b>危险参数一律拒绝</b>。目前所有调用方传的都是硬编码字面量
     * （{@code rewriteBatchedStatements} / {@code reWriteBatchedInserts}），
     * 没有用户输入能走到这里——这道校验挡的是<b>将来</b>：
     * 一旦有人把任务配置里的某个字段接到这里，{@code socketFactory} /
     * {@code allowLoadLocalInfile} 就会立刻变成一条可用的注入路径，
     * 而那时候没人会想起来这个方法直接拼进 JDBC URL。
     */
    public void setJdbcOption(String key, String value) {
        if (key != null && !key.isEmpty() && value != null) {
            JdbcUrlSafety.requireSafeUrl("jdbc:x://h/db?" + key + "=" + value);
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
     * 传输层加密材料（档位 + 证书路径）。默认 {@link SslMaterial#disabled()}，行为与之前完全一致。
     *
     * <p>档位映射与 SSLContext 构造统一收在 {@link SslMaterial}——此前这段 switch 在仓库里
     * 抄了三份（这里、后端的 {@code JdbcSslOptions}、增量的 {@code targetSslParams}），
     * 且已经开始漂移。
     */
    private SslMaterial ssl = SslMaterial.disabled();

    public void setSslMode(String sslMode) {
        if (sslMode != null && !sslMode.trim().isEmpty()) {
            this.ssl = SslMaterial.of(sslMode, ssl.rootCert());
        }
    }

    public String getSslMode() {
        return ssl.mode();
    }

    public void setSslRootCert(String sslRootCert) {
        this.ssl = SslMaterial.of(ssl.mode(), sslRootCert);
    }

    /** 完整的加密材料（含客户端证书/keystore），供需要 SSLContext 的链路取用。 */
    public SslMaterial getSsl() {
        return ssl;
    }

    public void setSsl(SslMaterial ssl) {
        this.ssl = ssl != null ? ssl : SslMaterial.disabled();
    }

    public boolean isSslEnabled() {
        return ssl.enabled();
    }

    /**
     * 完整 JDBC URL。
     *
     * <p>URL 这里仍然带查询串（与后端不同）——引擎侧没有走 Properties 改造，
     * 理由是这边<b>没有开放的注入面</b>：host/port/database 在构造时已过
     * {@link JdbcUrlSafety}，加密参数由 {@code SslMaterial} 按枚举档位生成，
     * 额外参数只接受白名单外可拒的硬编码字面量。而 LOB 流式参数
     * （{@code LobJdbc.withStreamingParams}）与两个判据都依赖 URL 带查询串，
     * 强行改成 Properties 是在数据面上冒无收益的风险。
     *
     * <p>出口再过一次 {@code requireSafeUrl} 作为兜底：将来若有人新增了一条
     * 拼装路径并绕过了构造期校验，这里会拦下。
     */
    public String getJdbcUrl() {
        return JdbcUrlSafety.requireSafeUrl(buildJdbcUrl());
    }

    private String buildJdbcUrl() {
        if ("postgresql".equals(dbType)) {
            String currentSchema = (schema != null && !schema.isEmpty()) ? schema : "public";
            return withExtraOptions(String.format(
                    "jdbc:postgresql://%s:%d/%s?currentSchema=%s&stringtype=unspecified&%s",
                    host, port, database, currentSchema, ssl.pgUrlParams()));
        }
        if ("oracle".equals(dbType)) {
            // Oracle 使用 service name 方式连接: jdbc:oracle:thin:@host:port/service
            // database 字段存储服务名/SID。开启 TLS 时协议与 URL 结构都要换（TCPS + DESCRIPTION），
            // 查询串在 Oracle 的 thin URL 里根本不存在，信任材料走连接属性
            // （见 SslMaterial#applyOracleProperties）。
            String service = (database != null && !database.isEmpty()) ? database : "ORCL";
            if (ssl.enabled()) {
                return ssl.oracleTcpsUrl(host, port, service);
            }
            return String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);
        }
        return withExtraOptions(String.format(
                "jdbc:mysql://%s:%d/%s?%s&serverTimezone=UTC&characterEncoding=utf8&autoReconnect=true&connectTimeout=30000&socketTimeout=0",
                host, port, database, ssl.mysqlUrlParams()));
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

    /**
     * 不指定库名的连接（建库用）。与 {@link #getJdbcUrl()} 走同一份加密材料——
     * 此前这里硬编码 {@code useSSL=false}，等于"业务连接加密了、建库那一跳明文"。
     */
    public String getRootJdbcUrl() {
        return JdbcUrlSafety.requireSafeUrl(buildRootJdbcUrl());
    }

    private String buildRootJdbcUrl() {
        if ("postgresql".equals(dbType)) {
            return String.format("jdbc:postgresql://%s:%d/postgres?stringtype=unspecified&%s",
                                 host, port, ssl.pgUrlParams());
        }
        if ("oracle".equals(dbType)) {
            String service = (database != null && !database.isEmpty()) ? database : "ORCL";
            if (ssl.enabled()) {
                return ssl.oracleTcpsUrl(host, port, service);
            }
            return String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);
        }
        return String.format("jdbc:mysql://%s:%d/?%s&serverTimezone=UTC&characterEncoding=utf8",
                           host, port, ssl.mysqlUrlParams());
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
