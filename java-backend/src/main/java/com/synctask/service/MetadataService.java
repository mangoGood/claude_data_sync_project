package com.synctask.service;

import com.synctask.dto.DatabaseInfo;
import com.synctask.dto.TableInfo;
import com.synctask.dto.ValidationResult;
import com.synctask.util.DataSourcePoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class MetadataService {

    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    /** TiCDC OpenAPI 地址，用于 TiDB 源增量任务的预校验（与 agent 的 ticdc.api.url 指向同一服务）。 */
    @org.springframework.beans.factory.annotation.Value("${sync.ticdc.api-url:http://127.0.0.1:18300}")
    private String ticdcApiUrl;

    // 口令部分用 * 而非 +：空口令实例（如默认安装的 TiDB root）连接串形如
    // mysql://root:@host:port，用 + 会整体匹配失败并报“连接串格式不正确”。
    /**
     * 连接串正则。第一道闸门（第二道见 {@code JdbcUrlSafety}）。
     *
     * <p>原来的 database 组是 {@code (.*)}，端口是无长度限制的 {@code \d+}。前者让
     * {@code db?allowLoadLocalInfile=true&…} 这类 payload 完整通过校验并被拼进 JDBC URL；
     * 后者让 {@code :99999999999} 走到 {@code Integer.parseInt} 才炸出难懂的
     * NumberFormatException。这里把两处都收到字符集/长度上。
     */
    private static final Pattern CONNECTION_PATTERN = Pattern.compile(
        "(?:mysql|postgresql|oracle|mongodb|elastic|redis)://([^:@/]+):([^@]*)@"
            + "([A-Za-z0-9._\\-\\[\\]:]+):(\\d{1,5})(?:/([A-Za-z0-9_$.\\-]*))?"
    );

    public static class ParsedConnection {
        public String username;
        public String password;
        public String host;
        public int port;
        public String database;
        public String type;

        public ParsedConnection(String username, String password, String host, int port, String database, String type) {
            this.username = username;
            this.password = password;
            this.host = host;
            this.port = port;
            this.database = database;
            this.type = type;
        }
        
        public boolean isPostgresql() {
            return "postgresql".equals(type);
        }

        public boolean isOracle() {
            return "oracle".equals(type);
        }

        public boolean isMongo() {
            return "mongodb".equals(type);
        }

        public boolean isElastic() {
            return "elasticsearch".equals(type);
        }

        public boolean isRedis() {
            return "redis".equals(type);
        }
    }

    public ParsedConnection parseConnection(String connectionStr) {
        if (connectionStr == null || connectionStr.isEmpty()) {
            throw new IllegalArgumentException("连接串不能为空");
        }

        String dbType;
        if (connectionStr.startsWith("postgresql://")) {
            dbType = "postgresql";
        } else if (connectionStr.startsWith("oracle://")) {
            dbType = "oracle";
        } else if (connectionStr.startsWith("mongodb://")) {
            dbType = "mongodb";
        } else if (connectionStr.startsWith("elastic://")) {
            dbType = "elasticsearch";
        } else if (connectionStr.startsWith("redis://")) {
            dbType = "redis";
        } else {
            dbType = "mysql";
        }

        Matcher matcher = CONNECTION_PATTERN.matcher(connectionStr);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("连接串格式不正确，正确格式: mysql://user:pass@host:port 或 postgresql://user:pass@host:port 或 oracle://user:pass@host:port/service");
        }

        String username = matcher.group(1);
        String password = matcher.group(2);
        // host / database 直接进 JDBC URL 的 format，必须过闸门：正则里 database 组是
        // (.*)，问号与 & 一路放行，等于把驱动参数的控制权交给了调用方
        // （allowLoadLocalInfile → 读本机任意文件；socketFactory / autoDeserialize → RCE）。
        String host = com.synctask.util.JdbcUrlSafety.requireSafeHost(matcher.group(3));
        int port = com.synctask.util.JdbcUrlSafety.requireSafePort(Integer.parseInt(matcher.group(4)));
        String database = com.synctask.util.JdbcUrlSafety.requireSafeDatabase(matcher.group(5));

        return new ParsedConnection(username, password, host, port, database, dbType);
    }

    public static class ConnectionTestResult {
        public boolean connected;
        public String errorType;
        public String errorMessage;
        public String suggestion;
        /** 服务端视角的加密状态。null = 没探测（非 JDBC 链路）或问不出来。 */
        public Boolean encrypted;
        public String tlsVersion;
        public String tlsCipher;

        public ConnectionTestResult(boolean connected, String errorType, String errorMessage, String suggestion) {
            this.connected = connected;
            this.errorType = errorType;
            this.errorMessage = errorMessage;
            this.suggestion = suggestion;
        }

        ConnectionTestResult withTls(com.synctask.util.TlsStateProbe.TlsState state) {
            if (state != null) {
                this.encrypted = state.encrypted;
                this.tlsVersion = state.version;
                this.tlsCipher = state.cipher;
            }
            return this;
        }
    }

    /**
     * 任务级加密配置。null / DISABLED = 与历史行为完全一致（明文，走部署级 env 那套）。
     *
     * <p>做成一个小对象而不是两个散参数：源与目标各有一份，散参数在调用链上极易接反，
     * 而"接反"的表现是目标端用了源端的证书——报错信息会指向证书本身，非常难查。
     */
    public static class SslConfig {
        public final String mode;
        public final com.synctask.util.CertMaterial material;

        public SslConfig(String mode, com.synctask.util.CertMaterial material) {
            this.mode = mode;
            this.material = material;
        }

        boolean enabled() {
            return mode != null && !mode.trim().isEmpty()
                    && !"DISABLED".equalsIgnoreCase(mode.trim());
        }

        /**
         * 是否<b>必须</b>加密（REQUIRED 及以上）。
         *
         * <p>PREFERRED 不在其中——按驱动的既定语义，它在服务端不支持时会退回明文。
         * 拿它当"必须加密"来判会把正常的降级误报成失败。
         */
        boolean mustEncrypt() {
            if (!enabled()) {
                return false;
            }
            String m = mode.trim().toUpperCase();
            return "REQUIRED".equals(m) || "VERIFY_CA".equals(m) || "VERIFY_IDENTITY".equals(m);
        }
    }

    public ConnectionTestResult testConnectionDetailed(String connectionStr, String expectedType) {
        return testConnectionDetailed(connectionStr, expectedType, null);
    }

    public ConnectionTestResult testConnectionDetailed(String connectionStr, String expectedType, SslConfig ssl) {
        ParsedConnection conn = parseConnection(connectionStr);
        boolean isPg = conn.isPostgresql();
        boolean isOracle = conn.isOracle();
        boolean isMongo = conn.isMongo();
        boolean expectPg = "postgresql".equalsIgnoreCase(expectedType);
        boolean expectOracle = "oracle".equalsIgnoreCase(expectedType);
        boolean expectMongo = "mongodb".equalsIgnoreCase(expectedType);

        // MongoDB 不走 JDBC，类型互斥后单独处理
        if (expectMongo && !isMongo) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望MongoDB数据库，但连接串格式不匹配", "连接串应以 mongodb:// 开头");
        }
        if (!expectMongo && isMongo) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "连接串为MongoDB格式，但所选数据库类型不是MongoDB", "请检查数据库类型是否正确");
        }
        if (isMongo) {
            return testMongoConnectionDetailed(conn);
        }

        // Redis 走 RESP（Jedis），类型互斥后单独处理
        boolean isRedis = conn.isRedis();
        boolean expectRedis = "redis".equalsIgnoreCase(expectedType);
        if (expectRedis && !isRedis) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望Redis，但连接串格式不匹配", "连接串应以 redis:// 开头");
        }
        if (!expectRedis && isRedis) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "连接串为Redis格式，但所选数据库类型不是Redis", "请检查数据库类型是否正确");
        }
        if (isRedis) {
            return testRedisConnectionDetailed(conn);
        }

        // Elasticsearch 走 HTTP REST，类型互斥后单独处理
        boolean isElastic = conn.isElastic();
        boolean expectElastic = "elasticsearch".equalsIgnoreCase(expectedType);
        if (expectElastic && !isElastic) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望Elasticsearch，但连接串格式不匹配", "连接串应以 elastic:// 开头");
        }
        if (!expectElastic && isElastic) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "连接串为Elasticsearch格式，但所选数据库类型不是Elasticsearch", "请检查数据库类型是否正确");
        }
        if (isElastic) {
            return testElasticConnectionDetailed(conn);
        }

        try {
            if (isPg) {
                Class.forName("org.postgresql.Driver");
            } else if (isOracle) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
        } catch (ClassNotFoundException e) {
            return new ConnectionTestResult(false, "DRIVER_NOT_FOUND", 
                "数据库驱动未找到: " + e.getMessage(), "请确保依赖中包含对应的数据库驱动");
        }

        if (expectPg && !isPg) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望PostgreSQL数据库，但连接串格式不匹配", "请检查数据库类型是否正确");
        }
        if (expectOracle && !isOracle) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望Oracle数据库，但连接串格式不匹配", "请检查数据库类型是否正确");
        }
        if (!expectPg && isPg) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望MySQL/Oracle数据库，但连接串格式为PostgreSQL", "请检查数据库类型是否正确");
        }
        if (!expectOracle && isOracle) {
            return new ConnectionTestResult(false, "DB_TYPE_MISMATCH",
                "期望MySQL/PostgreSQL数据库，但连接串格式为Oracle", "请检查数据库类型是否正确");
        }

        // 任务级加密优先；没配（或配 DISABLED）时回落到部署级 env 那套，行为与之前一致
        boolean taskSsl = ssl != null && ssl.enabled();
        String jdbcUrl;
        java.util.Properties connProps = new java.util.Properties();
        if (conn.username != null) {
            connProps.setProperty("user", conn.username);
        }
        if (conn.password != null) {
            connProps.setProperty("password", conn.password);
        }
        // 参数一律进 connProps，URL 只留标识符：库名来自用户连接串，
        // 拼进查询串前面就等于把驱动参数的控制权交出去（见 JdbcConnections 的说明）。
        if (isPg) {
            String pgSsl = taskSsl
                    ? com.synctask.util.JdbcSslOptions.postgres(ssl.mode, ssl.material)
                    : com.synctask.util.JdbcSslOptions.postgres();
            String pgDb = (conn.database != null && !conn.database.isEmpty()) ? conn.database : "postgres";
            jdbcUrl = com.synctask.util.JdbcConnections.postgresUrl(conn.host, conn.port, pgDb);
            connProps.setProperty("connectTimeout", "15");
            connProps.setProperty("socketTimeout", "15");
            connProps.setProperty("stringtype", "unspecified");
            connProps.putAll(com.synctask.util.JdbcConnections.parseParams(pgSsl));
        } else if (isOracle) {
            // Oracle 的 thin URL 不带查询串：开启 TLS 要换协议（TCPS）与端口（通常 2484），
            // 信任材料走连接属性。见 OracleSslSupport。
            String service = com.synctask.util.JdbcUrlSafety.requireSafeDatabase(
                    (conn.database != null && !conn.database.isEmpty()) ? conn.database : "ORCL");
            String oraHost = com.synctask.util.JdbcUrlSafety.requireSafeHost(conn.host);
            int oraPort = com.synctask.util.JdbcUrlSafety.requireSafePort(conn.port);
            jdbcUrl = taskSsl
                    ? com.synctask.util.OracleSslSupport.tcpsUrl(oraHost, oraPort, service,
                            com.synctask.util.OracleSslSupport.serverDn())
                    : String.format("jdbc:oracle:thin:@%s:%d/%s", oraHost, oraPort, service);
            if (taskSsl) {
                com.synctask.util.OracleSslSupport.applyProperties(connProps, ssl.mode, ssl.material);
            }
        } else {
            String mysqlSsl = taskSsl
                    ? com.synctask.util.JdbcSslOptions.mysql(ssl.mode, ssl.material)
                    : com.synctask.util.JdbcSslOptions.mysql();
            String myDb = (conn.database != null && !conn.database.isEmpty()) ? conn.database : null;
            jdbcUrl = com.synctask.util.JdbcConnections.mysqlUrl(conn.host, conn.port, myDb);
            connProps.setProperty("serverTimezone", "UTC");
            connProps.setProperty("characterEncoding", "utf8");
            connProps.setProperty("connectTimeout", "15000");
            connProps.setProperty("socketTimeout", "15000");
            connProps.setProperty("allowPublicKeyRetrieval", "true");
            connProps.putAll(com.synctask.util.JdbcConnections.parseParams(mysqlSsl));
        }

        // 测试连接必须用一次性直连（DriverManager），不能走连接池：
        // 测试语义是"验证当前输入的凭证"，复用池中既有连接会掩盖凭证错误，
        // 且一次性测试不应催生常驻池。
        try (Connection connection = java.sql.DriverManager.getConnection(jdbcUrl, connProps)) {
            if (connection.isValid(5)) {
                // TiDB 用 mysql:// 连接串走 MySQL 驱动，连接串无从区分，按所选类型给出提示
                String dbTypeName = isPg ? "PostgreSQL"
                        : (isOracle ? "Oracle" : ("tidb".equalsIgnoreCase(expectedType) ? "TiDB" : "MySQL"));
                String probeType = isPg ? "postgresql" : (isOracle ? "oracle" : "mysql");
                com.synctask.util.TlsStateProbe.TlsState tls =
                        com.synctask.util.TlsStateProbe.probe(connection, probeType);

                // 要求加密却探到明文 = 最危险的那种"成功"：连上了、也没报错，但数据在网上是明文。
                // 这里必须判失败，不能只在提示里写一句。
                if (ssl != null && ssl.mustEncrypt() && Boolean.FALSE.equals(tls.encrypted)) {
                    return new ConnectionTestResult(false, "SSL_NOT_ENCRYPTED",
                            "已选择 " + ssl.mode + " 但连接实际未加密",
                            "服务端可能未开启 SSL。请确认数据库已配置证书（MySQL: have_ssl=YES；"
                                    + "PostgreSQL: ssl=on），且账号未被限制在非加密端口")
                            .withTls(tls);
                }
                String msg = dbTypeName + "连接成功";
                if (tls.encrypted != null) {
                    msg += "（" + tls.describe() + "）";
                }
                return new ConnectionTestResult(true, null, msg, null).withTls(tls);
            } else {
                return new ConnectionTestResult(false, "CONNECTION_FAILED", "连接验证失败", "请检查数据库服务器状态");
            }
        // 注：这里没有单独 catch SSLHandshakeException——它是 IOException 分支的受检异常，
        // JDBC 驱动一律把它包进自己的 SQLException 再抛，直接 catch 反而编译不过。
        // TLS 失败的识别统一由 sslFailureResult() 沿 cause 链完成。
        } catch (java.sql.SQLInvalidAuthorizationSpecException e) {
            return new ConnectionTestResult(false, "AUTH_FAILED",
                "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
        } catch (java.sql.SQLNonTransientConnectionException e) {
            // SSL 失败要先判：驱动会把它包成普通的连接异常，报文里常常还带着
            // "authentication" 之类的词，落到下面的分支就会被误报成"用户名密码错误"，
            // 让人对着一个根本没错的口令查半天。
            ConnectionTestResult sslResult = sslFailureResult(e);
            if (sslResult != null) {
                return sslResult;
            }
            String msg = e.getMessage();
            if (msg != null && (msg.contains("Access denied") || msg.contains("authentication"))) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            ConnectionTestResult sslFallback = sslNotSupportedFallback(ssl, msg);
            if (sslFallback != null) {
                return sslFallback;
            }
            return new ConnectionTestResult(false, "NETWORK_ERROR",
                "网络连接失败：" + e.getMessage(), "请检查数据库服务器地址和端口是否正确，以及网络是否可达");
        } catch (com.mysql.cj.exceptions.WrongArgumentException e) {
            return new ConnectionTestResult(false, "AUTH_FAILED",
                "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
        } catch (java.sql.SQLException e) {
            ConnectionTestResult sslResult = sslFailureResult(e);
            if (sslResult != null) {
                return sslResult;
            }
            String msg = e.getMessage();
            if (msg != null && (msg.contains("Access denied") || msg.contains("authentication") || msg.contains("password") || msg.contains("ORA-01017"))) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            // "Communications link failure" 是 MySQL 驱动在 TCP 根本连不上时的顶层报文
            // （底层往往是 Connection refused，但顶层不含该词），最常见就是地址/端口填错——
            // 归到 NETWORK_ERROR 并给出"检查地址和端口"的明确指引，别让用户对着裸驱动报文猜。
            if (msg != null && (msg.contains("Connection refused") || msg.contains("timed out")
                    || msg.contains("timeout") || msg.contains("ORA-12541")
                    || msg.contains("Communications link failure") || msg.contains("No route to host")
                    || msg.contains("Connection reset"))) {
                return new ConnectionTestResult(false, "NETWORK_ERROR",
                    "网络不可达：无法建立到数据库的连接", "请检查主机地址和端口是否正确、数据库服务是否已启动且可从本机访问");
            }
            ConnectionTestResult sslFallback = sslNotSupportedFallback(ssl, msg);
            if (sslFallback != null) {
                return sslFallback;
            }
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        } catch (Exception e) {
            if (e instanceof java.util.concurrent.TimeoutException || 
                (e.getCause() != null && e.getCause() instanceof java.util.concurrent.TimeoutException)) {
                return new ConnectionTestResult(false, "TIMEOUT",
                    "连接超时：20秒内未连接到数据库服务器", "请检查数据库服务器是否可达，以及防火墙设置");
            }
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        }
    }

    /**
     * 沿 cause 链找 TLS 相关失败，找到就给出可操作的结论；不是 TLS 问题返回 null。
     *
     * <p>必须走 cause 链而不是只看顶层报文：JDBC 驱动一律把 {@code SSLHandshakeException}
     * 包进自己的 {@code SQLException}，顶层那句话往往只是"Communications link failure"
     * 或干脆带上 "authentication"——按顶层报文归类会把证书问题报成网络或口令问题。
     */
    private ConnectionTestResult sslFailureResult(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof java.security.cert.CertificateExpiredException) {
                return new ConnectionTestResult(false, "SSL_CERT_INVALID",
                        "证书校验失败：证书已过期（" + t.getMessage() + "）",
                        "请更换未过期的证书，或让数据库侧续期服务端证书");
            }
            if (t instanceof java.security.cert.CertPathValidatorException
                    || t instanceof java.security.cert.CertificateException) {
                return new ConnectionTestResult(false, "SSL_CERT_INVALID",
                        "证书校验失败：" + t.getMessage(),
                        "服务端证书不是所选 CA 签发的，或证书链不完整。"
                                + "若档位是 VERIFY_IDENTITY，还需要证书的 CN/SAN 与所填主机名一致");
            }
            if (t instanceof javax.net.ssl.SSLHandshakeException) {
                // 握手失败里最常见的两类要分开给建议，否则提示词等于没说：
                //   ·主机名不符——档位选了 VERIFY_IDENTITY，但用 IP 连、证书里写的是域名
                //   ·证书链不通——选错 CA 或压根没选
                String m = t.getMessage() == null ? "" : t.getMessage();
                if (m.contains("identity verification") || m.contains("does not match")
                        || m.contains("No subject alternative")) {
                    return new ConnectionTestResult(false, "SSL_CERT_INVALID",
                            "证书主机名校验失败：" + m,
                            "VERIFY_IDENTITY 要求证书的 CN/SAN 与所填主机名完全一致。"
                                    + "请改用证书上的主机名连接、给证书补上对应的 SAN，"
                                    + "或把档位降到 VERIFY_CA（仍校验证书链，不校验主机名）");
                }
                if (m.contains("CertPathValidatorException") || m.contains("trust anchors")
                        || m.contains("unable to find valid certification path")) {
                    return new ConnectionTestResult(false, "SSL_CERT_INVALID",
                            "证书链校验失败：" + m,
                            "服务端证书不是所选 CA 签发的。请确认选的是该实例对应的 CA 证书"
                                    + "（VERIFY_CA 及以上必须选证书，留空会回落到 JVM 默认信任库，自签 CA 不在其中）");
                }
                return new ConnectionTestResult(false, "SSL_HANDSHAKE_FAILED",
                        "TLS 握手失败：" + m,
                        "请确认服务端已开启 SSL、端口是 TLS 端口（Oracle 通常是 2484 而非 1521），"
                                + "以及所选证书与该服务端匹配");
            }
            if (t instanceof javax.net.ssl.SSLException) {
                return new ConnectionTestResult(false, "SSL_HANDSHAKE_FAILED",
                        "TLS 连接失败：" + t.getMessage(),
                        "请确认服务端 SSL 配置，以及所选档位与证书是否匹配");
            }
        }
        // 还有一类根本不抛 SSLException：驱动自己发现"服务端不支持 SSL"就直接拒绝建连
        // （Connector/J: "SSL Connection required, but not provided by server."）。
        // 这类报文落到下面会被归成 NETWORK_ERROR，把人指去查地址和端口——而地址端口完全是对的。
        //
        // 注意 pgjdbc 的同一句话是**按 JVM locale 本地化**的（中文环境下是"服务器不支援 SSL 连线"），
        // 所以英文子串匹配靠不住。这里只做尽力而为的文本识别，真正兜底的是
        // sslNotSupportedFallback()——它不看报文，只看"要求了加密却没连上"这个上下文。
        String msg = e == null ? null : e.getMessage();
        if (msg != null) {
            String lower = msg.toLowerCase(java.util.Locale.ROOT);
            if (lower.contains("ssl connection required")
                    || lower.contains("does not support ssl")
                    || lower.contains("ssl is required")
                    || msg.contains("不支援 SSL") || msg.contains("不支持 SSL")) {
                return sslNotSupported(msg);
            }
        }
        return null;
    }

    /**
     * 最后兜底：要求了加密、连接又失败、且前面所有分支都没能归类。
     *
     * <p>必须排在认证/网络判定<b>之后</b>——否则一个口令打错也会被说成 SSL 问题。
     * 排在最后时，剩下的可能性里"服务端不支持 SSL"占绝大多数，而给出一个能动手的方向
     * 远好过把驱动的原始报文（很可能还是本地化过的）直接甩给用户。原始报文仍然带上。
     */
    private ConnectionTestResult sslNotSupportedFallback(SslConfig ssl, String rawMessage) {
        if (ssl == null || !ssl.mustEncrypt()) {
            return null;
        }
        return sslNotSupported(rawMessage);
    }

    private ConnectionTestResult sslNotSupported(String rawMessage) {
        return new ConnectionTestResult(false, "SSL_NOT_SUPPORTED",
                "服务端可能未开启 SSL：" + rawMessage,
                "请确认该数据库实例已开启 SSL（MySQL: 配好 ssl_cert/ssl_key 后 have_ssl=YES；"
                        + "PostgreSQL: postgresql.conf 设 ssl=on 并在 pg_hba.conf 放行 hostssl），"
                        + "或把加密档位改回「不加密」");
    }

    /**
     * 构建一次性 MongoClient。使用 directConnection 只连指定节点，避免驱动按副本集
     * 配置里的内部主机名（容器 hostname 等）重路由导致不可达；副本集属性仍可通过
     * hello 命令读取。调用方负责 close。
     */
    private com.mongodb.client.MongoClient buildMongoClient(ParsedConnection conn) {
        String userInfo = "";
        if (conn.username != null && !conn.username.isEmpty()) {
            userInfo = urlEncode(conn.username) + ":" + urlEncode(conn.password) + "@";
        }
        String uri = String.format(
            "mongodb://%s%s:%d/?authSource=admin&directConnection=true"
                + "&connectTimeoutMS=15000&socketTimeoutMS=15000&serverSelectionTimeoutMS=15000",
            userInfo, conn.host, conn.port);
        return com.mongodb.client.MongoClients.create(uri);
    }

    private static String urlEncode(String s) {
        return java.net.URLEncoder.encode(s == null ? "" : s, java.nio.charset.StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    /** 对 Mongo 节点执行 hello 命令（4.x 之前叫 isMaster，hello 自 4.4.2 起可用并向后兼容路由）。 */
    private org.bson.Document mongoHello(com.mongodb.client.MongoClient client) {
        try {
            return client.getDatabase("admin").runCommand(new org.bson.Document("hello", 1));
        } catch (com.mongodb.MongoCommandException e) {
            return client.getDatabase("admin").runCommand(new org.bson.Document("isMaster", 1));
        }
    }

    /** mongos 的 hello 响应带 msg=isdbgrid（无 setName）——用于识别分片集群路由节点。 */
    private static boolean isMongos(org.bson.Document hello) {
        return "isdbgrid".equals(hello.getString("msg"));
    }

    private ConnectionTestResult testMongoConnectionDetailed(ParsedConnection conn) {
        try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
            org.bson.Document hello = mongoHello(client);
            String setName = hello.getString("setName");
            String suffix = isMongos(hello)
                    ? "（分片集群 mongos）"
                    : (setName != null ? "（副本集: " + setName + "）" : "（独立节点，非副本集）");
            return new ConnectionTestResult(true, null, "MongoDB连接成功" + suffix, null);
        } catch (com.mongodb.MongoSecurityException e) {
            return new ConnectionTestResult(false, "AUTH_FAILED",
                "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
        } catch (com.mongodb.MongoTimeoutException e) {
            // 认证失败在驱动内也可能表现为服务器选择超时，从 cause 链里甄别
            if (hasMongoAuthFailureCause(e)) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            return new ConnectionTestResult(false, "NETWORK_ERROR",
                "网络连接失败：" + e.getMessage(), "请检查MongoDB地址和端口是否正确，以及网络是否可达");
        } catch (Exception e) {
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        }
    }

    private boolean hasMongoAuthFailureCause(Throwable e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("Authentication failed")
                || msg.contains("MongoSecurityException")
                || msg.contains("SCRAM"));
    }

    // ==================== Elasticsearch（HTTP REST，不走 JDBC） ====================

    /** GET {path}，返回 [statusCode, body]；网络级失败抛异常。 */
    private String[] esHttpGet(ParsedConnection conn, String path) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(15))
                .build();
        java.net.http.HttpRequest.Builder b = java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create("http://" + conn.host + ":" + conn.port + path))
                .timeout(java.time.Duration.ofSeconds(15));
        if (conn.username != null && !conn.username.isEmpty()) {
            String basic = java.util.Base64.getEncoder().encodeToString(
                    (conn.username + ":" + (conn.password == null ? "" : conn.password))
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            b.header("Authorization", "Basic " + basic);
        }
        java.net.http.HttpResponse<String> resp = client.send(b.GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
        return new String[]{String.valueOf(resp.statusCode()), resp.body()};
    }

    private ConnectionTestResult testElasticConnectionDetailed(ParsedConnection conn) {
        try {
            String[] resp = esHttpGet(conn, "/");
            int status = Integer.parseInt(resp[0]);
            if (status == 401 || status == 403) {
                return new ConnectionTestResult(false, "AUTH_FAILED",
                    "认证失败：用户名或密码错误", "请检查用户名和密码是否正确");
            }
            if (status >= 300) {
                return new ConnectionTestResult(false, "CONNECTION_FAILED",
                    "连接失败：HTTP " + status, "请检查Elasticsearch服务状态");
            }
            String version = "";
            try {
                com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(resp[1]).getAsJsonObject();
                version = json.getAsJsonObject("version").get("number").getAsString();
            } catch (Exception ignore) {
                // 版本解析失败不影响连通性结论
            }
            return new ConnectionTestResult(true, null,
                "Elasticsearch连接成功" + (version.isEmpty() ? "" : "（版本 " + version + "）"), null);
        } catch (java.net.ConnectException | java.net.http.HttpConnectTimeoutException e) {
            return new ConnectionTestResult(false, "NETWORK_ERROR",
                "网络连接失败：" + e.getMessage(), "请检查Elasticsearch地址和端口是否正确，以及网络是否可达");
        } catch (Exception e) {
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        }
    }

    // ==================== Redis（RESP，走 Jedis，不走 JDBC） ====================

    /**
     * 构建一次性 Jedis。用户名为空或 "default" 时走单参 AUTH（兼容 requirepass，Redis 5/6 通用）；
     * 真实 ACL 用户才走双参 AUTH。调用方负责 close。
     */
    private redis.clients.jedis.Jedis buildJedis(ParsedConnection conn) {
        redis.clients.jedis.DefaultJedisClientConfig.Builder cfg =
                redis.clients.jedis.DefaultJedisClientConfig.builder()
                        .connectionTimeoutMillis(15000)
                        .socketTimeoutMillis(15000);
        if (conn.username != null && !conn.username.isEmpty() && !"default".equalsIgnoreCase(conn.username)) {
            cfg.user(conn.username);
        }
        if (conn.password != null && !conn.password.isEmpty()) {
            cfg.password(conn.password);
        }
        return new redis.clients.jedis.Jedis(
                new redis.clients.jedis.HostAndPort(conn.host, conn.port), cfg.build());
    }

    private ConnectionTestResult testRedisConnectionDetailed(ParsedConnection conn) {
        try (redis.clients.jedis.Jedis jedis = buildJedis(conn)) {
            String pong = jedis.ping();
            if (pong != null && "PONG".equalsIgnoreCase(pong)) {
                String version = "";
                try {
                    for (String line : jedis.info("server").split("\\r?\\n")) {
                        if (line.startsWith("redis_version:")) {
                            version = line.substring("redis_version:".length()).trim();
                            break;
                        }
                    }
                } catch (Exception ignore) {
                    // 版本解析失败不影响连通性结论
                }
                return new ConnectionTestResult(true, null,
                    "Redis连接成功" + (version.isEmpty() ? "" : "（版本 " + version + "）"), null);
            }
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接验证失败（PING 未返回 PONG）", "请检查 Redis 服务状态");
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            // NOAUTH / WRONGPASS / invalid username-password pair
            return new ConnectionTestResult(false, "AUTH_FAILED",
                "认证失败：" + e.getMessage(), "请检查用户名和密码是否正确（无密码 Redis 请留空密码）");
        } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
            return new ConnectionTestResult(false, "NETWORK_ERROR",
                "网络连接失败：" + e.getMessage(), "请检查 Redis 地址和端口是否正确，以及网络是否可达");
        } catch (Exception e) {
            return new ConnectionTestResult(false, "CONNECTION_FAILED",
                "连接失败：" + e.getMessage(), "请检查连接参数是否正确");
        }
    }

    /** 枚举 Redis 有数据的逻辑库索引（INFO keyspace），并保证 db0 存在——供向导按整库勾选。 */
    private List<String> listRedisDatabases(ParsedConnection conn) {
        List<String> dbs = new ArrayList<>();
        try (redis.clients.jedis.Jedis jedis = buildJedis(conn)) {
            java.util.TreeSet<Integer> found = new java.util.TreeSet<>();
            for (String line : jedis.info("keyspace").split("\\r?\\n")) {
                // 形如 db0:keys=5,expires=0,avg_ttl=0
                if (line.startsWith("db") && line.contains(":")) {
                    try {
                        found.add(Integer.parseInt(line.substring(2, line.indexOf(':'))));
                    } catch (NumberFormatException ignore) {
                        // 跳过异常行
                    }
                }
            }
            found.add(0); // db0 恒可选（即使当前为空，增量期也可能写入）
            for (Integer db : found) {
                dbs.add(String.valueOf(db));
            }
            logger.info("查询到 {} 个有数据的 Redis 逻辑库", dbs.size());
        } catch (Exception e) {
            logger.error("查询 Redis 逻辑库失败: {}", e.getMessage());
            throw new RuntimeException("查询数据库列表失败: " + e.getMessage());
        }
        return dbs;
    }

    public List<String> listSchemas(String connectionStr, String database) {
        ParsedConnection conn = parseConnection(connectionStr);
        if (!conn.isPostgresql() && !conn.isOracle()) {
            throw new IllegalArgumentException("listSchemas 仅支持PostgreSQL和Oracle数据库");
        }

        List<String> schemas = new ArrayList<>();
        try {
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else {
                Class.forName("oracle.jdbc.OracleDriver");
            }
            String jdbcUrl = buildJdbcUrl(conn, database);
            try (Connection connection = DataSourcePoolManager.getConnection(jdbcUrl, conn.username, conn.password, driverProps(conn))) {
                String sql;
                if (conn.isPostgresql()) {
                    sql = "SELECT schema_name FROM information_schema.schemata " +
                          "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') " +
                          "ORDER BY schema_name";
                } else {
                    // Oracle: 列出用户可访问的 schema（排除系统 schema）
                    sql = "SELECT username FROM all_users " +
                          "WHERE username NOT IN ('SYS','SYSTEM','OUTLN','DBSNMP','APPQOSSYS','WMSYS','XDB','ORDSYS','MDSYS','CTXSYS','EXFSYS','OLAPSYS','ORDDATA','LBACSYS','ANONYMOUS','APEX_030200','FLOWS_FILES','APEX_PUBLIC_USER','SYSMAN','OWBSYS','SPATIAL_WFS_ADMIN_USR','SPATIAL_CSW_ADMIN_USR') " +
                          "ORDER BY username";
                }
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        schemas.add(rs.getString(1));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("查询schema列表失败: {}", e.getMessage());
            throw new RuntimeException("查询schema列表失败: " + e.getMessage());
        }
        return schemas;
    }

    public List<TableInfo> listTables(String connectionStr, String database, String schema) {
        ParsedConnection conn = parseConnection(connectionStr);
        if (conn.isPostgresql()) {
            List<TableInfo> tables = new ArrayList<>();
            try {
                Class.forName("org.postgresql.Driver");
                String jdbcUrl = buildJdbcUrl(conn, database);
                try (Connection connection = DataSourcePoolManager.getConnection(jdbcUrl, conn.username, conn.password, driverProps(conn))) {
                    String effectiveSchema = (schema != null && !schema.isEmpty()) ? schema : "public";
                    try (PreparedStatement stmt = connection.prepareStatement(
                             "SELECT tablename FROM pg_tables WHERE schemaname = ?")) {
                        stmt.setString(1, effectiveSchema);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                String tableName = rs.getString(1);
                                long rows = getPgRowCount(connection, tableName);
                                tables.add(new TableInfo(tableName, rows, "", "TABLE"));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("查询表列表失败: {}", e.getMessage());
                throw new RuntimeException("查询表列表失败: " + e.getMessage());
            }
            return tables;
        }
        if (conn.isOracle()) {
            List<TableInfo> tables = new ArrayList<>();
            try {
                Class.forName("oracle.jdbc.OracleDriver");
                String jdbcUrl = buildJdbcUrl(conn, database);
                try (Connection connection = DataSourcePoolManager.getConnection(jdbcUrl, conn.username, conn.password, driverProps(conn))) {
                    String owner = (schema != null && !schema.isEmpty()) ? schema.toUpperCase() : conn.username.toUpperCase();
                    try (PreparedStatement stmt = connection.prepareStatement(
                             "SELECT table_name FROM all_tables WHERE owner = ? ORDER BY table_name")) {
                        stmt.setString(1, owner);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                String tableName = rs.getString(1);
                                tables.add(new TableInfo(tableName, 0, "", "TABLE"));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("查询Oracle表列表失败: {}", e.getMessage());
                throw new RuntimeException("查询Oracle表列表失败: " + e.getMessage());
            }
            return tables;
        }
        return listTables(connectionStr, database);
    }

    /**
     * JDBC URL。<b>只放标识符，不带任何查询串</b>——驱动参数走
     * {@link #driverProps(ParsedConnection)} 的 Properties。
     *
     * <p>此前这里是 {@code String.format("jdbc:mysql://%s:%d/%s?...", host, port, database)}，
     * 而 database 来自用户提供的连接串。库名里混一个 {@code ?}，后面全部变成驱动参数：
     * {@code allowLoadLocalInfile} 让恶意服务端读走本机文件，{@code socketFactory} /
     * {@code autoDeserialize} 直接是 RCE。字符集校验（{@link com.synctask.util.JdbcUrlSafety}）
     * 已经挡了一道，这里再从结构上断掉——URL 里根本没有 {@code ?} 可供接管。
     */
    private String buildJdbcUrl(ParsedConnection conn, String database) {
        String db = (database != null && !database.isEmpty()) ? database : null;
        if (conn.isPostgresql()) {
            return com.synctask.util.JdbcConnections.postgresUrl(conn.host, conn.port, db);
        }
        if (conn.isOracle()) {
            // Oracle thin URL 本来就没有查询串这个概念，信任材料走连接属性；
            // 但服务名同样来自用户输入，仍要过闸门
            String service = db != null ? db
                    : (conn.database != null && !conn.database.isEmpty() ? conn.database : "ORCL");
            return String.format("jdbc:oracle:thin:@%s:%d/%s",
                    com.synctask.util.JdbcUrlSafety.requireSafeHost(conn.host),
                    com.synctask.util.JdbcUrlSafety.requireSafePort(conn.port),
                    com.synctask.util.JdbcUrlSafety.requireSafeDatabase(service));
        }
        return com.synctask.util.JdbcConnections.mysqlUrl(conn.host, conn.port, db);
    }

    /**
     * 与 {@link #buildJdbcUrl} 配套的驱动参数。取值与改造前拼在 URL 里的那串逐项一致。
     *
     * <p>Oracle 走 thin URL + 连接属性，这里返回的只有用户名口令。
     */
    private java.util.Properties driverProps(ParsedConnection conn) {
        if (conn.isPostgresql()) {
            return com.synctask.util.JdbcConnections.postgresProps(
                    conn.username, conn.password,
                    com.synctask.util.JdbcConnections.pgSchema("public"));
        }
        if (conn.isOracle()) {
            java.util.Properties p = new java.util.Properties();
            if (conn.username != null) p.setProperty("user", conn.username);
            p.setProperty("password", conn.password == null ? "" : conn.password);
            return p;
        }
        return com.synctask.util.JdbcConnections.mysqlProps(conn.username, conn.password, null);
    }
    
    private String buildJdbcUrl(ParsedConnection conn) {
        return buildJdbcUrl(conn, conn.database);
    }

    public boolean testConnection(String connectionStr) {
        ParsedConnection conn = parseConnection(connectionStr);

        if (conn.isMongo()) {
            try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
                client.getDatabase("admin").runCommand(new org.bson.Document("ping", 1));
                return true;
            } catch (Exception e) {
                logger.error("测试MongoDB连接失败: {}", e.getMessage());
                return false;
            }
        }

        try {
            Connection connection;
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else if (conn.isOracle()) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
            connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, null),
                    conn.username, conn.password, driverProps(conn));
            
            boolean valid = connection.isValid(5);
            connection.close();
            return valid;
        } catch (SQLException e) {
            logger.error("测试连接失败: {}", e.getMessage());
            return false;
        } catch (ClassNotFoundException e) {
            logger.error("数据库驱动未找到: {}", e.getMessage());
            return false;
        }
    }

    public List<String> listDatabases(String connectionStr) {
        ParsedConnection conn = parseConnection(connectionStr);

        List<String> databases = new ArrayList<>();

        if (conn.isMongo()) {
            try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
                for (String dbName : client.listDatabaseNames()) {
                    if (!isMongoSystemDatabase(dbName)) {
                        databases.add(dbName);
                    }
                }
                logger.info("查询到 {} 个MongoDB数据库", databases.size());
                return databases;
            } catch (Exception e) {
                logger.error("查询MongoDB数据库列表失败: {}", e.getMessage());
                throw new RuntimeException("查询数据库列表失败: " + e.getMessage());
            }
        }

        if (conn.isRedis()) {
            // Redis 的“数据库”即逻辑库索引（db0..dbN）；向导按整库勾选，无表/集合层级。
            return listRedisDatabases(conn);
        }

        try {
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else if (conn.isOracle()) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
            
            try (Connection connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, null),
                    conn.username, conn.password, driverProps(conn))) {
                
                if (conn.isPostgresql()) {
                    try (Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT datname FROM pg_database WHERE datistemplate = false")) {
                        while (rs.next()) {
                            String dbName = rs.getString(1);
                            if (!isPgSystemDatabase(dbName)) {
                                databases.add(dbName);
                            }
                        }
                    }
                } else if (conn.isOracle()) {
                    // Oracle 没有"数据库"概念，使用服务名/SID 作为标识，直接返回连接串中指定的服务名
                    String service = (conn.database != null && !conn.database.isEmpty()) ? conn.database : "ORCL";
                    databases.add(service);
                } else {
                    DatabaseMetaData metaData = connection.getMetaData();
                    ResultSet rs = metaData.getCatalogs();
                    
                    while (rs.next()) {
                        String dbName = rs.getString("TABLE_CAT");
                        if (!isSystemDatabase(dbName)) {
                            databases.add(dbName);
                        }
                    }
                }
                
                logger.info("查询到 {} 个数据库", databases.size());
            }
        } catch (SQLException e) {
            logger.error("查询数据库列表失败: {}", e.getMessage());
            throw new RuntimeException("查询数据库列表失败: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("数据库驱动未找到: {}", e.getMessage());
            throw new RuntimeException("数据库驱动未找到: " + e.getMessage());
        }
        
        return databases;
    }
    
    private boolean isPgSystemDatabase(String dbName) {
        return "postgres".equalsIgnoreCase(dbName) ||
               "template0".equalsIgnoreCase(dbName) ||
               "template1".equalsIgnoreCase(dbName);
    }

    private boolean isSystemDatabase(String dbName) {
        return "information_schema".equalsIgnoreCase(dbName) ||
               "mysql".equalsIgnoreCase(dbName) ||
               "performance_schema".equalsIgnoreCase(dbName) ||
               "sys".equalsIgnoreCase(dbName);
    }

    private boolean isMongoSystemDatabase(String dbName) {
        return "admin".equalsIgnoreCase(dbName) ||
               "local".equalsIgnoreCase(dbName) ||
               "config".equalsIgnoreCase(dbName);
    }

    /** 列过滤支持的 MySQL 类型（整数/bit/浮点定点/日期时间），供前端“列名过滤”页签按类型放行 */
    private static final Set<String> FILTERABLE_COLUMN_TYPES = new HashSet<>(Arrays.asList(
        "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "bit",
        "decimal", "numeric", "float", "double",
        "date", "datetime", "timestamp", "time", "year"
    ));

    /** 列过滤支持的 PostgreSQL 类型（information_schema.columns.data_type 口径）：整数/定点/浮点/布尔/日期时间。 */
    private static final Set<String> FILTERABLE_PG_COLUMN_TYPES = new HashSet<>(Arrays.asList(
        "smallint", "integer", "bigint", "numeric", "decimal", "real", "double precision",
        "boolean", "date",
        "timestamp without time zone", "timestamp with time zone",
        "time without time zone", "time with time zone"
    ));

    /**
     * 查询表的列信息（列处理页面用，支持 MySQL 与 PostgreSQL 同构源）。
     * 返回每列的 name/dataType/columnType/primaryKey/filterable。
     */
    public List<java.util.Map<String, Object>> listColumns(String connectionStr, String database, String table) {
        ParsedConnection conn = parseConnection(connectionStr);
        if (conn.isPostgresql()) {
            return listPostgresColumns(conn, database, table);
        }
        if (conn.isMongo()) {
            return listMongoFields(conn, database, table);
        }
        if (conn.isOracle() || conn.isElastic()) {
            throw new RuntimeException("列处理目前仅支持 MySQL / PostgreSQL / MongoDB 同构数据源");
        }
        List<java.util.Map<String, Object>> columns = new ArrayList<>();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Connection connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, database), conn.username, conn.password, driverProps(conn));
                 PreparedStatement stmt = connection.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY FROM information_schema.COLUMNS " +
                     "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
                stmt.setString(1, database);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        java.util.Map<String, Object> col = new java.util.LinkedHashMap<>();
                        String dataType = rs.getString("DATA_TYPE");
                        col.put("name", rs.getString("COLUMN_NAME"));
                        col.put("dataType", dataType);
                        col.put("columnType", rs.getString("COLUMN_TYPE"));
                        col.put("primaryKey", "PRI".equalsIgnoreCase(rs.getString("COLUMN_KEY")));
                        col.put("filterable", dataType != null
                                && FILTERABLE_COLUMN_TYPES.contains(dataType.toLowerCase()));
                        columns.add(col);
                    }
                }
            }
            logger.info("表 {}.{} 查询到 {} 个列", database, table, columns.size());
        } catch (SQLException e) {
            logger.error("查询列信息失败: {}", e.getMessage());
            throw new RuntimeException("查询列信息失败: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("数据库驱动未找到: " + e.getMessage());
        }
        return columns;
    }

    /**
     * 查询 PostgreSQL 表的列信息（列处理页面用）。connectionStr 携带 PG 数据库名，
     * database 参数是 PG schema（与 syncObjects 的库键一致，PG schema 对应库级概念）。
     */
    private List<java.util.Map<String, Object>> listPostgresColumns(ParsedConnection conn, String schema, String table) {
        String effectiveSchema = (schema != null && !schema.isEmpty()) ? schema : "public";
        List<java.util.Map<String, Object>> columns = new ArrayList<>();
        try {
            Class.forName("org.postgresql.Driver");
            try (Connection connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, conn.database), conn.username, conn.password, driverProps(conn))) {
                // 主键列集合
                Set<String> pkColumns = new HashSet<>();
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT kcu.column_name FROM information_schema.table_constraints tc " +
                        "JOIN information_schema.key_column_usage kcu " +
                        "  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " +
                        "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = ? AND tc.table_name = ?")) {
                    stmt.setString(1, effectiveSchema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            pkColumns.add(rs.getString(1));
                        }
                    }
                }
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale " +
                        "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
                    stmt.setString(1, effectiveSchema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            java.util.Map<String, Object> col = new java.util.LinkedHashMap<>();
                            String colName = rs.getString("column_name");
                            String dataType = rs.getString("data_type");
                            col.put("name", colName);
                            col.put("dataType", dataType);
                            col.put("columnType", buildPgColumnType(rs, dataType));
                            col.put("primaryKey", pkColumns.contains(colName));
                            col.put("filterable", dataType != null
                                    && FILTERABLE_PG_COLUMN_TYPES.contains(dataType.toLowerCase()));
                            columns.add(col);
                        }
                    }
                }
            }
            logger.info("PG 表 {}.{} 查询到 {} 个列", effectiveSchema, table, columns.size());
        } catch (SQLException e) {
            logger.error("查询 PG 列信息失败: {}", e.getMessage());
            throw new RuntimeException("查询列信息失败: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("数据库驱动未找到: " + e.getMessage());
        }
        return columns;
    }

    /**
     * 采样 MongoDB 集合的字段（列处理页面用）。Mongo 无固定 schema：抽样若干文档取并集字段，
     * 按首个非空值的 BSON 类型推断 dataType 与 filterable（数值/日期/布尔可过滤，与 mysql/pg 口径一致；
     * 字符串/对象/数组不可过滤）。{@code _id} 标记 primaryKey，不参与过滤/映射。
     * database=库名、table=集合名（与 syncObjects 的库键一致）。
     */
    private List<java.util.Map<String, Object>> listMongoFields(ParsedConnection conn, String database, String collection) {
        final int SAMPLE = 200;
        java.util.LinkedHashMap<String, java.util.Map<String, Object>> fields = new java.util.LinkedHashMap<>();
        try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
            com.mongodb.client.MongoCollection<org.bson.Document> coll =
                    client.getDatabase(database).getCollection(collection);
            try (com.mongodb.client.MongoCursor<org.bson.Document> cursor =
                         coll.find().limit(SAMPLE).iterator()) {
                while (cursor.hasNext()) {
                    org.bson.Document doc = cursor.next();
                    for (java.util.Map.Entry<String, Object> e : doc.entrySet()) {
                        java.util.Map<String, Object> col = fields.get(e.getKey());
                        boolean firstConcrete = (col == null) || "null".equals(col.get("dataType"));
                        if (col == null) {
                            col = new java.util.LinkedHashMap<>();
                            col.put("name", e.getKey());
                            col.put("primaryKey", "_id".equals(e.getKey()));
                            col.put("dataType", "null");
                            col.put("columnType", "null");
                            col.put("filterable", false);
                            fields.put(e.getKey(), col);
                        }
                        // 首个非空值决定类型/可过滤性；_id 不可过滤（是删除/定位键，不做列过滤）
                        if (firstConcrete && e.getValue() != null) {
                            String bsonType = bsonTypeName(e.getValue());
                            col.put("dataType", bsonType);
                            col.put("columnType", bsonType);
                            col.put("filterable", !"_id".equals(e.getKey()) && MONGO_FILTERABLE_TYPES.contains(bsonType));
                        }
                    }
                }
            }
            logger.info("Mongo 集合 {}.{} 采样到 {} 个字段", database, collection, fields.size());
        } catch (Exception e) {
            logger.error("采样 Mongo 字段失败: {}", e.getMessage());
            throw new RuntimeException("查询列信息失败: " + e.getMessage());
        }
        return new ArrayList<>(fields.values());
    }

    private static final Set<String> MONGO_FILTERABLE_TYPES = new HashSet<>(java.util.Arrays.asList(
            "int", "long", "double", "decimal", "date", "bool"));

    /** BSON 运行时值 → 展示用类型名（与 filterable 口径对应）。 */
    private static String bsonTypeName(Object v) {
        if (v instanceof Integer) return "int";
        if (v instanceof Long) return "long";
        if (v instanceof Double || v instanceof Float) return "double";
        if (v instanceof org.bson.types.Decimal128) return "decimal";
        if (v instanceof java.util.Date) return "date";
        if (v instanceof Boolean) return "bool";
        if (v instanceof String) return "string";
        if (v instanceof org.bson.types.ObjectId) return "objectId";
        if (v instanceof org.bson.Document) return "object";
        if (v instanceof java.util.List) return "array";
        return v.getClass().getSimpleName().toLowerCase();
    }

    /** 组装 PG 列的展示类型串：varchar(n) / numeric(p,s) / 原始 data_type。 */
    private String buildPgColumnType(ResultSet rs, String dataType) throws SQLException {
        if (dataType == null) {
            return "";
        }
        long charLen = rs.getLong("character_maximum_length");
        if (!rs.wasNull() && charLen > 0) {
            return dataType + "(" + charLen + ")";
        }
        int precision = rs.getInt("numeric_precision");
        boolean precisionNull = rs.wasNull();
        int scale = rs.getInt("numeric_scale");
        if (!precisionNull && ("numeric".equalsIgnoreCase(dataType) || "decimal".equalsIgnoreCase(dataType))) {
            return dataType + "(" + precision + "," + scale + ")";
        }
        return dataType;
    }

    public List<TableInfo> listTables(String connectionStr, String database) {
        ParsedConnection conn = parseConnection(connectionStr);

        List<TableInfo> tables = new ArrayList<>();

        if (conn.isMongo()) {
            try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
                com.mongodb.client.MongoDatabase db = client.getDatabase(database);
                for (String name : db.listCollectionNames()) {
                    if (name.startsWith("system.")) {
                        continue;
                    }
                    long docs = db.getCollection(name).estimatedDocumentCount();
                    tables.add(new TableInfo(name, docs, "", "COLLECTION"));
                }
                logger.info("MongoDB数据库 {} 查询到 {} 个集合", database, tables.size());
                return tables;
            } catch (Exception e) {
                logger.error("查询MongoDB集合列表失败: {}", e.getMessage());
                throw new RuntimeException("查询表列表失败: " + e.getMessage());
            }
        }

        if (conn.isRedis()) {
            // Redis 无表/集合层级（同步对象为整个逻辑库），返回空列表。
            return tables;
        }

        try {
            if (conn.isPostgresql()) {
                Class.forName("org.postgresql.Driver");
            } else if (conn.isOracle()) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }

            try (Connection connection = DataSourcePoolManager.getConnection(
                    buildJdbcUrl(conn, database),
                    conn.username, conn.password, driverProps(conn))) {
                
                if (conn.isPostgresql()) {
                    try (Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery(
                             "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")) {
                        while (rs.next()) {
                            String tableName = rs.getString(1);
                            long rows = getPgRowCount(connection, tableName);
                            tables.add(new TableInfo(tableName, rows, "", "TABLE"));
                        }
                    }
                } else if (conn.isOracle()) {
                    String owner = (database != null && !database.isEmpty()) ? database.toUpperCase() : conn.username.toUpperCase();
                    try (PreparedStatement stmt = connection.prepareStatement(
                             "SELECT table_name FROM all_tables WHERE owner = ? ORDER BY table_name")) {
                        stmt.setString(1, owner);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                String tableName = rs.getString(1);
                                tables.add(new TableInfo(tableName, 0, "", "TABLE"));
                            }
                        }
                    }
                } else {
                    DatabaseMetaData metaData = connection.getMetaData();
                    ResultSet rs = metaData.getTables(database, null, "%", new String[]{"TABLE"});
                    
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        long rows = getRowCount(connection, database, tableName);
                        String size = getTableSize(connection, database, tableName);
                        String engine = getTableEngine(metaData, database, tableName);
                        
                        tables.add(new TableInfo(tableName, rows, size, engine));
                    }
                }
                
                logger.info("数据库 {} 查询到 {} 个表", database, tables.size());
            }
        } catch (SQLException e) {
            logger.error("查询表列表失败: {}", e.getMessage());
            throw new RuntimeException("查询表列表失败: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("数据库驱动未找到: {}", e.getMessage());
            throw new RuntimeException("数据库驱动未找到: " + e.getMessage());
        }
        
        return tables;
    }
    
    private long getPgRowCount(Connection connection, String tableName) {
        // 表名是标识符无法参数化：对引号转义（表名来自服务端元数据枚举，转义防御异常命名）
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM \"" + tableName.replace("\"", "\"\"") + "\"")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.warn("获取PG表 {} 行数失败: {}", tableName, e.getMessage());
        }
        return 0;
    }

    private long getRowCount(Connection connection, String database, String tableName) {
        // 表名是标识符无法参数化：对反引号转义（表名来自服务端元数据枚举，转义防御异常命名）
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM `" + tableName.replace("`", "``") + "`")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.warn("获取表 {} 行数失败: {}", tableName, e.getMessage());
        }
        return 0;
    }

    private String getTableSize(Connection connection, String database, String tableName) {
        try (PreparedStatement stmt = connection.prepareStatement(
                 "SELECT ROUND(data_length + index_length) as size_bytes " +
                 "FROM information_schema.tables " +
                 "WHERE table_schema = ? AND table_name = ?")) {
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long bytes = rs.getLong("size_bytes");
                    return formatSize(bytes);
                }
            }
        } catch (SQLException e) {
            logger.warn("获取表 {} 大小失败: {}", tableName, e.getMessage());
        }
        return "0 B";
    }

    private String getTableEngine(DatabaseMetaData metaData, String database, String tableName) {
        try (ResultSet rs = metaData.getTables(database, null, tableName, new String[]{"TABLE"})) {
            if (rs.next()) {
                return rs.getString("TABLE_TYPE");
            }
        } catch (SQLException e) {
            logger.warn("获取表 {} 引擎失败: {}", tableName, e.getMessage());
        }
        return "UNKNOWN";
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    public DatabaseInfo getDatabaseWithTables(String connectionStr, String database) {
        DatabaseInfo dbInfo = new DatabaseInfo(database);
        
        try {
            List<TableInfo> tables = listTables(connectionStr, database);
            dbInfo.setTables(tables);
            dbInfo.setAccessible(true);
        } catch (Exception e) {
            dbInfo.setAccessible(false);
            dbInfo.setErrorMessage(e.getMessage());
            logger.error("获取数据库 {} 信息失败: {}", database, e.getMessage());
        }
        
        return dbInfo;
    }

    public ValidationResult validateForSubscribe(String sourceConnection, String sourceType) {
        ValidationResult result = new ValidationResult();
        boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceType);
        boolean sourceIsOracle = "oracle".equalsIgnoreCase(sourceType);

        ParsedConnection sourceConn = parseConnection(sourceConnection);

        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn, sourceIsPg),
                sourceConn.username, sourceConn.password, driverProps(sourceConn))) {

            String dbTypeName = sourceIsPg ? "PostgreSQL" : (sourceIsOracle ? "Oracle" : "MySQL");
            result.addItem("源库连接", "源数据库连接检查", true,
                    dbTypeName + "源库连接成功", "info");

            if (sourceIsPg) {
                // 订阅恒为增量：PG 版本号 + WAL + 复制槽/发送进程 + 复制权限 + REPLICA IDENTITY + 外键
                checkPgSourceVersionSupported(sourceDb, result, true);
                checkPgWalConfig(sourceDb, result);
                checkPgReplicationSlot(sourceDb, result);
                checkPgWalSenders(sourceDb, result, true);
                checkPgReplicationPermissions(sourceDb, result, true);
                checkPgReplicaIdentity(sourceDb, result, true);
                checkPgForeignKeys(sourceDb, result);
            } else if (sourceIsOracle) {
                checkOracleVersionSupported(sourceDb, result);
                checkOracleLogMinerConfig(sourceDb, result);
                checkOracleArchiveLog(sourceDb, result);
                checkOracleForeignKeys(sourceDb, result);
            } else {
                checkBinlogEnabled(sourceDb, result);
                checkBinlogFormat(sourceDb, result);
                checkBinlogRowImage(sourceDb, result);
                    checkBinlogRowValueOptions(sourceDb, result);
                    checkBinlogRowMetadata(sourceDb, result);
                String sourceVersion = getMySQLVersion(sourceDb);
                checkSourceVersionSupported(sourceVersion, result);
                checkServerId(sourceDb, sourceVersion, result);
                checkStorageEngine(sourceDb, result);
                checkForeignKeyConstraints(sourceDb, result);
            }

            checkSubscribePermissions(sourceDb, sourceIsPg, sourceIsOracle, result);

        } catch (SQLException e) {
            logger.error("订阅校验失败: {}", e.getMessage());
            result.addItem("源库连接", "源数据库连接检查", false,
                    "连接失败: " + e.getMessage(), "error");
        }

        finalizeAllPassed(result);

        return result;
    }

    private void checkOracleLogMinerConfig(Connection conn, ValidationResult result) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT log_mode FROM v$database")) {
                if (rs.next()) {
                    String logMode = rs.getString(1);
                    if ("ARCHIVELOG".equalsIgnoreCase(logMode)) {
                        result.addItem("归档模式", "Oracle归档模式检查", true,
                                "数据库处于ARCHIVELOG模式，支持LogMiner", "info");
                    } else {
                        result.addItem("归档模式", "Oracle归档模式检查", false,
                                "数据库处于" + logMode + "模式，需切换到ARCHIVELOG模式才能使用LogMiner", "error");
                    }
                }
            }
        } catch (SQLException e) {
            result.addItem("归档模式", "Oracle归档模式检查", true,
                    "无法检查归档模式（可能权限不足）: " + e.getMessage(), "warning");
        }
    }

    private void checkOracleArchiveLog(Connection conn, ValidationResult result) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM v$archived_log WHERE archived = 'YES'")) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    result.addItem("归档日志", "Oracle归档日志可用性检查", true,
                            "检测到 " + count + " 个归档日志", "info");
                }
            }
        } catch (SQLException e) {
            result.addItem("归档日志", "Oracle归档日志可用性检查", true,
                    "无法检查归档日志（可能权限不足）: " + e.getMessage(), "warning");
        }
    }

    private void checkPgReplicationSlot(Connection conn, ValidationResult result) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT slot_name, plugin, slot_type FROM pg_replication_slots WHERE slot_type = 'logical' LIMIT 5")) {
                java.util.List<String> slots = new java.util.ArrayList<>();
                while (rs.next()) {
                    slots.add(rs.getString("slot_name") + "(" + rs.getString("plugin") + ")");
                }
                if (!slots.isEmpty()) {
                    result.addItem("复制槽", "PostgreSQL逻辑复制槽检查", true,
                            "检测到逻辑复制槽: " + String.join(", ", slots), "info");
                } else {
                    result.addItem("复制槽", "PostgreSQL逻辑复制槽检查", true,
                            "暂无逻辑复制槽，订阅任务将自动创建", "info");
                }
            }
        } catch (SQLException e) {
            result.addItem("复制槽", "PostgreSQL逻辑复制槽检查", true,
                    "无法检查复制槽（可能权限不足）: " + e.getMessage(), "warning");
        }
    }

    private void checkSubscribePermissions(Connection conn, boolean isPg, boolean isOracle, ValidationResult result) {
        try {
            if (isPg) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT has_database_privilege(current_database(), 'connect') AND " +
                         "has_database_privilege(current_database(), 'usage')")) {
                    if (rs.next() && rs.getBoolean(1)) {
                        result.addItem("源库权限", "订阅所需源库权限检查", true,
                                "当前用户具有源库连接和使用权限", "info");
                    } else {
                        result.addItem("源库权限", "订阅所需源库权限检查", false,
                                "当前用户缺少源库连接或使用权限", "error");
                    }
                }
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT pg_is_in_recovery()")) {
                    if (rs.next() && rs.getBoolean(1)) {
                        result.addItem("主从角色", "源库主从角色检查", false,
                                "当前连接的是PostgreSQL从库，无法进行逻辑复制", "error");
                    }
                }
            } else if (isOracle) {
                // Oracle: 检查执行 DBMS_LOGMNR 所需的权限
                boolean hasExecuteCatalogRole = false;
                boolean hasExecuteAnyProcedure = false;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT granted_role FROM user_role_privs WHERE granted_role IN ('EXECUTE_CATALOG_ROLE','DBA')")) {
                    while (rs.next()) {
                        String role = rs.getString(1);
                        if ("EXECUTE_CATALOG_ROLE".equalsIgnoreCase(role) || "DBA".equalsIgnoreCase(role)) {
                            hasExecuteCatalogRole = true;
                        }
                    }
                }
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT privilege FROM user_sys_privs WHERE privilege = 'EXECUTE ANY PROCEDURE'")) {
                    if (rs.next()) {
                        hasExecuteAnyProcedure = true;
                    }
                } catch (SQLException ignored) {}

                if (hasExecuteCatalogRole || hasExecuteAnyProcedure) {
                    result.addItem("源库权限", "LogMiner订阅所需权限检查", true,
                            "当前用户具有执行DBMS_LOGMNR所需权限", "info");
                } else {
                    result.addItem("源库权限", "LogMiner订阅所需权限检查", false,
                            "当前用户缺少EXECUTE CATALOG ROLE或EXECUTE ANY PROCEDURE权限，无法执行LogMiner", "error");
                }

                // 检查是否为主库（非备库）
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT database_role FROM v$database")) {
                    if (rs.next()) {
                        String role = rs.getString(1);
                        if (!"PRIMARY".equalsIgnoreCase(role)) {
                            result.addItem("主从角色", "源库主从角色检查", false,
                                    "当前连接的是Oracle " + role + "，建议连接到PRIMARY数据库进行LogMiner", "warning");
                        }
                    }
                } catch (SQLException ignored) {}
            } else {
                // MySQL 8.x 使用 Super_priv/Repl_slave_priv/Reload_priv，5.x 使用 SUPER/REPLICATION_SLAVE/RELOAD
                boolean hasRepl = false;
                boolean hasReload = false;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT Super_priv, Repl_slave_priv, Reload_priv FROM mysql.user WHERE user = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)")) {
                    if (rs.next()) {
                        hasRepl = "Y".equalsIgnoreCase(rs.getString("Repl_slave_priv"));
                        hasReload = "Y".equalsIgnoreCase(rs.getString("Reload_priv"));
                    }
                } catch (SQLException e8) {
                    // fallback for MySQL 5.x
                    try (Statement stmt2 = conn.createStatement();
                         ResultSet rs2 = stmt2.executeQuery(
                             "SELECT SUPER, REPLICATION_SLAVE, RELOAD FROM mysql.user WHERE user = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)")) {
                        if (rs2.next()) {
                            hasRepl = rs2.getBoolean("REPLICATION_SLAVE");
                            hasReload = rs2.getBoolean("RELOAD");
                        }
                    }
                }
                if (hasRepl) {
                    result.addItem("源库权限", "Binlog订阅所需权限检查", true,
                            "当前用户具有REPLICATION SLAVE权限", "info");
                } else {
                    result.addItem("源库权限", "Binlog订阅所需权限检查", false,
                            "当前用户缺少REPLICATION SLAVE权限，无法读取Binlog", "error");
                }
                if (!hasReload) {
                    result.addItem("RELOAD权限", "Binlog订阅建议具有RELOAD权限", false,
                            "当前用户缺少RELOAD权限，可能无法刷新Binlog状态", "warning");
                }
            }
        } catch (SQLException e) {
            result.addItem("源库权限", "订阅所需源库权限检查", true,
                    "无法检查权限（非致命）: " + e.getMessage(), "warning");
        }
    }

    public ValidationResult validateForMigration(String sourceConnection, String targetConnection,
                                                  String migrationMode, String sourceType, String targetType) {
        return validateForMigration(sourceConnection, targetConnection, migrationMode, sourceType, targetType, null);
    }

    public ValidationResult validateForMigration(String sourceConnection, String targetConnection,
                                                  String migrationMode, String sourceType, String targetType,
                                                  String drMode) {
        ValidationResult result = new ValidationResult();

        boolean sourceIsMongo = "mongodb".equalsIgnoreCase(sourceType);
        boolean targetIsMongo = "mongodb".equalsIgnoreCase(targetType);
        if (sourceIsMongo || targetIsMongo) {
            return validateForMongoMigration(sourceConnection, targetConnection, migrationMode,
                    sourceIsMongo, targetIsMongo, drMode, result);
        }

        boolean sourceIsEs = "elasticsearch".equalsIgnoreCase(sourceType);
        boolean targetIsEs = "elasticsearch".equalsIgnoreCase(targetType);
        if (sourceIsEs || targetIsEs) {
            return validateForElasticMigration(sourceConnection, targetConnection, migrationMode,
                    sourceIsEs, sourceType, result);
        }

        boolean sourceIsRedis = "redis".equalsIgnoreCase(sourceType);
        boolean targetIsRedis = "redis".equalsIgnoreCase(targetType);
        if (sourceIsRedis || targetIsRedis) {
            return validateForRedisMigration(sourceConnection, targetConnection, migrationMode,
                    sourceIsRedis, targetIsRedis, result);
        }

        boolean sourceIsPg = "postgresql".equalsIgnoreCase(sourceType);
        boolean sourceIsOracle = "oracle".equalsIgnoreCase(sourceType);
        // TiDB 讲 MySQL 协议（连接串同为 mysql://，JDBC 走 MySQL 驱动），但增量出口是 TiCDC
        // 而不是 binlog——必须从 bothMysql 里摘出来单独校验，否则会去查 log_bin/binlog_format，
        // 在 TiDB 上恒为关闭，把本来正常的任务误判成不可创建。
        boolean sourceIsTidb = "tidb".equalsIgnoreCase(sourceType);
        boolean targetIsPg = "postgresql".equalsIgnoreCase(targetType);
        boolean bothMysql = !sourceIsPg && !sourceIsOracle && !sourceIsTidb && !targetIsPg;
        boolean tidbToMysql = sourceIsTidb && !targetIsPg;
        boolean mysqlToPg = !sourceIsPg && !sourceIsOracle && !sourceIsTidb && targetIsPg;
        boolean pgToMysql = sourceIsPg && !targetIsPg;
        boolean bothPg = sourceIsPg && targetIsPg;
        boolean oracleToPg = sourceIsOracle && targetIsPg;
        boolean bidirectional = "BIDIRECTIONAL".equalsIgnoreCase(drMode);
        boolean isDr = "BIDIRECTIONAL".equalsIgnoreCase(drMode) || "UNIDIRECTIONAL".equalsIgnoreCase(drMode);

        boolean needIncrement = "fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode);

        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);

        // 灾备任务（单向/双向）源库与目标库必须是不同实例：同一实例做灾备无隔离意义，实例故障源/目标一起丢。
        // 预连接强制拦截（error 阻断启动），不因能否连通而放行。
        if (isDr && sameInstance(sourceConn, targetConn)) {
            result.addItem("灾备源目标隔离", "灾备任务源库与目标库必须是不同的数据库实例", false,
                    "源库与目标库指向同一实例（" + sourceConn.host + ":" + sourceConn.port
                            + "），灾备任务要求源库与目标库为不同的 " + (sourceIsPg ? "PostgreSQL" : "MySQL") + " 实例",
                    "error");
        }

        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn),
                sourceConn.username, sourceConn.password, driverProps(sourceConn));
             Connection targetDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(targetConn),
                targetConn.username, targetConn.password, driverProps(targetConn))) {

            String sourceLabel = sourceIsPg ? "PostgreSQL"
                    : (sourceIsOracle ? "Oracle" : (sourceIsTidb ? "TiDB" : "MySQL"));
            result.addItem("源库连接", "源数据库连接检查", true,
                sourceLabel + "源库连接成功", "info");
            result.addItem("目标库连接", "目标数据库连接检查", true,
                targetIsPg ? "PostgreSQL目标库连接成功" : "MySQL目标库连接成功", "info");

            if (bothMysql) {
                String sourceVersion = getMySQLVersion(sourceDb);
                String targetVersion = getMySQLVersion(targetDb);

                checkSourceVersionSupported(sourceVersion, result);

                if (needIncrement) {
                    checkBinlogEnabled(sourceDb, result);
                    checkBinlogFormat(sourceDb, result);
                    checkBinlogRowImage(sourceDb, result);
                    checkBinlogRowValueOptions(sourceDb, result);
                    checkBinlogRowMetadata(sourceDb, result);
                    checkServerId(sourceDb, sourceVersion, result);
                }

                checkVersionCompatibility(sourceVersion, targetVersion, result);
                checkStorageEngine(sourceDb, result);
                checkForeignKeyConstraints(sourceDb, result);
                checkSqlModeCompatibility(sourceDb, targetDb, result);
                checkSourcePermissions(sourceDb, migrationMode, result);
                checkTargetPermissions(targetDb, targetVersion, result);

                // 双向灾备：反向通道 B→A 的 capture 跑在目标库 B，B 也需开启 binlog；且源/目标 server_id 须不同。
                if (bidirectional && needIncrement) {
                    checkBidirectionalTargetMysql(targetDb, result);
                    checkServerIdDistinct(sourceDb, targetDb, result);
                }
            }

            if (tidbToMysql) {
                // TiDB → MySQL：版本 / TiCDC 服务（增量前提）/ 目标端兼容性与权限。
                // 不查 binlog 相关变量——TiDB 没有 binlog，增量由 TiCDC changefeed 承担。
                String sourceVersion = getMySQLVersion(sourceDb);
                String targetVersion = getMySQLVersion(targetDb);
                checkTidbSourceVersion(sourceVersion, result);
                if (needIncrement) {
                    checkTicdcService(result);
                }
                checkStorageEngine(sourceDb, result);
                checkForeignKeyConstraints(sourceDb, result);
                checkSqlModeCompatibility(sourceDb, targetDb, result);
                checkTidbSourcePermissions(sourceDb, migrationMode, result);
                checkTargetPermissions(targetDb, targetVersion, result);
            }

            if (mysqlToPg) {
                String sourceVersion = getMySQLVersion(sourceDb);
                checkSourceVersionSupported(sourceVersion, result);
                if (needIncrement) {
                    checkBinlogEnabled(sourceDb, result);
                    checkBinlogFormat(sourceDb, result);
                    checkBinlogRowImage(sourceDb, result);
                    checkBinlogRowValueOptions(sourceDb, result);
                    checkBinlogRowMetadata(sourceDb, result);
                    checkServerId(sourceDb, sourceVersion, result);
                }
                checkStorageEngine(sourceDb, result);
                checkForeignKeyConstraints(sourceDb, result);
                checkSourcePermissions(sourceDb, migrationMode, result);
            }

            if (pgToMysql) {
                // PG→MySQL: 源端 PG 版本号 + WAL 配置 + 复制权限/发送进程 + REPLICA IDENTITY + 外键
                checkPgSourceVersionSupported(sourceDb, result, needIncrement);
                checkPgWalConfig(sourceDb, result);
                checkPgWalSenders(sourceDb, result, needIncrement);
                checkPgReplicationPermissions(sourceDb, result, needIncrement);
                checkPgReplicaIdentity(sourceDb, result, needIncrement);
                checkPgForeignKeys(sourceDb, result);
            }

            if (bothPg) {
                // PG→PG（含单向/双向灾备）：源端 PG 版本号；增量捕获需要源端 WAL 为 logical。
                checkPgSourceVersionSupported(sourceDb, result, needIncrement);
                if (needIncrement) {
                    checkPgWalConfig(sourceDb, result);
                    checkPgReplicationSlot(sourceDb, result);
                    checkPgWalSenders(sourceDb, result, needIncrement);
                    checkPgReplicationPermissions(sourceDb, result, needIncrement);
                    checkPgReplicaIdentity(sourceDb, result, needIncrement);
                    // 双向灾备：反向通道（B→A）的 capture 跑在目标库 B 上，故 B 也需 logical WAL。
                    if (bidirectional) {
                        checkPgWalConfigLabeled(targetDb, result, "目标库");
                    }
                }
                checkPgForeignKeys(sourceDb, result);
            }

            if (oracleToPg) {
                // Oracle→PG: 源端 Oracle 版本号 + LogMiner/归档日志配置 + 外键
                checkOracleVersionSupported(sourceDb, result);
                checkOracleLogMinerConfig(sourceDb, result, migrationMode);
                checkOracleForeignKeys(sourceDb, result);
            }

        } catch (SQLException e) {
            logger.error("数据库校验失败: {}", e.getMessage());
            if (!result.getCheckItems().stream().anyMatch(item -> !item.isPassed())) {
                result.addItem("连接检查", "数据库连接检查", false, "连接失败: " + e.getMessage(), "error");
            }
        }

        finalizeAllPassed(result);

        return result;
    }

    /**
     * MySQL → Elasticsearch 同步预校验。
     * 检查项：类型组合（ES 仅可作目标、源必须 MySQL）、源库连接、增量所需 binlog 配置
     * （复用 mysql→mysql 的三项检查）、目标 ES 连接/集群健康/版本。
     */
    private ValidationResult validateForElasticMigration(String sourceConnection, String targetConnection,
                                                         String migrationMode, boolean sourceIsEs,
                                                         String sourceType, ValidationResult result) {
        if (sourceIsEs) {
            result.addItem("类型组合", "Elasticsearch 只能作为同步目标", false,
                    "Elasticsearch 不支持作为同步源，仅支持 MySQL 到 Elasticsearch", "error");
            result.setAllPassed(false);
            return result;
        }
        if (!"mysql".equalsIgnoreCase(sourceType)) {
            result.addItem("类型组合", "Elasticsearch 目标仅支持 MySQL 源", false,
                    "到 Elasticsearch 的同步目前仅支持 MySQL 源（binlog 增量捕获）", "error");
            result.setAllPassed(false);
            return result;
        }

        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);
        if (!targetConn.isElastic()) {
            result.addItem("连接串格式", "Elasticsearch 连接串格式检查", false,
                    "目标连接串应以 elastic:// 开头", "error");
            result.setAllPassed(false);
            return result;
        }

        // 源库：连接 + 增量所需 binlog 配置（与 mysql→mysql 相同的三项检查）
        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn), sourceConn.username, sourceConn.password, driverProps(sourceConn))) {
            result.addItem("源库连接", "源数据库连接检查", true, "MySQL源库连接成功", "info");
            String sourceVersion = getMySQLVersion(sourceDb);
            checkSourceVersionSupported(sourceVersion, result);
            if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                checkBinlogEnabled(sourceDb, result);
                checkBinlogFormat(sourceDb, result);
                checkBinlogRowImage(sourceDb, result);
                    checkBinlogRowValueOptions(sourceDb, result);
                    checkBinlogRowMetadata(sourceDb, result);
                checkServerId(sourceDb, sourceVersion, result);
            }
            checkStorageEngine(sourceDb, result);
            checkForeignKeyConstraints(sourceDb, result);
            checkSourcePermissions(sourceDb, migrationMode, result);
        } catch (SQLException e) {
            logger.error("MySQL 源库校验失败: {}", e.getMessage());
            result.addItem("源库连接", "源数据库连接检查", false, "连接失败: " + e.getMessage(), "error");
        }

        // 目标 ES：连接 + 集群健康 + 版本
        try {
            String[] info = esHttpGet(targetConn, "/");
            int status = Integer.parseInt(info[0]);
            if (status == 401 || status == 403) {
                result.addItem("目标库连接", "目标Elasticsearch连接检查", false,
                        "认证失败：用户名或密码错误", "error");
            } else if (status >= 300) {
                result.addItem("目标库连接", "目标Elasticsearch连接检查", false,
                        "连接失败: HTTP " + status, "error");
            } else {
                result.addItem("目标库连接", "目标Elasticsearch连接检查", true,
                        "Elasticsearch目标连接成功", "info");

                String version = null;
                try {
                    version = com.google.gson.JsonParser.parseString(info[1]).getAsJsonObject()
                            .getAsJsonObject("version").get("number").getAsString();
                } catch (Exception ignore) {
                    // 版本信息缺失不阻塞
                }
                result.addItem("目标库版本", "目标Elasticsearch版本检查", true,
                        version != null ? "Elasticsearch " + version : "无法获取版本信息（非致命）",
                        version != null ? "info" : "warning");

                try {
                    String[] health = esHttpGet(targetConn, "/_cluster/health");
                    String clusterStatus = com.google.gson.JsonParser.parseString(health[1])
                            .getAsJsonObject().get("status").getAsString();
                    boolean healthy = !"red".equalsIgnoreCase(clusterStatus);
                    result.addItem("目标集群健康", "目标Elasticsearch集群健康检查", healthy,
                            "集群状态: " + clusterStatus + (healthy ? "" : "，red 状态无法可靠写入"),
                            healthy ? "info" : "error");
                } catch (Exception e) {
                    result.addItem("目标集群健康", "目标Elasticsearch集群健康检查", true,
                            "无法获取集群健康（非致命）: " + e.getMessage(), "warning");
                }
            }
        } catch (Exception e) {
            logger.error("Elasticsearch 目标校验失败: {}", e.getMessage());
            result.addItem("目标库连接", "目标Elasticsearch连接检查", false,
                    "连接失败: " + e.getMessage(), "error");
        }

        finalizeAllPassed(result);
        return result;
    }

    /**
     * MongoDB 同步预校验：仅支持 mongodb→mongodb（副本集到副本集）。
     * 检查项：类型组合互斥、源/目标连接、源/目标副本集（Change Streams 依赖副本集 oplog）、
     * 目标节点可写（Primary）、服务器版本。
     */
    private ValidationResult validateForMongoMigration(String sourceConnection, String targetConnection,
                                                       String migrationMode,
                                                       boolean sourceIsMongo, boolean targetIsMongo,
                                                       String drMode, ValidationResult result) {
        if (!sourceIsMongo || !targetIsMongo) {
            result.addItem("类型组合", "MongoDB 仅支持 MongoDB 到 MongoDB 的同步", false,
                    "MongoDB 只能与 MongoDB 互相同步，不支持与其它数据库类型组合", "error");
            result.setAllPassed(false);
            return result;
        }

        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);
        if (!sourceConn.isMongo() || !targetConn.isMongo()) {
            result.addItem("连接串格式", "MongoDB 连接串格式检查", false,
                    "连接串应以 mongodb:// 开头", "error");
            result.setAllPassed(false);
            return result;
        }

        boolean bidirectional = "BIDIRECTIONAL".equalsIgnoreCase(drMode);
        boolean isDr = bidirectional || "UNIDIRECTIONAL".equalsIgnoreCase(drMode);

        // 与 MySQL/PG 灾备同一条铁律：源与目标必须是不同实例，否则实例故障时两端一起丢。
        if (isDr && sameInstance(sourceConn, targetConn)) {
            result.addItem("灾备源目标隔离", "灾备任务源库与目标库必须是不同的数据库实例", false,
                    "源库与目标库指向同一实例（" + sourceConn.host + ":" + sourceConn.port
                            + "），灾备任务要求源库与目标库为不同的 MongoDB 副本集实例", "error");
        }

        checkMongoEndpoint(sourceConn, "源库", true, migrationMode, result);
        checkMongoEndpoint(targetConn, "目标库", false, migrationMode, result);

        // 双向灾备：反向通道 B→A 把 A 当写入目标，A 必须是可写 Primary；
        // 同时防回环依赖多文档事务写 origin 标记，两端都必须是副本集/分片集群（4.0+）。
        if (bidirectional) {
            checkMongoBidirectionalSourceWritable(sourceConn, result);
        }

        finalizeAllPassed(result);
        return result;
    }

    /**
     * 双向灾备补充项：源库 A 也会被反向通道写入，必须是可写 Primary。
     * 单向灾备下 A 只读不写，故此项仅在 BIDIRECTIONAL 时检查。
     */
    private void checkMongoBidirectionalSourceWritable(ParsedConnection conn, ValidationResult result) {
        try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
            org.bson.Document hello = mongoHello(client);
            Boolean writable = hello.getBoolean("isWritablePrimary");
            if (writable == null) {
                writable = hello.getBoolean("ismaster");
            }
            boolean isWritable = Boolean.TRUE.equals(writable);
            result.addItem("源库可写（双向）", "双向灾备下源库同时是反向通道的写入目标", isWritable,
                    isWritable ? "源节点为 Primary，可接收反向通道写入"
                            : "源节点不可写（非 Primary）；双向灾备两端都会被写入，请连接副本集 Primary 节点",
                    isWritable ? "info" : "error");
        } catch (Exception e) {
            result.addItem("源库可写（双向）", "双向灾备下源库同时是反向通道的写入目标", false,
                    "检查失败: " + e.getMessage(), "error");
        }
    }

    /**
     * Redis → Redis 同步预校验。检查类型组合（仅 Redis↔Redis）、源/目标连接（PING），
     * 增量说明（PSYNC 复制流，源库默认允许复制），以及目标可写（非只读副本）。
     */
    private ValidationResult validateForRedisMigration(String sourceConnection, String targetConnection,
                                                       String migrationMode,
                                                       boolean sourceIsRedis, boolean targetIsRedis,
                                                       ValidationResult result) {
        if (!sourceIsRedis || !targetIsRedis) {
            result.addItem("类型组合", "Redis 仅支持 Redis 到 Redis 的同步", false,
                    "Redis 只能与 Redis 互相同步，不支持与其它数据库类型组合", "error");
            result.setAllPassed(false);
            return result;
        }

        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);
        if (!sourceConn.isRedis() || !targetConn.isRedis()) {
            result.addItem("连接串格式", "Redis 连接串格式检查", false,
                    "连接串应以 redis:// 开头", "error");
            result.setAllPassed(false);
            return result;
        }

        boolean needIncrement = "fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode);
        checkRedisEndpoint(sourceConn, "源库", true, needIncrement, result);
        checkRedisEndpoint(targetConn, "目标库", false, needIncrement, result);

        finalizeAllPassed(result);
        return result;
    }

    private void checkRedisEndpoint(ParsedConnection conn, String side, boolean isSource,
                                    boolean needIncrement, ValidationResult result) {
        try (redis.clients.jedis.Jedis jedis = buildJedis(conn)) {
            String pong = jedis.ping();
            if (pong == null || !"PONG".equalsIgnoreCase(pong)) {
                result.addItem(side + "连接", side + "Redis连接检查", false,
                        side + " PING 未返回 PONG", "error");
                return;
            }
            result.addItem(side + "连接", side + "Redis连接检查", true,
                    "Redis" + side + "连接成功", "info");

            // 解析 INFO replication 的 role（master / slave）
            String role = "master";
            try {
                for (String line : jedis.info("replication").split("\\r?\\n")) {
                    if (line.startsWith("role:")) {
                        role = line.substring("role:".length()).trim();
                        break;
                    }
                }
            } catch (Exception ignore) {
                // role 解析失败不阻断（按 master 处理）
            }

            if (isSource) {
                if (needIncrement) {
                    result.addItem("增量方式", "Redis 增量走 PSYNC 复制流", true,
                            "增量同步通过 PSYNC 复制协议实现（RDB 全量 + 命令流增量），源库默认允许复制，无需额外配置",
                            "info");
                }
            } else {
                // 目标需可写：目标若本身是某主库的只读副本，RESTORE/写入会被拒绝。
                boolean writable = !"slave".equalsIgnoreCase(role) && !"replica".equalsIgnoreCase(role);
                result.addItem("目标库可写", "目标 Redis 需为可写（非只读副本）", writable,
                        writable ? "目标 Redis 可写入"
                                : "目标 Redis 当前为副本（replica），只读，无法写入；请使用主库作为目标",
                        writable ? "info" : "error");
            }
        } catch (Exception e) {
            result.addItem(side + "连接", side + "Redis连接检查", false,
                    side + "连接失败: " + e.getMessage(), "error");
        }
    }

    private void checkMongoEndpoint(ParsedConnection conn, String side, boolean isSource,
                                    String migrationMode, ValidationResult result) {
        boolean needIncrement = "fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode);

        try (com.mongodb.client.MongoClient client = buildMongoClient(conn)) {
            org.bson.Document hello = mongoHello(client);
            result.addItem(side + "连接", side + "MongoDB连接检查", true,
                    "MongoDB" + side + "连接成功", "info");

            // 账号权限：源端 Change Streams 需读权限、目标端需写权限
            checkMongoPermissions(client, side, result);

            // 副本集/分片集群检查：源端 Change Streams 依赖副本集 oplog；分片集群经 mongos
            // 打开的 change stream 会聚合全部 shard 的写入（官方集群级 CDC 机制），等同通过。
            // 分片集群请直接填 mongos 地址，勿逐个 shard 直连——那会读到 chunk 迁移的孤儿文档。
            String setName = hello.getString("setName");
            boolean isReplicaSet = setName != null && !setName.isEmpty();
            boolean isSharded = isMongos(hello);
            boolean streamCapable = isReplicaSet || isSharded;
            String topology = isSharded ? "分片集群（mongos）"
                    : (isReplicaSet ? "副本集（" + setName + "）" : "独立节点");
            if (isSource) {
                boolean passed = streamCapable || !needIncrement;
                String message = streamCapable
                        ? "源库为" + topology + "，支持 Change Streams 增量捕获"
                        : "源库为独立节点" + (needIncrement ? "，增量同步（Change Streams）需要副本集或分片集群（mongos）" : "，仅全量可运行，建议使用副本集");
                result.addItem("源库副本集", "增量同步需要源库为副本集或分片集群", passed, message,
                        needIncrement && !streamCapable ? "error" : (streamCapable ? "info" : "warning"));
            } else {
                String message = streamCapable
                        ? "目标库为" + topology
                        : "目标库为独立节点，本功能定义为副本集/分片集群间同步，建议使用副本集或 mongos";
                result.addItem("目标库副本集", "目标库应为副本集或分片集群", streamCapable, message,
                        streamCapable ? "info" : "error");

                // 目标可写：directConnection 连的节点必须是 Primary 才能写入
                Boolean writable = hello.getBoolean("isWritablePrimary");
                if (writable == null) {
                    writable = hello.getBoolean("ismaster");
                }
                boolean isWritable = Boolean.TRUE.equals(writable);
                result.addItem("目标库可写", "目标节点需为 Primary（可写）", isWritable,
                        isWritable ? "目标节点为 Primary，可写入" : "目标节点不可写（非 Primary），请连接副本集 Primary 节点",
                        isWritable ? "info" : "error");
            }

            // 版本检查：Change Streams 需要 4.0+（resumeAfter 恢复语义稳定）
            try {
                org.bson.Document buildInfo = client.getDatabase("admin")
                        .runCommand(new org.bson.Document("buildInfo", 1));
                String version = buildInfo.getString("version");
                boolean versionOk = true;
                if (version != null && isSource && needIncrement) {
                    try {
                        int major = Integer.parseInt(version.split("\\.")[0]);
                        versionOk = major >= 4;
                    } catch (NumberFormatException ignore) {
                        // 无法解析版本号时不阻塞
                    }
                }
                result.addItem(side + "版本", side + "MongoDB版本检查", versionOk,
                        "MongoDB " + version + (versionOk ? "" : "，增量同步（Change Streams）需要 4.0 及以上版本"),
                        versionOk ? "info" : "error");
            } catch (Exception e) {
                result.addItem(side + "版本", side + "MongoDB版本检查", true,
                        "无法获取版本信息（非致命）: " + e.getMessage(), "warning");
            }
        } catch (Exception e) {
            logger.error("MongoDB {} 校验失败: {}", side, e.getMessage());
            result.addItem(side + "连接", side + "MongoDB连接检查", false,
                    "连接失败: " + e.getMessage(), "error");
        }
    }

    /**
     * 检查 Oracle 源库的 LogMiner/归档日志配置（增量同步所需）。
     */
    private void checkOracleLogMinerConfig(Connection conn, ValidationResult result, String migrationMode) {
        boolean needIncrement = "fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode);

        // 1. 检查归档日志模式（增量同步需要 ARCHIVELOG）
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT LOG_MODE FROM V$DATABASE")) {
            if (rs.next()) {
                String logMode = rs.getString(1);
                boolean passed = !needIncrement || "ARCHIVELOG".equalsIgnoreCase(logMode);
                String message = "ARCHIVELOG".equalsIgnoreCase(logMode)
                        ? "数据库处于 ARCHIVELOG 模式，支持 LogMiner 增量捕获"
                        : "数据库处于 " + logMode + " 模式" + (needIncrement ? "，增量同步需要 ARCHIVELOG 模式" : "");
                result.addItem("归档日志模式", "增量同步需要源数据库处于 ARCHIVELOG 模式", passed, message, needIncrement ? "error" : "info");
            }
        } catch (SQLException e) {
            result.addItem("归档日志模式", "增量同步需要源数据库处于 ARCHIVELOG 模式", false,
                "检查失败: " + e.getMessage(), "warning");
        }

        if (!needIncrement) {
            return;
        }

        // 2. 检查补充日志（LogMiner 需要 PRIMARY KEY 补充日志才能识别 PK 列）
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT SUPPLEMENTAL_LOG_DATA_PK, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE")) {
            if (rs.next()) {
                String pkLog = rs.getString(1);
                String allLog = rs.getString(2);
                boolean pkEnabled = "YES".equalsIgnoreCase(pkLog);
                boolean allEnabled = "YES".equalsIgnoreCase(allLog);
                boolean passed = pkEnabled || allEnabled;
                String message = passed
                        ? (allEnabled ? "已启用 ALL 补充日志" : "已启用 PRIMARY KEY 补充日志")
                        : "未启用补充日志，增量同步需要至少启用 PRIMARY KEY 补充日志";
                result.addItem("补充日志", "增量同步需要源数据库启用补充日志", passed, message, "error");
            }
        } catch (SQLException e) {
            result.addItem("补充日志", "增量同步需要源数据库启用补充日志", false,
                "检查失败: " + e.getMessage(), "warning");
        }

        // 3. 检查当前用户是否有 LogMiner 相关权限（SELECT ANY TRANSACTION / EXECUTE ON DBMS_LOGMNR）
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT PRIVILEGE FROM USER_SYS_PRIVS WHERE PRIVILEGE IN ('SELECT ANY TRANSACTION','SELECT ANY DICTIONARY')")) {
            boolean hasSelectAnyTxn = false;
            boolean hasSelectAnyDict = false;
            while (rs.next()) {
                String priv = rs.getString(1);
                if ("SELECT ANY TRANSACTION".equalsIgnoreCase(priv)) hasSelectAnyTxn = true;
                if ("SELECT ANY DICTIONARY".equalsIgnoreCase(priv)) hasSelectAnyDict = true;
            }
            boolean passed = hasSelectAnyTxn || hasSelectAnyDict;
            String message = passed
                    ? "用户具备 LogMiner 所需权限(" + (hasSelectAnyTxn ? "SELECT ANY TRANSACTION" : "SELECT ANY DICTIONARY") + ")"
                    : "用户缺少 LogMiner 所需权限（SELECT ANY TRANSACTION 或 SELECT ANY DICTIONARY）";
            result.addItem("LogMiner权限", "增量同步需要 SELECT ANY TRANSACTION 或 SELECT ANY DICTIONARY 权限", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("LogMiner权限", "增量同步需要 LogMiner 相关权限", false,
                "检查失败: " + e.getMessage(), "warning");
        }
    }
    
    private void checkPgWalConfig(Connection conn, ValidationResult result) {
        checkPgWalConfigLabeled(conn, result, "源库");
    }

    /** WAL 级别检查（label 区分源库/目标库；双向灾备时目标库也需 logical WAL 供反向 capture）。 */
    private void checkPgWalConfigLabeled(Connection conn, ValidationResult result, String label) {
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
                if (rs.next()) {
                    String walLevel = rs.getString(1);
                    boolean passed = "logical".equalsIgnoreCase(walLevel);
                    String message = passed ? label + "WAL级别为logical，支持增量同步" :
                        label + "WAL级别为" + walLevel + "，增量同步需要设置为logical";
                    result.addItem(label + "WAL级别", "增量同步需要" + label + "WAL级别为logical", passed, message, "error");
                }
            }
        } catch (SQLException e) {
            result.addItem(label + "WAL级别", "增量同步需要" + label + "WAL级别为logical", false,
                "检查失败: " + e.getMessage(), "warning");
        }
    }
    
    private String buildJdbcUrl(ParsedConnection conn, boolean isPg) {
        if (isPg) {
            // 与另外两个重载一致：URL 只放标识符，stringtype 走 driverProps
            return com.synctask.util.JdbcConnections.postgresUrl(
                    conn.host, conn.port, conn.database);
        }
        return buildJdbcUrl(conn);
    }

    public ValidationResult validateForMigration(String sourceConnection, String targetConnection, String migrationMode) {
        ValidationResult result = new ValidationResult();
        
        ParsedConnection sourceConn = parseConnection(sourceConnection);
        ParsedConnection targetConn = parseConnection(targetConnection);
        
        try (Connection sourceDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(sourceConn),
                sourceConn.username, sourceConn.password, driverProps(sourceConn));
             Connection targetDb = DataSourcePoolManager.getConnection(
                buildJdbcUrl(targetConn),
                targetConn.username, targetConn.password, driverProps(targetConn))) {
            
            String sourceVersion = getMySQLVersion(sourceDb);
            String targetVersion = getMySQLVersion(targetDb);
            
            if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                checkBinlogEnabled(sourceDb, result);
                checkBinlogFormat(sourceDb, result);
                checkBinlogRowImage(sourceDb, result);
                    checkBinlogRowValueOptions(sourceDb, result);
                    checkBinlogRowMetadata(sourceDb, result);
                checkServerId(sourceDb, sourceVersion, result);
            }
            
            checkVersionCompatibility(sourceVersion, targetVersion, result);
            checkSqlModeCompatibility(sourceDb, targetDb, result);
            checkSourcePermissions(sourceDb, migrationMode, result);
            checkTargetPermissions(targetDb, targetVersion, result);
            
        } catch (SQLException e) {
            logger.error("数据库校验失败: {}", e.getMessage());
            result.addItem("连接检查", "数据库连接检查", false, "连接失败: " + e.getMessage(), "error");
        }
        
        finalizeAllPassed(result);
        
        return result;
    }

    private String getMySQLVersion(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT VERSION()")) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return "unknown";
    }

    private void checkBinlogEnabled(Connection conn, ValidationResult result) {
        try {
            String logBin = getVariable(conn, "log_bin");
            boolean passed = "ON".equalsIgnoreCase(logBin) || "1".equals(logBin);
            String message = passed ? "Binlog已开启" : "Binlog未开启，增量同步需要开启binlog";
            result.addItem("Binlog开启状态", "增量同步需要源数据库开启binlog", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("Binlog开启状态", "增量同步需要源数据库开启binlog", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkBinlogFormat(Connection conn, ValidationResult result) {
        try {
            String format = getVariable(conn, "binlog_format");
            boolean passed = "ROW".equalsIgnoreCase(format);
            String message = passed ? "Binlog格式为ROW" : "当前Binlog格式为" + format + "，需要设置为ROW";
            result.addItem("Binlog格式", "增量同步需要Binlog格式为ROW", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("Binlog格式", "增量同步需要Binlog格式为ROW", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkBinlogRowImage(Connection conn, ValidationResult result) {
        try {
            String rowImage = getVariable(conn, "binlog_row_image");
            if (rowImage == null || rowImage.isEmpty()) {
                result.addItem("Binlog Row Image", "binlog_row_image需设置为FULL", true, "参数不存在(可能是MySQL 5.5及以下版本)，跳过检查", "warning");
                return;
            }
            boolean passed = "FULL".equalsIgnoreCase(rowImage);
            String message = passed ? "binlog_row_image为FULL" : "当前binlog_row_image为" + rowImage + "，需要设置为FULL";
            result.addItem("Binlog Row Image", "binlog_row_image需设置为FULL", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("Binlog Row Image", "binlog_row_image需设置为FULL", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    /**
     * {@code binlog_row_value_options} 必须为空。
     *
     * <p>设成 {@code PARTIAL_JSON} 后，用 JSON_SET/JSON_REPLACE/JSON_REMOVE 改 JSON 列时，
     * MySQL 写的是<b>差量</b>——事件类型变成 {@code PARTIAL_UPDATE_ROWS_EVENT}，binlog 连接器
     * 不认识这个编码。实测这类 UPDATE 会整条消失（连同同一语句里改的其它普通列），
     * 现在抽取端会以 E3020 停下来上报，但更该做的是在任务开始前就拦住。
     *
     * <p>这和 Debezium / DTS / DMS 的口径一致：它们同样要求这个参数为空。
     */
    private void checkBinlogRowValueOptions(Connection conn, ValidationResult result) {
        try {
            String opts = getVariable(conn, "binlog_row_value_options");
            boolean passed = opts == null || opts.trim().isEmpty();
            String message = passed
                    ? (opts == null ? "参数不存在（MySQL 5.7 及以下）或为空" : "binlog_row_value_options 为空")
                    : "当前 binlog_row_value_options=" + opts + "，需要置空（SET GLOBAL binlog_row_value_options='')"
                      + "：该参数会让 JSON 列的 UPDATE 以差量形式写入 binlog，增量链路无法解析，"
                      + "这类 UPDATE 会整条丢失";
            result.addItem("Binlog JSON 差量", "binlog_row_value_options 需为空", passed, message, "error");
        } catch (SQLException e) {
            // 5.7 及以下没有这个参数，查不到即视为通过
            result.addItem("Binlog JSON 差量", "binlog_row_value_options 需为空", true,
                    "参数不存在（MySQL 5.7 及以下），跳过检查", "warning");
        }
    }

    /**
     * 建议 {@code binlog_row_metadata=FULL}（不阻断）。
     *
     * <p>MySQL 8.0.1+ 的这个参数会把<b>列名</b>放进 TABLE_MAP 事件本身。抽取端拿到的就是与行值
     * 同一时刻的列布局，不必再去查 {@code information_schema} 的<b>当前</b>定义——后者在链路有
     * 延迟又恰好做了 ALTER TABLE 时会让整行的值与列错位，写进去的是合法值、看不出异常。
     * 保持 MINIMAL 也能跑：抽取端会用事件里的列数兜底校验，对不上就停下来报 E3021，不会静默写坏。
     */
    private void checkBinlogRowMetadata(Connection conn, ValidationResult result) {
        try {
            String meta = getVariable(conn, "binlog_row_metadata");
            if (meta == null) {
                return;   // 5.7 及以下没这个参数，不提示
            }
            boolean full = "FULL".equalsIgnoreCase(meta.trim());
            result.addItem("Binlog 列元数据", "建议 binlog_row_metadata=FULL", full,
                    full ? "binlog_row_metadata=FULL，列名随事件下发"
                         : "当前 binlog_row_metadata=" + meta + "。建议设为 FULL：列名会随 binlog 事件一起"
                           + "下发，抽取端就不必依赖源库当前的表定义——同步有延迟时做 ALTER TABLE 加列/删列，"
                           + "按当前定义解析会让整行的值与列错位。保持 MINIMAL 不影响正常同步，"
                           + "但遇到这种情况任务会停下来报 E3021 而不是自动继续",
                    "warning");
        } catch (SQLException e) {
            // 读不到就不提示：这只是一条建议项
        }
    }

    private void checkServerId(Connection conn, String version, ValidationResult result) {
        try {
            String serverIdStr = getVariable(conn, "server_id");
            if (serverIdStr == null || serverIdStr.isEmpty() || "0".equals(serverIdStr)) {
                result.addItem("Server ID", "增量同步需要设置server_id", false, "server_id未设置或为0", "error");
                return;
            }
            
            long serverId = Long.parseLong(serverIdStr);
            boolean passed;
            String message;
            
            if (isVersionAtLeast(version, "5.7.0")) {
                passed = serverId >= 1 && serverId <= 4294967296L;
                message = passed ? "server_id=" + serverId + "，符合要求" : "server_id=" + serverId + "，MySQL 5.7+需要设置在1-4294967296之间";
            } else {
                passed = serverId >= 2 && serverId <= 4294967296L;
                message = passed ? "server_id=" + serverId + "，符合要求" : "server_id=" + serverId + "，MySQL 5.6及以下需要设置在2-4294967296之间";
            }
            
            result.addItem("Server ID", "增量同步需要设置server_id", passed, message, "error");
        } catch (Exception e) {
            result.addItem("Server ID", "增量同步需要设置server_id", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkVersionCompatibility(String sourceVersion, String targetVersion, ValidationResult result) {
        boolean passed = compareVersions(sourceVersion, targetVersion) <= 0;
        String message = passed 
            ? "源数据库版本(" + sourceVersion + ") <= 目标数据库版本(" + targetVersion + ")" 
            : "源数据库版本(" + sourceVersion + ") > 目标数据库版本(" + targetVersion + ")，可能导致兼容性问题";
        result.addItem("版本兼容性", "源数据库版本不能高于目标数据库版本", passed, message, "error");
    }

    private void checkSqlModeCompatibility(Connection sourceDb, Connection targetDb, ValidationResult result) {
        try {
            String sourceSqlMode = getVariable(sourceDb, "sql_mode");
            String targetSqlMode = getVariable(targetDb, "sql_mode");
            
            Set<String> sourceModes = parseSqlMode(sourceSqlMode);
            Set<String> targetModes = parseSqlMode(targetSqlMode);
            
            boolean passed = sourceModes.equals(targetModes);
            String message = passed 
                ? "sql_mode一致" 
                : "源数据库sql_mode(" + sourceSqlMode + ")与目标数据库(" + targetSqlMode + ")不一致";
            result.addItem("SQL Mode一致性", "源和目标数据库sql_mode需要一致", passed, message, "warning");
        } catch (SQLException e) {
            result.addItem("SQL Mode一致性", "源和目标数据库sql_mode需要一致", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkSourcePermissions(Connection conn, String migrationMode, ValidationResult result) {
        try {
            Set<String> grantedPrivileges = getGrantedPrivileges(conn);
            
            Set<String> requiredPrivileges = new HashSet<>(Arrays.asList("SELECT", "SHOW VIEW", "EVENT"));
            
            if ("fullAndIncre".equals(migrationMode) || "increment".equals(migrationMode)) {
                requiredPrivileges.addAll(Arrays.asList("LOCK TABLES", "REPLICATION SLAVE", "REPLICATION CLIENT"));
            }
            
            Set<String> missingPrivileges = new HashSet<>();
            for (String priv : requiredPrivileges) {
                boolean hasPriv = grantedPrivileges.contains(priv) || 
                                  grantedPrivileges.contains("ALL PRIVILEGES") ||
                                  grantedPrivileges.contains("ALL");
                if (!hasPriv) {
                    missingPrivileges.add(priv);
                }
            }
            
            boolean passed = missingPrivileges.isEmpty();
            String mode = "fullAndIncre".equals(migrationMode) ? "全量+增量" : "full".equals(migrationMode) ? "全量" : "增量";
            String message = passed 
                ? mode + "同步所需权限已具备" 
                : mode + "同步缺少权限: " + String.join(", ", missingPrivileges);
            result.addItem("源数据库权限", mode + "同步需要SELECT、SHOW VIEW、EVENT等权限", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("源数据库权限", "检查源数据库账号权限", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    private void checkTargetPermissions(Connection conn, String targetVersion, ValidationResult result) {
        try {
            Set<String> grantedPrivileges = getGrantedPrivileges(conn);
            
            Set<String> requiredPrivileges = new HashSet<>(Arrays.asList(
                "SELECT", "CREATE", "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", 
                "CREATE VIEW", "CREATE ROUTINE", "REFERENCES"
            ));
            
            if (isVersionInRange(targetVersion, "8.0.14", "8.0.18")) {
                requiredPrivileges.add("SESSION_VARIABLES_ADMIN");
            }
            
            Set<String> missingPrivileges = new HashSet<>();
            for (String priv : requiredPrivileges) {
                boolean hasPriv = grantedPrivileges.contains(priv) || 
                                  grantedPrivileges.contains("ALL PRIVILEGES") ||
                                  grantedPrivileges.contains("ALL");
                if (!hasPriv) {
                    missingPrivileges.add(priv);
                }
            }
            
            boolean passed = missingPrivileges.isEmpty();
            String message = passed 
                ? "目标数据库所需权限已具备" 
                : "目标数据库缺少权限: " + String.join(", ", missingPrivileges);
            result.addItem("目标数据库权限", "目标数据库需要SELECT、CREATE、DROP、INSERT、UPDATE、ALTER等权限", passed, message, "error");
        } catch (SQLException e) {
            result.addItem("目标数据库权限", "检查目标数据库账号权限", false, "检查失败: " + e.getMessage(), "error");
        }
    }

    // ==================== 新增预检项（对齐阿里云 DTS 预检查） ====================

    /**
     * 汇总校验结论：仅 error 级未通过项阻断启动（allPassed=false）；warning 级仅提示、不阻断。
     * 这样"存储引擎/外键约束/sql_mode 不一致"等提示项不会硬卡住可以正常运行的同步。
     */
    private void finalizeAllPassed(ValidationResult result) {
        boolean blocked = result.getCheckItems().stream()
                .anyMatch(i -> !i.isPassed() && "error".equalsIgnoreCase(i.getSeverity()));
        result.setAllPassed(!blocked);
    }

    /** 源库版本号检查（MySQL）：低于 5.6 判为不支持（error）；5.6.x 可用但建议升级（warning）；5.7+ 通过。 */
    private void checkSourceVersionSupported(String sourceVersion, ValidationResult result) {
        if (sourceVersion == null || sourceVersion.isEmpty() || "unknown".equalsIgnoreCase(sourceVersion)) {
            result.addItem("源库版本号", "源库 MySQL 版本需不低于 5.6", true,
                    "无法获取源库版本号，跳过版本检查", "warning");
            return;
        }
        if (!isVersionAtLeast(sourceVersion, "5.6.0")) {
            result.addItem("源库版本号", "源库 MySQL 版本需不低于 5.6", false,
                    "源库版本为 " + sourceVersion + "，低于最低支持版本 5.6，可能无法正常同步", "error");
        } else if (!isVersionAtLeast(sourceVersion, "5.7.0")) {
            result.addItem("源库版本号", "源库 MySQL 版本需不低于 5.6", false,
                    "源库版本为 " + sourceVersion + "，建议升级到 5.7 及以上以获得更稳定的增量捕获", "warning");
        } else {
            result.addItem("源库版本号", "源库 MySQL 版本需不低于 5.6", true,
                    "源库版本为 " + sourceVersion + "，受支持", "info");
        }
    }

    /**
     * TiDB 源库版本检查。TiDB 的 {@code VERSION()} 形如 {@code 8.0.11-TiDB-v8.5.0}：
     * 前半段是它兼容的 MySQL 协议版本，真正的内核版本在 {@code -TiDB-v} 之后。
     * TiCDC 的 canal-json + enable-tidb-extension（本链路增量所依赖的消息格式）在 6.5 起才稳定。
     */
    private void checkTidbSourceVersion(String versionString, ValidationResult result) {
        String desc = "源库需为 TiDB 6.5 及以上（TiCDC canal-json 增量前提）";
        if (versionString == null || versionString.isEmpty() || "unknown".equalsIgnoreCase(versionString)) {
            result.addItem("源库版本号", desc, true, "无法获取源库版本号，跳过版本检查", "warning");
            return;
        }
        Matcher m = Pattern.compile("TiDB-v(\\d+\\.\\d+\\.\\d+)", Pattern.CASE_INSENSITIVE).matcher(versionString);
        if (!m.find()) {
            result.addItem("源库版本号", desc, false,
                    "源库版本号 " + versionString + " 中未识别出 TiDB 版本，请确认所选源库类型与实际实例一致",
                    "warning");
            return;
        }
        String tidbVersion = m.group(1);
        if (!isVersionAtLeast(tidbVersion, "6.5.0")) {
            result.addItem("源库版本号", desc, false,
                    "源库为 TiDB " + tidbVersion + "，低于 6.5，TiCDC canal-json 增量可能不可用", "error");
        } else {
            result.addItem("源库版本号", desc, true, "源库为 TiDB " + tidbVersion + "，受支持", "info");
        }
    }

    /**
     * TiCDC 服务可达性检查（仅增量任务）。TiDB 不提供 binlog dump，增量完全依赖 TiCDC changefeed；
     * TiCDC 不可达时任务能建但增量永远没有数据，属于必须前置暴露的阻断项。
     */
    private void checkTicdcService(ValidationResult result) {
        String desc = "增量同步需要 TiCDC 服务可用（TiDB 无 binlog，增量走 changefeed）";
        String apiUrl = ticdcApiUrl == null ? "" : ticdcApiUrl.trim();
        while (apiUrl.endsWith("/")) {
            apiUrl = apiUrl.substring(0, apiUrl.length() - 1);
        }
        if (apiUrl.isEmpty()) {
            result.addItem("TiCDC 服务", desc, false, "未配置 TiCDC 地址（sync.ticdc.api-url）", "error");
            return;
        }
        java.net.HttpURLConnection conn = null;
        try {
            // TiCDC 不是 agent：协议由 sync.ticdc.api-url 自己决定（https 时用户自己写 https://），
            // 不能走 AgentHttpSupport —— 它会按 agent 的 TLS 开关把 http 改写成 https
            conn = (java.net.HttpURLConnection) new java.net.URL(apiUrl + "/api/v2/status").openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            int code = conn.getResponseCode();
            if (code == 200) {
                result.addItem("TiCDC 服务", desc, true, "TiCDC 服务可用: " + apiUrl, "info");
            } else {
                result.addItem("TiCDC 服务", desc, false,
                        "TiCDC 服务返回 HTTP " + code + " (" + apiUrl + ")", "error");
            }
        } catch (Exception e) {
            result.addItem("TiCDC 服务", desc, false,
                    "无法连接 TiCDC 服务 " + apiUrl + ": " + e.getMessage(), "error");
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * TiDB 源库权限检查。与 MySQL 源的差别：增量不经复制协议，因此不要求
     * REPLICATION SLAVE / REPLICATION CLIENT / LOCK TABLES——变更由 TiCDC 从 TiKV 直接拉取。
     */
    private void checkTidbSourcePermissions(Connection conn, String migrationMode, ValidationResult result) {
        String mode = "fullAndIncre".equals(migrationMode) ? "全量+增量"
                : "full".equals(migrationMode) ? "全量" : "增量";
        String desc = mode + "同步需要 SELECT、SHOW VIEW 等读取权限";
        try {
            Set<String> granted = getGrantedPrivileges(conn);
            Set<String> missing = new HashSet<>();
            for (String priv : Arrays.asList("SELECT", "SHOW VIEW")) {
                boolean has = granted.contains(priv) || granted.contains("ALL PRIVILEGES") || granted.contains("ALL");
                if (!has) {
                    missing.add(priv);
                }
            }
            boolean passed = missing.isEmpty();
            result.addItem("源数据库权限", desc, passed,
                    passed ? mode + "同步所需权限已具备" : mode + "同步缺少权限: " + String.join(", ", missing),
                    "error");
        } catch (SQLException e) {
            result.addItem("源数据库权限", desc, false, "检查失败: " + e.getMessage(), "error");
        }
    }

    /** PG 源库版本号检查：逻辑复制（增量）需要 PostgreSQL 9.4 及以上。 */
    private void checkPgSourceVersionSupported(Connection conn, ValidationResult result, boolean needIncrement) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW server_version")) {
            if (rs.next()) {
                String version = rs.getString(1);
                boolean ok = !needIncrement || isVersionAtLeast(version, "9.4.0");
                result.addItem("源库版本号", "PostgreSQL 逻辑复制需要 9.4 及以上", ok,
                        ok ? "源库版本为 PostgreSQL " + version + "，受支持"
                           : "源库版本为 PostgreSQL " + version + "，逻辑复制增量同步需要 9.4 及以上",
                        ok ? "info" : "error");
            }
        } catch (SQLException e) {
            result.addItem("源库版本号", "PostgreSQL 版本检查", true,
                    "版本检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    /** 存储引擎检查（MySQL 源）：非 InnoDB（尤其 MyISAM）缺事务/行级锁，增量同步一致性存在风险。 */
    private void checkStorageEngine(Connection conn, ValidationResult result) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT table_schema, table_name, engine FROM information_schema.tables " +
                "WHERE table_type = 'BASE TABLE' " +
                "AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys') " +
                "AND (engine IS NULL OR engine <> 'InnoDB') " +
                "ORDER BY table_schema, table_name LIMIT 50")) {
            try (ResultSet rs = ps.executeQuery()) {
                List<String> nonInnodb = new ArrayList<>();
                while (rs.next()) {
                    nonInnodb.add(rs.getString(1) + "." + rs.getString(2) + "(" + rs.getString(3) + ")");
                }
                if (nonInnodb.isEmpty()) {
                    result.addItem("存储引擎", "源库表建议使用 InnoDB 引擎", true,
                            "源库所有表均为 InnoDB 引擎", "info");
                } else {
                    String list = String.join(", ", nonInnodb.subList(0, Math.min(10, nonInnodb.size())));
                    if (nonInnodb.size() > 10) list += " 等";
                    result.addItem("存储引擎", "源库表建议使用 InnoDB 引擎", false,
                            "存在 " + nonInnodb.size() + " 个非 InnoDB 引擎表（MyISAM 等不支持事务/行级锁，增量一致性存在风险）: " + list,
                            "warning");
                }
            }
        } catch (SQLException e) {
            result.addItem("存储引擎", "源库表建议使用 InnoDB 引擎", true,
                    "存储引擎检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    /** 约束完整性检查（MySQL 源）：源库存在外键时提示——外键不随数据自动同步，父/子表写入顺序可能失败。 */
    private void checkForeignKeyConstraints(Connection conn, ValidationResult result) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT DISTINCT table_schema, table_name, referenced_table_schema, referenced_table_name " +
                "FROM information_schema.key_column_usage " +
                "WHERE referenced_table_name IS NOT NULL " +
                "AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys') " +
                "ORDER BY table_schema, table_name LIMIT 100")) {
            try (ResultSet rs = ps.executeQuery()) {
                List<String> fks = new ArrayList<>();
                while (rs.next()) {
                    fks.add(rs.getString(1) + "." + rs.getString(2) + " → "
                            + rs.getString(3) + "." + rs.getString(4));
                }
                if (fks.isEmpty()) {
                    result.addItem("约束完整性", "外键约束不会自动同步，需确认同步范围", true,
                            "源库无外键约束", "info");
                } else {
                    String list = String.join(", ", fks.subList(0, Math.min(8, fks.size())));
                    if (fks.size() > 8) list += " 等";
                    result.addItem("约束完整性", "外键约束不会自动同步，需确认同步范围", false,
                            "源库存在 " + fks.size() + " 个外键依赖，外键不会自动同步到目标；" +
                            "请确认同步范围已包含相关父表，避免父/子表写入顺序导致失败: " + list,
                            "warning");
                }
            }
        } catch (SQLException e) {
            result.addItem("约束完整性", "外键约束不会自动同步，需确认同步范围", true,
                    "约束完整性检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    // ---- Oracle 源库补充预检项 ----

    /** Oracle 源库版本号检查：LogMiner 增量捕获建议 11g 及以上。 */
    private void checkOracleVersionSupported(Connection conn, ValidationResult result) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT version FROM v$instance")) {
            if (rs.next()) {
                String v = rs.getString(1);
                boolean ok = isVersionAtLeast(v, "11.0.0.0");
                result.addItem("源库版本号", "Oracle LogMiner 建议 11g 及以上", ok,
                        ok ? "源库版本为 Oracle " + v + "，受支持"
                           : "源库版本为 Oracle " + v + "，LogMiner 增量捕获建议 11g 及以上", ok ? "info" : "warning");
            }
        } catch (SQLException e) {
            result.addItem("源库版本号", "Oracle 版本检查", true,
                    "版本检查失败（可能权限不足，非致命）: " + e.getMessage(), "warning");
        }
    }

    /** 约束完整性检查（Oracle 源，当前用户 schema）：外键不随数据自动同步，父/子表写入顺序可能失败。 */
    private void checkOracleForeignKeys(Connection conn, ValidationResult result) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM user_constraints WHERE constraint_type = 'R' AND status = 'ENABLED'")) {
            if (rs.next()) {
                int n = rs.getInt(1);
                if (n == 0) {
                    result.addItem("约束完整性", "外键约束不会自动同步，需确认同步范围", true,
                            "源库(当前用户)无启用的外键约束", "info");
                } else {
                    result.addItem("约束完整性", "外键约束不会自动同步，需确认同步范围", false,
                            "源库存在 " + n + " 个启用的外键约束，外键不会自动同步到目标；请确认同步范围已包含相关父表", "warning");
                }
            }
        } catch (SQLException e) {
            result.addItem("约束完整性", "外键约束检查", true,
                    "外键检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    // ---- PostgreSQL 源库补充预检项 ----

    /** PG 逻辑复制需要 max_wal_senders > 0。 */
    private void checkPgWalSenders(Connection conn, ValidationResult result, boolean needIncrement) {
        if (!needIncrement) return;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW max_wal_senders")) {
            if (rs.next()) {
                int n = 0;
                try { n = Integer.parseInt(rs.getString(1).trim()); } catch (Exception ignore) {}
                boolean ok = n > 0;
                result.addItem("WAL发送进程", "逻辑复制需要 max_wal_senders > 0", ok,
                        ok ? "max_wal_senders=" + n + "，满足逻辑复制" : "max_wal_senders=" + n + "，逻辑复制需要大于 0",
                        ok ? "info" : "error");
            }
        } catch (SQLException e) {
            result.addItem("WAL发送进程", "逻辑复制需要 max_wal_senders > 0", true,
                    "检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    /** PG 源库复制权限：逻辑复制需要 REPLICATION 属性或超级用户。 */
    private void checkPgReplicationPermissions(Connection conn, ValidationResult result, boolean needIncrement) {
        if (!needIncrement) return;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user")) {
            if (rs.next()) {
                boolean ok = rs.getBoolean(1);
                result.addItem("源库复制权限", "逻辑复制需要 REPLICATION 或超级用户权限", ok,
                        ok ? "当前用户具备逻辑复制所需权限（REPLICATION/superuser）"
                           : "当前用户缺少 REPLICATION 权限，无法建立逻辑复制", ok ? "info" : "error");
            }
        } catch (SQLException e) {
            result.addItem("源库复制权限", "逻辑复制需要 REPLICATION 权限", true,
                    "权限检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    /** 约束完整性检查（PG 源，排除系统 schema）。 */
    private void checkPgForeignKeys(Connection conn, ValidationResult result) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT count(*) FROM information_schema.table_constraints " +
                     "WHERE constraint_type = 'FOREIGN KEY' " +
                     "AND constraint_schema NOT IN ('pg_catalog','information_schema')")) {
            if (rs.next()) {
                int n = rs.getInt(1);
                if (n == 0) {
                    result.addItem("约束完整性", "外键约束不会自动同步，需确认同步范围", true, "源库无外键约束", "info");
                } else {
                    result.addItem("约束完整性", "外键约束不会自动同步，需确认同步范围", false,
                            "源库存在 " + n + " 个外键约束，外键不会自动同步到目标；请确认同步范围已包含相关父表", "warning");
                }
            }
        } catch (SQLException e) {
            result.addItem("约束完整性", "外键约束检查", true,
                    "外键检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    /** PG REPLICA IDENTITY 检查：无主键且 REPLICA IDENTITY 非 FULL 的表，逻辑复制 UPDATE/DELETE 无法定位行。 */
    private void checkPgReplicaIdentity(Connection conn, ValidationResult result, boolean needIncrement) {
        if (!needIncrement) return;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid " +
                     "WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') " +
                     "AND c.relreplident IN ('d','n') " +
                     "AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) " +
                     "ORDER BY 1,2 LIMIT 50")) {
            List<String> bad = new ArrayList<>();
            while (rs.next()) bad.add(rs.getString(1) + "." + rs.getString(2));
            if (bad.isEmpty()) {
                result.addItem("REPLICA IDENTITY", "增量 UPDATE/DELETE 需要主键或 REPLICA IDENTITY FULL", true,
                        "所有表均有主键或已配置 REPLICA IDENTITY", "info");
            } else {
                String list = String.join(", ", bad.subList(0, Math.min(10, bad.size())));
                if (bad.size() > 10) list += " 等";
                result.addItem("REPLICA IDENTITY", "增量 UPDATE/DELETE 需要主键或 REPLICA IDENTITY FULL", false,
                        "以下表无主键且 REPLICA IDENTITY 非 FULL，逻辑复制 UPDATE/DELETE 无法定位行（" + bad.size() + " 个）: " + list,
                        "warning");
            }
        } catch (SQLException e) {
            result.addItem("REPLICA IDENTITY", "增量 UPDATE/DELETE 需要主键或 REPLICA IDENTITY FULL", true,
                    "检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    // ---- MongoDB 源库补充预检项 ----

    /** Mongo 账号权限：源端 Change Streams 需读权限、目标端需写权限；无认证角色时告警。 */
    private void checkMongoPermissions(com.mongodb.client.MongoClient client, String side, ValidationResult result) {
        try {
            org.bson.Document status = client.getDatabase("admin")
                    .runCommand(new org.bson.Document("connectionStatus", 1));
            org.bson.Document authInfo = status.get("authInfo", org.bson.Document.class);
            java.util.List<?> roles = authInfo != null ? authInfo.getList("authenticatedUserRoles", Object.class) : null;
            if (roles == null || roles.isEmpty()) {
                result.addItem(side + "权限", side + "MongoDB 账号权限检查", false,
                        side + "未认证或无任何角色；源端 Change Streams 需读取权限、目标端需写入权限", "warning");
            } else {
                result.addItem(side + "权限", side + "MongoDB 账号权限检查", true,
                        side + "账号角色: " + roles, "info");
            }
        } catch (Exception e) {
            result.addItem(side + "权限", side + "MongoDB 账号权限检查", true,
                    side + "权限检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    // ---- 灾备（DR）源/目标实例隔离 ----

    /** 源/目标是否指向同一数据库实例（host 归一化后 host+port 相同即视为同实例）。 */
    private boolean sameInstance(ParsedConnection a, ParsedConnection b) {
        if (a == null || b == null) return false;
        return normHost(a.host).equals(normHost(b.host)) && a.port == b.port;
    }

    /** host 归一化：本机各写法（localhost/127.0.0.1/::1/0.0.0.0）视为同一主机。 */
    private String normHost(String h) {
        if (h == null) return "";
        String s = h.trim().toLowerCase();
        if (s.equals("localhost") || s.equals("127.0.0.1") || s.equals("::1") || s.equals("0.0.0.0")) {
            return "127.0.0.1";
        }
        return s;
    }

    // ---- 双向灾备（DR）补充预检项 ----

    /** 双向灾备 MySQL：反向通道 B→A 的 capture 跑在目标库 B，B 也需开启 binlog(ROW)。 */
    private void checkBidirectionalTargetMysql(Connection targetDb, ValidationResult result) {
        try {
            String logBin = getVariable(targetDb, "log_bin");
            boolean on = "ON".equalsIgnoreCase(logBin) || "1".equals(logBin);
            result.addItem("目标库Binlog", "双向灾备反向通道需要目标库开启 binlog", on,
                    on ? "目标库 binlog 已开启" : "目标库 binlog 未开启，双向灾备反向通道无法捕获",
                    on ? "info" : "error");
            String format = getVariable(targetDb, "binlog_format");
            boolean row = "ROW".equalsIgnoreCase(format);
            result.addItem("目标库Binlog格式", "双向灾备反向通道需要目标库 binlog_format=ROW", row,
                    row ? "目标库 binlog_format=ROW" : "目标库 binlog_format=" + format + "，需为 ROW",
                    row ? "info" : "error");
        } catch (SQLException e) {
            result.addItem("目标库Binlog", "双向灾备反向通道需要目标库开启 binlog", false,
                    "检查失败: " + e.getMessage(), "warning");
        }
    }

    /** 双向灾备 MySQL：源/目标 server_id 必须不同（互为主从的复制拓扑要求）。 */
    private void checkServerIdDistinct(Connection sourceDb, Connection targetDb, ValidationResult result) {
        try {
            String s = getVariable(sourceDb, "server_id");
            String t = getVariable(targetDb, "server_id");
            boolean distinct = s != null && !s.equals(t);
            result.addItem("Server ID 唯一性", "双向灾备源/目标 server_id 必须不同", distinct,
                    distinct ? "源 server_id=" + s + "，目标 server_id=" + t + "，满足唯一"
                             : "源与目标 server_id 相同(" + s + ")，双向灾备复制拓扑要求二者不同",
                    distinct ? "info" : "error");
        } catch (SQLException e) {
            result.addItem("Server ID 唯一性", "双向灾备源/目标 server_id 必须不同", true,
                    "检查失败（非致命）: " + e.getMessage(), "warning");
        }
    }

    private String getVariable(Connection conn, String variableName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE '" + variableName + "'")) {
            if (rs.next()) {
                return rs.getString("Value");
            }
        }
        return null;
    }

    private Set<String> getGrantedPrivileges(Connection conn) throws SQLException {
        Set<String> privileges = new HashSet<>();
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW GRANTS")) {
            while (rs.next()) {
                String grant = rs.getString(1);
                Set<String> extracted = parseGrants(grant);
                privileges.addAll(extracted);
            }
        }
        
        return privileges;
    }

    private Set<String> parseGrants(String grant) {
        Set<String> privileges = new HashSet<>();
        
        int start = grant.indexOf("GRANT ");
        int end = grant.indexOf(" ON ");
        if (start >= 0 && end > start) {
            String privStr = grant.substring(start + 6, end).trim();
            String[] privs = privStr.split(",");
            for (String priv : privs) {
                privileges.add(priv.trim().toUpperCase());
            }
        }
        
        return privileges;
    }

    private Set<String> parseSqlMode(String sqlMode) {
        Set<String> modes = new HashSet<>();
        if (sqlMode != null && !sqlMode.isEmpty()) {
            String[] parts = sqlMode.split(",");
            for (String part : parts) {
                modes.add(part.trim().toUpperCase());
            }
        }
        return modes;
    }

    private int compareVersions(String v1, String v2) {
        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");
        
        int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            int num1 = i < parts1.length ? parseVersionPart(parts1[i]) : 0;
            int num2 = i < parts2.length ? parseVersionPart(parts2[i]) : 0;
            if (num1 != num2) {
                return num1 - num2;
            }
        }
        return 0;
    }

    private int parseVersionPart(String part) {
        try {
            return Integer.parseInt(part.replaceAll("[^0-9].*", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private boolean isVersionAtLeast(String version, String minVersion) {
        return compareVersions(version, minVersion) >= 0;
    }

    private boolean isVersionInRange(String version, String minVersion, String maxVersion) {
        return compareVersions(version, minVersion) >= 0 && compareVersions(version, maxVersion) <= 0;
    }
}
