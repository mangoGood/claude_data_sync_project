package com.migration.common.ssl;

import com.migration.config.DatabaseConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SslMaterial} 的行为锁定。
 *
 * <p>本类的重点不是"新代码能跑"，而是 <b>B1 承诺的零行为变化</b>：统一之前有三份各自演化的
 * 档位 switch，合并时任何一个字符的漂移都会让某条链路的 URL 变样。因此第一组用例把三份旧实现
 * 逐字符抄进来做对照——它们是"当时线上跑的那个字符串"的快照，不是对新实现的复述。
 */
class SslMaterialTest {

    // ============ 三份旧实现的逐字符快照（合并前的行为基线，勿"优化"） ============

    /** 旧 {@code DatabaseConfig.mysqlSslParams()} / {@code JdbcSslOptions.mysql()}。 */
    private static String legacyMysql(String mode, String cert) {
        if ("DISABLED".equalsIgnoreCase(mode)) {
            return "useSSL=false";
        }
        StringBuilder sb = new StringBuilder("sslMode=").append(mode);
        if (cert != null && !cert.isEmpty()) {
            sb.append("&trustCertificateKeyStoreUrl=file:").append(cert);
        }
        return sb.toString();
    }

    /** 旧 {@code DatabaseConfig.pgSslParams()}。 */
    private static String legacyPg(String mode, String cert) {
        String m;
        switch (mode) {
            case "PREFERRED":       m = "prefer"; break;
            case "REQUIRED":        m = "require"; break;
            case "VERIFY_CA":       m = "verify-ca"; break;
            case "VERIFY_IDENTITY": m = "verify-full"; break;
            default:                m = "disable";
        }
        StringBuilder sb = new StringBuilder("sslmode=").append(m);
        if (!"DISABLED".equalsIgnoreCase(mode) && cert != null && !cert.isEmpty()) {
            sb.append("&sslrootcert=").append(cert);
        }
        return sb.toString();
    }

    private static final String[] MODES =
            {"DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"};
    /** 刻意用非 p12 后缀：那是合并前唯一可配的形态，也是逐字符可比的前提。 */
    private static final String[] CERTS = {"", "/etc/ssl/ca.pem", "/opt/certs/root-ca.crt"};

    @Test
    @DisplayName("MySQL 参数段与旧实现逐字符相同（全档位 × 有无证书）")
    void mysqlParamsMatchLegacy() {
        for (String mode : MODES) {
            for (String cert : CERTS) {
                assertEquals(legacyMysql(mode, cert),
                             SslMaterial.of(mode, cert).mysqlUrlParams(),
                             "mode=" + mode + " cert=" + cert);
            }
        }
    }

    @Test
    @DisplayName("PostgreSQL 参数段与旧实现逐字符相同（全档位 × 有无证书）")
    void pgParamsMatchLegacy() {
        for (String mode : MODES) {
            for (String cert : CERTS) {
                assertEquals(legacyPg(mode, cert),
                             SslMaterial.of(mode, cert).pgUrlParams(),
                             "mode=" + mode + " cert=" + cert);
            }
        }
    }

    @Test
    @DisplayName("未配置 = DISABLED = 与历史行为逐字符相同")
    void unconfiguredIsDisabled() {
        for (SslMaterial m : new SslMaterial[]{
                SslMaterial.disabled(),
                SslMaterial.of(null, null),
                SslMaterial.of("", ""),
                SslMaterial.of("  ", null),
                SslMaterial.from(new Properties(), "source"),
                SslMaterial.from(null, "target")}) {
            assertEquals("DISABLED", m.mode());
            assertFalse(m.enabled());
            assertEquals("useSSL=false", m.mysqlUrlParams());
            assertEquals("sslmode=disable", m.pgUrlParams());
        }
    }

    // ============ 归一：合并时刻意修掉的那处分裂 ============

    /** 旧 {@code ContinuousIncrementMain.targetSslParams()} 的 PG 分支——与上面那份的守卫条件不同。 */
    private static String legacyIncrementPg(String mode, String cert) {
        String m;
        switch (mode) {
            case "PREFERRED":       m = "prefer"; break;
            case "REQUIRED":        m = "require"; break;
            case "VERIFY_CA":       m = "verify-ca"; break;
            case "VERIFY_IDENTITY": m = "verify-full"; break;
            default:                m = "disable";
        }
        return "sslmode=" + m + (cert.isEmpty() || "disable".equals(m) ? "" : "&sslrootcert=" + cert);
    }

    @Test
    @DisplayName("合法档位下两份 PG 旧实现一致（合并的前提）")
    void legacyPgVariantsAgreeOnValidModes() {
        for (String mode : MODES) {
            for (String cert : CERTS) {
                assertEquals(legacyPg(mode, cert), legacyIncrementPg(mode, cert),
                             "mode=" + mode + " cert=" + cert);
            }
        }
    }

    @Test
    @DisplayName("非法档位：三份旧实现给三种结果，其中两种是静默明文——故改为构造即抛")
    void invalidModeThrows() {
        String bad = "REQUIRE";   // REQUIRED 少个 D，最常见的手滑
        String cert = "/etc/ssl/ca.pem";

        // 同一个拼写错误，合并前的三份实现给出三种互不相同的结果：
        //   DatabaseConfig(PG)          → 明文，但证书还挂着（看起来像配好了）
        assertEquals("sslmode=disable&sslrootcert=" + cert, legacyPg(bad, cert));
        //   ContinuousIncrementMain(PG) → 明文，连证书都没了
        assertEquals("sslmode=disable", legacyIncrementPg(bad, cert));
        //   DatabaseConfig(MySQL)       → 原样拼进 URL，驱动连接时才报一句难懂的错
        assertEquals("sslMode=REQUIRE&trustCertificateKeyStoreUrl=file:" + cert,
                     legacyMysql(bad, cert));
        // 也就是说：同一份配置，MySQL 链路报错、PG 链路静默明文——最坏的组合。

        for (String v : new String[]{"REQUIRE", "true", "on", "ssl", "VERIFY", "DISABLE"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> SslMaterial.of(v, null), "应拒绝: " + v);
            assertTrue(e.getMessage().contains("VERIFY_IDENTITY"), "错误信息要列出合法取值");
        }
    }

    @Test
    @DisplayName("大小写与空白归一")
    void modeIsNormalized() {
        assertEquals("REQUIRED", SslMaterial.of("required", null).mode());
        assertEquals("VERIFY_CA", SslMaterial.of("  verify_ca  ", null).mode());
    }

    // ============ 档位语义 ============

    @Test
    @DisplayName("mustEncrypt 不含 PREFERRED —— 它会静默退回明文")
    void preferredIsNotMustEncrypt() {
        assertTrue(SslMaterial.of("PREFERRED", null).enabled());
        assertFalse(SslMaterial.of("PREFERRED", null).mustEncrypt(),
                "PREFERRED 在服务端不支持时退回明文，不能作为'必须加密'的判据");

        assertTrue(SslMaterial.of("REQUIRED", null).mustEncrypt());
        assertTrue(SslMaterial.of("VERIFY_CA", null).mustEncrypt());
        assertTrue(SslMaterial.of("VERIFY_IDENTITY", null).mustEncrypt());
        assertFalse(SslMaterial.disabled().mustEncrypt());
    }

    @Test
    @DisplayName("verifyCa / verifyIdentity 的边界")
    void verificationLadder() {
        assertFalse(SslMaterial.of("REQUIRED", null).verifyCa());
        assertTrue(SslMaterial.of("VERIFY_CA", null).verifyCa());
        assertFalse(SslMaterial.of("VERIFY_CA", null).verifyIdentity());
        assertTrue(SslMaterial.of("VERIFY_IDENTITY", null).verifyIdentity());
        assertTrue(SslMaterial.of("VERIFY_IDENTITY", null).verifyCa());
    }

    // ============ 从 config.properties 读取 ============

    @Test
    @DisplayName("from(props, prefix) 认 source/target 两套且互不串台")
    void readsPerPrefix() {
        Properties p = new Properties();
        p.setProperty("source.db.ssl.mode", "VERIFY_CA");
        p.setProperty("source.db.ssl.root.cert", "/certs/src/truststore.p12");
        p.setProperty("target.db.ssl.mode", "REQUIRED");

        SslMaterial src = SslMaterial.from(p, "source");
        SslMaterial tgt = SslMaterial.from(p, "target");

        assertEquals("VERIFY_CA", src.mode());
        assertEquals("/certs/src/truststore.p12", src.rootCert());
        assertEquals("REQUIRED", tgt.mode());
        assertEquals("", tgt.rootCert());
    }

    @Test
    @DisplayName("p12 信任库要显式声明类型，否则驱动按 JKS 读会失败")
    void pkcs12StoreTypeIsDeclared() {
        String params = SslMaterial.of("VERIFY_CA", "/certs/truststore.p12").mysqlUrlParams();
        assertTrue(params.contains("trustCertificateKeyStoreUrl=file:/certs/truststore.p12"));
        assertTrue(params.contains("trustCertificateKeyStoreType=PKCS12"));

        // 非 p12 路径不写 Type —— 这正是与旧实现逐字符一致的那条路径
        assertFalse(SslMaterial.of("VERIFY_CA", "/certs/ca.pem").mysqlUrlParams()
                            .contains("KeyStoreType"));
    }

    @Test
    @DisplayName("mTLS：客户端材料进 URL，PG 的 sslkey 必须是 .pk8")
    void mutualTlsParams() {
        SslMaterial m = SslMaterial.of("VERIFY_IDENTITY", "/c/truststore.p12",
                "/c/client-cert.pem", "/c/client-key.pk8", "/c/keystore.p12", "s3cr3t");
        assertTrue(m.mutualTls());

        String mysql = m.mysqlUrlParams();
        assertTrue(mysql.contains("clientCertificateKeyStoreUrl=file:/c/keystore.p12"));
        assertTrue(mysql.contains("clientCertificateKeyStoreType=PKCS12"));
        assertTrue(mysql.contains("clientCertificateKeyStorePassword=s3cr3t"));

        String pg = m.pgUrlParams();
        assertEquals("sslmode=verify-full&sslrootcert=/c/truststore.p12"
                     + "&sslcert=/c/client-cert.pem&sslkey=/c/client-key.pk8", pg);
    }

    @Test
    @DisplayName("toString 不泄露库口令")
    void toStringHidesPassword() {
        String s = SslMaterial.of("VERIFY_CA", "/c/ts.p12", "/c/c.pem", "/c/k.pk8",
                                  "/c/ks.p12", "topsecret").toString();
        assertFalse(s.contains("topsecret"), "库口令不能出现在日志里");
    }

    // ============ Oracle：TLS 下 URL 结构整个换掉 ============

    @Test
    @DisplayName("Oracle 默认钉 TLS 1.2 —— 不钉则 JDK 先提 1.3，多数 Oracle 服务端握不上手")
    void oracleTlsVersionDefaultsTo12() {
        Properties p = new Properties();
        SslMaterial.of("REQUIRED", "/c/ts.p12").applyOracleProperties(p);
        assertEquals("1.2", p.getProperty("oracle.net.ssl_version"),
                "实测：不钉版本时报 TNS-00542 SSL Handshake failed，报文里没有半个字提到版本");

        Properties cfg = new Properties();
        cfg.setProperty("source.db.ssl.mode", "REQUIRED");
        cfg.setProperty("source.db.ssl.oracle.version", "1.3");
        Properties p2 = new Properties();
        SslMaterial.from(cfg, "source").applyOracleProperties(p2);
        assertEquals("1.3", p2.getProperty("oracle.net.ssl_version"), "服务端支持时可覆盖");
    }

    @Test
    @DisplayName("Oracle VERIFY_IDENTITY 需要在描述串里给出期望 DN（它比的是完整 DN，不是主机名）")
    void oracleServerDnMatch() {
        Properties cfg = new Properties();
        cfg.setProperty("source.db.ssl.mode", "VERIFY_IDENTITY");
        cfg.setProperty("source.db.ssl.oracle.server.dn", "CN=localhost,O=synctask,C=CN");
        String url = SslMaterial.from(cfg, "source").oracleTcpsUrl("localhost", 2484, "FREEPDB1");
        assertTrue(url.contains("(SSL_SERVER_DN_MATCH=yes)"), url);
        assertTrue(url.contains("(SSL_SERVER_CERT_DN=\"CN=localhost,O=synctask,C=CN\")"), url);
        // 括号必须配平，否则驱动直接报 "Invalid connection string format"
        assertEquals(url.chars().filter(c -> c == '(').count(),
                     url.chars().filter(c -> c == ')').count(), "描述串括号不配平: " + url);

        // 低于 VERIFY_IDENTITY 的档位不写 DN —— 那是身份校验专属，写了会让 REQUIRED 也去比 DN
        Properties cfg2 = new Properties();
        cfg2.setProperty("source.db.ssl.mode", "VERIFY_CA");
        cfg2.setProperty("source.db.ssl.oracle.server.dn", "CN=localhost,O=synctask,C=CN");
        assertFalse(SslMaterial.from(cfg2, "source").oracleTcpsUrl("h", 2484, "S")
                        .contains("SSL_SERVER_CERT_DN"));
    }

    @Test
    @DisplayName("Oracle 开启 TLS 后走 TCPS DESCRIPTION，且信任材料走连接属性")
    void oracleTcps() {
        SslMaterial m = SslMaterial.of("VERIFY_IDENTITY", "/c/truststore.p12");
        assertEquals("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=db1)"
                     + "(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
                     m.oracleTcpsUrl("db1", 2484, "FREEPDB1"));

        Properties p = new Properties();
        m.applyOracleProperties(p);
        assertEquals("/c/truststore.p12", p.getProperty("javax.net.ssl.trustStore"));
        assertEquals("PKCS12", p.getProperty("javax.net.ssl.trustStoreType"));
        assertEquals("true", p.getProperty("oracle.net.ssl_server_dn_match"));

        // VERIFY_CA 只校验链、不校验主机名
        Properties p2 = new Properties();
        SslMaterial.of("VERIFY_CA", "/c/truststore.p12").applyOracleProperties(p2);
        assertEquals("false", p2.getProperty("oracle.net.ssl_server_dn_match"));

        // 关闭时一个属性都不写
        Properties p3 = new Properties();
        SslMaterial.disabled().applyOracleProperties(p3);
        assertTrue(p3.isEmpty());
    }

    // ============ DatabaseConfig 的 URL 全景（含此前漏掉的建库连接） ============

    @Test
    @DisplayName("DatabaseConfig 默认不加密时 URL 与历史逐字符相同")
    void databaseConfigDefaultUrlsUnchanged() {
        DatabaseConfig mysql = new DatabaseConfig("h", 3306, "db", "u", "p", "mysql");
        assertEquals("jdbc:mysql://h:3306/db?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
                     + "&autoReconnect=true&connectTimeout=30000&socketTimeout=0",
                     mysql.getJdbcUrl());
        assertEquals("jdbc:mysql://h:3306/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8",
                     mysql.getRootJdbcUrl());

        DatabaseConfig pg = new DatabaseConfig("h", 5432, "db", "u", "p", "postgresql");
        assertEquals("jdbc:postgresql://h:5432/db?currentSchema=public&stringtype=unspecified"
                     + "&sslmode=disable", pg.getJdbcUrl());

        DatabaseConfig ora = new DatabaseConfig("h", 1521, "FREEPDB1", "u", "p", "oracle");
        assertEquals("jdbc:oracle:thin:@h:1521/FREEPDB1", ora.getJdbcUrl());
    }

    @Test
    @DisplayName("建库连接（getRootJdbcUrl）也跟着加密，不再是明文的那一跳")
    void rootUrlHonoursSsl() {
        DatabaseConfig mysql = new DatabaseConfig("h", 3306, "db", "u", "p", "mysql");
        mysql.setSslMode("REQUIRED");
        assertTrue(mysql.getRootJdbcUrl().contains("sslMode=REQUIRED"));
        assertFalse(mysql.getRootJdbcUrl().contains("useSSL=false"));

        DatabaseConfig pg = new DatabaseConfig("h", 5432, "db", "u", "p", "postgresql");
        pg.setSslMode("VERIFY_CA");
        assertTrue(pg.getRootJdbcUrl().contains("sslmode=verify-ca"));
    }

    @Test
    @DisplayName("setSslMode / setSslRootCert 互不覆盖（顺序无关）")
    void settersCompose() {
        DatabaseConfig a = new DatabaseConfig("h", 3306, "db", "u", "p", "mysql");
        a.setSslMode("VERIFY_CA");
        a.setSslRootCert("/c/ca.pem");

        DatabaseConfig b = new DatabaseConfig("h", 3306, "db", "u", "p", "mysql");
        b.setSslRootCert("/c/ca.pem");
        b.setSslMode("VERIFY_CA");

        assertEquals(a.getJdbcUrl(), b.getJdbcUrl());
        assertTrue(a.getJdbcUrl().contains("sslMode=VERIFY_CA"));
        assertTrue(a.getJdbcUrl().contains("trustCertificateKeyStoreUrl=file:/c/ca.pem"));
    }

    @Test
    @DisplayName("REQUIRED 档位下 SSLContext 可用（加密但不校验证书）")
    void sslContextForRequired() throws Exception {
        assertNotNull(SslMaterial.of("REQUIRED", null).sslContext());
    }
}
