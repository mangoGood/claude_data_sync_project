package com.migration.common.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Locale;
import java.util.Properties;

/**
 * 传输层加密的<b>唯一</b>出口：档位语义、驱动参数、SSLContext 材料，全平台一处定义。
 *
 * <p><b>为什么要有这个类</b>：在它之前，同一段"档位 → 驱动参数"的 switch 在仓库里抄了三份——
 * {@code DatabaseConfig.mysqlSslParams()/pgSslParams()}、后端的 {@code JdbcSslOptions}、
 * 以及增量自己拼 URL 时的 {@code ContinuousIncrementMain.targetSslParams()}。三份已经开始漂移
 * （见下方"归一"一节），而待接入的链路还有十条（binlog 抓取、PG 复制槽、Oracle LogMiner、
 * Mongo、Redis 命令通道与 PSYNC 通道、ES、Kafka、TiCDC）。再抄下去必然出现
 * "有的链路加密了、有的没有"——而这一类问题的表现是<b>任务全绿、数据在网上明文</b>，
 * 比不支持加密更危险。
 *
 * <p><b>档位</b>（与 MySQL 的 sslMode 同义，PG/Oracle 按语义映射）：
 * <pre>
 *   DISABLED         不加密（默认，历史行为）
 *   PREFERRED        尽力加密；服务端不支持则**静默明文**——所以它带告警，不做默认值
 *   REQUIRED         必须加密，但不校验证书
 *   VERIFY_CA        必须加密 + 校验服务端证书由可信 CA 签发
 *   VERIFY_IDENTITY  在 VERIFY_CA 之上再校验证书 CN/SAN 与连接主机名一致
 * </pre>
 *
 * <p><b>归一（相对三份旧实现的行为差异，仅此一处）</b>：非法档位字符串此前的处置是分裂的——
 * MySQL 侧原样拼进 URL（驱动在连接时报一句难懂的错），PG 侧则映射成 {@code disable}
 * <b>静默退回明文</b>。同一个拼写错误，一边报错一边明文，是最坏的组合。这里统一为<b>构造即抛</b>，
 * 在任务启动前就暴露。合法输入的输出与三份旧实现逐字符相同。
 */
public final class SslMaterial {

    public static final String DISABLED = "DISABLED";
    public static final String PREFERRED = "PREFERRED";
    public static final String REQUIRED = "REQUIRED";
    public static final String VERIFY_CA = "VERIFY_CA";
    public static final String VERIFY_IDENTITY = "VERIFY_IDENTITY";

    private static final String[] VALID_MODES =
            {DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY};

    private final String mode;
    /** CA / 信任材料路径。MySQL·Oracle·Kafka 期望 keystore（p12/jks）。 */
    private final String rootCert;
    /**
     * PEM 形态的 CA（PostgreSQL 的 {@code sslrootcert} 只吃 PEM，给它 p12 会直接报错）。
     * 空则回落到 {@link #rootCert}——只配了一份材料时按原样用，行为与合并前一致。
     */
    private final String rootCertPem;
    /** 客户端证书（mTLS，PEM）。 */
    private final String clientCert;
    /** 客户端私钥。PG 需要 PKCS8 <b>DER</b>（.pk8），PEM 会被 pgjdbc 拒绝。 */
    private final String clientKey;
    /** 客户端 keystore（p12/jks，MySQL·Oracle·Kafka 的 mTLS 材料）。 */
    private final String clientKeyStore;
    /** truststore / keystore 的口令（两者共用一个，由证书归一时随机生成）。 */
    private final String storePassword;

    private SslMaterial(String mode, String rootCert, String rootCertPem, String clientCert,
                        String clientKey, String clientKeyStore, String storePassword) {
        this.mode = mode;
        this.rootCert = nullToEmpty(rootCert);
        this.rootCertPem = nullToEmpty(rootCertPem);
        this.clientCert = nullToEmpty(clientCert);
        this.clientKey = nullToEmpty(clientKey);
        this.clientKeyStore = nullToEmpty(clientKeyStore);
        this.storePassword = nullToEmpty(storePassword);
    }

    // ==================== 构造 ====================

    /** 不加密（默认档位）。 */
    public static SslMaterial disabled() {
        return new SslMaterial(DISABLED, null, null, null, null, null, null);
    }

    /**
     * 引擎侧：从 config.properties 的 {@code <prefix>.db.ssl.*} 读取。
     *
     * @param prefix {@code "source"} 或 {@code "target"}
     */
    public static SslMaterial from(Properties props, String prefix) {
        if (props == null) {
            return disabled();
        }
        String p = prefix + ".db.ssl.";
        // 库口令在 config.properties 里是 ENC: 密文（与库口令同一套 AES-GCM），读出时解密；
        // 旧的明文值原样返回。忘了这一步的表现是 keystore 口令错、报"证书损坏"。
        String storePass = com.migration.common.crypto.CredentialCipher.decrypt(
                props.getProperty(p + "store.password", ""));
        return new SslMaterial(normalizeMode(props.getProperty(p + "mode")),
                  props.getProperty(p + "root.cert"),
                  props.getProperty(p + "root.cert.pem"),
                  props.getProperty(p + "client.cert"),
                  props.getProperty(p + "client.key"),
                  props.getProperty(p + "client.keystore"),
                  storePass);
    }

    /** 只有档位与信任材料（控制面、以及尚未接入 mTLS 的链路）。 */
    public static SslMaterial of(String mode, String rootCert) {
        return of(mode, rootCert, null, null, null, null);
    }

    public static SslMaterial of(String mode, String rootCert, String clientCert, String clientKey,
                                 String clientKeyStore, String storePassword) {
        return new SslMaterial(normalizeMode(mode), rootCert, null, clientCert, clientKey,
                               clientKeyStore, storePassword);
    }

    /**
     * 档位归一。null / 空 → DISABLED（未配置即历史行为）；非法值 → 抛。
     *
     * <p>非法值不能"宽容处理"：宽容的结果是明文，而调用方以为自己开了加密。
     */
    public static String normalizeMode(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return DISABLED;
        }
        String v = raw.trim().toUpperCase(Locale.ROOT);
        for (String valid : VALID_MODES) {
            if (valid.equals(v)) {
                return v;
            }
        }
        throw new IllegalArgumentException(
                "非法的 SSL 档位: '" + raw + "'。可选值: DISABLED / PREFERRED / REQUIRED / "
                        + "VERIFY_CA / VERIFY_IDENTITY");
    }

    // ==================== 语义 ====================

    public String mode() {
        return mode;
    }

    public boolean enabled() {
        return !DISABLED.equals(mode);
    }

    /** 是否校验服务端证书链（VERIFY_CA 及以上）。 */
    public boolean verifyCa() {
        return VERIFY_CA.equals(mode) || VERIFY_IDENTITY.equals(mode);
    }

    /** 是否校验主机名与证书 CN/SAN 一致。 */
    public boolean verifyIdentity() {
        return VERIFY_IDENTITY.equals(mode);
    }

    /**
     * 是否<b>必须</b>加密（REQUIRED 及以上）。
     *
     * <p>PREFERRED 不在其中——它在服务端不支持时会退回明文，这是驱动的既定语义。
     * 运行期"探到明文即 fail-stop"的判断要用这个方法，不能用 {@link #enabled()}。
     */
    public boolean mustEncrypt() {
        return REQUIRED.equals(mode) || verifyCa();
    }

    /** 是否配了客户端证书（双向认证）。 */
    public boolean mutualTls() {
        return enabled() && (!clientKeyStore.isEmpty() || (!clientCert.isEmpty() && !clientKey.isEmpty()));
    }

    public String rootCert() {
        return rootCert;
    }

    /** PG 用的 PEM 形态 CA；没单独给就回落到 {@link #rootCert()}。 */
    public String pgRootCert() {
        return rootCertPem.isEmpty() ? rootCert : rootCertPem;
    }

    public String storePassword() {
        return storePassword;
    }

    // ==================== 驱动参数 ====================

    /**
     * MySQL / TiDB 的 JDBC URL 参数段（不含前后的 {@code &}）。
     *
     * <p>Connector/J 8 的 {@code sslMode} 取代了老的 {@code useSSL}/{@code requireSSL} 组合；
     * 关闭时仍写 {@code useSSL=false} 而不是 {@code sslMode=DISABLED}，是为了与既有 URL
     * 逐字符一致（全仓有判据比对 URL 文本）。
     */
    public String mysqlUrlParams() {
        if (!enabled()) {
            return "useSSL=false";
        }
        StringBuilder sb = new StringBuilder("sslMode=").append(mode);
        if (!rootCert.isEmpty()) {
            sb.append("&trustCertificateKeyStoreUrl=file:").append(rootCert);
            appendStoreDetails(sb, "trustCertificateKeyStore", rootCert);
        }
        if (!clientKeyStore.isEmpty()) {
            sb.append("&clientCertificateKeyStoreUrl=file:").append(clientKeyStore);
            appendStoreDetails(sb, "clientCertificateKeyStore", clientKeyStore);
        }
        return sb.toString();
    }

    /** p12 需要显式声明类型与口令；jks 是驱动默认类型，为保持既有 URL 文本不变而不写。 */
    private void appendStoreDetails(StringBuilder sb, String prop, String path) {
        if (isPkcs12(path)) {
            sb.append('&').append(prop).append("Type=PKCS12");
        }
        if (!storePassword.isEmpty()) {
            sb.append('&').append(prop).append("Password=").append(storePassword);
        }
    }

    private static boolean isPkcs12(String path) {
        String p = path.toLowerCase(Locale.ROOT);
        return p.endsWith(".p12") || p.endsWith(".pfx");
    }

    /**
     * PostgreSQL 的 JDBC URL 参数段。
     *
     * <p>注意 {@code sslkey} 必须是 PKCS8 <b>DER</b>（{@code .pk8}）：pgjdbc 读不了 PKCS1 PEM，
     * 报的是 {@code Unsupported key type}，与"证书不对"完全不像。
     */
    public String pgUrlParams() {
        StringBuilder sb = new StringBuilder("sslmode=").append(pgMode());
        if (enabled()) {
            String ca = pgRootCert();
            if (!ca.isEmpty()) {
                sb.append("&sslrootcert=").append(ca);
            }
            if (!clientCert.isEmpty() && !clientKey.isEmpty()) {
                sb.append("&sslcert=").append(clientCert).append("&sslkey=").append(clientKey);
            }
        }
        return sb.toString();
    }

    /** PG 的 sslmode 取值。 */
    public String pgMode() {
        switch (mode) {
            case PREFERRED:       return "prefer";
            case REQUIRED:        return "require";
            case VERIFY_CA:       return "verify-ca";
            case VERIFY_IDENTITY: return "verify-full";
            default:              return "disable";
        }
    }

    /**
     * Oracle 的 TCPS 连接描述串。
     *
     * <p>Oracle 与 MySQL/PG 的差别不只是参数名：TLS 要换<b>协议与端口</b>
     * （TCPS，监听通常是 2484 而不是 1521），URL 结构也从 {@code @host:port/service}
     * 变成完整的 DESCRIPTION，查询串在这里根本不存在。信任材料走连接属性，见
     * {@link #applyOracleProperties(Properties)}。
     */
    public String oracleTcpsUrl(String host, int port, String service) {
        return "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=" + host
                + ")(PORT=" + port + "))(CONNECT_DATA=(SERVICE_NAME=" + service + ")))";
    }

    /** Oracle 的信任/密钥材料与主机名校验开关（走 {@code DriverManager} 的连接属性）。 */
    public void applyOracleProperties(Properties p) {
        if (!enabled()) {
            return;
        }
        if (!rootCert.isEmpty()) {
            p.setProperty("javax.net.ssl.trustStore", rootCert);
            p.setProperty("javax.net.ssl.trustStoreType", isPkcs12(rootCert) ? "PKCS12" : "JKS");
            if (!storePassword.isEmpty()) {
                p.setProperty("javax.net.ssl.trustStorePassword", storePassword);
            }
        }
        if (!clientKeyStore.isEmpty()) {
            p.setProperty("javax.net.ssl.keyStore", clientKeyStore);
            p.setProperty("javax.net.ssl.keyStoreType", isPkcs12(clientKeyStore) ? "PKCS12" : "JKS");
            if (!storePassword.isEmpty()) {
                p.setProperty("javax.net.ssl.keyStorePassword", storePassword);
            }
        }
        p.setProperty("oracle.net.ssl_server_dn_match", String.valueOf(verifyIdentity()));
    }

    /** Kafka 客户端的安全参数（订阅目标 / 控制面共用）。 */
    public void applyKafka(java.util.Map<String, Object> sink) {
        if (!enabled()) {
            return;
        }
        sink.put("security.protocol", "SSL");
        if (!rootCert.isEmpty()) {
            sink.put("ssl.truststore.location", rootCert);
            sink.put("ssl.truststore.type", isPkcs12(rootCert) ? "PKCS12" : "JKS");
            if (!storePassword.isEmpty()) {
                sink.put("ssl.truststore.password", storePassword);
            }
        }
        if (!clientKeyStore.isEmpty()) {
            sink.put("ssl.keystore.location", clientKeyStore);
            sink.put("ssl.keystore.type", isPkcs12(clientKeyStore) ? "PKCS12" : "JKS");
            if (!storePassword.isEmpty()) {
                sink.put("ssl.keystore.password", storePassword);
                sink.put("ssl.key.password", storePassword);
            }
        }
        // 空串 = 关闭主机名校验。VERIFY_IDENTITY 以下都不校验主机名，与 MySQL/PG 档位语义对齐。
        if (!verifyIdentity()) {
            sink.put("ssl.endpoint.identification.algorithm", "");
        }
    }

    /**
     * 构造 {@link SSLContext}——binlog 抓取客户端、Mongo、Redis（命令通道与 PSYNC 通道）、
     * ES、TiCDC 都只认这个对象，不认 URL 参数。
     *
     * <p>档位 &lt; VERIFY_CA 时返回<b>信任所有证书</b>的 context：这不是疏漏，而是
     * REQUIRED 的定义本身（加密但不校验），与 MySQL {@code sslMode=REQUIRED} 一致。
     */
    public SSLContext sslContext() throws GeneralSecurityException, IOException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(keyManagers(), effectiveTrustManagers(), null);
        return ctx;
    }

    /**
     * 公开 KeyManager/TrustManager，供只能"初始化一个已有 SSLContext"的客户端使用
     * （mysql-binlog-connector 的 {@code DefaultSSLSocketFactory#initSSLContext} 就是这样）。
     */
    public javax.net.ssl.KeyManager[] keyManagers() throws GeneralSecurityException, IOException {
        if (clientKeyStore.isEmpty()) {
            return null;
        }
        KeyStore ks = loadStore(clientKeyStore);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, storePassword.toCharArray());
        return kmf.getKeyManagers();
    }

    /** 按档位选出的信任管理器：VERIFY_CA 及以上校验证书链，以下"加密但不校验"。 */
    public javax.net.ssl.TrustManager[] effectiveTrustManagers()
            throws GeneralSecurityException, IOException {
        return verifyCa() ? trustManagers() : TrustAllManager.ARRAY;
    }

    public javax.net.ssl.TrustManager[] trustManagers() throws GeneralSecurityException, IOException {
        if (rootCert.isEmpty()) {
            return null;   // 回落到 JVM 默认信任库
        }
        KeyStore ts = loadStore(rootCert);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf.getTrustManagers();
    }

    private KeyStore loadStore(String path) throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance(isPkcs12(path) ? "PKCS12" : "JKS");
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            ks.load(in, storePassword.isEmpty() ? null : storePassword.toCharArray());
        }
        return ks;
    }

    /** REQUIRED / PREFERRED 档位下的"加密但不校验"信任管理器。 */
    private static final class TrustAllManager implements javax.net.ssl.X509TrustManager {
        static final javax.net.ssl.TrustManager[] ARRAY = {new TrustAllManager()};

        @Override
        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[0];
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s.trim();
    }

    @Override
    public String toString() {
        // 绝不打印 storePassword：config.properties 里它是 ENC: 的，日志里不能是明文
        return "SslMaterial{mode=" + mode + ", rootCert=" + (rootCert.isEmpty() ? "-" : rootCert)
                + ", mTLS=" + mutualTls() + '}';
    }
}
