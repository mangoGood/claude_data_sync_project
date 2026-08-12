package com.synctask.util;

import java.util.Locale;
import java.util.Properties;

/**
 * Oracle 的 TLS 接入。
 *
 * <p>Oracle 与 MySQL/PostgreSQL 的差别不只是参数名换一换：
 * <ul>
 *   <li><b>协议变了</b>——从 TCP 换成 TCPS，监听端口通常也从 1521 换成 2484；</li>
 *   <li><b>URL 结构变了</b>——从 {@code @host:port/service} 变成完整的 DESCRIPTION，
 *       thin URL 里根本没有"查询串"这个位置可以挂参数；</li>
 *   <li><b>信任材料走连接属性</b>——{@code javax.net.ssl.trustStore*}，不是 URL。</li>
 * </ul>
 * 因此不能像另外两家那样"拼一段参数"了事，单独一个类放这套映射。
 *
 * <p>与数据面 {@code com.migration.common.ssl.SslMaterial} 的 Oracle 部分是镜像关系
 * （后端刻意不依赖 migration-common，见 {@link JdbcSslOptions} 类注释）。
 */
public final class OracleSslSupport {

    private OracleSslSupport() {
    }

    /** TCPS 连接描述串。 */
    public static String tcpsUrl(String host, int port, String service) {
        return "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=" + host
                + ")(PORT=" + port + "))(CONNECT_DATA=(SERVICE_NAME=" + service + ")))";
    }

    /**
     * 把信任/密钥材料与主机名校验开关写进连接属性。
     *
     * <p>{@code oracle.net.ssl_server_dn_match} 只在 VERIFY_IDENTITY 下开——
     * 它对应的是"证书里的 DN 要和连上的服务端一致"，与 MySQL 的 VERIFY_IDENTITY、
     * PG 的 verify-full 是同一档语义。
     */
    public static void applyProperties(Properties p, String mode, CertMaterial material) {
        String normalized = JdbcSslOptions.normalizeMode(mode);
        if ("DISABLED".equals(normalized)) {
            return;
        }
        if (material != null && material.truststore() != null) {
            p.setProperty("javax.net.ssl.trustStore", material.truststore());
            p.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
            p.setProperty("javax.net.ssl.trustStorePassword", material.storePassword());
        }
        if (material != null && material.keystore() != null) {
            p.setProperty("javax.net.ssl.keyStore", material.keystore());
            p.setProperty("javax.net.ssl.keyStoreType", "PKCS12");
            p.setProperty("javax.net.ssl.keyStorePassword", material.storePassword());
        }
        p.setProperty("oracle.net.ssl_server_dn_match",
                String.valueOf("VERIFY_IDENTITY".equals(normalized)));
    }

    /** 开启 TLS 时端口通常要改。用于在 UI/日志里给一句提示，不做强制改写。 */
    public static boolean looksLikeNonTlsPort(int port) {
        return port == 1521;
    }

    static boolean isPkcs12(String path) {
        String p = path == null ? "" : path.toLowerCase(Locale.ROOT);
        return p.endsWith(".p12") || p.endsWith(".pfx");
    }
}
