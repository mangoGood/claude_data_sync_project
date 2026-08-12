package com.synctask.util;

/**
 * 控制面连接的传输层加密参数。
 *
 * <p><b>为什么控制面也要管</b>：数据面（capture / full / increment）的 TLS 在第 7 批已经补了，
 * 但后端自己也直连用户数据库——元数据探查、连接校验、数据校验、<b>内容对比</b>——
 * 而内容对比是**逐行把两端的业务数据读回来比**的。这些连接一直硬编码
 * {@code useSSL=false}，等于"数据面加密了、控制面照样明文把同一批数据拉一遍"，
 * 比不加密更容易让人误判。
 *
 * <p>档位与数据面 {@code com.migration.common.ssl.SslMaterial} 完全一致：
 * {@code DISABLED（默认）| PREFERRED | REQUIRED | VERIFY_CA | VERIFY_IDENTITY}。
 * 默认 DISABLED = 与之前行为完全相同。
 *
 * <p><b>为什么是镜像而不是直接依赖 migration-common</b>：后端是 Spring Boot 应用，
 * 而 migration-common 会把 mysql-connector / ojdbc / HikariCP / logback 一并拽进来，
 * 与 Spring BOM 的版本仲裁打架（本仓库在 migration-mongo 上已经吃过一次驱动降级混包的亏）。
 * 后端对 {@code CredentialCipher}、{@code KafkaSecurity} 采用的也是同一个做法。
 * 两侧的逐字符一致由 {@code JdbcSslOptionsMirrorTest} 的全档位矩阵守住。
 *
 * <p>取值来源是环境变量而不是每个任务的配置：控制面的连接是后端进程发起的，
 * 一个部署环境要么整体走 TLS、要么整体不走，按任务配反而会出现
 * "同一个库有的连接加密有的不加密"这种没人能推理的状态。
 * （任务级证书在 B2/B3 批次接入，届时这里会多一个按任务取的重载。）
 */
public final class JdbcSslOptions {

    private static final String[] VALID_MODES =
            {"DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"};

    private JdbcSslOptions() {
    }

    /** 控制面到用户数据库的加密档位（源/目标共用一个开关）。 */
    public static String mode() {
        return normalizeMode(System.getenv("CONTROL_PLANE_DB_SSL_MODE"));
    }

    /**
     * 档位归一。null / 空 → DISABLED；非法值 → 抛。
     *
     * <p>非法值不能宽容处理：宽容的结果是<b>明文</b>，而运维以为自己开了加密。
     * 与 {@code SslMaterial#normalizeMode} 同语义。
     */
    static String normalizeMode(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return "DISABLED";
        }
        String v = raw.trim().toUpperCase(java.util.Locale.ROOT);
        for (String valid : VALID_MODES) {
            if (valid.equals(v)) {
                return v;
            }
        }
        throw new IllegalArgumentException(
                "非法的 CONTROL_PLANE_DB_SSL_MODE: '" + raw + "'。可选值: DISABLED / PREFERRED / "
                        + "REQUIRED / VERIFY_CA / VERIFY_IDENTITY");
    }

    public static boolean enabled() {
        return !"DISABLED".equals(mode());
    }

    private static String rootCert() {
        String v = System.getenv("CONTROL_PLANE_DB_SSL_ROOT_CERT");
        return v == null ? "" : v.trim();
    }

    /**
     * MySQL 的 SSL 参数段（不含前后的 {@code &}）。
     * Connector/J 8 的 {@code sslMode} 取代了老的 {@code useSSL}/{@code requireSSL} 组合。
     */
    public static String mysql() {
        return mysql(mode(), rootCert());
    }

    /** 供镜像一致性测试注入用。 */
    static String mysql(String mode, String rootCert) {
        if ("DISABLED".equals(mode)) {
            return "useSSL=false";
        }
        StringBuilder sb = new StringBuilder("sslMode=").append(mode);
        if (!rootCert.isEmpty()) {
            sb.append("&trustCertificateKeyStoreUrl=file:").append(rootCert);
            if (isPkcs12(rootCert)) {
                sb.append("&trustCertificateKeyStoreType=PKCS12");
            }
        }
        return sb.toString();
    }

    /**
     * 任务级 MySQL 参数段：用任务自己选的档位与证书，而不是部署级 env。
     *
     * <p>控制面此前只有一个全局开关，理由是"一个环境要么整体走 TLS、要么整体不走"。
     * 但**内容对比**这类功能是后端直连用户库逐行读业务数据的，任务既然为源/目标各配了
     * 证书，控制面就该用同一份——否则同一批数据换条路又明文走了一遍。
     *
     * @param material 任务证书物化后的证书包；null = 不带证书（REQUIRED 及以下够用）
     */
    public static String mysql(String mode, CertMaterial material) {
        String normalized = normalizeMode(mode);
        if ("DISABLED".equals(normalized)) {
            return "useSSL=false";
        }
        StringBuilder sb = new StringBuilder("sslMode=").append(normalized);
        if (material != null && material.truststore() != null) {
            sb.append("&trustCertificateKeyStoreUrl=file:").append(material.truststore())
              .append("&trustCertificateKeyStoreType=PKCS12")
              .append("&trustCertificateKeyStorePassword=").append(material.storePassword());
        }
        if (material != null && material.keystore() != null) {
            sb.append("&clientCertificateKeyStoreUrl=file:").append(material.keystore())
              .append("&clientCertificateKeyStoreType=PKCS12")
              .append("&clientCertificateKeyStorePassword=").append(material.storePassword());
        }
        return sb.toString();
    }

    /** 任务级 PostgreSQL 参数段。注意 sslkey 必须是 PKCS8 DER（.pk8）。 */
    public static String postgres(String mode, CertMaterial material) {
        String normalized = normalizeMode(mode);
        StringBuilder sb = new StringBuilder("sslmode=").append(pgMode(normalized));
        if (!"DISABLED".equals(normalized) && material != null) {
            if (material.caPem() != null) {
                sb.append("&sslrootcert=").append(material.caPem());
            }
            if (material.clientCertPem() != null && material.clientKeyPk8() != null) {
                sb.append("&sslcert=").append(material.clientCertPem())
                  .append("&sslkey=").append(material.clientKeyPk8());
            }
        }
        return sb.toString();
    }

    /** PostgreSQL 的 sslmode 段。 */
    public static String postgres() {
        return postgres(mode(), rootCert());
    }

    static String pgMode(String mode) {
        switch (mode) {
            case "PREFERRED":       return "prefer";
            case "REQUIRED":        return "require";
            case "VERIFY_CA":       return "verify-ca";
            case "VERIFY_IDENTITY": return "verify-full";
            default:                return "disable";
        }
    }

    static String postgres(String mode, String rootCert) {
        StringBuilder sb = new StringBuilder("sslmode=").append(pgMode(mode));
        if (!"DISABLED".equals(mode) && !rootCert.isEmpty()) {
            sb.append("&sslrootcert=").append(rootCert);
        }
        return sb.toString();
    }

    private static boolean isPkcs12(String path) {
        String p = path.toLowerCase(java.util.Locale.ROOT);
        return p.endsWith(".p12") || p.endsWith(".pfx");
    }
}
