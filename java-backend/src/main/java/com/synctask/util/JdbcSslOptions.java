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
 * <p>档位与数据面 {@code DatabaseConfig} 完全一致，避免两边各有一套说法：
 * {@code DISABLED（默认）| PREFERRED | REQUIRED | VERIFY_CA | VERIFY_IDENTITY}。
 * 默认 DISABLED = 与之前行为完全相同。
 *
 * <p>取值来源是环境变量而不是每个任务的配置：控制面的连接是后端进程发起的，
 * 一个部署环境要么整体走 TLS、要么整体不走，按任务配反而会出现
 * "同一个库有的连接加密有的不加密"这种没人能推理的状态。
 */
public final class JdbcSslOptions {

    private JdbcSslOptions() {
    }

    /** 控制面到用户数据库的加密档位（源/目标共用一个开关）。 */
    public static String mode() {
        String v = System.getenv("CONTROL_PLANE_DB_SSL_MODE");
        if (v == null || v.trim().isEmpty()) {
            return "DISABLED";
        }
        return v.trim().toUpperCase();
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
        if (!enabled()) {
            return "useSSL=false";
        }
        StringBuilder sb = new StringBuilder("sslMode=").append(mode());
        if (!rootCert().isEmpty()) {
            sb.append("&trustCertificateKeyStoreUrl=file:").append(rootCert());
        }
        return sb.toString();
    }

    /** PostgreSQL 的 sslmode 段。 */
    public static String postgres() {
        String m;
        switch (mode()) {
            case "PREFERRED":       m = "prefer"; break;
            case "REQUIRED":        m = "require"; break;
            case "VERIFY_CA":       m = "verify-ca"; break;
            case "VERIFY_IDENTITY": m = "verify-full"; break;
            default:                m = "disable";
        }
        StringBuilder sb = new StringBuilder("sslmode=").append(m);
        if (enabled() && !rootCert().isEmpty()) {
            sb.append("&sslrootcert=").append(rootCert());
        }
        return sb.toString();
    }
}
