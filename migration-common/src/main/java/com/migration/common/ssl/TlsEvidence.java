package com.migration.common.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 运行期从<b>服务端视角</b>确认连接到底加没加密，并在"要求加密却是明文"时 fail-stop。
 *
 * <p><b>为什么必须有这一步</b>：客户端参数写了 {@code sslMode=REQUIRED} 不等于连接真的加密了。
 * 驱动版本不对会<b>静默忽略</b>未知参数（Connector/J 5.1 就完全不认 {@code sslMode}），
 * {@code PREFERRED} 在服务端不支持时按定义就退回明文，代理/中间件也可能在中间把 TLS 摘掉。
 * 只看自己的配置和日志，"以为加密了其实没有"是查不出来的——而它比不加密更危险，
 * 因为它能通过入网评审。
 *
 * <p>取证一律用服务端自己的说法：
 * <pre>
 *   MySQL/TiDB   SHOW STATUS LIKE 'Ssl_cipher' / 'Ssl_version'
 *   PostgreSQL   SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()
 * </pre>
 * 问不出来就<b>如实记为未知</b>，不猜、不填"已加密"。
 */
public final class TlsEvidence {

    private static final Logger logger = LoggerFactory.getLogger(TlsEvidence.class);

    /** {@code encrypted} 为 null 表示"没能问出来"，与 false（确认明文）不是一回事。 */
    public static final class State {
        public final Boolean encrypted;
        public final String version;
        public final String cipher;

        State(Boolean encrypted, String version, String cipher) {
            this.encrypted = encrypted;
            this.version = version;
            this.cipher = cipher;
        }

        public String describe() {
            if (encrypted == null) {
                return "未知（无法从服务端读取加密状态）";
            }
            if (!encrypted) {
                return "明文";
            }
            StringBuilder sb = new StringBuilder("已加密");
            if (version != null && !version.isEmpty()) {
                sb.append(' ').append(version);
            }
            if (cipher != null && !cipher.isEmpty()) {
                sb.append(" / ").append(cipher);
            }
            return sb.toString();
        }
    }

    public static final State UNKNOWN = new State(null, null, null);

    private TlsEvidence() {
    }

    /**
     * 探测并记录；档位 &ge; REQUIRED 而实测明文时抛异常（E5007）。
     *
     * <p>抛而不是只告警：调用方明确要求了加密，实际却是明文——继续跑就是在"用户以为加密"的
     * 前提下把业务数据明文搬一遍。这正是本工程要根治的那一类。
     *
     * @param label  日志里标识是哪一跳（如 {@code "源库"} / {@code "目标库"}）
     * @param dbType mysql / tidb / postgresql；其它类型不探测（返回 UNKNOWN，不阻断）
     */
    public static State verify(Connection conn, String dbType, SslMaterial ssl, String label) {
        State state = probe(conn, dbType);
        if (ssl != null && ssl.mustEncrypt() && Boolean.FALSE.equals(state.encrypted)) {
            throw new IllegalStateException(
                    "[E5007] " + label + "要求加密（档位 " + ssl.mode() + "）但连接实际未加密。"
                            + "服务端可能未开启 SSL，或连的不是 TLS 端口。");
        }
        if (ssl != null && ssl.enabled()) {
            if (Boolean.TRUE.equals(state.encrypted)) {
                logger.info("{}传输加密已生效（服务端确认）: 档位={} {}", label, ssl.mode(), state.describe());
            } else if (state.encrypted == null) {
                logger.warn("{}配置了传输加密（档位={}），但无法从服务端确认实际状态", label, ssl.mode());
            } else {
                // 走到这里说明档位是 PREFERRED：驱动语义允许退回明文，不阻断，但必须说出来
                logger.warn("{}档位 {} 在服务端不支持 SSL 时会退回明文，当前实测为明文。"
                        + "若必须加密请改用 REQUIRED 及以上", label, ssl.mode());
            }
        }
        return state;
    }

    public static State probe(Connection conn, String dbType) {
        if (conn == null) {
            return UNKNOWN;
        }
        String t = dbType == null ? "" : dbType.trim().toLowerCase();
        try {
            if ("postgresql".equals(t)) {
                return probePostgres(conn);
            }
            if ("mysql".equals(t) || "tidb".equals(t)) {
                return probeMysql(conn);
            }
        } catch (Exception e) {
            logger.debug("读取加密状态失败 (dbType={}): {}", dbType, e.getMessage());
        }
        return UNKNOWN;
    }

    private static State probeMysql(Connection conn) throws Exception {
        String cipher = statusValue(conn, "Ssl_cipher");
        if (cipher == null) {
            return UNKNOWN;
        }
        if (cipher.isEmpty()) {
            return new State(false, null, null);
        }
        String version = statusValue(conn, "Ssl_version");
        return new State(true, emptyToNull(version), cipher);
    }

    private static String statusValue(Connection conn, String name) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW STATUS LIKE '" + name + "'")) {
            return rs.next() ? nullSafe(rs.getString(2)) : null;
        }
    }

    private static State probePostgres(Connection conn) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()")) {
            if (!rs.next()) {
                return UNKNOWN;
            }
            if (!rs.getBoolean(1)) {
                return new State(false, null, null);
            }
            return new State(true, emptyToNull(nullSafe(rs.getString(2))),
                             emptyToNull(nullSafe(rs.getString(3))));
        }
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s.trim();
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }
}
