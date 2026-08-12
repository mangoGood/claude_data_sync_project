package com.synctask.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 从<b>服务端视角</b>问一句"这条连接到底加没加密"。
 *
 * <p>这是整套 TLS 方案里最关键的一小块。客户端参数配了 {@code sslMode=REQUIRED} 不等于
 * 连接真的加密了——驱动版本不对会<b>静默忽略</b>未知参数（Connector/J 5.1 就完全不认
 * {@code sslMode}），{@code PREFERRED} 在服务端不支持时按定义就会退回明文。
 * 只看我们自己的配置和日志，"以为加密了其实没有"是查不出来的，而它比不加密更危险：
 * 因为它能通过入网评审。
 *
 * <p>所以判据一律取服务端自己的说法：
 * <pre>
 *   MySQL/TiDB   SHOW STATUS LIKE 'Ssl_cipher' / 'Ssl_version'   （会话级状态变量）
 *   PostgreSQL   SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()
 * </pre>
 * 取不到就<b>如实说取不到</b>（{@link #unknown}），不猜、不填"已加密"。
 */
public final class TlsStateProbe {

    private static final Logger logger = LoggerFactory.getLogger(TlsStateProbe.class);

    /** 探测结果。{@code encrypted} 为 null 表示"没能问出来"，与 false（明文）不是一回事。 */
    public static final class TlsState {
        public final Boolean encrypted;
        public final String version;
        public final String cipher;

        TlsState(Boolean encrypted, String version, String cipher) {
            this.encrypted = encrypted;
            this.version = version;
            this.cipher = cipher;
        }

        /** 给人看的一句话。 */
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

    public static final TlsState UNKNOWN = new TlsState(null, null, null);

    private TlsStateProbe() {
    }

    public static TlsState unknown() {
        return UNKNOWN;
    }

    /**
     * @param dbType mysql / tidb / postgresql / 其它（其它一律返回 unknown，不猜）
     */
    public static TlsState probe(Connection conn, String dbType) {
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
            // 探测失败不能影响连接测试本身的结论——它是附加信息，不是判据的全部
            logger.debug("读取加密状态失败 (dbType={}): {}", dbType, e.getMessage());
        }
        return UNKNOWN;
    }

    private static TlsState probeMysql(Connection conn) throws Exception {
        String cipher = statusValue(conn, "Ssl_cipher");
        if (cipher == null) {
            return UNKNOWN;
        }
        if (cipher.isEmpty()) {
            return new TlsState(false, null, null);
        }
        String version = statusValue(conn, "Ssl_version");
        return new TlsState(true, version == null || version.isEmpty() ? null : version, cipher);
    }

    /** {@code SHOW STATUS} 的会话级变量：第二列是值。变量不存在时返回 null（= 问不出来）。 */
    private static String statusValue(Connection conn, String name) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW STATUS LIKE '" + name + "'")) {
            return rs.next() ? nullSafe(rs.getString(2)) : null;
        }
    }

    private static TlsState probePostgres(Connection conn) throws Exception {
        String sql = "SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()";
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                return UNKNOWN;
            }
            boolean ssl = rs.getBoolean(1);
            if (!ssl) {
                return new TlsState(false, null, null);
            }
            return new TlsState(true, nullSafe(rs.getString(2)), nullSafe(rs.getString(3)));
        }
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s.trim();
    }
}
