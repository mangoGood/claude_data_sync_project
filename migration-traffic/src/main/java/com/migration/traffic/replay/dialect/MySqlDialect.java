package com.migration.traffic.replay.dialect;

import com.migration.common.ssl.SslMaterial;
import com.migration.traffic.capture.StatementClassifier;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.migration.common.crypto.CredentialCipher;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * MySQL 方言。
 *
 * <p>这里的每一行都是从原 {@code SessionRunner}/{@code TrafficReplayRunner} 平移过来的，
 * 行为一字未改——B1 是纯重构批次，验收标准就是"MySQL 的一切照旧"。
 */
public final class MySqlDialect implements TargetDialect {

    private static final Logger logger = LoggerFactory.getLogger(MySqlDialect.class);

    @Override
    public TrafficEngine engine() {
        return TrafficEngine.MYSQL;
    }

    @Override
    public Connection connect(Properties props, String db) throws SQLException {
        return DriverManager.getConnection(jdbcUrl(props),
                props.getProperty("target.db.username", "root"),
                CredentialCipher.decrypt(props.getProperty("target.db.password", "")));
    }

    @Override
    public String describeTarget(Properties props, String db) {
        return "mysql://" + props.getProperty("target.db.host", "localhost")
                + ":" + props.getProperty("target.db.port", defaultPort());
    }

    /** URL 与改造前逐字符一致——判据里有比对 URL 文本的用例。 */
    static String jdbcUrl(Properties props) {
        String host = props.getProperty("target.db.host", "localhost");
        String port = props.getProperty("target.db.port", "3306");
        return String.format("jdbc:mysql://%s:%s/?%s"
                        + "&serverTimezone=UTC&characterEncoding=utf8&allowMultiQueries=false"
                        + "&allowPublicKeyRetrieval=true&connectTimeout=15000&socketTimeout=600000",
                host, port, SslMaterial.from(props, "target").mysqlUrlParams());
    }

    @Override
    public boolean connectionBoundToDatabase() {
        return false;
    }

    @Override
    public String defaultPort() {
        return "3306";
    }

    /**
     * 对齐源库的语义环境。
     *
     * <p>同一条 SQL 在 {@code sql_mode}/{@code time_zone} 不同的两个实例上行为就是不一样的
     * （严格模式下报错的插入，宽松模式下会被截断后写进去）。不对齐的话，回放报告里的
     * "差异"有一半是环境差异，没法用。
     */
    @Override
    public void onConnect(Connection c, SourceFingerprint fp) {
        if (fp == null) return;
        exec(c, "SET SESSION sql_mode = '" + esc(fp.sqlMode) + "'", fp.sqlMode);
        if (fp.timeZone != null && !fp.timeZone.isEmpty() && !"SYSTEM".equalsIgnoreCase(fp.timeZone)) {
            exec(c, "SET SESSION time_zone = '" + esc(fp.timeZone) + "'", fp.timeZone);
        }
        if (fp.charset != null && !fp.charset.isEmpty()) {
            exec(c, "SET NAMES " + fp.charset.replaceAll("[^A-Za-z0-9_]", ""), fp.charset);
        }
    }

    @Override
    public String switchSchema(Connection c, String db, String schema) {
        if (db == null || db.isEmpty()) return null;
        try (Statement st = c.createStatement()) {
            st.execute("USE `" + db.replace("`", "``") + "`");
            return db;
        } catch (SQLException e) {
            // 目标库可能还没建这个库（录制里的 CREATE DATABASE 稍后才会回放）。
            // 不能因此让整条会话失败，交给语句自己去报错。
            logger.debug("切库到 {} 失败: {}", db, e.getMessage());
            return null;
        }
    }

    @Override
    public String identity(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT @@server_uuid")) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    @Override
    public String dangerousReason(String sql) {
        if (sql == null) return null;
        int s = StatementClassifier.skipLeadingPublic(sql);
        if (s >= sql.length()) return null;
        String w0 = StatementClassifier.wordAtPublic(sql, s, 0);
        String w1 = StatementClassifier.wordAtPublic(sql, s, 1);
        String w2 = StatementClassifier.wordAtPublic(sql, s, 2);

        switch (w0) {
            case "DROP":
                if ("DATABASE".equals(w1) || "SCHEMA".equals(w1)) return "DROP DATABASE/SCHEMA：会删掉目标库的整个库";
                if ("USER".equals(w1)) return "DROP USER：会删掉目标实例上的账号";
                if ("ROLE".equals(w1)) return "DROP ROLE：会删掉目标实例上的角色";
                return null;
            case "RENAME":
                return "USER".equals(w1) ? "RENAME USER：改的是目标实例的账号" : null;
            case "SET":
                if ("GLOBAL".equals(w1)) return "SET GLOBAL：改的是目标实例的全局状态，不是业务流量";
                if ("PERSIST".equals(w1) || "PERSIST_ONLY".equals(w1)) return "SET PERSIST：会把目标实例的全局配置持久化改掉";
                return null;
            case "SHUTDOWN":
                return "SHUTDOWN：会把目标实例关掉";
            case "RESET":
                return "RESET " + w1 + "：会重置目标实例的复制/日志状态";
            case "PURGE":
                return "PURGE BINARY LOGS：会删掉目标实例的 binlog";
            case "FLUSH":
                if ("TABLES".equals(w1) && "WITH".equals(w2)) return "FLUSH TABLES WITH READ LOCK：会把目标库整库锁死";
                return null;
            case "INSTALL":
            case "UNINSTALL":
                return w0 + " PLUGIN/COMPONENT：会改变目标实例的插件装配";
            case "GRANT":
                return containsAllOnAll(sql) ? "GRANT ALL ON *.*：会在目标实例上提权" : null;
            case "ALTER":
                return "INSTANCE".equals(w1) ? "ALTER INSTANCE：改的是目标实例本身" : null;
            case "LOCK":
                return "INSTANCE".equals(w1) ? "LOCK INSTANCE FOR BACKUP：会阻塞目标实例的写入" : null;
            default:
                return null;
        }
    }

    private static boolean containsAllOnAll(String sql) {
        String u = sql.toUpperCase(Locale.ROOT);
        int on = u.indexOf(" ON ");
        if (on < 0) return false;
        // GRANT ALL [PRIVILEGES] ON *.*
        return u.substring(0, on).contains("ALL") && u.substring(on).replace(" ", "").startsWith("ON*.*");
    }

    @Override
    public TxEffect txEffect(String sql, StatementClass k) {
        if (k != StatementClass.TCL || sql == null) return TxEffect.NONE;
        int s = StatementClassifier.skipLeadingPublic(sql);
        String w0 = StatementClassifier.wordAtPublic(sql, s, 0);
        if ("BEGIN".equals(w0) || "START".equals(w0)) return TxEffect.OPEN;
        if ("COMMIT".equals(w0) || "ROLLBACK".equals(w0)) return TxEffect.CLOSE;
        return TxEffect.NONE;
    }

    @Override
    public boolean disableAutoCommit() {
        return false;
    }

    @Override
    public boolean isConnectionLost(SQLException e) {
        String state = e.getSQLState();
        return state != null && (state.startsWith("08") || "HY000".equals(state) && e.getErrorCode() == 2013);
    }

    /**
     * MySQL 录制里不会有绑定参数（{@code Execute} 行给的就是参数已替换的完整 SQL），
     * 所以这条路径正常走不到。留着是为了 v2 格式的完备性 —— 万一将来换了捕获通道
     * （审计插件/抓包）能拿到未替换的语句，绑定语义在这里已经定义好了。
     */
    @Override
    public void bind(PreparedStatement ps, List<String> binds) throws SQLException {
        if (binds == null) return;
        for (int i = 0; i < binds.size(); i++) {
            String v = binds.get(i);
            if (v == null) {
                ps.setNull(i + 1, Types.VARCHAR);
            } else {
                ps.setString(i + 1, v);
            }
        }
    }

    private static void exec(Connection c, String sql, String value) {
        if (value == null || value.isEmpty()) return;
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            logger.warn("会话环境对齐失败（{}）: {}", sql, e.getMessage());
        }
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("'", "''");
    }
}
