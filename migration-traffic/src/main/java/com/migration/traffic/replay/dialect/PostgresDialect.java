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
import java.util.Properties;

/**
 * PostgreSQL 方言。
 *
 * <p>与 MySQL 的三处本质差异，每一处都会静默产生错误结果：
 * <ol>
 *   <li><b>连接终生绑定一个库</b>：PG 没有 {@code USE}。库名在建连时定死，
 *       会话里能切的只有 {@code search_path}。</li>
 *   <li><b>参数是文本、没有类型</b>：日志里的 {@code Parameters: $1 = '7'} 不带类型信息。
 *       按 {@code setString} 绑到 {@code WHERE id = $1}（id 是 int）会得到
 *       {@code operator does not exist: integer = character varying}。
 *       连接参数 <b>{@code stringtype=unspecified}</b> 让服务端按上下文自己推断，
 *       这是 PG 回放能跑起来的前提。</li>
 *   <li><b>{@code COPY … FROM PROGRAM} 会在数据库服务器上执行 shell 命令</b>，
 *       必须进黑名单——一条录制回放到别人的库上，等于在那台机器上执行任意命令。</li>
 * </ol>
 */
public final class PostgresDialect implements TargetDialect {

    private static final Logger logger = LoggerFactory.getLogger(PostgresDialect.class);

    @Override
    public TrafficEngine engine() {
        return TrafficEngine.POSTGRESQL;
    }

    @Override
    public Connection connect(Properties props, String db) throws SQLException {
        return DriverManager.getConnection(jdbcUrl(props, db),
                props.getProperty("target.db.username", "postgres"),
                CredentialCipher.decrypt(props.getProperty("target.db.password", "")));
    }

    @Override
    public String describeTarget(Properties props, String db) {
        return "postgresql://" + props.getProperty("target.db.host", "localhost")
                + ":" + props.getProperty("target.db.port", defaultPort()) + "/" + databaseOf(props, db);
    }

    static String jdbcUrl(Properties props, String db) {
        String host = props.getProperty("target.db.host", "localhost");
        String port = props.getProperty("target.db.port", "5432");
        String ssl = SslMaterial.from(props, "target").pgUrlParams();
        // stringtype=unspecified 是 PG 回放的前提：日志里的参数值不带类型，
        // 按 varchar 绑到 int 列上会直接报 operator does not exist
        return String.format("jdbc:postgresql://%s:%s/%s?stringtype=unspecified"
                        + "&connectTimeout=15&socketTimeout=600&ApplicationName=synctask-traffic-replay%s",
                host, port, databaseOf(props, db), ssl == null || ssl.isEmpty() ? "" : "&" + ssl);
    }

    private static String databaseOf(Properties props, String db) {
        if (db != null && !db.isEmpty()) return db;
        String cfg = props.getProperty("target.db.database", "");
        return cfg.isEmpty() ? "postgres" : cfg;
    }

    @Override
    public boolean connectionBoundToDatabase() {
        return true;
    }

    @Override
    public String defaultPort() {
        return "5432";
    }

    /**
     * 对齐源库的语义环境。
     *
     * <p>{@code DateStyle} 不一致时 {@code '01/02/2026'} 会被解释成两个不同的日期，
     * 而且<b>不报错</b>；{@code standard_conforming_strings} 不一致时字符串里的反斜杠
     * 含义就变了。这些差异不对齐，回放报告里的"差异"有一半是环境差异，没法用。
     */
    @Override
    public void onConnect(Connection c, SourceFingerprint fp) {
        if (fp == null) return;
        setConfig(c, "search_path", fp.searchPath);
        setConfig(c, "TimeZone", fp.timeZone);
        setConfig(c, "DateStyle", fp.dateStyle);
        setConfig(c, "IntervalStyle", fp.intervalStyle);
        setConfig(c, "standard_conforming_strings", fp.standardConformingStrings);
    }

    @Override
    public String switchSchema(Connection c, String db, String schema) {
        // 库由连接决定，这里只切 search_path
        if (schema == null || schema.isEmpty()) return null;
        setConfig(c, "search_path", schema);
        return db;
    }

    /**
     * 用 {@code set_config} 而不是 {@code SET x = 'v'}：
     * {@code search_path} 的值形如 {@code public,"$user"}，里面既有逗号又有双引号，
     * 拼进 {@code SET} 语句要处理标识符引用规则；{@code set_config} 收的是纯字符串，
     * 绑定参数送过去即可，没有任何拼接。
     */
    private static void setConfig(Connection c, String name, String value) {
        if (value == null || value.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("SELECT set_config(?, ?, false)")) {
            ps.setString(1, name);
            ps.setString(2, value);
            ps.execute();
        } catch (SQLException e) {
            logger.warn("会话环境对齐失败（{}={}）: {}", name, value, e.getMessage());
        }
    }

    @Override
    public String identity(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT system_identifier FROM pg_control_system()")) {
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

        switch (w0) {
            case "DROP":
                if ("DATABASE".equals(w1)) return "DROP DATABASE：会删掉目标实例上的整个库";
                if ("SCHEMA".equals(w1)) return "DROP SCHEMA：会删掉目标库里的整个 schema";
                if ("ROLE".equals(w1) || "USER".equals(w1)) return "DROP ROLE/USER：会删掉目标实例上的角色";
                if ("TABLESPACE".equals(w1)) return "DROP TABLESPACE：改的是目标实例的存储布局";
                if ("EXTENSION".equals(w1)) return "DROP EXTENSION：会卸掉目标库的扩展";
                return null;
            case "ALTER":
                if ("SYSTEM".equals(w1)) return "ALTER SYSTEM：会持久化改掉目标实例的配置";
                if (("ROLE".equals(w1) || "USER".equals(w1)) && containsWord(sql, "SUPERUSER")) {
                    return "ALTER ROLE … SUPERUSER：会在目标实例上提权";
                }
                return null;
            case "CREATE":
                if ("EXTENSION".equals(w1)) return "CREATE EXTENSION：会改变目标库的扩展装配";
                if (("ROLE".equals(w1) || "USER".equals(w1)) && containsWord(sql, "SUPERUSER")) {
                    return "CREATE ROLE … SUPERUSER：会在目标实例上建一个超级用户";
                }
                return null;
            case "COPY":
                // COPY … FROM/TO PROGRAM 在数据库服务器上执行 shell 命令
                return containsWord(sql, "PROGRAM")
                        ? "COPY … PROGRAM：会在目标数据库服务器上执行任意 shell 命令" : null;
            case "GRANT":
                return containsWord(sql, "SUPERUSER") || containsWord(sql, "pg_execute_server_program")
                        ? "GRANT：会在目标实例上提权" : null;
            case "SELECT": {
                // 函数形态的破坏性操作，只拦最要命的几个
                String u = sql.toUpperCase(java.util.Locale.ROOT);
                if (u.contains("PG_TERMINATE_BACKEND")) return "pg_terminate_backend：会掐断目标实例上的连接";
                if (u.contains("PG_PROMOTE")) return "pg_promote：会把目标备库提升为主库";
                if (u.contains("PG_DROP_REPLICATION_SLOT")) return "pg_drop_replication_slot：会删掉目标实例的复制槽";
                if (u.contains("PG_RELOAD_CONF")) return "pg_reload_conf：会重载目标实例的配置";
                return null;
            }
            default:
                return null;
        }
    }

    private static boolean containsWord(String sql, String word) {
        String u = sql.toUpperCase(java.util.Locale.ROOT);
        String w = word.toUpperCase(java.util.Locale.ROOT);
        int i = u.indexOf(w);
        while (i >= 0) {
            boolean leftOk = i == 0 || !isWordChar(u.charAt(i - 1));
            int end = i + w.length();
            boolean rightOk = end >= u.length() || !isWordChar(u.charAt(end));
            if (leftOk && rightOk) return true;
            i = u.indexOf(w, i + 1);
        }
        return false;
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    @Override
    public TxEffect txEffect(String sql, StatementClass k) {
        if (k != StatementClass.TCL || sql == null) return TxEffect.NONE;
        int s = StatementClassifier.skipLeadingPublic(sql);
        String w0 = StatementClassifier.wordAtPublic(sql, s, 0);
        if ("BEGIN".equals(w0) || "START".equals(w0)) return TxEffect.OPEN;
        // PG 的 END 是 COMMIT 的同义词
        if ("COMMIT".equals(w0) || "ROLLBACK".equals(w0) || "END".equals(w0)) return TxEffect.CLOSE;
        return TxEffect.NONE;
    }

    @Override
    public boolean disableAutoCommit() {
        return false;
    }

    @Override
    public boolean isConnectionLost(SQLException e) {
        String state = e.getSQLState();
        return state != null && state.startsWith("08");
    }

    /**
     * {@code $n} → {@code ?}。
     *
     * <p>实测不改写的后果：pgjdbc 在 {@code INSERT INTO b3 VALUES ($1,$2,$3)} 里
     * <b>一个参数标记都找不到</b>，报 "栏位索引超过许可范围：1，栏位数：0"——
     * 这句话跟"占位符语法不对"毫无关联，纯靠猜。
     */
    @Override
    public PreparedPlan prepare(String sql, List<String> binds) {
        return PgPlaceholderRewriter.rewrite(sql, binds);
    }

    /**
     * 绑定参数。
     *
     * <p>{@code Types.NULL} 让 pgjdbc 送 unspecified 类型的 NULL——与
     * {@code stringtype=unspecified} 是一套：类型信息在源端日志里就丢了，
     * 只能交给服务端按上下文推断。绑成空串是<b>另一个值</b>，不是 NULL。
     */
    @Override
    public void bind(PreparedStatement ps, List<String> binds) throws SQLException {
        if (binds == null) return;
        for (int i = 0; i < binds.size(); i++) {
            String v = binds.get(i);
            if (v == null) {
                ps.setNull(i + 1, Types.NULL);
            } else {
                ps.setString(i + 1, v);
            }
        }
    }
}
