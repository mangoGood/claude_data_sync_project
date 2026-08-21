package com.migration.traffic.replay.dialect;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.ssl.SslMaterial;
import com.migration.traffic.capture.StatementClassifier;
import com.migration.traffic.capture.oracle.OraclePlaceholders;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.StatementClass;
import com.migration.traffic.model.TrafficEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Oracle 方言。
 *
 * <p>四处必须做对的地方：
 * <ol>
 *   <li><b>autocommit 必须关</b>。JDBC 默认 {@code autocommit=true} 会把录制里的
 *       {@code INSERT…INSERT…ROLLBACK} 变成"两条已提交 + 一条空回滚"——事务语义整个消失，
 *       而且一条错误都不报。</li>
 *   <li><b>每条会话都要显式 {@code ALTER SESSION SET CURRENT_SCHEMA}</b>。
 *       Oracle 的默认 schema 是登录用户名，而回放用的是一个统一配置的账号；
 *       不设的话所有非限定表名都会解析到回放账号自己的 schema，报一堆"表不存在"的假错误。</li>
 *   <li><b>NLS 必须对齐</b>。{@code TO_DATE('01-02-26')} 在两套 {@code NLS_DATE_FORMAT} 下
 *       是两个不同的日期，<b>不报错、值不一样</b>。</li>
 *   <li><b>事务是隐式开启的</b>。Oracle 没有 {@code BEGIN}：首条 DML 即入事务，
 *       DDL 隐式提交。会话淘汰时"不淘汰事务中会话"的判据据此而来。</li>
 * </ol>
 */
public final class OracleDialect implements TargetDialect {

    private static final Logger logger = LoggerFactory.getLogger(OracleDialect.class);

    @Override
    public TrafficEngine engine() {
        return TrafficEngine.ORACLE;
    }

    @Override
    public Connection connect(Properties props, String db) throws SQLException {
        String host = props.getProperty("target.db.host", "localhost");
        int port = intOf(props.getProperty("target.db.port", defaultPort()), 1521);
        String service = serviceOf(props);
        SslMaterial ssl = SslMaterial.from(props, "target");

        Properties p = new Properties();
        p.setProperty("user", props.getProperty("target.db.username", ""));
        p.setProperty("password", CredentialCipher.decrypt(props.getProperty("target.db.password", "")));
        // Oracle 的 thin URL 没有查询串，信任材料只能走连接属性
        ssl.applyOracleProperties(p);

        String url = ssl.enabled()
                ? ssl.oracleTcpsUrl(host, port, service)
                : String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);
        Connection c = DriverManager.getConnection(url, p);
        c.setAutoCommit(false);
        return c;
    }

    @Override
    public String describeTarget(Properties props, String db) {
        return "oracle://" + props.getProperty("target.db.host", "localhost")
                + ":" + props.getProperty("target.db.port", defaultPort()) + "/" + serviceOf(props);
    }

    private static String serviceOf(Properties props) {
        String s = props.getProperty("target.db.database", "");
        if (s.isEmpty()) s = props.getProperty("target.db.service", "");
        return s.isEmpty() ? "ORCL" : s;
    }

    @Override
    public boolean connectionBoundToDatabase() {
        return false;
    }

    @Override
    public String defaultPort() {
        return "1521";
    }

    @Override
    public void onConnect(Connection c, SourceFingerprint fp) throws SQLException {
        c.setAutoCommit(false);
        if (fp == null) return;
        alterSession(c, "NLS_DATE_FORMAT", fp.nlsDateFormat);
        alterSession(c, "NLS_TIMESTAMP_FORMAT", fp.nlsTimestampFormat);
        alterSession(c, "NLS_TIMESTAMP_TZ_FORMAT", fp.nlsTimestampTzFormat);
        alterSession(c, "NLS_NUMERIC_CHARACTERS", fp.nlsNumericCharacters);
        alterSession(c, "NLS_SORT", fp.nlsSort);
        alterSession(c, "NLS_COMP", fp.nlsComp);
        alterSession(c, "TIME_ZONE", fp.timeZone);
        if (fp.defaultSchema != null && !fp.defaultSchema.isEmpty()) {
            switchSchema(c, fp.defaultSchema, null);
        }
    }

    /**
     * {@code ALTER SESSION SET CURRENT_SCHEMA}。
     *
     * <p>schema 名不加引号：Oracle 的对象名默认大写，录制里存的就是数据字典里的大写形态；
     * 加了双引号反而会让一个本来能解析的名字解析失败（引号内是精确大小写匹配）。
     * 因此只做标识符白名单校验，不做引用。
     */
    @Override
    public String switchSchema(Connection c, String db, String schema) {
        String target = schema != null && !schema.isEmpty() ? schema : db;
        if (target == null || target.isEmpty()) return null;
        if (!isPlainIdentifier(target)) {
            logger.warn("schema 名含非常规字符，跳过切换: {}", target);
            return null;
        }
        try (Statement st = c.createStatement()) {
            st.execute("ALTER SESSION SET CURRENT_SCHEMA = " + target);
            return target;
        } catch (SQLException e) {
            // 目标库上可能还没有这个 schema（录制里的 CREATE USER 稍后才回放），
            // 不能因此让整条会话失败
            logger.debug("切 CURRENT_SCHEMA 到 {} 失败: {}", target, e.getMessage());
            return null;
        }
    }

    private static boolean isPlainIdentifier(String s) {
        if (s.isEmpty() || s.length() > 128) return false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (!Character.isLetterOrDigit(ch) && ch != '_' && ch != '$' && ch != '#') return false;
        }
        return true;
    }

    /**
     * 实例身份 = {@code DBID / 容器名}。
     *
     * <p>两侧（录制指纹与目标库实测）必须走<b>同一套查询、产出同一种形状</b>，
     * 否则"同实例"会被判成"不同实例"——那是危险的方向，会真的放行一次自我回放。
     * 所以 {@code V$DATABASE} 读不到时两边都回落到 {@code DB_NAME}。
     */
    @Override
    public String identity(Connection c) {
        String dbid = readDbid(c);
        if (dbid == null) return null;
        String con = scalar(c, "SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM dual");
        return dbid + "/" + (con == null ? "" : con);
    }

    /** {@code V$DATABASE} 未必对回放账号开放；读不到就回落到 DB_NAME（形状保持一致）。 */
    public static String readDbid(Connection c) {
        String dbid = scalar(c, "SELECT dbid FROM v$database");
        if (dbid != null && !dbid.isEmpty()) return dbid;
        return scalar(c, "SELECT SYS_CONTEXT('USERENV','DB_NAME') FROM dual");
    }

    private static String scalar(Connection c, String sql) {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            logger.debug("读取 {} 失败: {}", sql, e.getMessage());
            return null;
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
                if ("USER".equals(w1)) return "DROP USER：会删掉目标实例上的账号（带 CASCADE 时连同其所有对象）";
                if ("TABLESPACE".equals(w1)) return "DROP TABLESPACE：改的是目标实例的存储布局";
                if ("PROFILE".equals(w1)) return "DROP PROFILE：改的是目标实例的口令策略";
                if ("DIRECTORY".equals(w1)) return "DROP DIRECTORY：改的是目标实例的文件系统映射";
                if ("PLUGGABLE".equals(w1)) return "DROP PLUGGABLE DATABASE：会删掉整个 PDB";
                return null;
            case "ALTER":
                if ("SYSTEM".equals(w1)) return "ALTER SYSTEM：改的是目标实例的全局状态";
                if ("DATABASE".equals(w1)) return "ALTER DATABASE：改的是目标实例本身（含 OPEN/MOUNT/归档模式）";
                if ("PLUGGABLE".equals(w1)) return "ALTER PLUGGABLE DATABASE：改的是目标 PDB 的开闭状态";
                if ("USER".equals(w1) && containsWord(sql, "IDENTIFIED")) {
                    return "ALTER USER … IDENTIFIED BY：会改掉目标实例上账号的口令";
                }
                return null;
            case "SHUTDOWN":
            case "STARTUP":
                return w0 + "：会改变目标实例的运行状态";
            case "CREATE":
                if ("DIRECTORY".equals(w1)) return "CREATE DIRECTORY：会给目标实例开一个文件系统入口";
                if ("PLUGGABLE".equals(w1)) return "CREATE PLUGGABLE DATABASE：会在目标实例上建库";
                return null;
            case "GRANT":
                return containsWord(sql, "DBA") || containsWord(sql, "SYSDBA")
                        || containsWord(sql, "SYSOPER") ? "GRANT DBA/SYSDBA：会在目标实例上提权" : null;
            case "PURGE":
                return "DBA_RECYCLEBIN".equals(w1) ? "PURGE DBA_RECYCLEBIN：会清空目标实例的回收站" : null;
            case "TRUNCATE":
                return null;    // 截断表是正常 DDL，不在黑名单
            default:
                return null;
        }
    }

    private static boolean containsWord(String sql, String word) {
        String u = sql.toUpperCase(Locale.ROOT);
        int i = u.indexOf(word);
        while (i >= 0) {
            boolean leftOk = i == 0 || !isWordChar(u.charAt(i - 1));
            int end = i + word.length();
            boolean rightOk = end >= u.length() || !isWordChar(u.charAt(end));
            if (leftOk && rightOk) return true;
            i = u.indexOf(word, i + 1);
        }
        return false;
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
    }

    /**
     * Oracle 没有 {@code BEGIN}：<b>首条 DML 即隐式开启事务</b>，DDL 隐式提交。
     *
     * <p>这个判定只服务于"淘汰会话时不能动事务中的会话"——淘汰一条事务中的连接，
     * 该事务已执行的语句会被服务端回滚掉，而后续语句照常执行，
     * 回放结果变得毫无意义且难以察觉。
     */
    @Override
    public TxEffect txEffect(String sql, StatementClass k) {
        if (k == null) return TxEffect.NONE;
        if (k == StatementClass.DML) return TxEffect.OPEN;
        if (k == StatementClass.DDL) return TxEffect.CLOSE;      // DDL 隐式提交
        if (k != StatementClass.TCL || sql == null) return TxEffect.NONE;
        int s = StatementClassifier.skipLeadingPublic(sql);
        String w0 = StatementClassifier.wordAtPublic(sql, s, 0);
        if ("COMMIT".equals(w0) || "ROLLBACK".equals(w0)) return TxEffect.CLOSE;
        // SET TRANSACTION / SAVEPOINT 都在事务里
        return TxEffect.OPEN;
    }

    @Override
    public boolean disableAutoCommit() {
        return true;
    }

    /**
     * 有占位符却没有绑定值 = 放不了。
     *
     * <p>最常见的是 sqlplus 的 {@code EXEC :v := …}（审计里是 {@code BEGIN :v := '…'; END;}）：
     * 占位符是赋值目标，审计的 SQL_BINDS 里没有它的值。原样执行必然 ORA-01008，
     * 而它对回放本来也没有意义——真正带值的是紧随其后的 DML。
     */
    @Override
    public String unreplayableReason(com.migration.traffic.model.TrafficRecord r) {
        if (r == null || r.q == null) return null;
        if (r.b != null && !r.b.isEmpty()) return null;
        if (OraclePlaceholders.rewrite(r.q).names.isEmpty()) return null;
        return "语句含绑定占位符但录制里没有对应的值（多半是 EXEC :v := … 这类赋值块），无法回放";
    }

    /**
     * 有绑定值才走 {@link PreparedStatement}。
     *
     * <p>特别地：录制里会有 {@code BEGIN :b1 := 2; END;} 这种<b>给绑定变量赋值</b>的
     * PL/SQL 块（sqlplus 的 {@code EXEC}），它自己没有绑定值。这类语句照原样执行会因为
     * 缺少绑定而报错，但它对回放毫无意义——真正带着值的是紧随其后的那条 DML
     * （审计里给的是<b>已经取到值</b>的 SQL_BINDS）。所以让它按普通语句执行、失败也无所谓，
     * 不去为它编造绑定值。
     */
    @Override
    public boolean needsPrepared(com.migration.traffic.model.TrafficRecord r) {
        return r.b != null && !r.b.isEmpty();
    }

    @Override
    public boolean isConnectionLost(SQLException e) {
        int code = e.getErrorCode();
        // ORA-03113 通信通道文件结束 / ORA-03114 未连接 / ORA-01012 未登录 / ORA-17008 连接已关闭
        if (code == 3113 || code == 3114 || code == 1012 || code == 17008 || code == 28) return true;
        String state = e.getSQLState();
        return state != null && state.startsWith("08");
    }

    /**
     * {@code :name} → {@code ?}，并按占位符出现顺序展开绑定值。
     *
     * <p>Oracle 的 thin 驱动本身也认 {@code :name}，但<b>重复引用的语义对不上</b>：
     * {@code WHERE a=:x OR b=:x} 在位置绑定下是两个参数，而审计的 {@code SQL_BINDS}
     * 对同一个名字只给一份值。统一改写成 {@code ?} 再按名字展开，两种情形就都对了。
     */
    @Override
    public PreparedPlan prepare(String sql, List<String> binds) {
        OraclePlaceholders.Rewritten r = OraclePlaceholders.rewrite(sql);
        if (r.names.isEmpty()) {
            return new PreparedPlan(sql, binds);
        }
        return new PreparedPlan(r.sql, OraclePlaceholders.expand(r.names, binds));
    }

    /**
     * 绑定参数。
     *
     * <p>Oracle 审计里的绑定值只有文本渲染（{@code #1(1):2}），类型信息在源端就丢了，
     * 靠 Oracle 的隐式转换 + NLS 对齐还原。{@code #1(0):} 是 <b>NULL</b>——
     * Oracle 里空串本来就等于 NULL，所以这里没有歧义，但绑成空串在 NOT NULL 列上照样会错。
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

    private static void alterSession(Connection c, String param, String value) {
        if (value == null || value.isEmpty()) return;
        try (Statement st = c.createStatement()) {
            st.execute("ALTER SESSION SET " + param + " = '" + value.replace("'", "''") + "'");
        } catch (SQLException e) {
            logger.warn("会话环境对齐失败（{}={}）: {}", param, value, e.getMessage());
        }
    }

    private static int intOf(String s, int def) {
        try {
            return Integer.parseInt(s.trim());
        } catch (RuntimeException e) {
            return def;
        }
    }
}
