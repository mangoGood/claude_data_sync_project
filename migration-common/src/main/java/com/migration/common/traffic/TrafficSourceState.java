package com.migration.common.traffic;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.io.AtomicFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * 流量复制期间被改动的<b>源端状态</b>，以及把它还原回去的能力。
 *
 * <p>为什么要单独一个类、还放在 common 里：还原这件事必须在<b>捕获进程已经不存在</b>的情况下
 * 也做得到。捕获进程被 {@code kill -9}、agent 整个硬崩、宿主断电——这些情形下
 * 谁来还原？只能是"下一个活着的人"：agent 的看门狗，或 agent 重启后的扫尾流程。
 * 而 agent 不依赖任何引擎模块（它只负责起子进程），所以这段逻辑落在 common。
 *
 * <p>不还原的后果三种引擎各不相同，但严重程度是同一级的：
 * <ul>
 *   <li><b>MySQL</b>：每条语句继续往 {@code mysql.general_log} 写，直到把源库 datadir 写满；</li>
 *   <li><b>PostgreSQL</b>：{@code log_statement=all} 继续把每条语句写进日志文件，撑爆日志盘；</li>
 *   <li><b>Oracle</b>：审计策略继续生效，审计记录堆进 AUDSYS（默认在 SYSAUX），
 *       SYSAUX 满会影响整个实例。</li>
 * </ul>
 *
 * <p>口令用与连接串同一套 AES-GCM（{@link CredentialCipher}）加密后落盘。
 */
public final class TrafficSourceState {

    private static final Logger logger = LoggerFactory.getLogger(TrafficSourceState.class);

    public static final String FILE_NAME = "source_state.properties";

    /** 还原属性前缀（引擎自定义键值对）。 */
    private static final String ATTR_PREFIX = "restore.";

    /** 引擎名：mysql / postgresql / oracle。缺省 mysql（老状态文件没有这个键）。 */
    public String engine = "mysql";

    public String host;
    public String port;
    public String username;
    /** 明文口令（内存中）；落盘时加密。 */
    public String password;
    /** 附加的 JDBC 参数（SSL 等），形如 {@code useSSL=false&...}。 */
    public String urlParams;
    /** PG：要连的数据库名；Oracle：服务名。MySQL 不用。 */
    public String database;

    /** 源库上 {@code general_log} 的原值（MySQL）。 */
    public String generalLog;
    /** 源库上 {@code log_output} 的原值（MySQL）。 */
    public String logOutput;

    /**
     * 引擎自定义的还原载荷。
     *
     * <p>PG 放四个 GUC 的原值与来源；Oracle 放策略名与<b>启用时用的确切范围</b>——
     * 后者不是可有可无的细节：实测 {@code NOAUDIT POLICY p} 不带 {@code BY <实体>} 时
     * 策略根本不会被停掉（{@code audit_unified_enabled_policies} 里还留着一行），
     * 于是审计继续往 SYSAUX 写，而 SQL 一个字的错都没报。
     */
    public final Map<String, String> attrs = new LinkedHashMap<>();

    /** 录制目录下的状态文件位置。 */
    public static File fileIn(File recordingDir) {
        return new File(recordingDir, FILE_NAME);
    }

    public TrafficSourceState put(String key, String value) {
        if (key != null && value != null) attrs.put(key, value);
        return this;
    }

    public String attr(String key, String def) {
        String v = attrs.get(key);
        return v == null || v.isEmpty() ? def : v;
    }

    /** 原子写出（写临时文件 → fsync → rename），避免留下半个文件让还原读到垃圾。 */
    public void save(File recordingDir) throws IOException {
        Properties p = new Properties();
        p.setProperty("source.engine", nz(engine));
        p.setProperty("source.host", nz(host));
        p.setProperty("source.port", nz(port));
        p.setProperty("source.username", nz(username));
        p.setProperty("source.password", CredentialCipher.encrypt(nz(password)));
        p.setProperty("source.url.params", nz(urlParams));
        p.setProperty("source.database", nz(database));
        p.setProperty("original.general_log", nz(generalLog));
        p.setProperty("original.log_output", nz(logOutput));
        for (Map.Entry<String, String> e : attrs.entrySet()) {
            p.setProperty(ATTR_PREFIX + e.getKey(), nz(e.getValue()));
        }
        AtomicFileWriter.writeProperties(fileIn(recordingDir),
                p, "流量复制：源端被改动的状态（用于兜底还原）");
    }

    /** 读回；文件不存在或不完整返回 null。 */
    public static TrafficSourceState load(File recordingDir) {
        File f = fileIn(recordingDir);
        if (!f.isFile()) return null;
        Properties p = new Properties();
        try (InputStream in = new FileInputStream(f)) {
            p.load(in);
        } catch (IOException e) {
            logger.warn("源库状态文件读取失败: {}", e.getMessage());
            return null;
        }
        TrafficSourceState s = new TrafficSourceState();
        s.engine = p.getProperty("source.engine", "mysql");
        if (s.engine.isEmpty()) s.engine = "mysql";
        s.host = p.getProperty("source.host", "");
        s.port = p.getProperty("source.port", "");
        s.username = p.getProperty("source.username", "");
        s.password = CredentialCipher.decrypt(p.getProperty("source.password", ""));
        s.urlParams = p.getProperty("source.url.params", "");
        s.database = p.getProperty("source.database", "");
        s.generalLog = p.getProperty("original.general_log", "");
        s.logOutput = p.getProperty("original.log_output", "");
        for (String name : p.stringPropertyNames()) {
            if (name.startsWith(ATTR_PREFIX)) {
                s.attrs.put(name.substring(ATTR_PREFIX.length()), p.getProperty(name));
            }
        }
        if (s.host.isEmpty() || s.port.isEmpty()) {
            logger.warn("源库状态文件缺少连接信息，无法兜底还原");
            return null;
        }
        return s;
    }

    /** 还原成功后删除状态文件——留着会让下次扫尾重复去连一个不需要还原的库。 */
    public static void clear(File recordingDir) {
        File f = fileIn(recordingDir);
        if (f.exists() && !f.delete()) {
            logger.warn("源库状态文件删除失败: {}", f.getAbsolutePath());
        }
    }

    /**
     * 连上源端把状态还原回去。
     *
     * <p>幂等：重复调用没有副作用（本来就是"设成某个确定值"）。
     *
     * @return true 表示<b>确认</b>已还原（不是"SQL 没报错"——PG 与 Oracle 都要回读校验）
     */
    public boolean restore() {
        String eng = engine == null ? "mysql" : engine.trim().toLowerCase(Locale.ROOT);
        try {
            switch (eng) {
                case "postgresql": case "postgres": case "pg":
                    return restorePostgres();
                case "oracle":
                    return restoreOracle();
                default:
                    return restoreMysql();
            }
        } catch (Exception e) {
            logger.error("源端状态兜底还原失败（{} {}:{}）——源端可能仍在记录语句流，请人工确认",
                    eng, host, port, e);
            return false;
        }
    }

    // ==================== MySQL ====================

    private boolean restoreMysql() {
        String url = String.format("jdbc:mysql://%s:%s/?%s&connectTimeout=10000&socketTimeout=30000",
                host, port, urlParams == null || urlParams.isEmpty() ? "useSSL=false" : urlParams);
        try (Connection c = DriverManager.getConnection(url, username, password);
             Statement st = c.createStatement()) {
            // 还原动作本身没必要进语句日志
            st.execute("SET SESSION sql_log_off = 1");
            boolean wasOn = "1".equals(generalLog) || "ON".equalsIgnoreCase(generalLog);
            st.execute("SET GLOBAL general_log = " + (wasOn ? "'ON'" : "'OFF'"));
            if (logOutput != null && !logOutput.isEmpty()) {
                st.execute("SET GLOBAL log_output = '" + logOutput.replace("'", "''") + "'");
            }
            st.execute("DROP TABLE IF EXISTS mysql.general_log_trf_read");
            st.execute("DROP TABLE IF EXISTS mysql.general_log_trf_next");
            logger.info("源库语句日志已兜底还原: {}:{} general_log={}, log_output={}",
                    host, port, wasOn ? "ON" : "OFF", logOutput);
            return true;
        } catch (SQLException e) {
            logger.error("源库语句日志兜底还原失败（{}:{}）——源库可能仍在写语句日志，请人工确认",
                    host, port, e);
            return false;
        }
    }

    // ==================== PostgreSQL ====================

    /** 捕获会改动的四个 GUC。顺序无所谓，但一个都不能漏。 */
    public static final String[] PG_GUCS = {
            "log_statement", "log_destination", "log_min_duration_statement", "log_duration"
    };

    /**
     * 还原 PG 的四个日志 GUC。
     *
     * <p><b>必须回读校验</b>：实测 {@code ALTER SYSTEM} 会被命令行参数（以及 include 文件）
     * 静默压过——SQL 返回成功、{@code pg_settings.setting} 纹丝不动、一个警告都没有。
     * 只看 SQL 有没有报错就宣布"还原完成"，等于把"日志盘被写满"这件事藏起来。
     */
    private boolean restorePostgres() {
        String db = database == null || database.isEmpty() ? "postgres" : database;
        String url = String.format("jdbc:postgresql://%s:%s/%s?connectTimeout=10%s",
                host, port, db, urlParams == null || urlParams.isEmpty() ? "" : "&" + urlParams);
        try (Connection c = DriverManager.getConnection(url, username, password)) {
            c.setAutoCommit(true);      // ALTER SYSTEM 不能在事务块里跑
            try (Statement st = c.createStatement()) {
                // 还原动作自身不必进日志
                trySilence(st);
                for (String guc : PG_GUCS) {
                    String prev = attrs.get("pg." + guc);
                    String source = attrs.get("pg.src." + guc);
                    if (prev == null) continue;
                    // 原来就来自默认值 / 命令行 / 配置文件的，RESET 回去比硬写一个值更忠实：
                    // 硬写会在 postgresql.auto.conf 里留下一条本来不存在的记录
                    if (prev.isEmpty() || "default".equalsIgnoreCase(nz(source))
                            || "command line".equalsIgnoreCase(nz(source))) {
                        st.execute("ALTER SYSTEM RESET " + guc);
                    } else {
                        st.execute("ALTER SYSTEM SET " + guc + " = '" + prev.replace("'", "''") + "'");
                    }
                }
                st.execute("SELECT pg_reload_conf()");
            }
            boolean ok = verifyPostgres(c);
            logger.info("PG 语句日志已兜底还原: {}:{}/{} 校验={}", host, port, db, ok ? "通过" : "未通过");
            return ok;
        } catch (SQLException e) {
            logger.error("PG 语句日志兜底还原失败（{}:{}）——源库可能仍在写语句日志，请人工确认",
                    host, port, e);
            return false;
        }
    }

    /** reload 是异步的，给它一点时间再回读；只校验最要命的那个（log_statement）。 */
    private boolean verifyPostgres(Connection c) {
        String want = attrs.get("pg.log_statement");
        if (want == null) return true;
        for (int i = 0; i < 10; i++) {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT setting FROM pg_settings WHERE name = 'log_statement'")) {
                if (rs.next()) {
                    String now = rs.getString(1);
                    if (want.isEmpty() ? "none".equalsIgnoreCase(now) : want.equalsIgnoreCase(now)) {
                        return true;
                    }
                }
            } catch (SQLException ignored) {
                // 下一轮再试
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        logger.error("PG log_statement 未能还原到 '{}' —— 源库仍在记录全部语句，会把日志盘写满，请人工确认",
                want.isEmpty() ? "none" : want);
        return false;
    }

    private static void trySilence(Statement st) {
        try {
            st.execute("SET log_statement = 'none'");
        } catch (SQLException ignored) {
            // 没有 SET ON PARAMETER 权限时忽略：多几行日志不影响还原本身
        }
    }

    // ==================== Oracle ====================

    /**
     * 停用并删除本次捕获创建的审计策略。
     *
     * <p><b>{@code NOAUDIT} 必须镜像 {@code AUDIT} 时用的范围</b>：实测
     * {@code AUDIT POLICY p BY app_user} 之后只发 {@code NOAUDIT POLICY p}（不带 BY），
     * 策略仍然留在 {@code audit_unified_enabled_policies} 里 —— 审计照旧在写，
     * 而两条 SQL 都返回成功。所以启用范围必须落盘、还原时原样镜像、事后回查归零。
     */
    private boolean restoreOracle() {
        String svc = database == null || database.isEmpty() ? "" : database;
        String url = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + svc;
        String policy = attrs.get("ora.policy");
        if (policy == null || policy.isEmpty()) {
            logger.info("Oracle 状态文件里没有审计策略名，无需还原");
            return true;
        }
        // 捕获会建<b>两条</b>策略：系统级 ACTIONS ALL + 对象级 SELECT ON（后者专抓多表 SELECT）。
        // 只停一条等于审计还开着一半
        java.util.List<String> policies = new java.util.ArrayList<>();
        policies.add(policy);
        String objPolicy = attrs.get("ora.policy.object");
        if (objPolicy != null && !objPolicy.isEmpty()) policies.add(objPolicy);

        String scope = nz(attrs.get("ora.scope"));          // 形如 "BY APP_USER" / "EXCEPT TRFCAP" / ""
        try (Connection c = DriverManager.getConnection(url, username, password);
             Statement st = c.createStatement()) {
            for (String p : policies) {
                // NOAUDIT 与 AUDIT <b>不对称</b>：BY 要原样带上，EXCEPT 反而不能带
                // （ORA-46352），带了会连同后面的 DROP 一起失败，审计就一直开着
                execQuiet(st, "NOAUDIT POLICY " + p
                        + (scope.toUpperCase(Locale.ROOT).startsWith("BY ") ? " " + scope : ""));
                execQuiet(st, "DROP AUDIT POLICY " + p);
            }
            boolean ok = verifyOracle(st, policies);
            logger.info("Oracle 审计策略已兜底还原: {}:{}/{} policy={} 校验={}",
                    host, port, svc, policies, ok ? "通过" : "未通过");
            return ok;
        } catch (SQLException e) {
            logger.error("Oracle 审计策略兜底还原失败（{}:{}）——审计可能仍在写 AUDSYS，请人工确认",
                    host, port, e);
            return false;
        }
    }

    private boolean verifyOracle(Statement st, java.util.List<String> policies) {
        StringBuilder in = new StringBuilder();
        for (String p : policies) {
            if (in.length() > 0) in.append(',');
            in.append('\'').append(p.toUpperCase(Locale.ROOT).replace("'", "''")).append('\'');
        }
        try (ResultSet rs = st.executeQuery(
                "SELECT COUNT(*) FROM audit_unified_enabled_policies WHERE policy_name IN (" + in + ")")) {
            if (rs.next() && rs.getInt(1) == 0) return true;
        } catch (SQLException e) {
            logger.warn("回查审计策略状态失败: {}", e.getMessage());
            return false;
        }
        logger.error("Oracle 审计策略 {} 仍处于启用状态 —— 审计记录会继续堆进 AUDSYS，请人工确认", policies);
        return false;
    }

    private static void execQuiet(Statement st, String sql) {
        try {
            st.execute(sql);
        } catch (SQLException e) {
            // 策略已经不在了会报 ORA-46357 之类，属于"已经还原干净"，不该当失败——
            // 最终结论以回查为准
            logger.debug("还原语句执行失败（以回查结论为准）: {} -> {}", sql, e.getMessage());
        }
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
