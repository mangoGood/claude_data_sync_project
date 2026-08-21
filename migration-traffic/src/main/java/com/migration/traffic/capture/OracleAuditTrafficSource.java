package com.migration.traffic.capture;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.ssl.SslMaterial;
import com.migration.traffic.TrafficErrorStatus;
import com.migration.traffic.capture.oracle.OracleBindParser;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.TrafficEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 基于 Oracle <b>统一审计</b>（{@code UNIFIED_AUDIT_TRAIL}）的语句流捕获。
 *
 * <p>为什么是它：redo/LogMiner 里没有 {@code SELECT}；{@code V$SQL} 是共享游标，
 * 没有逐次执行的序与时刻；ASH 是 1 秒采样还要 Diagnostics Pack 许可；
 * 传统审计的 {@code SQL_TEXT} 只有 {@code VARCHAR2(4000)}（会静默截断，
 * 而且 23ai 已经把它移除了）。统一审计是唯一同时满足"有 SELECT / 完整 / 带绑定值"的通道。
 *
 * <p>实测（Oracle AI Database 26ai Free 23.26.2.0.0）确认的关键点：
 * <ul>
 *   <li>{@code SQL_TEXT} 与 {@code SQL_BINDS} 都是 <b>CLOB</b>，不截断；</li>
 *   <li>{@code RETURN_CODE} 直接是 ORA 错误号（942/955/1），富化免费；</li>
 *   <li>口令由 Oracle 自己抹成 {@code IDENTIFIED BY *}，与 MySQL 同款不可回放；</li>
 *   <li>{@code SESSIONID} 是 3.0e17 量级的 NUMBER，<b>不能按 int 读</b>；</li>
 *   <li>审计记录写入后 <b>~50ms 内</b>就能查到（视图含内存队列，不必先 FLUSH），
 *       但读游标仍要留滞后窗口，见 {@link #LAG_SECONDS_DEFAULT}。</li>
 * </ul>
 *
 * <p><b>这条通道有一个必须说清楚的天花板</b>（实测，不是推测）：
 * <ul>
 *   <li>{@code ACTIONS ALL}（系统级）会漏掉<b>多表 SELECT</b>。实测同一批语句里
 *       {@code SELECT * FROM b4a}、{@code SELECT * FROM app_user.b4a}、{@code SELECT 1 FROM dual}
 *       都被审计了，唯独 {@code SELECT a.v FROM b4a a JOIN b4b b ON …} <b>一行都没有</b>——
 *       不报错、不告警，就是没有。真实业务负载里 join 遍地都是，
 *       只用 {@code ACTIONS ALL} 等于把一大块 SELECT 流量静默丢掉。</li>
 *   <li>补救办法是加<b>对象级</b>动作（{@code ACTIONS SELECT ON <schema>.<table>}），
 *       它能抓到 join —— 但代价是<b>一条语句按对象数出多行</b>
 *       （实测 join 出两行：APP_USER.B4A 与 APP_USER.B4B）。
 *       照单全收就是同一条语句被回放两遍。两行的 {@code ENTRY_ID} 不同、
 *       但 <b>{@code STATEMENT_ID} 相同</b>——去重就靠它。</li>
 *   <li><b>对象级动作不能和 {@code ACTIONS ALL} 放在同一条策略里</b>：实测
 *       {@code ACTIONS ALL, SELECT ON t1, SELECT ON t2} 与
 *       {@code ALTER AUDIT POLICY … ADD ACTIONS SELECT ON …} 两种写法都能建成功、
 *       都不报错，而 join 照样一行不出。必须拆成<b>两条策略</b>同时启用
 *       （一条 {@code ACTIONS ALL}、一条纯对象级），这样才两头都抓得到。</li>
 * </ul>
 */
public final class OracleAuditTrafficSource implements TrafficSource {

    private static final Logger logger = LoggerFactory.getLogger(OracleAuditTrafficSource.class);

    /**
     * 读游标的滞后窗口（秒）。
     *
     * <p>审计记录是"语句结束时生成"的，一条长语句的记录会晚于它开始的时刻出现。
     * 游标若只是 {@code event_timestamp > :last}，这类记录会被<b>永久跨过</b>——
     * 典型的"任务全绿、数据已经丢了"。留一个窗口只读足够旧的记录，
     * 再配合 {@code (SESSIONID, ENTRY_ID)} 去重，才不会漏。
     */
    private static final long LAG_SECONDS_DEFAULT = 3L;

    private Connection conn;
    private String jdbcUrl;
    private Properties connProps;
    private String taskId = "unknown";
    private String service;

    private final SourceFingerprint fingerprint = new SourceFingerprint();

    /**
     * 本次捕获创建的审计策略名与启用范围——还原时必须原样镜像。
     *
     * <p>是<b>两条</b>：{@code policyName} 走 {@code ACTIONS ALL}（覆盖 DML/DDL/DCL/TCL 与单表 SELECT），
     * {@code objectPolicyName} 走纯对象级 {@code SELECT ON …}（专门抓多表 SELECT）。
     * 两者不能合并成一条，见类注释。
     */
    private String policyName;
    private String objectPolicyName;
    private String policyScope = "";
    private volatile boolean policyEnabled;
    private volatile boolean closed;
    private volatile boolean restored;

    private long t0Micros;
    private long lagSeconds = LAG_SECONDS_DEFAULT;
    private long pollIntervalMs = 1000L;
    private long lastPollAt;
    private long batchLimit = 20_000L;
    private long lastBatchRows = -1;

    /** 游标：已消费到的时刻，以及该时刻上已经消费过的 (会话, 序号)。 */
    private Timestamp cursorTs;
    private final Set<String> consumedAtCursor = new LinkedHashSet<>();
    /** 已消费的 (会话, STATEMENT_ID)：对象级审计下同一条语句会出多行，靠它collapse 成一条。 */
    private final Set<String> statementKeys = new LinkedHashSet<>();
    /** 采集账号名，用于滤掉自身语句（强制审计的策略 DDL 挡不住）。 */
    private String captureUser;

    /** 要额外做对象级 SELECT 审计的 schema（不配就抓不到多表 SELECT，见类注释）。 */
    private final List<String> selectSchemas = new ArrayList<>();
    private int selectMaxObjects = 200;

    private boolean purgeAudit = true;
    private Timestamp lastPurgedTo;
    private long purgeIntervalMs = 60_000L;
    private long lastPurgeAt;

    @Override
    public void open(Properties cfg) throws Exception {
        this.taskId = cfg.getProperty("task.id", "unknown");
        String host = cfg.getProperty("source.db.host", "localhost");
        int port = intOf(cfg.getProperty("source.db.port", "1521"), 1521);
        this.service = cfg.getProperty("source.db.database", "");
        if (service.isBlank()) service = cfg.getProperty("source.db.service", "ORCL");
        String user = cfg.getProperty("source.db.username", "");
        String password = CredentialCipher.decrypt(cfg.getProperty("source.db.password", ""));

        SslMaterial ssl = SslMaterial.from(cfg, "source");
        this.connProps = new Properties();
        this.captureUser = user;
        connProps.setProperty("user", user);
        connProps.setProperty("password", password);
        ssl.applyOracleProperties(connProps);
        this.jdbcUrl = ssl.enabled()
                ? ssl.oracleTcpsUrl(host, port, service)
                : String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, service);

        this.lagSeconds = longProp(cfg, "traffic.capture.oracle.lag.seconds", LAG_SECONDS_DEFAULT);
        this.pollIntervalMs = longProp(cfg, "traffic.capture.oracle.poll.ms", 1000L);
        this.batchLimit = longProp(cfg, "traffic.capture.oracle.batch.rows", 20_000L);
        this.purgeAudit = Boolean.parseBoolean(cfg.getProperty("traffic.capture.oracle.purge", "true"));
        this.purgeIntervalMs = longProp(cfg, "traffic.capture.oracle.purge.interval.ms", 60_000L);
        this.policyName = cfg.getProperty("traffic.capture.oracle.policy",
                "SYNCTASK_TRF_" + sanitize(taskId));
        this.objectPolicyName = policyName + "_O";
        this.selectMaxObjects = (int) longProp(cfg, "traffic.capture.oracle.select.max.objects", 200L);
        String selSchemas = cfg.getProperty("traffic.capture.oracle.select.schemas", "");
        if (selSchemas.isBlank()) selSchemas = cfg.getProperty("traffic.capture.databases", "");
        for (String x : selSchemas.split(",")) {
            String v = x.trim().replaceAll("[^A-Za-z0-9_$#]", "");
            if (!v.isEmpty()) selectSchemas.add(v.toUpperCase(Locale.ROOT));
        }

        connect();
        readFingerprint();
        requireUnifiedAuditing();
        // 原点必须在建策略<b>之前</b>取：先取原点再开审计，最坏是漏掉极短一瞬的语句；
        // 反过来会让最早几条语句算出负偏移
        this.t0Micros = readSourceNowMicros();
        this.cursorTs = readSourceTimestamp();
        enableAuditPolicy(cfg);
        logger.info("Oracle 统一审计捕获已开启: {}, policy={} {}, t0={}us, 滞后窗口={}s",
                jdbcUrl, policyName, policyScope, t0Micros, lagSeconds);
    }

    @Override
    public String captureBackend() {
        return "ORA_UNIFIED_AUDIT";
    }

    @Override
    public long t0Micros() {
        return t0Micros;
    }

    private void connect() throws SQLException {
        conn = DriverManager.getConnection(jdbcUrl, connProps);
        conn.setAutoCommit(true);
    }

    private void readFingerprint() {
        fingerprint.engine = TrafficEngine.ORACLE.wireName();
        fingerprint.version = scalar("SELECT banner_full FROM v$version WHERE ROWNUM = 1");
        if (fingerprint.version == null) {
            fingerprint.version = scalar("SELECT banner FROM v$version WHERE ROWNUM = 1");
        }
        // 身份串两侧必须<b>同形</b>：读不到 V$DATABASE 时两边都回落 DB_NAME，
        // 否则"同实例"会被判成"不同实例"——那是危险的方向
        fingerprint.dbid = com.migration.traffic.replay.dialect.OracleDialect.readDbid(conn);
        fingerprint.conName = scalar("SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM dual");
        fingerprint.conUid = scalar("SELECT SYS_CONTEXT('USERENV','CON_ID') FROM dual");
        fingerprint.defaultSchema = scalar("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual");
        fingerprint.dbName = service;
        fingerprint.nlsDateFormat = nlsParam("NLS_DATE_FORMAT");
        fingerprint.nlsTimestampFormat = nlsParam("NLS_TIMESTAMP_FORMAT");
        fingerprint.nlsTimestampTzFormat = nlsParam("NLS_TIMESTAMP_TZ_FORMAT");
        fingerprint.nlsNumericCharacters = nlsParam("NLS_NUMERIC_CHARACTERS");
        fingerprint.nlsSort = nlsParam("NLS_SORT");
        fingerprint.nlsComp = nlsParam("NLS_COMP");
        fingerprint.characterSet = scalar(
                "SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_CHARACTERSET'");
        fingerprint.charset = fingerprint.characterSet;
        fingerprint.timeZone = scalar("SELECT SESSIONTIMEZONE FROM dual");
    }

    private String nlsParam(String name) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT value FROM nls_session_parameters WHERE parameter = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    private void requireUnifiedAuditing() throws SQLException {
        String v = scalar("SELECT value FROM v$option WHERE parameter = 'Unified Auditing'");
        if (v != null && !"TRUE".equalsIgnoreCase(v)) {
            String msg = "源库未启用统一审计（Unified Auditing=" + v + "）。"
                    + "12c 以下或以混合模式运行的实例无法用这条通道捕获语句流";
            TrafficErrorStatus.report(taskId, "E3129", msg);
            throw new SQLException(msg);
        }
    }

    /**
     * 建立并启用审计策略。
     *
     * <p>{@code ONLY TOPLEVEL} 不是可选项：不加的话 Oracle 自己的递归字典 SQL
     * 会把审计淹掉（一条业务语句能带出几十条递归 SQL），录制既没法看也没法回放。
     *
     * <p>范围（{@code BY} / {@code EXCEPT}）必须记下来：还原时 {@code NOAUDIT} 要原样镜像，
     * 否则策略<b>根本不会被停掉</b>（实测，见 {@link #close()}）。
     */
    private void enableAuditPolicy(Properties cfg) throws SQLException {
        String users = cfg.getProperty("traffic.capture.users", "").trim();
        String except = cfg.getProperty("traffic.capture.oracle.except", "").trim();
        if (!users.isEmpty()) {
            policyScope = "BY " + joinIdentifiers(users);
        } else if (!except.isEmpty()) {
            policyScope = "EXCEPT " + joinIdentifiers(except);
        } else {
            // 不限定范围时，至少把采集账号自己排掉：它的轮询 SQL 会被审计，
            // 那是纯自噪声（实测 sqlplus 自己的探测语句也会被完整记下来）
            String self = connProps.getProperty("user", "");
            policyScope = self.isEmpty() ? "" : "EXCEPT " + joinIdentifiers(self);
        }

        try (Statement st = conn.createStatement()) {
            // 上一轮没还原干净的同名策略：先清掉，否则 CREATE 会撞名
            dropExistingPolicy(st);
            st.execute("CREATE AUDIT POLICY " + policyName + " ACTIONS ALL ONLY TOPLEVEL");
            st.execute("AUDIT POLICY " + policyName + (policyScope.isEmpty() ? "" : " " + policyScope));
            policyEnabled = true;
            createObjectSelectPolicy(st);
        } catch (SQLException e) {
            TrafficErrorStatus.report(taskId, "E3129",
                    "创建/启用审计策略失败（需要 AUDIT_ADMIN 角色）: " + e.getMessage());
            throw e;
        }
        if (!isPolicyEnabled()) {
            String msg = "审计策略 " + policyName + " 已下发但未出现在 audit_unified_enabled_policies 中，"
                    + "语句流不会产生，已拒绝启动";
            TrafficErrorStatus.report(taskId, "E3129", msg);
            throw new SQLException(msg);
        }
    }

    /**
     * 给指定 schema 的表补对象级 {@code SELECT} 动作。
     *
     * <p>为什么非补不可：{@code ACTIONS ALL} <b>抓不到多表 SELECT</b>（实测，见类注释）。
     * 不补的话，一个 join 遍地的业务库跑出来的录制会缺掉一大片查询流量，
     * 而且完全没有任何迹象——录制条数看着挺多，就是少了 join。
     *
     * <p>代价是同一条语句按对象数出多行，由 {@code (SESSIONID, STATEMENT_ID)} 去重收拾。
     */
    private void createObjectSelectPolicy(Statement st) {
        if (selectSchemas.isEmpty()) {
            logger.warn("未配置对象级 SELECT 审计的 schema —— 多表 SELECT（join）不会被捕获。"
                    + "如需完整的查询流量，请配置 traffic.capture.databases 或 "
                    + "traffic.capture.oracle.select.schemas");
            objectPolicyName = null;
            return;
        }
        List<String> objects = listTables();
        if (objects.isEmpty()) {
            logger.warn("schema {} 下没有可审计的表，多表 SELECT 仍然抓不到", selectSchemas);
            objectPolicyName = null;
            return;
        }
        StringBuilder sb = new StringBuilder("CREATE AUDIT POLICY ").append(objectPolicyName)
                .append(" ACTIONS ");
        for (int i = 0; i < objects.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("SELECT ON ").append(objects.get(i));
        }
        try {
            st.execute(sb.toString());
            st.execute("AUDIT POLICY " + objectPolicyName
                    + (policyScope.isEmpty() ? "" : " " + policyScope));
            logger.info("已为 {} 张表建立对象级 SELECT 审计策略 {}（多表 SELECT 才抓得到）",
                    objects.size(), objectPolicyName);
        } catch (SQLException e) {
            logger.error("建立对象级 SELECT 审计策略失败，多表 SELECT 将不会被捕获: {}", e.getMessage());
            objectPolicyName = null;
        }
    }

    private List<String> listTables() {
        List<String> out = new ArrayList<>();
        StringBuilder in = new StringBuilder();
        for (String s : selectSchemas) {
            if (in.length() > 0) in.append(',');
            in.append('\'').append(s.replace("'", "''")).append('\'');
        }
        // DBA_TABLES 优先：ALL_TABLES 只列出<b>采集账号自己有权访问的表</b>，
        // 而采集账号（只有审计相关权限）对业务 schema 通常一张表都看不到——
        // 结果是"补了个寂寞"，多表 SELECT 照样抓不到，且没有任何报错
        for (String view : new String[]{"dba_tables", "all_tables"}) {
            String sql = "SELECT owner || '.' || table_name FROM " + view + " WHERE owner IN (" + in
                    + ") AND ROWNUM <= " + Math.max(1, selectMaxObjects);
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) out.add(rs.getString(1));
                if (!out.isEmpty()) return out;
            } catch (SQLException e) {
                logger.debug("从 {} 列举待审计的表失败: {}", view, e.getMessage());
            }
        }
        if (out.isEmpty()) {
            logger.warn("列举 {} 下的表失败或为空（采集账号可能缺 SELECT_CATALOG_ROLE）", selectSchemas);
        }
        return out;
    }

    private void dropExistingPolicy(Statement st) {
        for (String name : new String[]{policyName, objectPolicyName}) {
            if (name == null) continue;
            try (ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) FROM audit_unified_policies WHERE policy_name = '"
                            + name.toUpperCase(Locale.ROOT) + "'")) {
                if (rs.next() && rs.getInt(1) > 0) {
                    logger.warn("发现同名审计策略 {}（上一轮可能未还原干净），先清理", name);
                    quiet(st, noauditFor(name, policyScope));
                    quiet(st, "NOAUDIT POLICY " + name);
                    quiet(st, "DROP AUDIT POLICY " + name);
                }
            } catch (SQLException e) {
                logger.debug("检查同名策略失败: {}", e.getMessage());
            }
        }
    }

    /** 两条策略里还有几条是启用的。还原成功的判据是它归零。 */
    private int enabledPolicyCount() {
        StringBuilder in = new StringBuilder("'").append(policyName.toUpperCase(Locale.ROOT)).append('\'');
        if (objectPolicyName != null) {
            in.append(",'").append(objectPolicyName.toUpperCase(Locale.ROOT)).append('\'');
        }
        String c = scalar("SELECT COUNT(*) FROM audit_unified_enabled_policies WHERE policy_name IN ("
                + in + ")");
        try {
            return c == null ? -1 : Integer.parseInt(c.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private boolean isPolicyEnabled() {
        return enabledPolicyCount() > 0;
    }

    @Override
    public List<RawStatement> poll() throws Exception {
        if (closed) return List.of();
        long now = System.currentTimeMillis();
        if (now - lastPollAt < pollIntervalMs) return List.of();
        lastPollAt = now;
        ensureConnection();

        List<RawStatement> out = new ArrayList<>();
        Set<String> seenThisRound = new HashSet<>();
        Timestamp maxTs = cursorTs;

        String sql = "SELECT event_timestamp, sessionid, entry_id, action_name, dbusername, "
                + "       object_schema, return_code, sql_text, sql_binds, client_program_name, "
                + "       userhost, statement_id "
                + "FROM unified_audit_trail "
                + "WHERE event_timestamp >= ? "
                + "  AND event_timestamp <= SYSTIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '" + lagSeconds + "' SECOND "
                + "ORDER BY event_timestamp, sessionid, entry_id "
                + "FETCH FIRST " + batchLimit + " ROWS ONLY";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, cursorTs);
            ps.setFetchSize(1000);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Timestamp ts = rs.getTimestamp(1);
                    BigDecimal sessionId = rs.getBigDecimal(2);
                    long entryId = rs.getLong(3);
                    String dedupKey = sessionId + "#" + entryId;
                    if (ts != null && ts.equals(cursorTs) && consumedAtCursor.contains(dedupKey)) {
                        continue;       // 重叠读进来的、上一轮已经消费过的
                    }
                    seenThisRound.add(dedupKey);
                    if (maxTs == null || (ts != null && ts.after(maxTs))) maxTs = ts;

                    // 采集账号自己的语句是纯自噪声。EXCEPT 挡不住它：
                    // CREATE/ALTER AUDIT POLICY 与 AUDIT/NOAUDIT 是 Oracle 的<b>强制审计</b>动作，
                    // 不管策略怎么配都会被记下来（实测我们自己的两条策略 DDL 就落在录制里）
                    String dbUser = rs.getString(5);
                    if (captureUser != null && captureUser.equalsIgnoreCase(dbUser)) continue;

                    // 对象级审计下，一条多表语句会按对象数出多行：ENTRY_ID 不同、
                    // STATEMENT_ID 相同。不去重就是同一条语句被回放多遍
                    String stmtKey = sessionId + "@" + rs.getLong(12);
                    if (!statementKeys.add(stmtKey)) continue;

                    RawStatement r = toRaw(rs, ts, sessionId);
                    if (r != null) out.add(r);
                }
            }
        }
        // 只保留最近一段的语句键，避免无界增长（同一条语句的多行必定紧邻）
        if (statementKeys.size() > 50_000) {
            statementKeys.clear();
        }

        if (maxTs != null && (cursorTs == null || maxTs.after(cursorTs))) {
            cursorTs = maxTs;
            consumedAtCursor.clear();
        }
        consumedAtCursor.addAll(seenThisRound);
        lastBatchRows = out.size();
        maybePurge();
        return out;
    }

    private RawStatement toRaw(ResultSet rs, Timestamp ts, BigDecimal sessionId) throws SQLException {
        String action = rs.getString(4);
        String sqlText = trimNul(clob(rs, 8));
        String binds = trimNul(clob(rs, 9));

        RawStatement r = new RawStatement();
        r.epochMicros = ts == null ? 0L : ts.getTime() * 1000L + (ts.getNanos() % 1_000_000) / 1000L;
        // SESSIONID 是 3.0e17 量级的 NUMBER：按 int 读会溢出，按 long 正好
        r.threadId = sessionId == null ? 0L : sessionId.longValue();
        r.userHost = nz(rs.getString(5)) + "@" + nz(rs.getString(11));
        // schema 上下文取 <b>DBUSERNAME</b>（登录用户）而不是 OBJECT_SCHEMA：
        // Oracle 的默认 CURRENT_SCHEMA 就是登录用户，而 OBJECT_SCHEMA 是"这条语句碰到的对象在谁名下"——
        // 匿名 PL/SQL 块的 OBJECT_SCHEMA 实测是 SYS，照它去 ALTER SESSION SET CURRENT_SCHEMA=SYS
        // 之后，同一会话里所有非限定表名都会解析错
        r.database = rs.getString(5);
        r.schema = r.database;
        r.errorCode = rs.getInt(7);
        r.binds = OracleBindParser.parse(binds);

        if ("LOGON".equalsIgnoreCase(action)) {
            r.commandType = "Connect";
            r.argument = "";
            return r;
        }
        if ("LOGOFF".equalsIgnoreCase(action)) {
            r.commandType = "Quit";
            r.argument = "";
            return r;
        }

        // 空 SQL_TEXT 的行要按 ACTION_NAME 还原：隐式 COMMIT 的文本就是空的，
        // 直接当"没有语句"丢掉会把事务边界一起丢掉
        if (sqlText == null || sqlText.isBlank()) {
            if ("COMMIT".equalsIgnoreCase(action) || "ROLLBACK".equalsIgnoreCase(action)) {
                sqlText = action.toUpperCase(Locale.ROOT);
            } else {
                return null;
            }
        }
        r.commandType = "Query";
        r.argument = sqlText;
        // Oracle 自己把口令抹成 *（与 MySQL 的 <secret> 同款），这类语句不可忠实回放
        r.redacted = isPasswordRedacted(sqlText);
        return r;
    }

    /**
     * 去掉审计里 CLOB 尾部的 NUL 与空白。
     *
     * <p>实测 {@code SQL_TEXT} 结尾带一个 {@code \u0000}：原样送给 Oracle JDBC 会报
     * ORA-00911 invalid character —— 一条本来完全正常的 INSERT 在回放时莫名其妙地失败，
     * 而录制文件里看着一切正常（NUL 在终端上是隐形的）。
     */
    public static String trimNul(String s) {
        if (s == null) return null;
        int end = s.length();
        while (end > 0) {
            char c = s.charAt(end - 1);
            if (c == '\u0000' || Character.isWhitespace(c)) {
                end--;
            } else {
                break;
            }
        }
        return end == s.length() ? s : s.substring(0, end);
    }

    /** {@code IDENTIFIED BY *} —— Oracle 在写审计时抹的，原文谁也拿不到。 */
    public static boolean isPasswordRedacted(String sql) {
        if (sql == null) return false;
        String u = sql.toUpperCase(Locale.ROOT);
        int at = u.indexOf("IDENTIFIED BY");
        if (at < 0) return false;
        int i = at + "IDENTIFIED BY".length();
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) i++;
        return i < sql.length() && sql.charAt(i) == '*';
    }

    /** CLOB 走字符流读：一批两万行、每行可能 32K，{@code getString} 会把子进程撑爆。 */
    private static String clob(ResultSet rs, int idx) throws SQLException {
        Reader r = rs.getCharacterStream(idx);
        if (r == null) return null;
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[4096];
        try {
            int n;
            while ((n = r.read(buf)) > 0) sb.append(buf, 0, n);
        } catch (java.io.IOException e) {
            throw new SQLException("读取 CLOB 失败", e);
        }
        return sb.toString();
    }

    /**
     * 清理已消费的审计记录。
     *
     * <p>不清理的后果是 Oracle 版的"写满磁盘"：{@code ACTIONS ALL} 的写入量与源库 QPS 同量级，
     * 全堆进 AUDSYS（默认在 SYSAUX），SYSAUX 满会影响<b>整个实例</b>。
     *
     * <p>清理点<b>绝不越过读游标</b>：清到游标之后就是把还没读的记录直接删掉。
     */
    private void maybePurge() {
        if (!purgeAudit || cursorTs == null) return;
        long now = System.currentTimeMillis();
        if (now - lastPurgeAt < purgeIntervalMs) return;
        lastPurgeAt = now;
        if (lastPurgedTo != null && !cursorTs.after(lastPurgedTo)) return;

        try (PreparedStatement ps = conn.prepareStatement(
                "BEGIN DBMS_AUDIT_MGMT.SET_LAST_ARCHIVE_TIMESTAMP("
                        + "audit_trail_type => DBMS_AUDIT_MGMT.AUDIT_TRAIL_UNIFIED, "
                        + "last_archive_time => ?); "
                        + "DBMS_AUDIT_MGMT.CLEAN_AUDIT_TRAIL("
                        + "audit_trail_type => DBMS_AUDIT_MGMT.AUDIT_TRAIL_UNIFIED, "
                        + "use_last_arch_timestamp => TRUE); END;")) {
            ps.setTimestamp(1, cursorTs);
            ps.execute();
            lastPurgedTo = cursorTs;
            logger.debug("已清理 {} 之前的审计记录", cursorTs);
        } catch (SQLException e) {
            // 清不掉不是"慢一点"：审计记录会一路堆到把 SYSAUX 撑爆，必须让人看见
            String msg = "清理已消费的审计记录失败（需要 AUDIT_ADMIN + DBMS_AUDIT_MGMT 执行权限）: "
                    + e.getMessage() + "。再不干预 AUDSYS/SYSAUX 会被撑爆";
            logger.error(msg);
            TrafficErrorStatus.report(taskId, "E3130", msg);
            purgeAudit = false;         // 只报一次，别每分钟刷一遍
        }
    }

    private void ensureConnection() throws SQLException {
        try {
            if (conn != null && conn.isValid(3)) return;
        } catch (SQLException ignored) {
            // 落到重连
        }
        logger.warn("Oracle 采集连接已失效，重连中");
        closeQuietly();
        connect();
    }

    @Override
    public SourceFingerprint fingerprint() {
        return fingerprint;
    }

    @Override
    public long backlog() {
        return lastBatchRows;
    }

    @Override
    public boolean isRestored() {
        return restored;
    }

    /**
     * 续录时沿用上一轮落盘的策略名与范围。
     *
     * <p>上一轮崩溃留下的策略必须用<b>它当初启用时的范围</b>去 NOAUDIT —— 范围记错了，
     * 策略停不掉（BY 不带就无效、EXCEPT 带上直接 ORA-46352），审计会一直写下去。
     */
    @Override
    public void overrideRestoreState(Map<String, String> original) {
        if (original == null || original.isEmpty()) return;
        String p = original.get("ora.policy");
        if (p != null && !p.isEmpty()) policyName = p;
        String po = original.get("ora.policy.object");
        if (po != null && !po.isEmpty()) objectPolicyName = po;
        String sc = original.get("ora.scope");
        if (sc != null) policyScope = sc;
    }

    @Override
    public Map<String, String> restoreState() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("ora.policy", nz(policyName));
        m.put("ora.policy.object", nz(objectPolicyName));
        m.put("ora.scope", nz(policyScope));
        return m;
    }

    /**
     * 停用并删除审计策略。
     *
     * <p><b>{@code NOAUDIT} 必须镜像 {@code AUDIT} 时用的范围</b>：实测
     * {@code AUDIT POLICY p BY app_user} 之后只发 {@code NOAUDIT POLICY p}（不带 BY），
     * 策略仍然留在 {@code audit_unified_enabled_policies} 里 —— 审计照旧在写，
     * 而两条 SQL 都返回成功。所以还原之后必须<b>回查归零</b>，不能只看 SQL 有没有报错。
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            if (conn != null && !conn.isClosed() && policyEnabled) {
                try (Statement st = conn.createStatement()) {
                    for (String name : new String[]{policyName, objectPolicyName}) {
                        if (name == null) continue;
                        quiet(st, noauditFor(name, policyScope));
                        quiet(st, "DROP AUDIT POLICY " + name);
                    }
                }
                restored = enabledPolicyCount() == 0;
                if (restored) {
                    logger.info("Oracle 审计策略已还原并回查归零: {}", policyName);
                } else {
                    String msg = "审计策略 " + policyName + " 仍处于启用状态——审计记录会继续堆进 AUDSYS，"
                            + "必须人工确认（NOAUDIT 必须带上启用时用的范围 " + policyScope + "）";
                    logger.error(msg);
                    TrafficErrorStatus.report(taskId, "E3126", msg);
                }
            } else if (!policyEnabled) {
                restored = true;
            }
        } catch (Exception e) {
            logger.error("还原 Oracle 审计策略失败——审计可能仍在写 AUDSYS，请人工确认", e);
        }
        closeQuietly();
    }

    // ==================== 小工具 ====================

    /**
     * 录制原点，epoch <b>微秒</b>。
     *
     * <p>{@code CAST(... AS DATE)} 会把小数秒截掉，只用它算出来的原点是<b>秒级</b>的——
     * 而审计里的 {@code EVENT_TIMESTAMP} 是微秒级，两者精度不一致会让所有偏移带上
     * 一个最多 1 秒的固定误差。这里把小数秒（{@code FF6}）单独取出来补回去。
     */
    private long readSourceNowMicros() {
        String v = scalar("SELECT TO_CHAR((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) "
                + "- TO_DATE('1970-01-01','YYYY-MM-DD')) * 86400 * 1000000 "
                + "+ TO_NUMBER(TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP),'FF6'))) FROM dual");
        try {
            return v == null ? System.currentTimeMillis() * 1000L
                    : new BigDecimal(v.trim()).longValue();
        } catch (RuntimeException e) {
            return System.currentTimeMillis() * 1000L;
        }
    }

    /** 游标起点：审计记录的 {@code EVENT_TIMESTAMP} 是 UTC，游标也必须是 UTC。 */
    private Timestamp readSourceTimestamp() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) FROM dual")) {
            return rs.next() ? rs.getTimestamp(1) : new Timestamp(System.currentTimeMillis());
        }
    }

    private String scalar(String sql) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            logger.debug("读取失败 {}: {}", sql, e.getMessage());
            return null;
        }
    }

    /**
     * 停用语句的正确形态——<b>与启用语句并不对称</b>，实测出来的三条规则：
     * <ul>
     *   <li>{@code AUDIT POLICY p BY u} → {@code NOAUDIT POLICY p BY u}。
     *       只发 {@code NOAUDIT POLICY p}（不带 BY）的话策略<b>根本不会被停掉</b>，
     *       而 SQL 返回成功；</li>
     *   <li>{@code AUDIT POLICY p EXCEPT u} → {@code NOAUDIT POLICY p}（<b>不能带 EXCEPT</b>）。
     *       带上会报 <b>ORA-46352: NOAUDIT statement with the EXCEPT clause is not allowed</b>，
     *       随后 DROP 也会因为策略仍启用而失败——审计就这么一直开着；</li>
     *   <li>无范围 → {@code NOAUDIT POLICY p}。</li>
     * </ul>
     * 无论哪条，最终结论都要靠回查 {@code audit_unified_enabled_policies} 归零，不能只看 SQL 报没报错。
     */
    public static String noauditFor(String policy, String scope) {
        String s = scope == null ? "" : scope.trim();
        if (s.toUpperCase(Locale.ROOT).startsWith("BY ")) {
            return "NOAUDIT POLICY " + policy + " " + s;
        }
        return "NOAUDIT POLICY " + policy;
    }

    private static void quiet(Statement st, String sql) {
        try {
            st.execute(sql);
        } catch (SQLException e) {
            // 策略已经不在了会报 ORA-46357 之类，属于"已经还原干净"；结论以回查为准
            logger.debug("语句执行失败（以回查结论为准）: {} -> {}", sql, e.getMessage());
        }
    }

    private void closeQuietly() {
        try {
            if (conn != null) conn.close();
        } catch (SQLException ignored) {
            // 关闭失败无处可去
        }
        conn = null;
    }

    /** 策略名只能是合法标识符，且要短——taskId 是 uuid，直接拼会超 128 字节上限。 */
    private static String sanitize(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toUpperCase(Locale.ROOT).toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == '_') sb.append(c);
            if (sb.length() >= 20) break;
        }
        return sb.length() == 0 ? "TASK" : sb.toString();
    }

    private static String joinIdentifiers(String csv) {
        StringBuilder sb = new StringBuilder();
        for (String s : csv.split(",")) {
            String v = s.trim().replaceAll("[^A-Za-z0-9_$#]", "");
            if (v.isEmpty()) continue;
            if (sb.length() > 0) sb.append(", ");
            sb.append(v.toUpperCase(Locale.ROOT));
        }
        return sb.toString();
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static int intOf(String s, int def) {
        try {
            return Integer.parseInt(s.trim());
        } catch (RuntimeException e) {
            return def;
        }
    }

    private static long longProp(Properties p, String key, long def) {
        try {
            String v = p.getProperty(key);
            return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
