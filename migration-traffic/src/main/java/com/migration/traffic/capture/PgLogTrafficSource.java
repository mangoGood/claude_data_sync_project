package com.migration.traffic.capture;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.ssl.SslMaterial;
import com.migration.traffic.TrafficErrorStatus;
import com.migration.traffic.capture.pg.PgLogEntry;
import com.migration.traffic.capture.pg.PgLogLineParser;
import com.migration.traffic.capture.pg.PgStatementAssembler;
import com.migration.traffic.model.RecordingManifest;
import com.migration.traffic.model.SourceFingerprint;
import com.migration.traffic.model.TrafficEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * 基于 PostgreSQL <b>服务端日志文件</b>的语句流捕获。
 *
 * <p>为什么是它：逻辑复制/WAL 里没有 {@code SELECT}（与 binlog 同一个理由出局），
 * {@code pg_stat_statements} 只有归一化模板、没有执行序也没有到达时刻，
 * {@code pg_stat_activity} 是采样。只有服务端日志同时满足"有 SELECT / 不采样 / 原文完整"。
 *
 * <p>而它<b>纯 JDBC 可达</b>（agent 与源库可以不同机）靠的是 PG 允许用 SQL 读自己的文件：
 * {@code pg_current_logfile()} 拿当前文件、{@code pg_read_binary_file(path, offset, len)}
 * 按字节偏移增量读。这也让 PG 成为三种引擎里<b>唯一有真位点</b>的一个——
 * 捕获重启后能接着读，通常不产生时间轴空洞。
 *
 * <p>实测踩到的五个坑，每一个都会让任务"全绿但录空"或"录错"：
 * <ol>
 *   <li><b>{@code logging_collector} 是 postmaster 参数</b>，关着就必须重启实例，
 *       我们不替用户重启数据库 → 预检 error（{@code E3127}）。</li>
 *   <li><b>{@code ALTER SYSTEM} 会被命令行参数静默压过</b>：SQL 返回成功、
 *       {@code pg_settings.setting} 纹丝不动、一个警告都没有 → 下发后必须回读校验（{@code E3128}）。</li>
 *   <li><b>{@code logging_collector=off} 时设 jsonlog 会被接受但什么都不产生</b>，
 *       检测点是 {@code pg_current_logfile()} 返回空。</li>
 *   <li><b>会话级消噪要单独发一条 SET</b>：同一个 simple query 里
 *       {@code SET …; SELECT …} 串在一起时整串会被一起记下来（PG 在处理查询串<b>之前</b>判定）。</li>
 *   <li><b>参数不替换进 SQL</b>：{@code execute <unnamed>: SELECT $1} + 另一份
 *       {@code Parameters: $1 = 'x'}，回放必须绑参。</li>
 * </ol>
 */
public final class PgLogTrafficSource implements TrafficSource {

    private static final Logger logger = LoggerFactory.getLogger(PgLogTrafficSource.class);

    /** 我们会改动的 GUC。原值与来源都要记下来，还原时按来源决定 SET 还是 RESET。 */
    private static final String[] GUCS = {
            "log_statement", "log_destination", "log_min_duration_statement", "log_duration"
    };

    private Connection conn;
    private String jdbcUrl;
    private String user;
    private String password;
    private String database;
    private String taskId = "unknown";

    private final SourceFingerprint fingerprint = new SourceFingerprint();
    private final Map<String, String> originalValues = new LinkedHashMap<>();
    private final Map<String, String> originalSources = new LinkedHashMap<>();

    private volatile boolean stateChanged;
    private volatile boolean closed;
    private volatile boolean restored;

    private PgLogLineParser parser;
    /** true = jsonlog（按行读、按键取值）；false = csvlog 降级档（PG 13/14）。 */
    private boolean jsonMode = true;
    private int ownPid;
    private long t0Micros;

    /** 当前正在读的日志文件与已读到的字节偏移——这就是位点。 */
    private String currentFile;
    private long offset;
    private long fileSize;

    /** 读块大小。日志行通常几百字节，1MB 一次能覆盖高 QPS 源库一轮的量。 */
    private long chunkBytes = 1L << 20;
    private long pollIntervalMs = 1000L;
    private long lastPollAt;
    private long lastBatchLines = -1;

    /**
     * 尾部未闭合语句组的宽限窗口。
     *
     * <p>一条语句的 {@code duration:}/{@code ERROR} 补充行紧跟在它后面（微秒级），
     * 但读取边界可能正好落在两者之间。策略是<b>把末尾那条语句留到下一轮再读</b>
     * （位点只推进到它的行首），从而永远不把一条语句和它的富化行拆开；
     * 若它被留了超过这个窗口还没等来后续行，说明那一组已经结束，直接放行。
     */
    private long groupWaitMs = 1500L;
    private long heldOffset = -1;
    private long heldSince;

    private boolean enrich;
    private boolean redactSecrets = true;
    private boolean logConnections;

    @Override
    public void open(Properties cfg) throws Exception {
        this.taskId = cfg.getProperty("task.id", "unknown");
        String host = cfg.getProperty("source.db.host", "localhost");
        String port = cfg.getProperty("source.db.port", "5432");
        this.user = cfg.getProperty("source.db.username", "postgres");
        this.password = CredentialCipher.decrypt(cfg.getProperty("source.db.password", ""));
        this.database = cfg.getProperty("source.db.database", "postgres");
        if (database == null || database.isBlank()) database = "postgres";
        String ssl = SslMaterial.from(cfg, "source").pgUrlParams();
        this.jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s?connectTimeout=15&socketTimeout=120%s",
                host, port, database, ssl == null || ssl.isEmpty() ? "" : "&" + ssl);

        this.enrich = Boolean.parseBoolean(cfg.getProperty("traffic.capture.enrich", "false"));
        this.redactSecrets = Boolean.parseBoolean(
                cfg.getProperty("traffic.capture.pg.redact.secrets", "true"));
        this.logConnections = Boolean.parseBoolean(
                cfg.getProperty("traffic.capture.pg.log.connections", "false"));
        this.chunkBytes = longProp(cfg, "traffic.capture.pg.chunk.bytes", 1L << 20);
        this.pollIntervalMs = longProp(cfg, "traffic.capture.pg.poll.ms", 1000L);
        this.groupWaitMs = longProp(cfg, "traffic.capture.pg.group.wait.ms", 1500L);

        connect();
        readFingerprint();
        requireLoggingCollector();
        enableStatementLog(cfg);
        // 原点在开日志之后取：PG 的日志从"现在"开始写，位点也从现在的文件末尾起，
        // 两者必须对齐，否则最早几条语句会算出负偏移
        this.t0Micros = readSourceNowMicros();
        locateCurrentFile(true);
        this.lastPollAt = 0L;
        logger.info("PG 语句日志捕获已开启: {}, 模式={}, 位点={}@{}, t0={}us",
                jdbcUrl, jsonMode ? "jsonlog" : "csvlog", currentFile, offset, t0Micros);
    }

    @Override
    public String captureBackend() {
        return jsonMode ? "PG_JSONLOG" : "PG_CSVLOG";
    }

    @Override
    public long t0Micros() {
        return t0Micros;
    }

    private void connect() throws SQLException {
        conn = DriverManager.getConnection(jdbcUrl, user, password);
        conn.setAutoCommit(true);       // ALTER SYSTEM 不能在事务块里跑
        silenceSelf();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
            if (rs.next()) ownPid = rs.getInt(1);
        }
    }

    /**
     * 让采集连接自己从日志里消失。
     *
     * <p>三条 {@code SET} <b>必须分开发</b>：实测同一个 simple query 里
     * {@code SET log_statement='none'; SELECT …} 串在一起时，PG 在处理整串<b>之前</b>
     * 就判定了要不要记录，于是连同后面的 SELECT 一起被记了下来。
     */
    private void silenceSelf() {
        for (String sql : new String[]{
                "SET log_statement = 'none'",
                "SET log_min_duration_statement = -1",
                "SET log_duration = off"}) {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            } catch (SQLException e) {
                // 没有 SET ON PARAMETER 权限时，我们自己的轮询会进日志——
                // 但 pid 过滤兜得住，只是白读一些字节
                logger.warn("会话级消噪失败（{}）：采集连接自身的语句会进日志，将按 pid 过滤: {}",
                        sql, e.getMessage());
            }
        }
    }

    private void readFingerprint() throws SQLException {
        fingerprint.engine = TrafficEngine.POSTGRESQL.wireName();
        fingerprint.version = scalar("SELECT version()");
        fingerprint.systemIdentifier = scalar("SELECT system_identifier FROM pg_control_system()");
        fingerprint.dbName = database;
        fingerprint.serverEncoding = setting("server_encoding");
        fingerprint.searchPath = setting("search_path");
        fingerprint.dateStyle = setting("DateStyle");
        fingerprint.intervalStyle = setting("IntervalStyle");
        fingerprint.standardConformingStrings = setting("standard_conforming_strings");
        fingerprint.timeZone = setting("TimeZone");
        fingerprint.logTimeZone = setting("log_timezone");
        fingerprint.charset = fingerprint.serverEncoding;

        ZoneId zone;
        try {
            zone = ZoneId.of(fingerprint.logTimeZone);
        } catch (RuntimeException e) {
            // log_timezone 认不出来时退回 UTC 并如实说明：拿日志末尾的缩写去猜是更坏的选择
            // （CST 既是美国中部也是中国标准时间，认错就是整条时间轴平移若干小时）
            logger.warn("源库 log_timezone={} 无法解析为时区，按 UTC 处理", fingerprint.logTimeZone);
            zone = ZoneId.of("UTC");
        }
        parser = new PgLogLineParser(zone);
    }

    /**
     * {@code logging_collector} 是 postmaster 参数——关着就必须重启实例。
     * 我们不替用户重启数据库，只能如实拦下来并给出确切的补救动作。
     */
    private void requireLoggingCollector() throws SQLException {
        String collector = setting("logging_collector");
        if (!"on".equalsIgnoreCase(collector)) {
            String msg = "源库 logging_collector=" + collector + "，语句流无处可读。"
                    + "它是 postmaster 参数，必须由 DBA 执行 ALTER SYSTEM SET logging_collector=on "
                    + "并重启实例后才能开始流量复制";
            TrafficErrorStatus.report(taskId, "E3127", msg);
            throw new SQLException(msg);
        }
    }

    /**
     * 开启语句日志，并<b>回读校验</b>。
     *
     * <p>回读不是保险起见：实测 {@code ALTER SYSTEM} 会被命令行参数压过——
     * {@code source='command line'} 时 {@code pg_settings.setting} 根本不变，
     * 而 SQL 返回的是成功。不校验就会得到一个"改了、没生效、录出空文件、全程零报错"的任务。
     */
    private void enableStatementLog(Properties cfg) throws SQLException {
        // 控制面的读写一律走<b>独立连接</b>。
        //
        // 原因很实在：采集连接自己 SET 过 log_statement（消噪），而 pg_settings.setting
        // 返回的是<b>当前会话的有效值</b>——在采集连接上读，读到的是我们自己刚设的 'none'，
        // 于是"原值"被记成 none、"生效校验"永远失败。实测就是这么炸的。
        try (Connection ctl = openControlConnection()) {
            for (String guc : GUCS) {
                originalValues.put(guc, setting(ctl, guc));
                originalSources.put(guc, settingSource(ctl, guc));
            }
            logger.info("源库日志开关原值: {} （来源 {}）", originalValues, originalSources);

            String wantDest = pickDestination(ctl, cfg);
            Map<String, String> want = new LinkedHashMap<>();
            want.put("log_destination", wantDest);
            want.put("log_statement", "all");
            // 富化就是把耗时也记下来。默认关：它让日志量翻倍（每条语句多一行）
            want.put("log_min_duration_statement", enrich ? "0" : "-1");
            // log_duration=on 会让同一条语句以 parse/bind/execute 三连出现 —— 必须显式关掉
            want.put("log_duration", "off");

            try {
                for (Map.Entry<String, String> e : want.entrySet()) {
                    exec(ctl, "ALTER SYSTEM SET " + e.getKey() + " = '" + e.getValue().replace("'", "''") + "'");
                }
                exec(ctl, "SELECT pg_reload_conf()");
            } catch (SQLException e) {
                TrafficErrorStatus.report(taskId, "E3120",
                        "开启源库语句日志失败（需要 superuser 或 ALTER SYSTEM ON PARAMETER 授权）: "
                                + e.getMessage());
                throw e;
            }
            stateChanged = true;
            verifyApplied(ctl, want);
        }
    }

    /** 控制面连接：不做会话级消噪，因此 {@code pg_settings.setting} 读到的就是全局有效值。 */
    private Connection openControlConnection() throws SQLException {
        Connection c = DriverManager.getConnection(jdbcUrl, user, password);
        c.setAutoCommit(true);              // ALTER SYSTEM 不能在事务块里跑
        return c;
    }

    /** PG ≥ 15 用 jsonlog；13/14 没有它，降级 csvlog（列位按版本维护，且多行 SQL 要 CSV 状态机）。 */
    private String pickDestination(Connection c, Properties cfg) {
        String forced = cfg.getProperty("traffic.capture.pg.log.destination", "");
        if (!forced.isBlank()) {
            jsonMode = forced.toLowerCase(Locale.ROOT).contains("json");
            return forced.trim();
        }
        jsonMode = serverMajorVersion(c) >= 15;
        return jsonMode ? "jsonlog" : "csvlog";
    }

    private int serverMajorVersion(Connection c) {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SHOW server_version_num")) {
            if (rs.next()) return rs.getInt(1) / 10000;
        } catch (SQLException e) {
            logger.warn("读取 server_version_num 失败，按 csvlog 降级处理: {}", e.getMessage());
        }
        return 0;
    }

    /** reload 是异步的：轮询回读，直到生效或超时。 */
    private void verifyApplied(Connection ctl, Map<String, String> want) throws SQLException {
        long deadline = System.currentTimeMillis() + 10_000L;
        Map<String, String> bad = new LinkedHashMap<>();
        while (System.currentTimeMillis() < deadline) {
            bad.clear();
            for (Map.Entry<String, String> e : want.entrySet()) {
                String now = setting(ctl, e.getKey());
                if (!matches(e.getKey(), e.getValue(), now)) {
                    bad.put(e.getKey(), now + "（来源 " + settingSource(ctl, e.getKey()) + "）");
                }
            }
            if (bad.isEmpty()) break;
            sleep(300);
        }
        if (!bad.isEmpty()) {
            String msg = "语句日志开关已下发但未生效（多半被命令行参数或 include 文件压过）: 期望 "
                    + want + "，实际 " + bad + "。继续跑只会录出一个空文件，已拒绝启动";
            TrafficErrorStatus.report(taskId, "E3128", msg);
            throw new SQLException(msg);
        }
    }

    private static boolean matches(String guc, String want, String actual) {
        if (actual == null) return false;
        if ("log_min_duration_statement".equals(guc)) {
            // PG 会把 0 显示成 "0"，-1 显示成 "-1"；带单位时可能是 "0ms"
            return actual.replaceAll("[^0-9-]", "").equals(want);
        }
        return want.equalsIgnoreCase(actual.trim());
    }

    /**
     * 定位当前日志文件。
     *
     * <p>{@code pg_current_logfile()} 返回空是<b>另一个静默陷阱</b>的检测点：
     * {@code logging_collector=off} 时设 {@code jsonlog} 会被接受
     * （{@code pg_settings.setting} 确实变了）但没有任何文件产生。
     *
     * @param fromEnd true = 从文件末尾开始（新录制只要"从现在起"的语句）
     */
    private void locateCurrentFile(boolean fromEnd) throws SQLException {
        String f = currentLogFile();
        if (f == null || f.isEmpty()) {
            String msg = "源库 pg_current_logfile() 为空——日志收集器没有产出文件（"
                    + "logging_collector 可能是 off，此时设 jsonlog 会被接受但什么都不产生）。"
                    + "语句流无处可读，已拒绝启动";
            TrafficErrorStatus.report(taskId, "E3127", msg);
            throw new SQLException(msg);
        }
        currentFile = f;
        fileSize = fileSize(f);
        offset = fromEnd ? fileSize : 0L;
    }

    private String currentLogFile() throws SQLException {
        String key = jsonMode ? "jsonlog" : "csvlog";
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_current_logfile(?)")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String v = rs.getString(1);
                    if (v != null && !v.isEmpty()) return v;
                }
            }
        }
        return scalar("SELECT pg_current_logfile()");
    }

    @Override
    public RecordingManifest.Checkpoint checkpoint() {
        return currentFile == null ? null : new RecordingManifest.Checkpoint(currentFile, offset);
    }

    /**
     * 从上次位点续读。
     *
     * <p>这是 PG 相对另外两种引擎最实在的优势：只要那个日志文件还没被轮转清掉，
     * 停摆期间的语句<b>还在文件里</b>，接着读就补回来了，不产生时间轴空洞。
     * 文件已经不在（被轮转删了/换了新文件）则老老实实从当前文件末尾开始，让上层记空洞。
     */
    @Override
    public void resumeFrom(RecordingManifest.Checkpoint cp) {
        if (cp == null || cp.file == null || cp.file.isEmpty()) return;
        try {
            long size = fileSize(cp.file);
            if (size < 0) {
                logger.warn("位点指向的日志文件已不存在（被轮转清理），无法续读: {}", cp.file);
                return;
            }
            if (cp.offset > size) {
                logger.warn("位点偏移 {} 超过文件长度 {}，按文件末尾处理: {}", cp.offset, size, cp.file);
            }
            currentFile = cp.file;
            offset = Math.min(cp.offset, size);
            fileSize = size;
            logger.info("已从位点续读: {}@{}", currentFile, offset);
        } catch (SQLException e) {
            logger.warn("按位点续读失败，将从当前文件末尾开始: {}", e.getMessage());
        }
    }

    @Override
    public List<RawStatement> poll() throws Exception {
        if (closed) return List.of();
        long now = System.currentTimeMillis();
        if (now - lastPollAt < pollIntervalMs) return List.of();
        lastPollAt = now;
        ensureConnection();

        List<PgLogEntry> entries = new ArrayList<>();
        readPending(entries);
        lastBatchLines = entries.size();
        return PgStatementAssembler.assemble(entries, ownPid, redactSecrets);
    }

    /** 读到当前可读的末尾，跨轮转时先把旧文件读干净再切新文件。 */
    private void readPending(List<PgLogEntry> out) throws SQLException {
        String live = currentLogFile();
        drainFile(currentFile, out, live != null && !live.equals(currentFile));

        if (live != null && !live.equals(currentFile)) {
            // 轮转了：中间可能还夹着别的文件（一轮之内转了两次），按修改时间补齐
            for (String mid : filesBetween(currentFile, live)) {
                drainFile(mid, out, true);
            }
            logger.info("日志文件轮转: {} → {}", currentFile, live);
            currentFile = live;
            offset = 0L;
            heldOffset = -1;
            drainFile(currentFile, out, false);
        }
    }

    /**
     * 把一个文件从 {@link #offset} 读到末尾。
     *
     * @param toEnd true = 这个文件已经被轮转掉了，一定要读干净（不留尾部宽限）
     */
    private void drainFile(String file, List<PgLogEntry> out, boolean toEnd) throws SQLException {
        if (file == null) return;
        long size = fileSize(file);
        if (size < 0) return;                 // 文件没了（被清理），只能放弃
        fileSize = size;
        StringBuilder pendingCsv = new StringBuilder();

        while (offset < size) {
            byte[] chunk = readBytes(file, offset, chunkBytes);
            if (chunk == null || chunk.length == 0) break;
            // 只处理完整行：最后一个换行符之后的字节留到下一轮（半行整行丢弃是错的，
            // 它下一轮就完整了）
            int lastNl = lastIndexOf(chunk, (byte) '\n');
            if (lastNl < 0) {
                if (chunk.length >= chunkBytes) {
                    // 一行超过一个块：加大块继续读，否则会死循环
                    chunkBytes = Math.min(chunkBytes * 4, 64L << 20);
                }
                break;
            }
            String text = new String(chunk, 0, lastNl + 1, StandardCharsets.UTF_8);
            long consumed = lastNl + 1L;
            long lineStart = offset;
            long emitUpTo = offset + consumed;

            List<long[]> spans = new ArrayList<>();      // {行起始绝对偏移, 行长度(字节)}
            List<String> lines = new ArrayList<>();
            splitLines(text, lineStart, lines, spans);

            long stop = toEnd ? emitUpTo : holdTail(lines, spans, emitUpTo);
            for (int i = 0; i < lines.size(); i++) {
                if (spans.get(i)[0] >= stop) break;
                parseLine(lines.get(i), pendingCsv, out);
            }
            offset = stop;
            if (stop < emitUpTo) break;                  // 尾部被留到下一轮，本轮到此为止
        }
    }

    /**
     * 决定本轮读到哪儿为止。
     *
     * <p>若末尾那行是一条语句，它的 {@code duration:}/{@code ERROR} 补充行可能还没写进来——
     * 把位点停在它的行首，下一轮连同后续行一起读，就永远不会把一条语句和它的富化行拆开。
     * 但不能无限等：连续两轮位点没动且超过宽限窗口，说明那一组确实结束了，放行。
     */
    private long holdTail(List<String> lines, List<long[]> spans, long emitUpTo) {
        if (lines.isEmpty()) return emitUpTo;
        PgLogEntry last = parseEntry(lines.get(lines.size() - 1));
        if (last == null || PgStatementAssembler.kindOf(last) != PgStatementAssembler.Kind.STATEMENT) {
            heldOffset = -1;
            return emitUpTo;
        }
        long start = spans.get(spans.size() - 1)[0];
        long now = System.currentTimeMillis();
        if (heldOffset == start) {
            if (now - heldSince >= groupWaitMs) {
                heldOffset = -1;
                return emitUpTo;                 // 等够了，那一组不会再有后续行
            }
        } else {
            heldOffset = start;
            heldSince = now;
        }
        return start;
    }

    private void parseLine(String line, StringBuilder pendingCsv, List<PgLogEntry> out) {
        if (line.isEmpty()) return;
        if (jsonMode) {
            PgLogEntry e = parser.parseJson(line);
            if (e != null) out.add(e);
            return;
        }
        // csvlog：字段里的换行原样落在文件里，引号闭合才算一条完整记录
        if (pendingCsv.length() > 0) pendingCsv.append('\n');
        pendingCsv.append(line);
        if (PgLogLineParser.csvRecordComplete(pendingCsv.toString())) {
            PgLogEntry e = parser.parseCsv(pendingCsv.toString());
            if (e != null) out.add(e);
            pendingCsv.setLength(0);
        }
    }

    private PgLogEntry parseEntry(String line) {
        return jsonMode ? parser.parseJson(line) : parser.parseCsv(line);
    }

    private static void splitLines(String text, long baseOffset, List<String> lines, List<long[]> spans) {
        int from = 0;
        long abs = baseOffset;
        while (from < text.length()) {
            int nl = text.indexOf('\n', from);
            if (nl < 0) break;
            String line = text.substring(from, nl);
            int bytes = line.getBytes(StandardCharsets.UTF_8).length + 1;
            lines.add(line);
            spans.add(new long[]{abs, bytes});
            abs += bytes;
            from = nl + 1;
        }
    }

    private static int lastIndexOf(byte[] a, byte b) {
        for (int i = a.length - 1; i >= 0; i--) {
            if (a[i] == b) return i;
        }
        return -1;
    }

    private byte[] readBytes(String file, long from, long len) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pg_read_binary_file(?, ?, ?, true)")) {
            ps.setString(1, file);
            ps.setLong(2, from);
            ps.setLong(3, len);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getBytes(1) : null;
            }
        }
    }

    /** 文件字节数；文件不存在返回 -1。 */
    private long fileSize(String file) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT size FROM pg_stat_file(?, true)")) {
            ps.setString(1, file);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long v = rs.getLong(1);
                    return rs.wasNull() ? -1L : v;
                }
            }
        } catch (SQLException e) {
            logger.debug("pg_stat_file({}) 失败: {}", file, e.getMessage());
        }
        return -1L;
    }

    /** 轮转过快时，两次 poll 之间可能夹着整份文件——按修改时间把它们补齐，否则那一段就丢了。 */
    private List<String> filesBetween(String oldFile, String liveFile) {
        List<String> out = new ArrayList<>();
        String dir = setting("log_directory");
        boolean relative = dir != null && !dir.startsWith("/");
        String prefix = relative ? dir + "/" : dir + "/";
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM pg_ls_logdir() WHERE modification >= "
                        + "(SELECT modification FROM pg_ls_logdir() WHERE ? LIKE '%' || name) "
                        + "AND NOT (? LIKE '%' || name) AND NOT (? LIKE '%' || name) "
                        + "ORDER BY modification")) {
            ps.setString(1, oldFile);
            ps.setString(2, oldFile);
            ps.setString(3, liveFile);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String n = rs.getString(1);
                    // 只补同一种目的地的文件（jsonlog 与 stderr 是两份）
                    if (jsonMode && !n.endsWith(".json")) continue;
                    if (!jsonMode && !n.endsWith(".csv")) continue;
                    out.add(prefix + n);
                }
            }
        } catch (SQLException e) {
            logger.warn("补齐轮转间隔文件失败（这一段可能丢失）: {}", e.getMessage());
        }
        return out;
    }

    private void ensureConnection() throws SQLException {
        try {
            if (conn != null && conn.isValid(3)) return;
        } catch (SQLException ignored) {
            // 落到重连
        }
        logger.warn("PG 采集连接已失效，重连中");
        closeQuietly();
        connect();
    }

    @Override
    public SourceFingerprint fingerprint() {
        return fingerprint;
    }

    @Override
    public long backlog() {
        if (currentFile == null) return -1L;
        // 未读字节数换算成"大约多少行"：日志行平均 300 字节上下，够当早期信号用
        long pending = Math.max(0L, fileSize - offset);
        return pending / 300L;
    }

    @Override
    public boolean isRestored() {
        return restored;
    }

    /** 续录时按上一轮落盘的原值还原，而不是按本轮读到的（那可能是我们自己留下的痕迹）。 */
    @Override
    public void overrideRestoreState(Map<String, String> original) {
        if (original == null || original.isEmpty()) return;
        for (String guc : GUCS) {
            String v = original.get("pg." + guc);
            if (v != null) originalValues.put(guc, v);
            String src = original.get("pg.src." + guc);
            if (src != null) originalSources.put(guc, src);
        }
        logger.info("按上一轮落盘的原值还原: {}", originalValues);
    }

    @Override
    public Map<String, String> restoreState() {
        Map<String, String> m = new LinkedHashMap<>();
        for (String guc : GUCS) {
            m.put("pg." + guc, originalValues.getOrDefault(guc, ""));
            m.put("pg.src." + guc, originalSources.getOrDefault(guc, ""));
        }
        return m;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            if (conn != null && !conn.isClosed() && stateChanged) {
                // 收尾前把还没读的读干净：位点上留着的那点尾巴也是数据
                restored = restoreSourceState();
            } else if (!stateChanged) {
                restored = true;
            }
        } catch (Exception e) {
            logger.error("还原源库语句日志开关失败——源库可能仍在把每条语句写进日志盘，请人工确认", e);
        }
        closeQuietly();
    }

    /** 还原四个 GUC 并<b>回读校验</b>：下发成功不等于生效（见 {@link #enableStatementLog}）。 */
    private boolean restoreSourceState() {
        String want = originalValues.getOrDefault("log_statement", "none");
        if (want == null || want.isEmpty()) want = "none";
        try (Connection ctl = openControlConnection()) {
            for (String guc : GUCS) {
                String prev = originalValues.get(guc);
                String src = originalSources.get(guc);
                if (prev == null) continue;
                if (prev.isEmpty() || "default".equalsIgnoreCase(nz(src))
                        || "command line".equalsIgnoreCase(nz(src))) {
                    exec(ctl, "ALTER SYSTEM RESET " + guc);
                } else {
                    exec(ctl, "ALTER SYSTEM SET " + guc + " = '" + prev.replace("'", "''") + "'");
                }
            }
            exec(ctl, "SELECT pg_reload_conf()");

            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                String now = setting(ctl, "log_statement");
                if (want.equalsIgnoreCase(nz(now))) {
                    logger.info("源库语句日志已还原: log_statement={}", now);
                    return true;
                }
                sleep(300);
            }
        } catch (SQLException e) {
            logger.error("还原语句日志开关失败: {}", e.getMessage());
            return false;
        }
        String msg = "源库 log_statement 未能还原到 " + want + " —— 源库仍在记录全部语句，"
                + "会把日志盘写满，必须人工确认";
        logger.error(msg);
        TrafficErrorStatus.report(taskId, "E3126", msg);
        return false;
    }

    // ==================== 小工具 ====================

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private long readSourceNowMicros() throws SQLException {
        String v = scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint");
        try {
            return v == null ? System.currentTimeMillis() * 1000L : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return System.currentTimeMillis() * 1000L;
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

    private String setting(String name) {
        return setting(conn, name);
    }

    private String setting(Connection c, String name) {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT setting FROM pg_settings WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            logger.debug("读取 GUC {} 失败: {}", name, e.getMessage());
            return null;
        }
    }

    private String settingSource(Connection c, String name) {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT source FROM pg_settings WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            return null;
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

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String nz(String s) {
        return s == null ? "" : s;
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
