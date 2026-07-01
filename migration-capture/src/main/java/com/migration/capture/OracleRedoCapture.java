package com.migration.capture;

import com.migration.common.AbstractCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Oracle Redo Log 捕获器。
 *
 * <p>使用 Oracle LogMiner 持续挖掘在线 redo 日志和归档日志，捕获 DML 变更（INSERT/UPDATE/DELETE）。
 * 输出格式与 MySQLBinlogCapture / PostgresWalCapture 保持一致：
 * <pre>
 *   eventType \001 scn \001 scnNumeric \001 timestamp \001 xid \001 eventData \n
 * </pre>
 * 其中 eventData 格式为：schema:OWNER table:TABLE_NAME primary_keys:COL1,COL2 new-tuple:{...} old-tuple:{...}
 *
 * <p>位点使用 SCN（System Change Number）作为断点续传标识。
 */
public class OracleRedoCapture extends AbstractCapture<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(OracleRedoCapture.class);

    private static final char FIELD_SEP = '\001';
    private static final char RECORD_SEP = '\n';

    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private String startScn;
    private String outputDir;
    private String taskId;

    private Connection conn;
    /** PDB 连接，用于查询源表数据（处理 UNSUPPORTED 操作时使用） */
    private Connection pdbConn;
    private BufferedWriter writer;
    private final AtomicLong eventCounter = new AtomicLong(0);
    private final AtomicLong fileCounter = new AtomicLong(0);
    private long maxEventsPerFile = 10000;
    private long currentFileEvents = 0;

    /** 当前已消费到的 SCN（字符串形式） */
    private volatile String currentScn;
    /** 当前已消费到的 SCN（数值形式，用于比较） */
    private volatile long currentScnNumeric;
    /** LogMiner 会话已添加的最大日志序列号 */
    private volatile int lastAddedLogSeq = -1;

    /**
     * Oracle PDB 中无法执行 DBMS_LOGMNR.ADD_LOGFILE / START_LOGMNR（ORA-65040），
     * 因此 LogMiner 操作必须在 CDB root 执行。以下配置用于建立 CDB root 连接。
     * 默认启用，自动从 V$DATABASE.NAME 推断 CDB service 名。
     */
    private boolean cdbEnabled = true;
    /** 显式指定的 CDB service 名；为空则自动用 V$DATABASE.NAME 推断 */
    private String cdbService = "";
    /** CDB 连接用户名（含角色，如 "sys as sysdba"） */
    private String cdbUsername = "sys";
    /** CDB 连接密码；默认复用源库密码 */
    private String cdbPassword = "";
    /** CDB 连接角色 */
    private String cdbRole = "SYSDBA";
    /** 是否已成功使用 CDB 连接 */
    private volatile boolean cdbConnected = false;

    /** 需要同步的 schema 集合（为空表示不过滤） */
    private Set<String> syncedSchemas = new HashSet<>();
    /** 需要同步的表集合（格式：OWNER.TABLE，为空表示不过滤） */
    private Set<String> syncedTables = new HashSet<>();

    /** 表列信息缓存：OWNER.TABLE -> 列名列表 */
    private final Map<String, List<String>> tableColumnsCache = new java.util.concurrent.ConcurrentHashMap<>();
    /** 表列类型缓存：OWNER.TABLE -> 列类型列表 */
    private final Map<String, List<String>> tableColumnTypesCache = new java.util.concurrent.ConcurrentHashMap<>();
    /** 表主键缓存：OWNER.TABLE -> 主键列名列表 */
    private final Map<String, List<String>> tablePrimaryKeysCache = new java.util.concurrent.ConcurrentHashMap<>();

    // 背压控制
    private volatile boolean backpressurePaused = false;
    private String backpressureSignalPath;

    /** LogMiner 扫描间隔（毫秒） */
    private long scanIntervalMs = 1000;
    /** 每次查询 LogMiner 的最大行数 */
    private int queryBatchSize = 1000;

    @Override
    protected void doInitialize() throws Exception {
        host = props.getProperty("source.db.host", "localhost");
        port = Integer.parseInt(props.getProperty("source.db.port", "1521"));
        database = props.getProperty("source.db.database", "ORCL");
        user = props.getProperty("source.db.username", "SYSTEM");
        password = props.getProperty("source.db.password", "");
        startScn = props.getProperty("capture.redo.scn", "");
        outputDir = props.getProperty("capture.output.dir", "binlog_output");
        taskId = props.getProperty("task.id", "unknown");
        maxEventsPerFile = Long.parseLong(props.getProperty("capture.max.events.per.file", "10000"));
        scanIntervalMs = Long.parseLong(props.getProperty("capture.redo.scan.interval", "1000"));
        queryBatchSize = Integer.parseInt(props.getProperty("capture.redo.batch.size", "1000"));
        backpressureSignalPath = "files/" + taskId + "/backpressure.signal";

        // CDB root 连接配置（LogMiner 必须在 CDB 执行，PDB 会报 ORA-65040）
        cdbEnabled = Boolean.parseBoolean(props.getProperty("capture.redo.cdb.enabled", "true"));
        cdbService = props.getProperty("capture.redo.cdb.service", "");
        cdbUsername = props.getProperty("capture.redo.cdb.username", "sys");
        cdbPassword = props.getProperty("capture.redo.cdb.password", password);
        cdbRole = props.getProperty("capture.redo.cdb.role", "SYSDBA");

        if (startScn.isEmpty()) {
            startScn = null;
        }

        parseSyncObjects();

        logger.info("Oracle Redo Capture initialized - host={}:{} database={} user={} outputDir={} taskId={} startScn={} syncedSchemas={} syncedTables={} cdbEnabled={} cdbService={}",
                host, port, database, user, outputDir, taskId, startScn, syncedSchemas, syncedTables, cdbEnabled, cdbService);
    }

    /**
     * 解析同步对象配置，构建 schema 和表过滤集合。
     * Oracle 中 schema 通常等于用户名（大写），配置格式与 MySQL/PG 一致：
     * migration.sync.objects={"SCHEMA_NAME":{"tables":["T1","T2"]}}
     */
    private void parseSyncObjects() {
        String syncObjectsJson = props.getProperty("migration.sync.objects", "");
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            syncObjectsJson = props.getProperty("sync.objects", "");
        }

        if (syncObjectsJson != null && !syncObjectsJson.isEmpty() && syncObjectsJson.startsWith("{")) {
            try {
                String json = syncObjectsJson.replace("\\\"", "\"");
                Pattern dbPattern = Pattern.compile("\"([^\"]+)\"\\s*:\\s*\\{\\s*\"tables\"\\s*:\\s*\\[([^\\]]*)\\]");
                Matcher matcher = dbPattern.matcher(json);
                while (matcher.find()) {
                    String schemaName = matcher.group(1).toUpperCase();
                    String tablesStr = matcher.group(2);
                    syncedSchemas.add(schemaName);
                    Pattern tablePattern = Pattern.compile("\"([^\"]+)\"");
                    Matcher tableMatcher = tablePattern.matcher(tablesStr);
                    while (tableMatcher.find()) {
                        syncedTables.add(schemaName + "." + tableMatcher.group(1).toUpperCase());
                    }
                }
            } catch (Exception e) {
                logger.warn("解析sync.objects失败，将捕获所有 schema 事件: {}", e.getMessage());
            }
        }

        if (syncedSchemas.isEmpty()) {
            String schemas = props.getProperty("migration.included.databases", "");
            if (schemas != null && !schemas.isEmpty()) {
                for (String s : schemas.split(",")) {
                    String trimmed = s.trim().toUpperCase();
                    if (!trimmed.isEmpty()) {
                        syncedSchemas.add(trimmed);
                    }
                }
            }
        }

        if (syncedTables.isEmpty()) {
            String tables = props.getProperty("migration.included.tables", "");
            if (tables != null && !tables.isEmpty()) {
                for (String t : tables.split(",")) {
                    String trimmed = t.trim().toUpperCase();
                    if (!trimmed.isEmpty()) {
                        syncedTables.add(trimmed);
                        if (trimmed.contains(".")) {
                            syncedSchemas.add(trimmed.substring(0, trimmed.indexOf('.')));
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void doStart() throws Exception {
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        openNewOutputFile();

        Class.forName("oracle.jdbc.OracleDriver");

        // 先连接 PDB（源库），用于探测 CDB 名称和起始 SCN
        String pdbUrl = String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, database);
        pdbConn = DriverManager.getConnection(pdbUrl, user, password);
        pdbConn.setAutoCommit(false);
        logger.info("Connected to Oracle PDB for metadata probe: {}", database);

        // 探测 CDB 名称（V$DATABASE.NAME），用于连接 CDB root 执行 LogMiner
        String cdbName = "";
        try (Statement stmt = pdbConn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT NAME FROM V$DATABASE")) {
            if (rs.next()) {
                cdbName = rs.getString(1);
                logger.info("Detected Oracle CDB name: {}", cdbName);
            }
        } catch (SQLException e) {
            logger.warn("无法查询 V$DATABASE.NAME（可能权限不足）: {}", e.getMessage());
        }

        // 确定起始 SCN
        if (startScn != null && !startScn.isEmpty()) {
            currentScn = startScn;
            currentScnNumeric = Long.parseLong(startScn);
            logger.info("Starting Oracle LogMiner from checkpoint SCN: {}", startScn);
        } else {
            // 从当前 SCN 开始
            try (Statement stmt = pdbConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
                if (rs.next()) {
                    currentScnNumeric = rs.getLong(1);
                    currentScn = String.valueOf(currentScnNumeric);
                    logger.info("Starting Oracle LogMiner from current SCN: {}", currentScn);
                }
            }
        }

        // 尝试连接 CDB root 执行 LogMiner（PDB 内执行会报 ORA-65040）
        cdbConnected = false;
        if (cdbEnabled && cdbName != null && !cdbName.isEmpty()) {
            String effectiveCdbService = (cdbService != null && !cdbService.isEmpty()) ? cdbService : cdbName;
            String cdbUrl = String.format("jdbc:oracle:thin:@%s:%d/%s", host, port, effectiveCdbService);
            // Oracle JDBC 中 sys as sysdba 通过 username 携带角色实现
            String cdbUserWithRole = cdbUsername + " as " + cdbRole;
            try {
                conn = DriverManager.getConnection(cdbUrl, cdbUserWithRole, cdbPassword);
                conn.setAutoCommit(false);
                cdbConnected = true;
                logger.info("Connected to Oracle CDB root (service={} user={}) for LogMiner", effectiveCdbService, cdbUserWithRole);
            } catch (SQLException e) {
                logger.warn("连接 CDB root 失败 ({}): {}，回退到 PDB 连接（LogMiner 可能受 ORA-65040 限制）", cdbUrl, e.getMessage());
            }
        }

        if (!cdbConnected) {
            // 回退：直接使用 PDB 连接（在 PDB 中 LogMiner 会受限，但保留降级行为）
            conn = pdbConn;
            logger.warn("使用 PDB 连接执行 LogMiner，可能遇到 ORA-65040 限制");
        } else {
            // CDB 已连接，保留 PDB 连接用于查询源表数据（处理 UNSUPPORTED 操作）
            logger.info("保留 PDB 连接用于 UNSUPPORTED 操作的数据查询");
        }

        // 统一 LogMiner 挖掘会话的日期/时间 NLS 为 ISO，避免 SQL_REDO 中 TO_DATE 使用本地化格式
        //（如中文月份 '03-3月 -25'）导致下游 PG 无法解析 timestamp。
        applyIsoNls(conn);
        if (pdbConn != null && pdbConn != conn) {
            applyIsoNls(pdbConn);
        }

        // 初始化 LogMiner 会话
        startLogMinerSession();

        Thread mineThread = new Thread(() -> {
            int consecutiveErrors = 0;
            while (running) {
                try {
                    int events = mineRedoLogs();
                    consecutiveErrors = 0;

                    if (events == 0) {
                        Thread.sleep(scanIntervalMs);
                    }

                    // 定期检查并添加新的归档日志
                    addNewArchiveLogsIfNeeded();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    consecutiveErrors++;
                    if (running) {
                        logger.error("Error in Oracle LogMiner (consecutive: {}): {}", consecutiveErrors, e.getMessage());
                        if (consecutiveErrors >= 5) {
                            logger.warn("Too many consecutive errors, attempting to restart LogMiner session...");
                            try {
                                restartLogMinerSession();
                                consecutiveErrors = 0;
                            } catch (Exception re) {
                                logger.error("Failed to restart LogMiner session: {}", re.getMessage());
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        } else {
                            try {
                                Thread.sleep(1000L * consecutiveErrors);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }
            }
        }, "Oracle-LogMiner-" + taskId);
        mineThread.setDaemon(true);
        mineThread.start();

        startBackpressureMonitor();

        logger.info("Oracle LogMiner capture started for task: {}", taskId);
    }

    /**
     * 将会话的日期/时间 NLS 统一为 ISO，确保 LogMiner SQL_REDO 中的日期可被目标库解析。
     */
    private void applyIsoNls(Connection c) {
        if (c == null) return;
        String[] stmts = {
                "ALTER SESSION SET NLS_DATE_LANGUAGE = 'AMERICAN'",
                "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'",
                "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'"
        };
        for (String s : stmts) {
            try (Statement st = c.createStatement()) {
                st.execute(s);
            } catch (SQLException e) {
                logger.warn("设置会话 NLS 失败: {} - {}", s, e.getMessage());
            }
        }
    }

    /**
     * 启动 LogMiner 会话，添加从 startScn 开始的在线 redo 日志。
     */
    private void startLogMinerSession() throws SQLException {
        // 先结束可能存在的旧会话
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); EXCEPTION WHEN OTHERS THEN NULL; END;");
        } catch (SQLException e) {
            logger.debug("No existing LogMiner session to end: {}", e.getMessage());
        }

        // 添加在线 redo 日志文件
        // 注意：必须先用独立 Statement 收集结果，再用另外的 Statement 执行 DDL，
        // 因为同一 Statement 执行新语句会自动关闭其 ResultSet（ORA-17010）。
        java.util.List<String> redoFiles = new java.util.ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT MEMBER FROM V$LOGFILE ORDER BY GROUP#")) {
            while (rs.next()) {
                String logFile = rs.getString(1);
                if (logFile != null && !logFile.isEmpty()) {
                    redoFiles.add(logFile);
                }
            }
        }

        int added = 0;
        for (String logFile : redoFiles) {
            try (Statement addStmt = conn.createStatement()) {
                addStmt.execute(String.format(
                        "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '%s', OPTIONS => %s); END;",
                        logFile, added == 0 ? "DBMS_LOGMNR.NEW" : "DBMS_LOGMNR.ADDFILE"));
                added++;
                logger.info("Added redo log file to LogMiner: {}", logFile);
            } catch (SQLException e) {
                logger.warn("Failed to add redo log file '{}': {}", logFile, e.getMessage());
            }
        }

        if (added == 0) {
            throw new SQLException("No redo log files could be added to LogMiner");
        }

        // 启动 LogMiner
        // 使用 DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + COMMITTED_DATA_ONLY 提高效率
        // 先尝试带 STARTSCN（仅处理起始 SCN 之后的记录），若起始 SCN 不在已添加日志范围内
        // （ORA-01291）则回退为不带 STARTSCN，靠 mineRedoLogs 的 SCN 过滤保证不重复。
        String baseOpts = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + " +
                "DBMS_LOGMNR.COMMITTED_DATA_ONLY + " +
                "DBMS_LOGMNR.PRINT_PRETTY_SQL";
        boolean started = false;
        if (currentScnNumeric > 0) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(
                        "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => %d, OPTIONS => %s); END;",
                        currentScnNumeric, baseOpts));
                started = true;
                logger.info("LogMiner session started with STARTSCN={}", currentScnNumeric);
            } catch (SQLException e) {
                logger.warn("START_LOGMNR with STARTSCN={} failed ({}), retrying without STARTSCN",
                        currentScnNumeric, e.getMessage());
            }
        }
        if (!started) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(
                        "BEGIN DBMS_LOGMNR.START_LOGMNR(OPTIONS => %s); END;", baseOpts));
                logger.info("LogMiner session started without STARTSCN (SCN filtering applied in query)");
            }
        }
    }

    /**
     * 重启 LogMiner 会话（从当前 SCN 继续）。
     */
    private synchronized void restartLogMinerSession() throws SQLException {
        logger.info("Restarting LogMiner session from SCN: {}", currentScn);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); EXCEPTION WHEN OTHERS THEN NULL; END;");
        } catch (SQLException e) {
            logger.debug("Error ending LogMiner session: {}", e.getMessage());
        }

        // 重新获取当前 SCN（如果之前没有记录）
        if (currentScnNumeric <= 0) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
                if (rs.next()) {
                    currentScnNumeric = rs.getLong(1);
                    currentScn = String.valueOf(currentScnNumeric);
                }
            }
        }

        startLogMinerSession();
    }

    /**
     * 检查并添加新的归档日志文件到 LogMiner 会话。
     */
    private void addNewArchiveLogsIfNeeded() throws SQLException {
        // 先收集归档日志列表，再用独立 Statement 添加（避免 ResultSet 被关闭）
        java.util.List<int[]> seqs = new java.util.ArrayList<>();
        java.util.List<String> arcFiles = new java.util.ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT SEQUENCE#, NAME FROM V$ARCHIVED_LOG " +
                     "WHERE ARCHIVED = 'YES' AND DELETED = 'NO' " +
                     "AND SEQUENCE# > " + lastAddedLogSeq + " " +
                     "ORDER BY SEQUENCE#")) {
            while (rs.next()) {
                int seq = rs.getInt(1);
                String arcFile = rs.getString(2);
                if (seq > lastAddedLogSeq) {
                    seqs.add(new int[]{seq});
                    arcFiles.add(arcFile);
                }
            }
        } catch (SQLException e) {
            logger.debug("Error checking for new archive logs: {}", e.getMessage());
            return;
        }

        for (int i = 0; i < arcFiles.size(); i++) {
            int seq = seqs.get(i)[0];
            String arcFile = arcFiles.get(i);
            try (Statement addStmt = conn.createStatement()) {
                addStmt.execute(String.format(
                        "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '%s', OPTIONS => DBMS_LOGMNR.ADDFILE); END;",
                        arcFile));
                lastAddedLogSeq = seq;
                logger.info("Added archive log file (seq={}): {}", seq, arcFile);
            } catch (SQLException e) {
                // ORA-01289: 日志文件已添加，属正常情况，静默跳过并记录 seq 避免重复尝试
                if (e.getMessage() != null && e.getMessage().contains("ORA-01289")) {
                    lastAddedLogSeq = seq;
                    logger.debug("Archive log already added (seq={}): {}", seq, arcFile);
                } else {
                    logger.warn("Failed to add archive log '{}': {}", arcFile, e.getMessage());
                }
            }
        }
    }

    /**
     * 从 V$LOGMNR_CONTENTS 查询变更并写入输出文件。
     *
     * @return 本次查询捕获的事件数
     */
    private int mineRedoLogs() throws SQLException, IOException {
        checkBackpressureSignal();
        if (backpressurePaused) {
            logger.debug("Backpressure paused, skipping LogMiner query");
            return 0;
        }

        int eventCount = 0;
        StringBuilder queryBuilder = new StringBuilder(
                "SELECT SCN, OPERATION, XID, SEG_OWNER, TABLE_NAME, SQL_REDO, TIMESTAMP, ROW_ID " +
                "FROM V$LOGMNR_CONTENTS WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE', 'UNSUPPORTED')");

        // 添加 SCN 过滤（只查大于当前 SCN 的记录）
        if (currentScnNumeric > 0) {
            queryBuilder.append(" AND SCN > ").append(currentScnNumeric);
        }

        // 添加 schema 过滤
        if (!syncedSchemas.isEmpty()) {
            queryBuilder.append(" AND SEG_OWNER IN (");
            boolean first = true;
            for (String schema : syncedSchemas) {
                if (!first) queryBuilder.append(",");
                queryBuilder.append("'").append(schema).append("'");
                first = false;
            }
            queryBuilder.append(")");
        }

        // 添加表过滤
        if (!syncedTables.isEmpty()) {
            queryBuilder.append(" AND (");
            boolean first = true;
            for (String table : syncedTables) {
                if (!first) queryBuilder.append(" OR ");
                String[] parts = table.split("\\.", 2);
                queryBuilder.append("(SEG_OWNER = '").append(parts[0])
                        .append("' AND TABLE_NAME = '").append(parts[1]).append("')");
                first = false;
            }
            queryBuilder.append(")");
        }

        queryBuilder.append(" ORDER BY SCN ASC");

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(queryBuilder.toString());
            pstmt.setFetchSize(queryBatchSize);
            rs = pstmt.executeQuery();

            while (rs.next() && running) {
                long scn = rs.getLong("SCN");
                String operation = rs.getString("OPERATION");
                String xid = rs.getString("XID");
                String segOwner = rs.getString("SEG_OWNER");
                String tableName = rs.getString("TABLE_NAME");
                String sqlRedo = rs.getString("SQL_REDO");
                Timestamp timestamp = rs.getTimestamp("TIMESTAMP");
                String rowId = rs.getString("ROW_ID");

                if (segOwner == null || tableName == null) {
                    continue;
                }

                segOwner = segOwner.toUpperCase();
                tableName = tableName.toUpperCase();

                // 对于 UNSUPPORTED 操作（通常因表含 LOB 列），使用 ROW_ID 从源表查询数据构建 INSERT 事件
                String eventData;
                if ("UNSUPPORTED".equalsIgnoreCase(operation)) {
                    if (rowId == null || rowId.isEmpty() || rowId.matches("A+")) {
                        // ROW_ID 全 A 表示无效，跳过
                        continue;
                    }
                    eventData = buildEventDataFromRowId(segOwner, tableName, rowId);
                    if (eventData == null) {
                        // 查询失败或行不存在（可能已删除），跳过
                        continue;
                    }
                    // UNSUPPORTED 操作统一当作 INSERT 处理（全列覆盖）
                    operation = "INSERT";
                } else {
                    // 解析 SQL_REDO 提取列值
                    eventData = buildEventData(segOwner, tableName, operation, sqlRedo);
                }

                long ts = timestamp != null ? timestamp.getTime() : System.currentTimeMillis();
                long xidNumeric = 0;
                try {
                    xidNumeric = Long.parseLong(xid, 16);
                } catch (Exception e) {
                    // ignore
                }

                StringBuilder sb = new StringBuilder();
                sb.append(operation.toUpperCase()).append(FIELD_SEP);
                sb.append(scn).append(FIELD_SEP);
                sb.append(scn).append(FIELD_SEP);
                sb.append(ts).append(FIELD_SEP);
                sb.append(xidNumeric).append(FIELD_SEP);
                sb.append(eventData);
                sb.append(RECORD_SEP);

                writer.write(sb.toString());
                writer.flush();

                currentScn = String.valueOf(scn);
                currentScnNumeric = scn;

                eventCount++;
                long total = eventCounter.incrementAndGet();
                currentFileEvents++;

                if (currentFileEvents >= maxEventsPerFile) {
                    rotateOutputFile();
                }

                if (total % 1000 == 0) {
                    logger.info("Captured {} Oracle redo events, current SCN: {}", total, currentScn);
                    savePosition();
                }
            }
        } finally {
            if (rs != null) try { rs.close(); } catch (SQLException e) { /* ignore */ }
            if (pstmt != null) try { pstmt.close(); } catch (SQLException e) { /* ignore */ }
        }

        if (eventCount > 0) {
            savePosition();
        }

        return eventCount;
    }

    /**
     * 构建 eventData 字符串，格式与 PostgreSQL/MySQL 一致：
     * schema:OWNER table:TABLE_NAME primary_keys:COL1,COL2 old-tuple:{...} new-tuple:{...}
     */
    private String buildEventData(String owner, String table, String operation, String sqlRedo) {
        StringBuilder sb = new StringBuilder();
        sb.append("schema:").append(owner).append(" table:").append(table);

        String key = owner + "." + table;
        List<String> pkColumns = fetchTablePrimaryKeys(owner, table);
        if (!pkColumns.isEmpty()) {
            sb.append(" primary_keys:").append(String.join(",", pkColumns));
        }

        // 尝试从 SQL_REDO 解析列值
        Map<String, String> columnValues = parseSqlRedo(sqlRedo, operation);
        if (!columnValues.isEmpty()) {
            String tupleStr = formatTuple(columnValues);
            if ("INSERT".equals(operation)) {
                sb.append(" new-tuple:{").append(tupleStr).append("}");
            } else if ("DELETE".equals(operation)) {
                sb.append(" old-tuple:{").append(tupleStr).append("}");
            } else if ("UPDATE".equals(operation)) {
                // UPDATE 的 SQL_REDO 包含 SET (new) 和 WHERE (old)
                Map<String, String> newValues = parseSqlRedoSetClause(sqlRedo);
                Map<String, String> oldValues = parseSqlRedoWhereClause(sqlRedo);
                if (!oldValues.isEmpty()) {
                    sb.append(" old-tuple:{").append(formatTuple(oldValues)).append("}");
                }
                if (!newValues.isEmpty()) {
                    sb.append(" new-tuple:{").append(formatTuple(newValues)).append("}");
                }
            }
        } else {
            // 无法解析时，保留原始 SQL_REDO
            if (sqlRedo != null && !sqlRedo.isEmpty()) {
                sb.append(" sql_redo:").append(sqlRedo.replace("\n", " ").replace("\r", " "));
            }
        }

        return sb.toString();
    }

    /**
     * 使用 ROW_ID 从源表查询数据，构建 INSERT 事件的 eventData。
     * 用于处理 UNSUPPORTED 操作（表含 LOB 列时 LogMiner 无法解析 SQL_REDO）。
     */
    private String buildEventDataFromRowId(String owner, String table, String rowId) {
        if (pdbConn == null) {
            logger.warn("PDB 连接不可用，无法通过 ROW_ID 查询源表数据");
            return null;
        }
        try {
            // 获取表列信息（使用 PDB 连接查询，因为 CDB root 的 ALL_TAB_COLUMNS 看不到 PDB 表）
            String key = owner + "." + table;
            List<String> columns = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();
            try (PreparedStatement colStmt = pdbConn.prepareStatement(
                    "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID")) {
                colStmt.setString(1, owner);
                colStmt.setString(2, table);
                try (ResultSet colRs = colStmt.executeQuery()) {
                    while (colRs.next()) {
                        columns.add(colRs.getString(1));
                        columnTypes.add(colRs.getString(2));
                    }
                }
            }

            if (columns.isEmpty()) {
                logger.warn("表 {} 无列信息，跳过 ROW_ID 查询", key);
                return null;
            }

            // 使用 ROWID 查询源表数据
            StringBuilder colList = new StringBuilder();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) colList.append(",");
                colList.append("\"").append(columns.get(i)).append("\"");
            }
            String querySql = "SELECT " + colList + " FROM \"" + owner + "\".\"" + table + "\" WHERE ROWID = ?";

            Map<String, String> columnValues = new LinkedHashMap<>();
            try (PreparedStatement pstmt = pdbConn.prepareStatement(querySql)) {
                pstmt.setString(1, rowId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        // 行不存在（可能已被删除）
                        return null;
                    }
                    for (int i = 0; i < columns.size(); i++) {
                        String col = columns.get(i);
                        String dataType = columnTypes.get(i).toUpperCase();
                        String valStr;
                        if ("CLOB".equals(dataType) || "NCLOB".equals(dataType)) {
                            // CLOB/NCLOB 使用 getString 获取文本内容
                            valStr = rs.getString(col);
                            if (rs.wasNull()) valStr = "NULL";
                        } else if ("BLOB".equals(dataType)) {
                            // BLOB 转换为 hex 字符串
                            java.sql.Blob blob = rs.getBlob(col);
                            if (blob == null) {
                                valStr = "NULL";
                            } else {
                                byte[] bytes = blob.getBytes(1, (int) blob.length());
                                valStr = "BASE64:" + java.util.Base64.getEncoder().encodeToString(bytes);
                            }
                        } else {
                            Object val = rs.getObject(col);
                            valStr = (val == null) ? "NULL" : val.toString();
                        }
                        columnValues.put(col, valStr);
                    }
                }
            }

            // 构建 eventData
            StringBuilder sb = new StringBuilder();
            sb.append("schema:").append(owner).append(" table:").append(table);
            // 使用 PDB 连接查询主键
            List<String> pkColumns = new ArrayList<>();
            try (PreparedStatement pkStmt = pdbConn.prepareStatement(
                    "SELECT CC.COLUMN_NAME FROM ALL_CONSTRAINTS C " +
                    "JOIN ALL_CONS_COLUMNS CC ON C.OWNER = CC.OWNER " +
                    "AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME " +
                    "WHERE C.OWNER = ? AND C.TABLE_NAME = ? AND C.CONSTRAINT_TYPE = 'P' " +
                    "ORDER BY CC.POSITION")) {
                pkStmt.setString(1, owner);
                pkStmt.setString(2, table);
                try (ResultSet pkRs = pkStmt.executeQuery()) {
                    while (pkRs.next()) {
                        pkColumns.add(pkRs.getString(1));
                    }
                }
            }
            if (!pkColumns.isEmpty()) {
                sb.append(" primary_keys:").append(String.join(",", pkColumns));
            }
            sb.append(" new-tuple:{").append(formatTuple(columnValues)).append("}");
            return sb.toString();

        } catch (SQLException e) {
            logger.warn("通过 ROW_ID={} 查询源表 {}.{} 失败: {}", rowId, owner, table, e.getMessage());
            return null;
        }
    }

    /**
     * 简单解析 SQL_REDO 中的列值。
     * LogMiner 使用 PRINT_PRETTY_SQL 选项，SQL_REDO 格式类似：
     * insert into "SCHEMA"."TABLE"("COL1","COL2") values ('val1','val2');
     */
    private Map<String, String> parseSqlRedo(String sqlRedo, String operation) {
        Map<String, String> values = new LinkedHashMap<>();
        if (sqlRedo == null || sqlRedo.isEmpty()) {
            return values;
        }

        try {
            if ("INSERT".equals(operation)) {
                return parseSqlRedoSetClause(sqlRedo);
            } else if ("DELETE".equals(operation)) {
                return parseSqlRedoWhereClause(sqlRedo);
            } else if ("UPDATE".equals(operation)) {
                return parseSqlRedoSetClause(sqlRedo);
            }
        } catch (Exception e) {
            logger.debug("Failed to parse SQL_REDO: {}", e.getMessage());
        }

        return values;
    }

    /**
     * 解析 INSERT 的 values 子句或 UPDATE 的 SET 子句。
     */
    private Map<String, String> parseSqlRedoSetClause(String sqlRedo) {
        Map<String, String> values = new LinkedHashMap<>();
        if (sqlRedo == null) return values;

        // 匹配 insert into "OWNER"."TABLE"("COL1","COL2") values (val1,val2)
        // 或 update "OWNER"."TABLE" set "COL1" = val1 where ...
        Pattern insertPattern = Pattern.compile(
                "insert into \"[^\"]+\"\\.\"[^\"]+\"\\(([^)]+)\\)\\s*values\\s*\\((.*)\\)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher insertMatcher = insertPattern.matcher(sqlRedo);
        if (insertMatcher.find()) {
            String columnsStr = insertMatcher.group(1);
            String valuesStr = insertMatcher.group(2);
            List<String> columns = parseColumnList(columnsStr);
            List<String> vals = parseValueList(valuesStr);
            for (int i = 0; i < columns.size() && i < vals.size(); i++) {
                values.put(columns.get(i), vals.get(i));
            }
            return values;
        }

        // PRINT_PRETTY_SQL 格式: insert into "OWNER"."TABLE" values "COL1" = val1, "COL2" = val2;
        // LogMiner PRINT_PRETTY_SQL 选项下 INSERT 不带列列表，而是 values "COL" = val 形式
        Pattern prettyInsertPattern = Pattern.compile(
                "insert into \"[^\"]+\"\\.\"[^\"]+\"\\s*values\\s+(.+?);?\\s*$",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher prettyInsertMatcher = prettyInsertPattern.matcher(sqlRedo);
        if (prettyInsertMatcher.find()) {
            String setClause = prettyInsertMatcher.group(1);
            List<String> assignments = splitSetAssignments(setClause);
            for (String assignment : assignments) {
                Pattern assignPattern = Pattern.compile("\"([^\"]+)\"\\s*=\\s*(.+)", Pattern.DOTALL);
                Matcher assignMatcher = assignPattern.matcher(assignment.trim());
                if (assignMatcher.find()) {
                    String col = assignMatcher.group(1);
                    String val = assignMatcher.group(2).trim();
                    if (val.endsWith(";")) val = val.substring(0, val.length() - 1).trim();
                    values.put(col, val);
                }
            }
            if (!values.isEmpty()) {
                return values;
            }
        }

        // update set "COL1" = val1, "COL2" = val2 where ...
        Pattern updatePattern = Pattern.compile(
                "set\\s+(.+?)\\s+where\\s+", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher updateMatcher = updatePattern.matcher(sqlRedo);
        if (updateMatcher.find()) {
            String setClause = updateMatcher.group(1);
            // 按 ", " 分割，但要注意值中可能包含逗号
            List<String> assignments = splitSetAssignments(setClause);
            for (String assignment : assignments) {
                Pattern assignPattern = Pattern.compile("\"([^\"]+)\"\\s*=\\s*(.+)", Pattern.DOTALL);
                Matcher assignMatcher = assignPattern.matcher(assignment.trim());
                if (assignMatcher.find()) {
                    String col = assignMatcher.group(1);
                    String val = assignMatcher.group(2).trim();
                    if (val.endsWith(";")) val = val.substring(0, val.length() - 1).trim();
                    values.put(col, val);
                }
            }
        }

        return values;
    }

    /**
     * 解析 WHERE 子句中的列值（DELETE/UPDATE 的旧值）。
     */
    private Map<String, String> parseSqlRedoWhereClause(String sqlRedo) {
        Map<String, String> values = new LinkedHashMap<>();
        if (sqlRedo == null) return values;

        Pattern wherePattern = Pattern.compile("where\\s+(.+?);?\\s*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher whereMatcher = wherePattern.matcher(sqlRedo);
        if (whereMatcher.find()) {
            String whereClause = whereMatcher.group(1);
            List<String> conditions = splitSetAssignments(whereClause);
            for (String cond : conditions) {
                Pattern condPattern = Pattern.compile("\"([^\"]+)\"\\s*=\\s*(.+)", Pattern.DOTALL);
                Matcher condMatcher = condPattern.matcher(cond.trim());
                if (condMatcher.find()) {
                    String col = condMatcher.group(1);
                    String val = condMatcher.group(2).trim();
                    if (val.endsWith(";")) val = val.substring(0, val.length() - 1).trim();
                    values.put(col, val);
                }
            }
        }

        return values;
    }

    private List<String> parseColumnList(String columnsStr) {
        List<String> columns = new ArrayList<>();
        Pattern colPattern = Pattern.compile("\"([^\"]+)\"");
        Matcher m = colPattern.matcher(columnsStr);
        while (m.find()) {
            columns.add(m.group(1));
        }
        return columns;
    }

    /**
     * 解析 values 子句中的值列表，处理字符串、NULL、数字等。
     */
    private List<String> parseValueList(String valuesStr) {
        List<String> values = new ArrayList<>();
        if (valuesStr == null || valuesStr.trim().isEmpty()) {
            return values;
        }

        String trimmed = valuesStr.trim();
        int i = 0;
        while (i < trimmed.length()) {
            // 跳过空格
            while (i < trimmed.length() && trimmed.charAt(i) == ' ') i++;
            if (i >= trimmed.length()) break;

            char c = trimmed.charAt(i);
            if (c == '\'') {
                // 字符串值
                StringBuilder sb = new StringBuilder("'");
                i++;
                while (i < trimmed.length()) {
                    if (trimmed.charAt(i) == '\'' && i + 1 < trimmed.length() && trimmed.charAt(i + 1) == '\'') {
                        sb.append("''");
                        i += 2;
                    } else if (trimmed.charAt(i) == '\'') {
                        sb.append("'");
                        i++;
                        break;
                    } else {
                        sb.append(trimmed.charAt(i));
                        i++;
                    }
                }
                values.add(sb.toString());
            } else {
                // 非字符串值（数字、NULL、函数等）
                StringBuilder sb = new StringBuilder();
                while (i < trimmed.length() && trimmed.charAt(i) != ',') {
                    sb.append(trimmed.charAt(i));
                    i++;
                }
                String val = sb.toString().trim();
                if (val.endsWith(";")) val = val.substring(0, val.length() - 1).trim();
                values.add(val);
            }

            // 跳过逗号
            while (i < trimmed.length() && (trimmed.charAt(i) == ',' || trimmed.charAt(i) == ' ')) i++;
        }

        return values;
    }

    /**
     * 分割 SET/WHERE 子句中的赋值表达式，处理值中可能包含的逗号。
     */
    private List<String> splitSetAssignments(String clause) {
        List<String> parts = new ArrayList<>();
        if (clause == null || clause.trim().isEmpty()) {
            return parts;
        }

        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        int parenDepth = 0;

        for (int i = 0; i < clause.length(); i++) {
            char c = clause.charAt(i);
            if (c == '\'' && i + 1 < clause.length() && clause.charAt(i + 1) == '\'') {
                current.append("''");
                i++;
            } else if (c == '\'') {
                inQuote = !inQuote;
                current.append(c);
            } else if (c == '(' && !inQuote) {
                parenDepth++;
                current.append(c);
            } else if (c == ')' && !inQuote) {
                parenDepth--;
                current.append(c);
            } else if (c == ',' && !inQuote && parenDepth == 0) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else if (c == ' ' && !inQuote && current.length() > 0
                    && current.charAt(current.length() - 1) == ' '
                    && (current.toString().trim().endsWith("and") || current.toString().trim().endsWith("AND"))) {
                // WHERE 子句中的 AND 分隔
                parts.add(current.toString().trim().replaceAll("(?i)\\s+and$", ""));
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts;
    }

    private String formatTuple(Map<String, String> values) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (!first) sb.append(",");
            sb.append(entry.getKey()).append(":")
              .append(entry.getValue() != null ? entry.getValue() : "[null]");
            first = false;
        }
        return sb.toString();
    }

    // ==================== 表元数据查询 ====================

    private List<String> fetchTableColumns(String owner, String table) {
        String key = owner + "." + table;
        return tableColumnsCache.computeIfAbsent(key, k -> {
            List<String> columns = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS " +
                    "WHERE OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID")) {
                stmt.setString(1, owner);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString(1));
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch columns for {}.{}: {}", owner, table, e.getMessage());
            }
            return columns;
        });
    }

    private List<String> fetchTableColumnTypes(String owner, String table) {
        String key = owner + "." + table;
        return tableColumnTypesCache.computeIfAbsent(key, k -> {
            List<String> types = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS " +
                    "WHERE OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID")) {
                stmt.setString(1, owner);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        types.add(rs.getString(1));
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch column types for {}.{}: {}", owner, table, e.getMessage());
            }
            return types;
        });
    }

    private List<String> fetchTablePrimaryKeys(String owner, String table) {
        String key = owner + "." + table;
        return tablePrimaryKeysCache.computeIfAbsent(key, k -> {
            List<String> pkColumns = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT CC.COLUMN_NAME FROM ALL_CONSTRAINTS C " +
                    "JOIN ALL_CONS_COLUMNS CC ON C.OWNER = CC.OWNER " +
                    "AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME " +
                    "WHERE C.OWNER = ? AND C.TABLE_NAME = ? AND C.CONSTRAINT_TYPE = 'P' " +
                    "ORDER BY CC.POSITION")) {
                stmt.setString(1, owner);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        pkColumns.add(rs.getString(1));
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch primary keys for {}.{}: {}", owner, table, e.getMessage());
            }
            return pkColumns;
        });
    }

    // ==================== 背压控制 ====================

    private void checkBackpressureSignal() {
        if (backpressureSignalPath == null) return;
        File signalFile = new File(backpressureSignalPath);
        if (!signalFile.exists()) {
            backpressurePaused = false;
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(signalFile))) {
            String firstLine = reader.readLine();
            boolean shouldPause = firstLine != null && "PAUSE".equalsIgnoreCase(firstLine.trim());
            if (shouldPause != backpressurePaused) {
                backpressurePaused = shouldPause;
                if (shouldPause) {
                    logger.warn("收到背压暂停信号，暂停 Oracle LogMiner 事件处理");
                } else {
                    logger.info("收到背压恢复信号，恢复 Oracle LogMiner 事件处理");
                }
            }
        } catch (IOException e) {
            logger.debug("读取背压信号文件失败: {}", e.getMessage());
        }
    }

    private void startBackpressureMonitor() {
        Thread monitor = new Thread(() -> {
            while (running) {
                try {
                    checkBackpressureSignal();
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.debug("背压监控异常: {}", e.getMessage());
                }
            }
        }, "Oracle-Backpressure-Monitor-" + taskId);
        monitor.setDaemon(true);
        monitor.start();
        logger.info("背压监控线程已启动, taskId={}", taskId);
    }

    // ==================== 生命周期管理 ====================

    @Override
    protected void doStop() throws Exception {
        // 结束 LogMiner 会话
        if (conn != null && !conn.isClosed()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); EXCEPTION WHEN OTHERS THEN NULL; END;");
            } catch (Exception e) {
                logger.warn("Error ending LogMiner session: {}", e.getMessage());
            }
        }

        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing Oracle connection: {}", e.getMessage());
            }
        }

        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (Exception e) {
                logger.warn("Error closing writer: {}", e.getMessage());
            }
        }

        savePosition();
        logger.info("Oracle Redo capture stopped. Total events captured: {}", eventCounter.get());
    }

    private synchronized void openNewOutputFile() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new java.util.Date());
        String fileName = String.format("binlog_%s_%04d.cap", timestamp, fileCounter.get());

        File outputFile = new File(outputDir, fileName);
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));
        currentFileEvents = 0;

        logger.info("Opened new Oracle redo capture output file: {}", outputFile.getAbsolutePath());
    }

    private synchronized void rotateOutputFile() throws IOException {
        fileCounter.incrementAndGet();
        openNewOutputFile();
        logger.info("Rotated to new capture output file after {} events", maxEventsPerFile);
    }

    private void savePosition() {
        if (currentScn == null) return;

        File positionFile = new File(outputDir, "capture_position.properties");
        Properties posProps = new Properties();
        posProps.setProperty("redo.scn", currentScn);
        posProps.setProperty("redo.scn.numeric", String.valueOf(currentScnNumeric));
        posProps.setProperty("last.update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));

        try (FileOutputStream fos = new FileOutputStream(positionFile)) {
            posProps.store(fos, "Oracle Redo Capture position for task: " + taskId);
        } catch (IOException e) {
            logger.warn("Failed to save Oracle redo capture position: {}", e.getMessage());
        }
    }

    public String getCurrentScn() {
        return currentScn;
    }

    public long getCurrentScnNumeric() {
        return currentScnNumeric;
    }

    public long getEventCount() {
        return eventCounter.get();
    }
}
