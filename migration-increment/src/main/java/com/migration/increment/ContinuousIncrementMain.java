package com.migration.increment;

import com.migration.common.MdcUtil;
import com.migration.common.ratelimit.RowRateLimiter;
import com.migration.common.watch.DirectoryChangeWatcher;
import com.migration.db.ConnectionPoolManager;
import com.migration.increment.checkpoint.SeqnoCheckpointManager;
import com.migration.thl.EncryptedTHLFileReader;
import com.migration.thl.THLEvent;
import com.migration.thl.THLFileReader;
import com.migration.thl.crypto.ThlEncryptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ContinuousIncrementMain {

    private static final Logger logger = LoggerFactory.getLogger(ContinuousIncrementMain.class);

    private String thlDirectory;
    private String targetHost;
    private int targetPort;
    private String targetDatabase;
    private String targetUser;
    private String targetPassword;
    private long scanInterval;
    /** 增量应用限速（行/秒配额落到执行层）：避免应用过快反压到 capture/binlog 读取而打挂源库。 */
    private RowRateLimiter rowRateLimiter;
    /** 双向同步/环路防护：启用后每个应用事务先写 origin 标记，供对端 capture 识别并跳过，防止回环。 */
    private boolean bidirectionalEnabled;
    /** 本节点标识（写入 origin 标记，供观测）。 */
    private String bidiNodeId;
    /** 标记表是否已在目标库确保存在（每进程一次）。 */
    private boolean markerTableReady = false;
    /** 事件驱动开关：true 时用 WatchService 监听 THL 目录变更（默认），false 回退固定间隔轮询。 */
    private boolean watchEnabled;
    /** 事件驱动下的兜底超时（ms）：即使无文件事件也会周期性重扫，兼作 macOS 轮询式 WatchService 的延迟上界。 */
    private long watchFallbackMs;
    private String taskId;
    private boolean isPostgresql;

    private Connection targetConnection;
    private THLToSqlConverter sqlConverter;
    private TypedDmlConverter typedDmlConverter;
    private SeqnoCheckpointManager checkpointManager;
    private Properties props;

    private AtomicBoolean running = new AtomicBoolean(true);
    private Map<String, Long> processedFiles = new LinkedHashMap<>();
    private String progressFile;

    private long lastExecutedSeqno = 0;

    private volatile long lastRtoMs = -1;
    private volatile long lastRtoReportTime = 0;
    private volatile long lastAppliedSourceTs = -1;
    private static final long RTO_REPORT_INTERVAL_MS = 3000;
    private static final int RTO_REPORT_EVENT_INTERVAL = 50;

    /** 表级延迟记录目录 */
    private String tableLatencyDir;

    /** THL 加密服务 */
    private ThlEncryptionService thlEncryptionService;

    /** 已处理THL文件保留数量（安全余量），超过此数量的已处理文件将被清理 */
    private int thlRetentionCount = 2;

    public static void main(String[] args) {
        com.migration.common.OracleNetCompat.apply();
        logger.info("=== 增量同步服务启动 ===");

        String configPath = null;
        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
            }
        }

        Properties props = new Properties();

        if (configPath != null) {
            try (InputStream input = new FileInputStream(configPath)) {
                props.load(input);
            } catch (IOException e) {
                logger.error("Failed to load config: {}", configPath, e);
                System.exit(1);
            }
        } else {
            String taskIdHint = System.getProperty("task.id", "unknown");
            String defaultConfig = "files/" + taskIdHint + "/config.properties";
            File configFile = new File(defaultConfig);
            if (configFile.exists()) {
                try (InputStream input = new FileInputStream(configFile)) {
                    props.load(input);
                } catch (IOException e) {
                    logger.error("Failed to load default config", e);
                    System.exit(1);
                }
            }
        }
        // 解密 config.properties 中的加密口令（ENC: 前缀）；历史明文配置无前缀，原样通过。
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

        String taskId = props.getProperty("task.id", System.getProperty("task.id", "unknown"));

        MdcUtil.setTaskId(taskId);
        MdcUtil.setProcessName("increment");

        try {
            ContinuousIncrementMain main = new ContinuousIncrementMain();
            main.initialize(props);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("收到关闭信号，停止增量同步服务...");
                main.stop();
                MdcUtil.clear();
            }));

            main.start();
        } catch (Exception e) {
            logger.error("增量同步服务异常退出", e);
            System.exit(1);
        }
    }

    public void initialize(Properties props) throws Exception {
        this.props = props;
        taskId = props.getProperty("task.id", "unknown");
        thlDirectory = props.getProperty("increment.thl.dir",
                "files/" + taskId + "/thl_output");
        targetHost = props.getProperty("target.db.host", "localhost");
        targetPort = Integer.parseInt(props.getProperty("target.db.port", "3306"));
        targetDatabase = props.getProperty("target.db.database", "");
        targetUser = props.getProperty("target.db.username", "root");
        targetPassword = props.getProperty("target.db.password", "");
        scanInterval = Long.parseLong(props.getProperty("increment.scan.interval", "3000"));
        long maxRowsPerSec = Long.parseLong(props.getProperty("increment.rate.limit.rows.per.sec", "0"));
        rowRateLimiter = new RowRateLimiter(maxRowsPerSec);
        if (!rowRateLimiter.isUnlimited()) {
            logger.info("增量限速已启用: {} 行/秒（配额落到执行层，避免应用过快打挂源库）", maxRowsPerSec);
        }
        watchEnabled = Boolean.parseBoolean(props.getProperty("increment.watch.enabled", "true"));
        watchFallbackMs = Long.parseLong(props.getProperty("increment.watch.fallback.ms", "1000"));
        bidirectionalEnabled = com.migration.common.bidi.BidiConstants.isEnabled(props);
        bidiNodeId = com.migration.common.bidi.BidiConstants.nodeId(props);
        isPostgresql = "postgresql".equalsIgnoreCase(props.getProperty("target.db.type", "mysql"));

        String checkpointPath = "./files/" + taskId + "/checkpoint/increment_checkpoint";
        checkpointManager = new SeqnoCheckpointManager(checkpointPath);

        lastExecutedSeqno = checkpointManager.loadSeqno();

        connectToTargetDatabase();

        sqlConverter = new THLToSqlConverter(props);
        // 类型化值管道（mysql→pg）：值经 PreparedStatement 参数绑定执行，事件不适用时自动回退文本路径
        typedDmlConverter = new TypedDmlConverter(props);
        // 注入 SchemaEvolutionService，启用 DDL 自动应用和在线 DDL 影子表过滤
        sqlConverter.setSchemaEvolutionService(
                new com.migration.increment.schema.SchemaEvolutionService(props, targetConnection));

        progressFile = "./files/" + taskId + "/checkpoint/.increment_progress";
        tableLatencyDir = "./files/" + taskId + "/binlog_output/table_latency";
        thlRetentionCount = Integer.parseInt(props.getProperty("increment.thl.retention.count", "2"));
        ensureDirExists(tableLatencyDir);
        // 初始化 THL 加密服务
        this.thlEncryptionService = new ThlEncryptionService(props);
        if (thlEncryptionService.isEnabled()) {
            logger.info("THL 文件加密已启用（increment 读取端）");
        }
        loadProgress();

        logger.info("增量同步服务初始化完成 - thlDir: {}, target: {}:{}/{}, lastSeqno: {}",
                thlDirectory, targetHost, targetPort, targetDatabase, lastExecutedSeqno);
    }

    private void connectToTargetDatabase() throws SQLException {
        String url;
        if (isPostgresql) {
            String jdbcUrl = props.getProperty("target.db.jdbc.url");
            if (jdbcUrl != null && !jdbcUrl.isEmpty()) {
                url = jdbcUrl;
            } else {
                // stringtype=unspecified：字符串参数由 PG 按列类型推断（interval/jsonb/时间等绑定依赖）
                url = "jdbc:postgresql://" + targetHost + ":" + targetPort + "/" + targetDatabase
                        + "?stringtype=unspecified";
            }
        } else {
            url = "jdbc:mysql://" + targetHost + ":" + targetPort + "/" + targetDatabase +
                    "?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8&allowPublicKeyRetrieval=true";
        }
        targetConnection = ConnectionPoolManager.getConnection(url, targetUser, targetPassword);
        logger.info("已连接目标数据库: {}:{}/{} (类型: {})", targetHost, targetPort, targetDatabase,
                isPostgresql ? "postgresql" : "mysql");
        if (bidirectionalEnabled) {
            ensureMarkerTable();
        }
    }

    /**
     * 双向同步：在目标库确保 origin 标记表存在。该表由本进程在每个应用事务里滚动更新一行，
     * 目标库 binlog 记下这次行事件后，对端 capture 靠它识别"复制而来的事务"并跳过，防止回环。
     */
    private void ensureMarkerTable() {
        if (markerTableReady) return;
        String table = com.migration.common.bidi.BidiConstants.MARKER_TABLE;
        String ddl = isPostgresql
                ? "CREATE TABLE IF NOT EXISTS \"" + table + "\" (id INT PRIMARY KEY, node_id VARCHAR(64), last_seqno BIGINT, updated_at BIGINT)"
                : "CREATE TABLE IF NOT EXISTS `" + table + "` (id INT PRIMARY KEY, node_id VARCHAR(64), last_seqno BIGINT, updated_at BIGINT)";
        try (Statement st = targetConnection.createStatement()) {
            boolean prevAuto = targetConnection.getAutoCommit();
            if (!prevAuto) targetConnection.setAutoCommit(true);
            st.execute(ddl);
            markerTableReady = true;
            logger.info("双向同步已启用，origin 标记表就绪: {} (nodeId={})", table, bidiNodeId);
        } catch (SQLException e) {
            logger.warn("创建 origin 标记表失败（双向防回环可能失效）: {}", e.getMessage());
        }
    }

    /**
     * 在当前应用事务里写入 origin 标记（作为事务首条语句，保证 binlog 里标记行事件先于业务 DML）。
     * 单行滚动更新固定主键，每个应用事务都产生一次行事件供对端 capture 识别。
     */
    private void writeOriginMarker(long seqno) throws SQLException {
        String table = com.migration.common.bidi.BidiConstants.MARKER_TABLE;
        int id = com.migration.common.bidi.BidiConstants.MARKER_ROW_ID;
        long now = System.currentTimeMillis();
        String sql = isPostgresql
                ? "INSERT INTO \"" + table + "\" (id, node_id, last_seqno, updated_at) VALUES (?, ?, ?, ?) " +
                  "ON CONFLICT (id) DO UPDATE SET node_id=EXCLUDED.node_id, last_seqno=EXCLUDED.last_seqno, updated_at=EXCLUDED.updated_at"
                : "INSERT INTO `" + table + "` (id, node_id, last_seqno, updated_at) VALUES (?, ?, ?, ?) " +
                  "ON DUPLICATE KEY UPDATE node_id=VALUES(node_id), last_seqno=VALUES(last_seqno), updated_at=VALUES(updated_at)";
        try (PreparedStatement ps = targetConnection.prepareStatement(sql)) {
            ps.setInt(1, id);
            ps.setString(2, bidiNodeId);
            ps.setLong(3, seqno);
            ps.setLong(4, now);
            ps.executeUpdate();
        }
    }

    public void start() {
        if (watchEnabled) {
            logger.info("启动THL文件持续处理（事件驱动: WatchService，兜底 {}ms）...", watchFallbackMs);
            startWatchDriven();
        } else {
            logger.info("启动THL文件持续处理（固定轮询: {}ms）...", scanInterval);
            startPolling();
        }

        close();
        logger.info("增量同步服务已停止");
    }

    /** 事件驱动主循环：先处理一遍，再阻塞等待 THL 目录出现变更（或兜底超时）后立即重扫。 */
    private void startWatchDriven() {
        try (DirectoryChangeWatcher watcher = new DirectoryChangeWatcher(thlDirectory)) {
            while (running.get()) {
                try {
                    scanAndProcessThlFiles();
                    // 阻塞到有新 THL 写入立即唤醒（Linux inotify ~ms），或兜底超时；返回值仅用于日志
                    watcher.awaitChange(watchFallbackMs);
                } catch (InterruptedException e) {
                    logger.info("增量同步线程被中断");
                    Thread.currentThread().interrupt();
                    break;
                } catch (RuntimeException e) {
                    logger.error("THL文件处理出现意外错误, 将继续处理", e);
                } catch (Exception e) {
                    logger.error("THL文件扫描出错", e);
                }
            }
        } catch (IOException e) {
            logger.warn("初始化目录监听失败，回退固定轮询: {}", e.getMessage());
            startPolling();
        }
    }

    /** 兼容回退：固定间隔轮询（increment.watch.enabled=false 时启用）。 */
    private void startPolling() {
        while (running.get()) {
            try {
                scanAndProcessThlFiles();
                Thread.sleep(scanInterval);
            } catch (InterruptedException e) {
                logger.info("增量同步线程被中断");
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                logger.error("THL文件处理出现意外错误, 将继续处理", e);
            } catch (Exception e) {
                logger.error("THL文件扫描出错", e);
            }
        }
    }

    public void stop() {
        running.set(false);
    }

    private void scanAndProcessThlFiles() {
        File thlDir = new File(thlDirectory);
        if (!thlDir.exists() || !thlDir.isDirectory()) {
            return;
        }

        File[] thlFiles = thlDir.listFiles((dir, name) ->
                name.endsWith(".thl") && !name.startsWith("."));

        if (thlFiles == null || thlFiles.length == 0) {
            return;
        }

        // 按文件名中的seqno数字排序，避免字符串排序导致 99 > 100
        // 文件名格式: binlog_YYYYMMDD_HHMMSS_XXXX_SEQ.thl
        Arrays.sort(thlFiles, (f1, f2) -> {
            long seq1 = extractSeqnoFromName(f1.getName());
            long seq2 = extractSeqnoFromName(f2.getName());
            return Long.compare(seq1, seq2);
        });

        // 最新文件：取修改时间最新的文件（extract持续写入的文件）
        String latestFileName = null;
        long latestModified = 0;
        for (File f : thlFiles) {
            if (f.lastModified() > latestModified) {
                latestModified = f.lastModified();
                latestFileName = f.getName();
            }
        }

        for (File thlFile : thlFiles) {
            if (!running.get()) break;

            Long lastProcessedSeqno = processedFiles.get(thlFile.getName());
            if (lastProcessedSeqno != null && lastProcessedSeqno == -1) {
                if (!thlFile.getName().equals(latestFileName)) {
                    continue;
                }
                processedFiles.remove(thlFile.getName());
            }

            processThlFile(thlFile, thlFile.getName().equals(latestFileName));
        }

        // 真实指标：apply 端积压 = 尚未应用完的 .thl 文件数（processedFiles != -1），落盘供 agent 采集
        writeApplyQueueDepth(thlFiles);
    }

    /**
     * 写入 apply 端积压量到 {@code binlog_output/apply_queue_depth}（extract→apply 积压，agent 采集）。
     * 取代随机 mock：0 表示已追平，随 extract 领先程度增长。
     */
    private void writeApplyQueueDepth(File[] thlFiles) {
        long pending = 0;
        for (File f : thlFiles) {
            Long seq = processedFiles.get(f.getName());
            if (seq == null || seq != -1) pending++; // 未标记完成即视为待应用
        }
        try {
            File dir = new File("./files/" + taskId + "/binlog_output");
            if (!dir.exists() && !dir.mkdirs()) return;
            File tmp = new File(dir, "apply_queue_depth.tmp");
            File dst = new File(dir, "apply_queue_depth");
            try (BufferedWriter w = new BufferedWriter(new FileWriter(tmp, false))) {
                w.write(Long.toString(pending));
            }
            if (!tmp.renameTo(dst)) tmp.delete();
        } catch (IOException e) {
            logger.debug("写 apply_queue_depth 失败: {}", e.getMessage());
        }
    }

    private void processThlFile(File thlFile, boolean isLatestFile) {
        String fileName = thlFile.getName();
        Long lastProcessedSeqno = processedFiles.get(fileName);

        if (lastProcessedSeqno != null && lastProcessedSeqno == -1) {
            return;
        }

        logger.info("处理THL文件: {}", fileName);
        int eventCount = 0;

        try (THLFileReader reader = createThlReader(thlFile.getAbsolutePath())) {
            THLEvent event;
            // readEventAfter 只返回 seqno > 已执行seqno 的事件：分帧格式下对已应用事件按字节跳过、
            // 不反序列化，大幅加快增量进程重启时“从 seqno 跳到当前位点”；旧格式自动回退为逐条反序列化跳过。
            while ((event = reader.readEventAfter(lastExecutedSeqno)) != null) {
                if (!running.get()) break;

                if (event.getType() == THLEvent.HEARTBEAT_EVENT) {
                    lastExecutedSeqno = event.getSeqno();

                    String binlogFile = (String) event.getMetadata("binlog_file");
                    Long binlogPosition = (Long) event.getMetadata("binlog_position");
                    checkpointManager.saveCheckpoint(
                            event.getSeqno(),
                            binlogFile != null ? binlogFile : "",
                            binlogPosition != null ? binlogPosition : 0,
                            event.getEventId() != null ? event.getEventId() : ""
                    );

                    if (event.getSourceTstamp() != null) {
                        long rtoMs = System.currentTimeMillis() - event.getSourceTstamp().getTime();
                        if (rtoMs >= 0) {
                            lastRtoMs = rtoMs;
                            lastRtoReportTime = System.currentTimeMillis();
                            lastAppliedSourceTs = event.getSourceTstamp().getTime();
                            writeRtoMetric(rtoMs);
                            logger.debug("Heartbeat RTO: {}ms (seqno={})", rtoMs, event.getSeqno());
                        }
                    }

                    continue;
                }

                // 类型化值管道优先（mysql→pg 且事件带 rows_typed）；不适用返回 null 走文本路径
                List<ParameterizedDml> typedDmls = typedDmlConverter.convert(event);
                List<String> sqlStatements = (typedDmls == null)
                        ? sqlConverter.convertToSql(event)
                        : java.util.Collections.emptyList();

                if (typedDmls != null && !typedDmls.isEmpty()) {
                    logger.info("为seqno={}生成了{}条参数化SQL（类型化值管道）", event.getSeqno(), typedDmls.size());
                } else if (sqlStatements != null && !sqlStatements.isEmpty()) {
                    logger.info("为seqno={}生成了{}条SQL语句", sqlStatements.size(), event.getSeqno());
                }

                // 按事务批量执行SQL，确保同一THL事件内的SQL原子性
                boolean txFailed = false;
                try {
                    if (targetConnection != null && !targetConnection.getAutoCommit()) {
                        targetConnection.setAutoCommit(false);
                    }
                    boolean origAutoCommit = targetConnection.getAutoCommit();
                    if (origAutoCommit) {
                        targetConnection.setAutoCommit(false);
                    }

                    // 双向防回环：数据事件在应用前先写 origin 标记（事务首条语句），
                    // 与业务 DML 原子提交进目标库 binlog，供对端 capture 识别并跳过。
                    if (bidirectionalEnabled && isDataChangeEvent(event)) {
                        try {
                            writeOriginMarker(event.getSeqno());
                        } catch (SQLException me) {
                            logger.warn("写 origin 标记失败 (seqno={})，本事务将不带标记: {}", event.getSeqno(), me.getMessage());
                        }
                    }

                    if (typedDmls != null) {
                        // 类型化路径：PreparedStatement 参数绑定执行
                        for (ParameterizedDml dml : typedDmls) {
                            try {
                                logger.info("执行参数化SQL (seqno={}): {}", event.getSeqno(), dml);
                                executeTypedInTransaction(dml);
                                eventCount++;
                            } catch (SQLException e) {
                                String errorMsg = e.getMessage();
                                if (errorMsg != null && (errorMsg.contains("Duplicate entry") || errorMsg.contains("duplicate key"))) {
                                    logger.warn("重复键忽略 (seqno={}): {}", event.getSeqno(), errorMsg);
                                } else if (errorMsg != null && (errorMsg.contains("Connection") || errorMsg.contains("timed out"))) {
                                    logger.error("目标库连接异常 (seqno={}): {}, 尝试重连", event.getSeqno(), errorMsg);
                                    reconnectTargetDatabase();
                                    try {
                                        executeTypedInTransaction(dml);
                                        eventCount++;
                                        logger.info("重连后参数化SQL重试成功 (seqno={})", event.getSeqno());
                                    } catch (SQLException retryEx) {
                                        txFailed = true;
                                        logger.error("重连后参数化SQL重试失败 (seqno={}): {}", event.getSeqno(), retryEx.getMessage());
                                        writeErrorStatus("E3004", "不可恢复的SQL错误: " + retryEx.getMessage(), event.getSeqno());
                                        break;
                                    }
                                } else {
                                    txFailed = true;
                                    logger.error("不可恢复的SQL错误 (seqno={}): {} - 错误: {}", event.getSeqno(), dml, errorMsg);
                                    writeErrorStatus("E3004", "不可恢复的SQL错误: " + errorMsg, event.getSeqno());
                                    break;
                                }
                            }
                        }
                    } else {
                    for (String sql : sqlStatements) {
                        try {
                            logger.info("执行SQL (seqno={}): {}", event.getSeqno(), sql.substring(0, Math.min(300, sql.length())));
                            executeSqlInTransaction(sql);
                            eventCount++;
                        } catch (SQLException e) {
                            String sqlUpper = sql.toUpperCase().trim();
                            String errorMsg = e.getMessage();
                            boolean isRecoverable = false;

                            if (errorMsg != null && (errorMsg.contains("Duplicate entry") || errorMsg.contains("1062"))) {
                                isRecoverable = true;
                                logger.warn("重复键忽略 (seqno={}): {}", event.getSeqno(), errorMsg);
                            } else if ((sqlUpper.startsWith("UPDATE") || sqlUpper.startsWith("DELETE"))
                                    && errorMsg != null
                                    && (errorMsg.contains("0 rows affected") || errorMsg.contains("not found"))) {
                                isRecoverable = true;
                                logger.warn("UPDATE/DELETE未影响任何行, 已忽略 (seqno={}): {}", event.getSeqno(), errorMsg);
                            } else if (errorMsg != null && (errorMsg.contains("Connection") || errorMsg.contains("Communications link failure") || errorMsg.contains("timed out"))) {
                                logger.error("目标库连接异常 (seqno={}): {}, 尝试重连", event.getSeqno(), errorMsg);
                                reconnectTargetDatabase();
                                try {
                                    executeSqlInTransaction(sql);
                                    eventCount++;
                                    logger.info("重连后SQL重试成功 (seqno={})", event.getSeqno());
                                    isRecoverable = true;
                                } catch (SQLException retryEx) {
                                    logger.error("重连后SQL重试失败 (seqno={}): {}", event.getSeqno(), retryEx.getMessage());
                                }
                            }

                            if (!isRecoverable) {
                                txFailed = true;
                                logger.error("不可恢复的SQL错误 (seqno={}): {} - 错误: {}", event.getSeqno(), sql.substring(0, Math.min(200, sql.length())), errorMsg);
                                writeErrorStatus("E3004", "不可恢复的SQL错误: " + errorMsg, event.getSeqno());
                                break;
                            }
                        }
                    }
                    } // end 文本路径 else

                    if (txFailed) {
                        targetConnection.rollback();
                        logger.warn("事务回滚 (seqno={})", event.getSeqno());
                    } else {
                        targetConnection.commit();
                    }

                    // 恢复原始autoCommit设置
                    if (origAutoCommit) {
                        targetConnection.setAutoCommit(true);
                    }
                } catch (SQLException txEx) {
                    logger.error("事务管理异常 (seqno={}): {}", event.getSeqno(), txEx.getMessage());
                    try { targetConnection.rollback(); } catch (SQLException ignored) {}
                    try { targetConnection.setAutoCommit(true); } catch (SQLException ignored) {}
                }

                // 限速：按本事件实际处理的行数计入配额窗口（typedDmls 与 sqlStatements 二选一，
                // 二者恰好对应两条互斥路径，各自的 size() 就是本事件的行数）
                long rowsThisEvent = typedDmls != null ? typedDmls.size()
                        : (sqlStatements != null ? sqlStatements.size() : 0);
                try {
                    rowRateLimiter.acquire(rowsThisEvent);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }

                lastExecutedSeqno = event.getSeqno();

                // 记录表级延迟（用于热力图）：优先从事件本身的 event_type 判断操作类型，
                // 类型化值管道命中时 sqlStatements 为空列表，仅靠 SQL 文本判断会把所有类型化路径的事件误标为 UNKNOWN
                if (!txFailed) {
                    String opType = determineOpTypeFromEvent(event, sqlStatements);
                    recordTableLatency(event, opType);
                }

                String binlogFile = (String) event.getMetadata("binlog_file");
                Long binlogPosition = (Long) event.getMetadata("binlog_position");
                checkpointManager.saveCheckpoint(
                        event.getSeqno(),
                        binlogFile != null ? binlogFile : "",
                        binlogPosition != null ? binlogPosition : 0,
                        event.getEventId() != null ? event.getEventId() : ""
                );

                if (event.getSourceTstamp() != null) {
                    long now = System.currentTimeMillis();
                    long rtoMs = now - event.getSourceTstamp().getTime();
                    lastAppliedSourceTs = event.getSourceTstamp().getTime();
                    if (rtoMs >= 0 && (eventCount % RTO_REPORT_EVENT_INTERVAL == 0 || now - lastRtoReportTime > RTO_REPORT_INTERVAL_MS)) {
                        lastRtoMs = rtoMs;
                        lastRtoReportTime = now;
                        writeRtoMetric(rtoMs);
                    }
                }

                if (eventCount % 100 == 0) {
                    logger.info("已处理{}个事件, 最后seqno: {}", eventCount, lastExecutedSeqno);
                }
            }
        } catch (Exception e) {
            logger.error("处理THL文件出错: {}, 跳过至下一个文件. 错误: {}", fileName, e.getMessage());
            // 不再抛出 RuntimeException，避免进程崩溃后反复重启在同一位置失败
            // 标记该文件已处理（跳过），继续处理下一个文件
            if (isLatestFile) {
                processedFiles.put(fileName, lastExecutedSeqno);
            } else {
                processedFiles.put(fileName, -1L);
            }
            saveProgress();
        }

        if (eventCount > 0) {
            logger.info("处理THL文件完成: {} -> 执行了{}个事件, 最后seqno: {}", fileName, eventCount, lastExecutedSeqno);
        }

        if (isLatestFile) {
            processedFiles.put(fileName, lastExecutedSeqno);
        } else {
            processedFiles.put(fileName, -1L);
            // 非最新文件已处理完成，触发清理已处理的THL文件
            cleanupProcessedThlFiles();
        }
        saveProgress();
    }

    /**
     * 清理已完全处理的THL文件，保留最近 thlRetentionCount 个已处理文件作为安全余量。
     * 只删除 processedFiles 中标记为 -1L 的文件（即非最新且已处理完毕的文件）。
     */
    private void cleanupProcessedThlFiles() {
        try {
            File thlDir = new File(thlDirectory);
            if (!thlDir.exists() || !thlDir.isDirectory()) return;

            File[] thlFiles = thlDir.listFiles((dir, name) ->
                    name.endsWith(".thl") && !name.startsWith("."));
            if (thlFiles == null || thlFiles.length == 0) return;

            // 按seqno排序
            Arrays.sort(thlFiles, (f1, f2) -> {
                long seq1 = extractSeqnoFromName(f1.getName());
                long seq2 = extractSeqnoFromName(f2.getName());
                return Long.compare(seq1, seq2);
            });

            // 收集已处理完成的文件（processedFiles中标记为-1L的）
            List<File> completedFiles = new ArrayList<>();
            for (File f : thlFiles) {
                Long state = processedFiles.get(f.getName());
                if (state != null && state == -1L) {
                    completedFiles.add(f);
                }
            }

            // 保留最近 thlRetentionCount 个已处理文件，删除其余的
            if (completedFiles.size() <= thlRetentionCount) {
                return;
            }

            int toDelete = completedFiles.size() - thlRetentionCount;
            for (int i = 0; i < toDelete; i++) {
                File f = completedFiles.get(i);
                if (f.delete()) {
                    processedFiles.remove(f.getName());
                    logger.info("已清理已处理的THL文件: {}", f.getName());
                } else {
                    logger.warn("清理THL文件失败: {}", f.getName());
                }
            }
            saveProgress();
        } catch (Exception e) {
            logger.warn("清理已处理THL文件时异常: {}", e.getMessage());
        }
    }

    private void executeSql(String sql) throws SQLException {
        if (sql == null || sql.trim().isEmpty()) return;

        // 检查目标库连接是否有效
        if (targetConnection == null || targetConnection.isClosed()) {
            logger.warn("目标库连接已关闭, 尝试重连");
            reconnectTargetDatabase();
            if (targetConnection == null || targetConnection.isClosed()) {
                throw new SQLException("重连目标数据库失败");
            }
        }

        String trimmedSql = sql.trim();

        if (isPostgresql) {
            if (trimmedSql.equalsIgnoreCase("COMMIT;") || trimmedSql.equalsIgnoreCase("COMMIT")) {
                if (!targetConnection.getAutoCommit()) {
                    targetConnection.commit();
                }
                return;
            }
            if (trimmedSql.toUpperCase().startsWith("SET SEARCH_PATH") ||
                trimmedSql.toUpperCase().startsWith("SET SEARCH_PATH")) {
                try (Statement stmt = targetConnection.createStatement()) {
                    stmt.execute(trimmedSql);
                }
                return;
            }
        }

        try (Statement stmt = targetConnection.createStatement()) {
            boolean hasResultSet = stmt.execute(trimmedSql);
            int updateCount = stmt.getUpdateCount();
            logger.info("SQL执行完成: autoCommit={}, hasResultSet={}, updateCount={}", 
                targetConnection.getAutoCommit(), hasResultSet, updateCount);
        }
    }

    /**
     * 类型化值管道：以 PreparedStatement 参数绑定执行单条 DML（值永不拼接进 SQL 文本）。
     * 事务边界由调用方控制。
     */
    private void executeTypedInTransaction(ParameterizedDml dml) throws SQLException {
        if (targetConnection == null || targetConnection.isClosed()) {
            throw new SQLException("目标数据库连接不可用");
        }
        try (PreparedStatement ps = targetConnection.prepareStatement(dml.getSql())) {
            List<Object> params = dml.getParams();
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.executeUpdate();
        }
    }

    /**
     * 在事务中执行单条SQL，不管理commit/rollback（由调用方控制事务边界）。
     * 跳过COMMIT语句，因为事务由外层统一提交。
     */
    private void executeSqlInTransaction(String sql) throws SQLException {
        if (sql == null || sql.trim().isEmpty()) return;

        if (targetConnection == null || targetConnection.isClosed()) {
            throw new SQLException("目标数据库连接不可用");
        }

        String trimmedSql = sql.trim();

        // 跳过COMMIT语句，事务由外层统一管理
        if (trimmedSql.equalsIgnoreCase("COMMIT;") || trimmedSql.equalsIgnoreCase("COMMIT")) {
            return;
        }

        // PostgreSQL SET SEARCH_PATH 需要在事务外执行
        if (isPostgresql && trimmedSql.toUpperCase().startsWith("SET SEARCH_PATH")) {
            try (Statement stmt = targetConnection.createStatement()) {
                stmt.execute(trimmedSql);
            }
            return;
        }

        try (Statement stmt = targetConnection.createStatement()) {
            stmt.execute(trimmedSql);
        }
    }

    /** 确保目录存在 */
    private void ensureDirExists(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    /** 从THL文件名中提取seqno数字，用于正确排序 */
    private long extractSeqnoFromName(String fileName) {
        // 文件名格式: binlog_YYYYMMDD_HHMMSS_XXXX_SEQ.thl
        // 提取最后一段数字（SEQ）
        String name = fileName.replace(".thl", "");
        int lastUnderscore = name.lastIndexOf('_');
        if (lastUnderscore >= 0 && lastUnderscore < name.length() - 1) {
            try {
                return Long.parseLong(name.substring(lastUnderscore + 1));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    /** 创建 THL 文件读取器（根据加密配置自动选择） */
    private THLFileReader createThlReader(String filePath) throws IOException {
        if (thlEncryptionService != null && thlEncryptionService.isEnabled()) {
            return new EncryptedTHLFileReader(filePath, thlEncryptionService);
        }
        return new THLFileReader(filePath);
    }

    /** 根据SQL列表判断操作类型 */
    private String determineOpType(List<String> sqlStatements) {
        if (sqlStatements == null || sqlStatements.isEmpty()) return "UNKNOWN";
        for (String sql : sqlStatements) {
            String upper = sql.trim().toUpperCase();
            if (upper.startsWith("INSERT")) return "INSERT";
            if (upper.startsWith("UPDATE")) return "UPDATE";
            if (upper.startsWith("DELETE")) return "DELETE";
            if (upper.startsWith("CREATE") || upper.startsWith("ALTER") || upper.startsWith("DROP")) return "DDL";
        }
        return "OTHER";
    }

    /**
     * 根据事件本身的 event_type 元数据判断操作类型（源自 mysql/oracle/pg 三种 capture 各自的命名）。
     * 类型化值管道命中时 sqlStatements 为空列表，{@link #determineOpType} 只能靠 SQL 文本判断，
     * 会把所有类型化路径的事件误标为 UNKNOWN；这里优先看事件自身的类型，仅在缺失/不识别时才回退文本判断。
     */
    /** 是否为数据变更（INSERT/UPDATE/DELETE）事件——仅这类事件写 origin 标记；DDL/心跳等不写。 */
    private boolean isDataChangeEvent(THLEvent event) {
        String t = (String) event.getMetadata("event_type");
        if (t == null) return false;
        switch (t) {
            case "INSERT": case "WRITE_ROWS": case "EXT_WRITE_ROWS":
            case "UPDATE": case "UPDATE_ROWS": case "EXT_UPDATE_ROWS":
            case "DELETE": case "DELETE_ROWS": case "EXT_DELETE_ROWS":
                return true;
            default:
                return false;
        }
    }

    private String determineOpTypeFromEvent(THLEvent event, List<String> sqlStatements) {
        String eventType = (String) event.getMetadata("event_type");
        if (eventType != null) {
            switch (eventType) {
                case "INSERT":
                case "WRITE_ROWS":
                case "EXT_WRITE_ROWS":
                    return "INSERT";
                case "UPDATE":
                case "UPDATE_ROWS":
                case "EXT_UPDATE_ROWS":
                    return "UPDATE";
                case "DELETE":
                case "DELETE_ROWS":
                case "EXT_DELETE_ROWS":
                    return "DELETE";
                case "QUERY":
                    return "DDL";
                default:
                    break;
            }
        }
        return determineOpType(sqlStatements);
    }

    /**
     * 记录表级同步延迟到文件，供热力图使用。
     * 文件格式：./files/{taskId}/binlog_output/table_latency/{tableName}.tsv
     * 每行：appliedTs\teventTs\tlatencyMs\topType
     */
    private void recordTableLatency(THLEvent event, String opType) {
        if (event == null || tableLatencyDir == null) return;
        String tableName = (String) event.getMetadata("table_name");
        if (tableName == null || tableName.isEmpty()) return;

        long appliedTs = System.currentTimeMillis();
        long eventTs = event.getSourceTstamp() != null ? event.getSourceTstamp().getTime() : appliedTs;
        long latencyMs = Math.max(0, appliedTs - eventTs);

        try {
            File dir = new File(tableLatencyDir);
            if (!dir.exists()) dir.mkdirs();
            String safeName = tableName.replaceAll("[^a-zA-Z0-9_]", "_");
            File file = new File(dir, safeName + ".tsv");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
                writer.write(String.format("%d\t%d\t%d\t%s%n", appliedTs, eventTs, latencyMs, opType));
            }
        } catch (IOException e) {
            logger.debug("记录表延迟失败: {}", e.getMessage());
        }
    }

    private void reconnectTargetDatabase() {
        // 关闭旧连接
        if (targetConnection != null) {
            try {
                targetConnection.close();
            } catch (SQLException e) {
                logger.warn("关闭旧目标库连接出错: {}", e.getMessage());
            }
        }

        // 尝试重连，最多3次
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                connectToTargetDatabase();
                logger.info("目标库重连成功, 第{}次尝试", attempt);
                return;
            } catch (SQLException e) {
                logger.warn("重连尝试 {}/3 失败: {}", attempt, e.getMessage());
                if (attempt < 3) {
                    try { Thread.sleep(3000); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        logger.error("3次重连目标库均失败");
    }

    private void loadProgress() {
        File file = new File(progressFile);
        if (!file.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split("\\|");
                if (parts.length >= 2) {
                    processedFiles.put(parts[0], Long.parseLong(parts[1]));
                }
            }
        } catch (Exception e) {
            logger.warn("加载增量同步进度出错", e);
        }
    }

    private void saveProgress() {
        File file = new File(progressFile);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (Map.Entry<String, Long> entry : processedFiles.entrySet()) {
                writer.write(entry.getKey() + "|" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            logger.warn("保存增量同步进度出错", e);
        }
    }

    private void writeRtoMetric(long rtoMs) {
        String metricsDir = "./files/" + taskId + "/binlog_output";
        File dir = new File(metricsDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File metricFile = new File(dir, "rto_metric");
        try (PrintWriter pw = new PrintWriter(new FileWriter(metricFile, false))) {
            pw.println(System.currentTimeMillis() + "|" + rtoMs + "|" + lastAppliedSourceTs);
        } catch (IOException e) {
            logger.warn("写入RTO指标失败: {}", e.getMessage());
        }
    }

    /**
     * 写入错误状态文件，供 agent 监控循环检测并上报 FAILED 状态。
     */
    private void writeErrorStatus(String errorCode, String errorMessage, long seqno) {
        String metricsDir = "./files/" + taskId + "/binlog_output";
        File dir = new File(metricsDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File errorFile = new File(dir, "error_status");
        try (PrintWriter pw = new PrintWriter(new FileWriter(errorFile, false))) {
            pw.println(System.currentTimeMillis() + "|" + errorCode + "|" + seqno + "|" + errorMessage.replace("|", "/"));
        } catch (IOException e) {
            logger.warn("写入错误状态文件失败: {}", e.getMessage());
        }
        logger.error("已写入错误状态文件: errorCode={}, seqno={}, message={}", errorCode, seqno, errorMessage);
    }

    public long getLastRtoMs() {
        return lastRtoMs;
    }

    public void close() {
        saveProgress();
        checkpointManager.close();
        if (targetConnection != null) {
            try {
                targetConnection.close();
            } catch (SQLException e) {
                logger.error("关闭目标库连接出错", e);
            }
        }
    }
}
