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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    /**
     * 人工裁决跳过的事件（fail-stop 后运维确认无法/无需修复时，经后端"跳过失败事件并重试"下发；
     * 命中的事件不应用、写死信记录后推进位点）。
     * 首选按 eventId（binlog文件:位点，increment.skip.event.ids）匹配——resume 后 capture 重读
     * binlog 会给同一事件重新分配 seqno，seqno 跨重启不稳定；seqno 集（increment.skip.seqnos）
     * 仅作为老错误信息（不带 eventId）的兼容兜底。
     */
    private final java.util.Set<Long> skipSeqnos = new java.util.HashSet<>();
    private final java.util.Set<String> skipEventIds = new java.util.HashSet<>();

    /**
     * 增量并行应用并发度（increment.apply.parallelism，默认 1=原串行路径不动）。
     * >1 时按表 hash 分片到 N 个 worker（各自独立目标连接），同表事件保序、跨表并发。
     * 跨表乱序由 FOREIGN_KEY_CHECKS=0 + 幂等 upsert 兜底（与全量并行、resume 整段重放同一套保证）。
     * DDL/心跳/被裁决跳过等非行事件作 barrier（先 drain 当前批，再串行处理）。
     */
    private int applyParallelism = 1;
    /** 单个并行批的事件数上限（increment.apply.batch.size）：批越大并行度利用越充分，但失败回退与内存占用越大。 */
    private int applyBatchSize = 500;
    /** 并行应用执行器（懒初始化，仅 applyParallelism>1 时创建；进程停止时关闭）。 */
    private ParallelApplyExecutor parallelExecutor;

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

        String skipSeqnosProp = props.getProperty("increment.skip.seqnos", "").trim();
        if (!skipSeqnosProp.isEmpty()) {
            for (String s : skipSeqnosProp.split(",")) {
                try {
                    skipSeqnos.add(Long.parseLong(s.trim()));
                } catch (NumberFormatException nfe) {
                    logger.warn("忽略非法的跳过 seqno: {}", s);
                }
            }
        }
        String skipEventIdsProp = props.getProperty("increment.skip.event.ids", "").trim();
        if (!skipEventIdsProp.isEmpty()) {
            for (String s : skipEventIdsProp.split(",")) {
                if (!s.trim().isEmpty()) skipEventIds.add(s.trim());
            }
        }
        if (!skipSeqnos.isEmpty() || !skipEventIds.isEmpty()) {
            logger.warn("人工裁决跳过事件已配置: eventIds={}, seqnos={}（命中事件不应用，写死信记录后推进位点）",
                    skipEventIds, skipSeqnos);
        }

        applyParallelism = Math.max(1, Integer.parseInt(props.getProperty("increment.apply.parallelism", "1")));
        applyBatchSize = Math.max(1, Integer.parseInt(props.getProperty("increment.apply.batch.size", "500")));
        if (applyParallelism > 1) {
            logger.info("增量并行应用已启用: 并发度={}, 批大小={}（按表分片，同表保序，跨表并发）",
                    applyParallelism, applyBatchSize);
        }
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

    /** 目标库 JDBC URL（串行主连接与并行 worker 连接共用，避免 URL 口径漂移）。 */
    private String buildTargetJdbcUrl() {
        if (isPostgresql) {
            String jdbcUrl = props.getProperty("target.db.jdbc.url");
            if (jdbcUrl != null && !jdbcUrl.isEmpty()) {
                return jdbcUrl;
            }
            // stringtype=unspecified：字符串参数由 PG 按列类型推断（interval/jsonb/时间等绑定依赖）
            return "jdbc:postgresql://" + targetHost + ":" + targetPort + "/" + targetDatabase
                    + "?stringtype=unspecified";
        }
        return "jdbc:mysql://" + targetHost + ":" + targetPort + "/" + targetDatabase +
                "?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8&allowPublicKeyRetrieval=true";
    }

    private void connectToTargetDatabase() throws SQLException {
        String url = buildTargetJdbcUrl();
        targetConnection = ConnectionPoolManager.getConnection(url, targetUser, targetPassword);
        logger.info("已连接目标数据库: {}:{}/{} (类型: {})", targetHost, targetPort, targetDatabase,
                isPostgresql ? "postgresql" : "mysql");
        // MySQL 目标关闭本会话外键检查：增量按 binlog 顺序应用本身满足约束，但部分表同步/
        // 列过滤会破坏引用完整性（父行被过滤而子行保留），幂等重放（重试续传）也可能暂时乱序。
        // 与全量搬数会话保持一致语义；重连走同一入口，新会话自动重设。
        if (!isPostgresql) {
            try (Statement st = targetConnection.createStatement()) {
                st.execute("SET FOREIGN_KEY_CHECKS=0");
            } catch (SQLException e) {
                logger.warn("设置 FOREIGN_KEY_CHECKS=0 失败（继续执行）: {}", e.getMessage());
            }
        }
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

        // 并行应用（opt-in）：默认 applyParallelism=1 时不进入此分支，串行路径完全不变
        if (applyParallelism > 1) {
            processThlFileParallel(thlFile, isLatestFile);
            return;
        }

        logger.info("处理THL文件: {}", fileName);
        int eventCount = 0;
        // 本文件是否被中途打断（优雅停止 / 不可恢复错误 fail-stop）：
        // 打断时绝不能把文件标记为“已处理完(-1)”，否则重启后整个文件被跳过造成数据丢失
        boolean aborted = false;

        try (THLFileReader reader = createThlReader(thlFile.getAbsolutePath())) {
            THLEvent event;
            // readEventAfter 只返回 seqno > 已执行seqno 的事件：分帧格式下对已应用事件按字节跳过、
            // 不反序列化，大幅加快增量进程重启时“从 seqno 跳到当前位点”；旧格式自动回退为逐条反序列化跳过。
            while ((event = reader.readEventAfter(lastExecutedSeqno)) != null) {
                if (!running.get()) { aborted = true; break; }

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

                // 人工裁决跳过：命中 eventId（跨重启稳定）或 seqno（老错误信息兼容）的事件不应用，
                // 写死信记录后按正常事件推进位点。放在转换之前——毒事件可能在转换阶段就抛异常，
                // 转换只在死信记录里尽力而为
                if ((!skipEventIds.isEmpty() && event.getEventId() != null && skipEventIds.contains(event.getEventId()))
                        || (!skipSeqnos.isEmpty() && skipSeqnos.contains(event.getSeqno()))) {
                    recordDeadLetter(event);
                    lastExecutedSeqno = event.getSeqno();
                    String skipBinlogFile = (String) event.getMetadata("binlog_file");
                    Long skipBinlogPosition = (Long) event.getMetadata("binlog_position");
                    checkpointManager.saveCheckpoint(
                            event.getSeqno(),
                            skipBinlogFile != null ? skipBinlogFile : "",
                            skipBinlogPosition != null ? skipBinlogPosition : 0,
                            event.getEventId() != null ? event.getEventId() : ""
                    );
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
                                        writeErrorStatus("E3004", "不可恢复的SQL错误: " + retryEx.getMessage(), event);
                                        break;
                                    }
                                } else {
                                    txFailed = true;
                                    logger.error("不可恢复的SQL错误 (seqno={}): {} - 错误: {}", event.getSeqno(), dml, errorMsg);
                                    writeErrorStatus("E3004", "不可恢复的SQL错误: " + errorMsg, event);
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
                                writeErrorStatus("E3004", "不可恢复的SQL错误: " + errorMsg, event);
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
                    txFailed = true;
                }

                // fail-stop：事务失败（已回滚）时绝不推进 lastExecutedSeqno/checkpoint，
                // 并停止整个增量处理——否则该事件在重试后被跳过（静默丢数），且失败点之后
                // 继续应用会造成乱序偏差（后续对同一行的 UPDATE 因"未影响任何行"被吞掉）。
                // 修复问题后重试/恢复将从本事件精确续传（应用侧幂等语义保证重放安全）。
                if (txFailed) {
                    logger.error("增量应用 fail-stop：seqno={} 未计入位点，停止处理；修复目标端问题后重试将从该事件继续",
                            event.getSeqno());
                    aborted = true;
                    running.set(false);
                    break;
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

        // 中途打断（fail-stop / 优雅停止）：只记录已应用到的 seqno，绝不标记 -1（已处理完）。
        // 否则重启后该文件被整体跳过，未应用的事件全部丢失。
        if (aborted) {
            processedFiles.put(fileName, lastExecutedSeqno);
            saveProgress();
            return;
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

    /** 已写过死信的 eventId（分带SQL/仅元数据两类）：resume 重放 binlog 会反复命中同一跳过事件，按类去重避免重复追加。 */
    private final java.util.Set<String> deadletterRecordedSql = new java.util.HashSet<>();
    private final java.util.Set<String> deadletterRecordedMeta = new java.util.HashSet<>();
    private boolean deadletterIndexLoaded = false;

    /** 启动后首次写死信前，把既有 deadletter.jsonl 中的 eventId 载入去重索引（跨进程重启防重）。 */
    private void loadDeadletterIndex() {
        deadletterIndexLoaded = true;
        File dlFile = new File("./files/" + taskId + "/deadletter.jsonl");
        if (!dlFile.exists()) return;
        java.util.regex.Pattern p = java.util.regex.Pattern.compile("\"eventId\":\"([^\"]*)\"");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(dlFile), java.nio.charset.StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                java.util.regex.Matcher m = p.matcher(line);
                if (m.find() && !m.group(1).isEmpty()) {
                    if (line.contains("\"statements\":[]")) deadletterRecordedMeta.add(m.group(1));
                    else deadletterRecordedSql.add(m.group(1));
                }
            }
        } catch (IOException e) {
            logger.warn("读取既有死信记录失败（去重索引不完整，可能出现重复记录）: {}", e.getMessage());
        }
    }

    /**
     * 死信记录：人工裁决跳过的事件逐条追加到 files/<taskId>/deadletter.jsonl（每行一个 JSON 对象），
     * 供 agent HTTP /api/agent/deadletter/<taskId> 读取、后端代理给 UI 展示。
     * SQL 为尽力转换（毒事件可能在转换阶段就失败，此时只记事件元数据）。
     * 同一 eventId 带SQL/仅元数据各至多记录一次——resume 重放会反复命中已跳过事件。
     */
    private void recordDeadLetter(THLEvent event) {
        if (!deadletterIndexLoaded) {
            loadDeadletterIndex();
        }
        List<String> statements = java.util.Collections.emptyList();
        try {
            statements = sqlConverter.convertToSql(event);
        } catch (Exception e) {
            logger.warn("死信事件 SQL 转换失败 (seqno={})，仅记录元数据: {}", event.getSeqno(), e.getMessage());
        }
        String eid = event.getEventId();
        if (eid != null && !eid.isEmpty()) {
            boolean firstOfKind = statements.isEmpty()
                    ? deadletterRecordedMeta.add(eid)
                    : deadletterRecordedSql.add(eid);
            if (!firstOfKind) {
                logger.info("死信已记录过 (eventId={}, seqno={})，跳过重复写入", eid, event.getSeqno());
                return;
            }
        }
        StringBuilder sb = new StringBuilder(256);
        sb.append('{')
          .append("\"ts\":").append(System.currentTimeMillis()).append(',')
          .append("\"seqno\":").append(event.getSeqno()).append(',')
          .append("\"eventId\":\"").append(jsonEscape(event.getEventId())).append("\",")
          .append("\"eventType\":\"").append(jsonEscape(String.valueOf(event.getMetadata("event_type")))).append("\",")
          .append("\"tableName\":\"").append(jsonEscape(String.valueOf(event.getMetadata("table_name")))).append("\",")
          .append("\"binlogFile\":\"").append(jsonEscape(String.valueOf(event.getMetadata("binlog_file")))).append("\",")
          .append("\"binlogPosition\":\"").append(jsonEscape(String.valueOf(event.getMetadata("binlog_position")))).append("\",")
          .append("\"reason\":\"manual-skip\",")
          .append("\"statements\":[");
        for (int i = 0; i < statements.size(); i++) {
            if (i > 0) sb.append(',');
            String stmt = statements.get(i);
            sb.append('"').append(jsonEscape(stmt.length() > 2000 ? stmt.substring(0, 2000) + "…" : stmt)).append('"');
        }
        sb.append("]}");

        File dlFile = new File("./files/" + taskId + "/deadletter.jsonl");
        dlFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(dlFile, true))) {
            pw.println(sb);
        } catch (IOException e) {
            logger.error("写入死信记录失败 (seqno={}): {}", event.getSeqno(), e.getMessage());
        }
        logger.warn("事件已按人工裁决跳过并记入死信: seqno={}, table={}, statements={}",
                event.getSeqno(), event.getMetadata("table_name"), statements.size());
    }

    private static String jsonEscape(String s) {
        if (s == null || "null".equals(s)) return "";
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
            }
        }
        return out.toString();
    }

    // ==================== 并行应用（increment.apply.parallelism>1） ====================
    //
    // 设计：转换在主线程按 seqno 顺序进行（规避转换器线程安全问题、保持 DDL 的 SchemaEvolution 副作用有序），
    // 仅"执行"并行——主线程把已转换事件按表 hash 分发到 N 个 worker，各 worker 在自己的连接上按序应用其表的事件。
    // barrier（DDL/心跳/被裁决跳过/无表名事件）先 drain 当前批，再主线程串行处理，随后继续。
    // checkpoint 只推进到"连续完成的低水位"：任一事件失败即 fail-stop，位点停在最小失败 seqno 之前；
    // 失败点之后已并行落库的其它表事件在 resume 时被幂等重放（与串行 fail-stop、resume 整段重放同一保证）。

    /** 一个待应用的已转换事件（typed 与 text 二选一）。 */
    private static final class WorkItem {
        final THLEvent event;
        final List<ParameterizedDml> typedDmls;
        final List<String> sqlStatements;
        WorkItem(THLEvent event, List<ParameterizedDml> typedDmls, List<String> sqlStatements) {
            this.event = event;
            this.typedDmls = typedDmls;
            this.sqlStatements = sqlStatements;
        }
    }

    /** 并行执行器：固定 N 线程 + N 个独立目标连接（跨文件复用，进程停止时关闭）。 */
    private final class ParallelApplyExecutor {
        final int n;
        final Connection[] conns;
        final ExecutorService pool;
        ParallelApplyExecutor(int n) throws SQLException {
            this.n = n;
            this.conns = new Connection[n];
            for (int i = 0; i < n; i++) {
                Connection c = ConnectionPoolManager.getConnection(buildTargetJdbcUrl(), targetUser, targetPassword);
                if (!isPostgresql) {
                    try (Statement st = c.createStatement()) { st.execute("SET FOREIGN_KEY_CHECKS=0"); }
                    catch (SQLException e) { logger.warn("worker 连接设置 FOREIGN_KEY_CHECKS=0 失败: {}", e.getMessage()); }
                }
                if (bidirectionalEnabled) ensureMarkerTableOn(c);
                conns[i] = c;
            }
            this.pool = Executors.newFixedThreadPool(n, r -> {
                Thread t = new Thread(r, "increment-apply-worker");
                t.setDaemon(true);
                return t;
            });
        }
        void close() {
            pool.shutdownNow();
            for (Connection c : conns) {
                try { if (c != null) c.close(); } catch (SQLException ignored) {}
            }
        }
    }

    private boolean isParallelizable(THLEvent event) {
        if (!isDataChangeEvent(event)) return false;
        String table = (String) event.getMetadata("table_name");
        return table != null && !table.isEmpty();
    }

    private void processThlFileParallel(File thlFile, boolean isLatestFile) {
        String fileName = thlFile.getName();
        logger.info("处理THL文件(并行 x{}): {}", applyParallelism, fileName);
        boolean aborted = false;

        try {
            if (parallelExecutor == null) {
                parallelExecutor = new ParallelApplyExecutor(applyParallelism);
            }
        } catch (SQLException e) {
            logger.error("初始化并行应用执行器失败，本文件跳过: {}", e.getMessage());
            return;
        }

        try (THLFileReader reader = createThlReader(thlFile.getAbsolutePath())) {
            List<WorkItem> batch = new ArrayList<>();
            // 读游标独立于已提交低水位 lastExecutedSeqno：批内事件读入后 lastExecutedSeqno 尚未推进，
            // 必须用单独游标前进，否则 readEventAfter 会反复返回批首事件。fail-stop 时文件按
            // lastExecutedSeqno（低水位）标记，resume 从低水位重读——批内已并行落库者被幂等重放
            long readCursor = lastExecutedSeqno;
            THLEvent event;
            while ((event = reader.readEventAfter(readCursor)) != null) {
                if (!running.get()) { aborted = true; break; }
                readCursor = event.getSeqno();

                boolean isSkip = (!skipEventIds.isEmpty() && event.getEventId() != null && skipEventIds.contains(event.getEventId()))
                        || (!skipSeqnos.isEmpty() && skipSeqnos.contains(event.getSeqno()));

                // barrier：心跳 / 被裁决跳过 / 非并行化（DDL、无表名）——先 drain 当前批再串行处理
                if (event.getType() == THLEvent.HEARTBEAT_EVENT || isSkip || !isParallelizable(event)) {
                    if (!flushBatch(batch)) { aborted = true; break; }
                    batch.clear();
                    if (!handleBarrierEvent(event, isSkip)) { aborted = true; break; }
                    continue;
                }

                // 并行化 DML：转换在主线程完成（保持有序、规避转换器线程安全）
                List<ParameterizedDml> typedDmls = typedDmlConverter.convert(event);
                List<String> sqlStatements = (typedDmls == null) ? sqlConverter.convertToSql(event) : null;
                batch.add(new WorkItem(event, typedDmls, sqlStatements));

                if (batch.size() >= applyBatchSize) {
                    if (!flushBatch(batch)) { aborted = true; break; }
                    batch.clear();
                }
            }
            if (!aborted) {
                if (!flushBatch(batch)) aborted = true;
                batch.clear();
            }
        } catch (Exception e) {
            logger.error("处理THL文件(并行)出错: {}, 错误: {}", fileName, e.getMessage(), e);
            if (isLatestFile) processedFiles.put(fileName, lastExecutedSeqno);
            else processedFiles.put(fileName, -1L);
            saveProgress();
            return;
        }

        // 中途打断（fail-stop / 优雅停止）：只记已应用到的 seqno，绝不标 -1（与串行同）
        if (aborted) {
            processedFiles.put(fileName, lastExecutedSeqno);
            saveProgress();
            return;
        }
        if (isLatestFile) {
            processedFiles.put(fileName, lastExecutedSeqno);
        } else {
            processedFiles.put(fileName, -1L);
            cleanupProcessedThlFiles();
        }
        saveProgress();
    }

    /** barrier 事件串行处理。返回 false 表示应 fail-stop/abort 停止本文件。 */
    private boolean handleBarrierEvent(THLEvent event, boolean isSkip) {
        // 心跳：仅推进位点 + RTO
        if (event.getType() == THLEvent.HEARTBEAT_EVENT) {
            advanceCheckpoint(event);
            reportRtoFromEvent(event, true);
            return true;
        }
        // 人工裁决跳过：写死信后推进位点（不应用）
        if (isSkip) {
            recordDeadLetter(event);
            advanceCheckpoint(event);
            return true;
        }
        // 其余（DDL / 无表名的数据事件）：主线程转换 + 在 worker[0] 连接上串行应用
        List<ParameterizedDml> typedDmls = typedDmlConverter.convert(event);
        List<String> sqlStatements = (typedDmls == null) ? sqlConverter.convertToSql(event) : null;
        Connection conn = parallelExecutor.conns[0];
        try {
            applyEventTx(event, conn, typedDmls, sqlStatements);
        } catch (SQLException e) {
            logger.error("并行模式 barrier 事件应用失败 fail-stop (seqno={}): {}", event.getSeqno(), e.getMessage());
            writeErrorStatus("E3004", "不可恢复的SQL错误: " + e.getMessage(), event);
            running.set(false);
            return false;
        }
        int rows = (typedDmls != null) ? typedDmls.size() : (sqlStatements != null ? sqlStatements.size() : 0);
        acquireRateLimit(rows);
        advanceCheckpoint(event);
        recordTableLatency(event, determineOpTypeFromEvent(event, sqlStatements));
        reportRtoFromEvent(event, false);
        return true;
    }

    /**
     * 应用一批并行化 DML：按表 hash 分片到 worker，各 worker 按序应用其表的事件。
     * 返回 true=全批成功（位点推进到批末）；false=有失败已 fail-stop（位点推进到最小失败 seqno 之前）。
     */
    private boolean flushBatch(List<WorkItem> batch) {
        if (batch.isEmpty()) return true;
        final int n = parallelExecutor.n;

        // 按表 hash 分片，保持各分片内 seqno 升序（batch 本身升序）
        List<List<WorkItem>> shards = new ArrayList<>(n);
        for (int i = 0; i < n; i++) shards.add(new ArrayList<>());
        for (WorkItem wi : batch) {
            String table = (String) wi.event.getMetadata("table_name");
            int idx = Math.floorMod(table.hashCode(), n);
            shards.get(idx).add(wi);
        }

        List<Future<Long>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final List<WorkItem> shard = shards.get(i);
            final Connection conn = parallelExecutor.conns[i];
            futures.add(parallelExecutor.pool.submit((Callable<Long>) () -> {
                for (WorkItem wi : shard) {
                    try {
                        applyEventTx(wi.event, conn, wi.typedDmls, wi.sqlStatements);
                        recordTableLatency(wi.event, determineOpTypeFromEvent(wi.event, wi.sqlStatements));
                    } catch (SQLException e) {
                        logger.error("并行 worker 应用失败 (seqno={}): {}", wi.event.getSeqno(), e.getMessage());
                        return wi.event.getSeqno(); // 该表在此 seqno 失败，停止本 worker（同表后续不再应用）
                    }
                }
                return Long.MAX_VALUE; // 无失败
            }));
        }

        long minFailed = Long.MAX_VALUE;
        boolean interrupted = false;
        for (Future<Long> f : futures) {
            try {
                minFailed = Math.min(minFailed, f.get());
            } catch (Exception e) {
                logger.error("并行 worker 执行异常: {}", e.getMessage());
                minFailed = Math.min(minFailed, batch.get(0).event.getSeqno()); // 保守：从批首失败
                interrupted = true;
            }
        }

        if (minFailed == Long.MAX_VALUE && !interrupted) {
            // 全批成功：位点推进到批末，限速按全批行数
            long rows = 0;
            for (WorkItem wi : batch) {
                rows += (wi.typedDmls != null) ? wi.typedDmls.size()
                        : (wi.sqlStatements != null ? wi.sqlStatements.size() : 0);
            }
            acquireRateLimit(rows);
            WorkItem last = batch.get(batch.size() - 1);
            advanceCheckpoint(last.event);
            reportRtoFromEvent(last.event, false);
            return true;
        }

        // 有失败：位点推进到"最小失败 seqno 之前"的批内最大 seqno（这些必已成功，minFailed 是全局最小失败）
        THLEvent lowWater = null;
        THLEvent failing = null;
        for (WorkItem wi : batch) {
            long s = wi.event.getSeqno();
            if (s < minFailed) lowWater = wi.event;
            if (s == minFailed) failing = wi.event;
        }
        if (lowWater != null) {
            advanceCheckpoint(lowWater);
        }
        if (failing != null) {
            writeErrorStatus("E3004", "不可恢复的SQL错误（并行应用）", failing);
        }
        logger.error("增量并行应用 fail-stop：最小失败 seqno={} 未计入位点，停止处理；修复后 resume 从该事件继续",
                minFailed == Long.MAX_VALUE ? -1 : minFailed);
        running.set(false);
        return false;
    }

    /** 推进 lastExecutedSeqno 并落 checkpoint（位点信息取自事件元数据）。 */
    private void advanceCheckpoint(THLEvent event) {
        lastExecutedSeqno = event.getSeqno();
        String binlogFile = (String) event.getMetadata("binlog_file");
        Long binlogPosition = (Long) event.getMetadata("binlog_position");
        checkpointManager.saveCheckpoint(
                event.getSeqno(),
                binlogFile != null ? binlogFile : "",
                binlogPosition != null ? binlogPosition : 0,
                event.getEventId() != null ? event.getEventId() : ""
        );
    }

    private void acquireRateLimit(long rows) {
        if (rows <= 0) return;
        try {
            rowRateLimiter.acquire(rows);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private void reportRtoFromEvent(THLEvent event, boolean heartbeat) {
        if (event.getSourceTstamp() == null) return;
        long now = System.currentTimeMillis();
        long rtoMs = now - event.getSourceTstamp().getTime();
        if (rtoMs < 0) return;
        lastAppliedSourceTs = event.getSourceTstamp().getTime();
        if (heartbeat || now - lastRtoReportTime > RTO_REPORT_INTERVAL_MS) {
            lastRtoMs = rtoMs;
            lastRtoReportTime = now;
            writeRtoMetric(rtoMs);
        }
    }

    private void ensureMarkerTableOn(Connection conn) {
        String table = com.migration.common.bidi.BidiConstants.MARKER_TABLE;
        String ddl = isPostgresql
                ? "CREATE TABLE IF NOT EXISTS \"" + table + "\" (id INT PRIMARY KEY, node_id VARCHAR(64), last_seqno BIGINT, updated_at BIGINT)"
                : "CREATE TABLE IF NOT EXISTS `" + table + "` (id INT PRIMARY KEY, node_id VARCHAR(64), last_seqno BIGINT, updated_at BIGINT)";
        try (Statement st = conn.createStatement()) {
            boolean prevAuto = conn.getAutoCommit();
            if (!prevAuto) conn.setAutoCommit(true);
            st.execute(ddl);
        } catch (SQLException e) {
            logger.warn("worker 连接创建 origin 标记表失败: {}", e.getMessage());
        }
    }

    /**
     * 在给定连接上以单事务应用一个事件已转换好的 DML（typed 或 text 二选一），提交或回滚。
     * 幂等容错规则与串行路径 {@link #processThlFile} 内联逻辑保持一致——重复键、UPDATE/DELETE 未影响行 视为成功；
     * 改动其一务必同步另一处。不做重连（并行 worker 上连接错误直接 fail-stop）。
     */
    private void applyEventTx(THLEvent event, Connection conn,
                             List<ParameterizedDml> typedDmls, List<String> sqlStatements) throws SQLException {
        boolean origAuto = conn.getAutoCommit();
        if (origAuto) conn.setAutoCommit(false);
        try {
            // 双向防回环：数据事件先写 origin 标记（事务首条语句），与业务 DML 原子提交
            if (bidirectionalEnabled && isDataChangeEvent(event)) {
                try { writeOriginMarkerOn(conn, event.getSeqno()); }
                catch (SQLException me) { logger.warn("写 origin 标记失败 (seqno={}): {}", event.getSeqno(), me.getMessage()); }
            }
            if (typedDmls != null) {
                for (ParameterizedDml dml : typedDmls) {
                    try {
                        try (PreparedStatement ps = conn.prepareStatement(dml.getSql())) {
                            List<Object> params = dml.getParams();
                            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
                            ps.executeUpdate();
                        }
                    } catch (SQLException e) {
                        String msg = e.getMessage();
                        if (msg != null && (msg.contains("Duplicate entry") || msg.contains("duplicate key"))) {
                            logger.warn("重复键忽略 (seqno={}): {}", event.getSeqno(), msg);
                        } else {
                            throw e;
                        }
                    }
                }
            } else if (sqlStatements != null) {
                for (String sql : sqlStatements) {
                    if (sql == null || sql.trim().isEmpty()) continue;
                    String trimmed = sql.trim();
                    if (trimmed.equalsIgnoreCase("COMMIT;") || trimmed.equalsIgnoreCase("COMMIT")) continue;
                    if (isPostgresql && trimmed.toUpperCase().startsWith("SET SEARCH_PATH")) {
                        try (Statement stmt = conn.createStatement()) { stmt.execute(trimmed); }
                        continue;
                    }
                    try {
                        try (Statement stmt = conn.createStatement()) { stmt.execute(trimmed); }
                    } catch (SQLException e) {
                        String up = trimmed.toUpperCase();
                        String msg = e.getMessage();
                        if (msg != null && (msg.contains("Duplicate entry") || msg.contains("1062"))) {
                            logger.warn("重复键忽略 (seqno={}): {}", event.getSeqno(), msg);
                        } else if ((up.startsWith("UPDATE") || up.startsWith("DELETE")) && msg != null
                                && (msg.contains("0 rows affected") || msg.contains("not found"))) {
                            logger.warn("UPDATE/DELETE 未影响任何行, 已忽略 (seqno={})", event.getSeqno());
                        } else {
                            throw e;
                        }
                    }
                }
            }
            conn.commit();
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        } finally {
            try { if (origAuto) conn.setAutoCommit(true); } catch (SQLException ignored) {}
        }
    }

    /** origin 标记写入的连接参数化版本（并行 worker 各自连接）。 */
    private void writeOriginMarkerOn(Connection conn, long seqno) throws SQLException {
        String table = com.migration.common.bidi.BidiConstants.MARKER_TABLE;
        int id = com.migration.common.bidi.BidiConstants.MARKER_ROW_ID;
        long now = System.currentTimeMillis();
        String sql = isPostgresql
                ? "INSERT INTO \"" + table + "\" (id, node_id, last_seqno, updated_at) VALUES (?, ?, ?, ?) " +
                  "ON CONFLICT (id) DO UPDATE SET node_id=EXCLUDED.node_id, last_seqno=EXCLUDED.last_seqno, updated_at=EXCLUDED.updated_at"
                : "INSERT INTO `" + table + "` (id, node_id, last_seqno, updated_at) VALUES (?, ?, ?, ?) " +
                  "ON DUPLICATE KEY UPDATE node_id=VALUES(node_id), last_seqno=VALUES(last_seqno), updated_at=VALUES(updated_at)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, id);
            ps.setString(2, bidiNodeId);
            ps.setLong(3, seqno);
            ps.setLong(4, now);
            ps.executeUpdate();
        }
    }

    private void writeErrorStatus(String errorCode, String errorMessage, THLEvent event) {
        long seqno = event.getSeqno();
        // eventId（binlog文件:位点）跨重启稳定，嵌入错误信息供后端"跳过失败事件"按稳定身份下发；
        // seqno 在 resume 重新提取后会变，仅作展示/兼容
        if (event.getEventId() != null && !event.getEventId().isEmpty()) {
            errorMessage = "[eventId=" + event.getEventId() + "] " + errorMessage;
        }
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
        if (parallelExecutor != null) {
            parallelExecutor.close();
        }
        if (targetConnection != null) {
            try {
                targetConnection.close();
            } catch (SQLException e) {
                logger.error("关闭目标库连接出错", e);
            }
        }
    }
}
