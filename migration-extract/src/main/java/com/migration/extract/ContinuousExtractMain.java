package com.migration.extract;

import com.migration.common.watch.DirectoryChangeWatcher;
import com.migration.thl.EncryptedTHLFileWriter;
import com.migration.thl.THLFileWriter;
import com.migration.thl.THLEvent;
import com.migration.thl.crypto.ThlEncryptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ContinuousExtractMain {

    private static final Logger logger = LoggerFactory.getLogger(ContinuousExtractMain.class);

    /**
     * 合成心跳标记：该心跳的 {@code sourceTstamp} 取自**本机时钟**，不可用于计算延迟。
     *
     * <p>与之相对的是 capture 写的 {@code SYNC_HEARTBEAT} —— 那条的时间戳取自源库时钟
     * （MySQL 走心跳表绕一圈回来，PG/Oracle 取源库当前时间），可以算延迟。
     */
    public static final String SYNTHETIC_HEARTBEAT = "synthetic_heartbeat";

    private static final long HEARTBEAT_IDLE_THRESHOLD_MS = 1000;
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    /** THL 文件最大大小（50MB），超过后轮转到新文件 */
    private static final long THL_FILE_MAX_SIZE_BYTES = 50 * 1024 * 1024;

    private com.migration.common.Extractor<byte[], THLEvent> extractor;
    private MySQLBinlogExtractor mysqlExtractor;
    private PostgresWalExtractor pgExtractor;
    private OracleRedoExtractor oracleExtractor;
    private String inputDir;
    private String outputDir;
    private long scanInterval;
    /** 事件驱动开关：true 时用 WatchService 监听 .cap 输入目录变更（默认），false 回退固定间隔轮询。 */
    private boolean watchEnabled;
    /** 事件驱动下的兜底超时（ms）：无文件事件时仍周期性重扫，保证心跳/背压逻辑照常运行。 */
    private long watchFallbackMs;
    private AtomicBoolean running = new AtomicBoolean(true);
    private String captureType;

    private Map<String, FileProgress> fileProgressMap = new LinkedHashMap<>();
    private String progressRecordFile;

    /**
     * XA 分支收集期间冻结的进度快照（见 {@link #saveProgress()}）。非 null 表示"进度不许往前落盘"，
     * 分支收口（prepare）后立刻清空恢复实时落盘。
     */
    private String heldProgressSnapshot;

    private BackpressureController backpressureController;

    private THLFileWriter currentThlWriter;
    private File currentThlFile;
    /** THL 文件全局递增索引，用于文件命名和排序 */
    private int globalThlIndex = 0;
    private volatile long lastRealEventTime = System.currentTimeMillis();
    private volatile long lastHeartbeatTime = 0;
    private long heartbeatSeqno = 0;

    /** 已处理cap文件保留数量（安全余量），超过此数量的已处理文件将被清理 */
    private int capRetentionCount = 2;

    /** .cap 多久没被写过才认为"capture 已经不写它了"（判错就是永久丢那段事件，宁可晚一点）。 */
    private long capSettleMs = 30000;

    /** THL 加密服务 */
    private ThlEncryptionService thlEncryptionService;

    // ---- 真实吞吐/积压指标（写入 binlog_output/ 供 agent 采集，取代随机 mock 数据）----
    /** 当前统计窗口起点 */
    private long rateWindowStartMs = System.currentTimeMillis();
    /** 当前统计窗口内累计抽取事件数 */
    private long rateWindowEvents = 0;
    /** 吞吐率统计窗口（ms）：窗口结束才折算成 events/sec 落盘，避免抖动 */
    private static final long RATE_WINDOW_MS = 1000;

    public static void main(String[] args) {
        com.migration.common.OracleNetCompat.apply();
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
        String taskId = props.getProperty("task.id", System.getProperty("task.id", "unknown"));

        // 单实例互斥 + 父进程看门狗（放在解密之前，见 CaptureMain 的说明）：
        // 两个 extract 同时扫同一批 .cap 会产出重复 THL
        com.migration.common.proc.ChildProcessBootstrap.init(taskId, "extract");

        // 解密 config.properties 中的加密口令（ENC: 前缀）；历史明文配置无前缀，原样通过。
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

        String captureType = props.getProperty("capture.type", "binlog").toLowerCase();
        logger.info("=== Migration Extract (Continuous) Starting (type={}) ===", captureType);

        try {
            ContinuousExtractMain main = new ContinuousExtractMain();
            main.initialize(props);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received, stopping extract...");
                main.stop();
            }));

            main.start();
        } catch (Exception e) {
            logger.error("Fatal error in Continuous Extract", e);
            System.exit(1);
        }
    }

    @SuppressWarnings("unchecked")
    public void initialize(Properties props) throws Exception {
        this.taskProps = props;
        this.inputDir = props.getProperty("extract.input.dir",
                "files/" + props.getProperty("task.id", "unknown") + "/binlog_output");
        this.outputDir = props.getProperty("extract.output.dir",
                "files/" + props.getProperty("task.id", "unknown") + "/thl_output");
        this.scanInterval = Long.parseLong(props.getProperty("extract.scan.interval", "3000"));
        this.watchEnabled = Boolean.parseBoolean(props.getProperty("extract.watch.enabled", "true"));
        this.watchFallbackMs = Long.parseLong(props.getProperty("extract.watch.fallback.ms", "1000"));
        this.progressRecordFile = outputDir + "/.extract_progress";
        this.capSettleMs = Long.parseLong(props.getProperty("extract.cap.settle.ms", "30000"));
        this.captureType = props.getProperty("capture.type", "binlog").toLowerCase();

        // 初始化背压控制器：高水位/低水位可配置
        String taskId = props.getProperty("task.id", System.getProperty("task.id", "unknown"));
        int highWatermark = Integer.parseInt(props.getProperty("backpressure.high.watermark", "5000"));
        int lowWatermark = Integer.parseInt(props.getProperty("backpressure.low.watermark", "1000"));
        this.backpressureController = new BackpressureController(taskId, highWatermark, lowWatermark);
        logger.info("背压控制器初始化: highWatermark={}, lowWatermark={}", highWatermark, lowWatermark);

        // 初始化 THL 加密服务
        this.thlEncryptionService = new ThlEncryptionService(props);
        if (thlEncryptionService.isEnabled()) {
            logger.info("THL 文件加密已启用");
        }

        if ("wal".equals(captureType) || "postgresql".equals(captureType)) {
            pgExtractor = new PostgresWalExtractor();
            this.extractor = (com.migration.common.Extractor<byte[], THLEvent>) pgExtractor;
            logger.info("Using PostgreSQL WAL Extractor");
        } else if ("redo".equals(captureType) || "oracle".equals(captureType)) {
            oracleExtractor = new OracleRedoExtractor();
            this.extractor = (com.migration.common.Extractor<byte[], THLEvent>) oracleExtractor;
            logger.info("Using Oracle Redo Extractor");
        } else if ("ticdc".equals(captureType) || "tidb".equals(captureType)) {
            // TiCDCExtractor 继承 MySQLBinlogExtractor：seqno/心跳/关闭等编排沿用 mysql 分支，
            // 只有“capture 记录 → THLEvent”的解析换成 canal-json
            mysqlExtractor = new TiCDCExtractor();
            this.extractor = (com.migration.common.Extractor<byte[], THLEvent>) mysqlExtractor;
            logger.info("Using TiDB TiCDC Extractor");
        } else {
            mysqlExtractor = new MySQLBinlogExtractor();
            this.extractor = (com.migration.common.Extractor<byte[], THLEvent>) mysqlExtractor;
            logger.info("Using MySQL Binlog Extractor");
        }
        this.extractor.initialize(props);

        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        // 扫描已有THL文件，确定全局起始索引，避免文件名冲突
        initializeThlIndex(outputDirFile);

        capRetentionCount = Integer.parseInt(props.getProperty("extract.cap.retention.count", "2"));

        loadProgress();

        logger.info("Continuous Extract initialized - type: {}, input: {}, output: {}, scanInterval: {}ms, thlMaxSize: {}MB",
                captureType, inputDir, outputDir, scanInterval, THL_FILE_MAX_SIZE_BYTES / (1024 * 1024));
    }

    /** 扫描输出目录中已有的THL文件，确定全局起始索引 */
    private void initializeThlIndex(File outputDirFile) {
        String[] existingThlFiles = outputDirFile.list((dir, name) ->
                name.endsWith(".thl") && !name.startsWith("."));
        if (existingThlFiles == null) return;

        for (String name : existingThlFiles) {
            long seq = extractSeqnoFromFileName(name);
            if (seq >= globalThlIndex) {
                globalThlIndex = (int) seq + 1;
            }
        }
        logger.info("THL文件全局起始索引: {}", globalThlIndex);
    }

    /** 从THL文件名中提取seqno数字，用于排序和索引初始化 */
    private long extractSeqnoFromFileName(String fileName) {
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

    public void start() {
        DirectoryChangeWatcher watcher = null;
        if (watchEnabled) {
            try {
                watcher = new DirectoryChangeWatcher(inputDir);
                logger.info("Starting continuous binlog extraction (event-driven WatchService, fallback {}ms)...", watchFallbackMs);
            } catch (IOException e) {
                logger.warn("初始化 .cap 目录监听失败，回退固定轮询 {}ms: {}", scanInterval, e.getMessage());
                watcher = null;
            }
        }
        if (watcher == null) {
            logger.info("Starting continuous binlog extraction (fixed polling {}ms)...", scanInterval);
        }

        try {
            while (running.get()) {
                try {
                    int eventsThisRound = scanAndProcessFiles();

                    if (eventsThisRound > 0) {
                        lastRealEventTime = System.currentTimeMillis();
                    }

                    writeHeartbeatIfNeeded();

                    // 背压控制：检测 THL 输出目录积压量，超阈值时暂停 capture
                    applyBackpressureIfNeeded();

                    // 真实指标：抽取吞吐率(events/sec) + cap/thl 两级积压量，落盘供 agent 采集
                    writeThroughputAndQueueMetrics(eventsThisRound);

                    // 事件驱动：阻塞到 capture 写入 .cap 立即唤醒（Linux inotify ~ms），或兜底超时；
                    // 回退模式：固定间隔轮询
                    if (watcher != null) {
                        watcher.awaitChange(watchFallbackMs);
                    } else {
                        Thread.sleep(scanInterval);
                    }
                } catch (InterruptedException e) {
                    logger.info("Extract thread interrupted");
                    Thread.currentThread().interrupt();
                    break;
                } catch (MySQLBinlogExtractor.ColumnLayoutMismatchException e) {
                    // 事件的列布局与源库当前定义对不上：继续解析就是整行错位的静默写坏
                    logger.error("列布局与源库当前定义不一致，停止抽取: {}", e.getMessage());
                    writeExtractErrorStatus("E3021", e.getMessage());
                    running.set(false);
                    break;
                } catch (MySQLBinlogExtractor.SchemaVersionMismatchException e) {
                    // 时序库算出的列布局与事件自带的列名矛盾：时序库跟丢了源库真实结构，
                    // 硬解就是整行错位的静默数据损坏
                    logger.error("表结构版本与事件列名不一致，停止抽取: {}", e.getMessage());
                    writeExtractErrorStatus("E3024", e.getMessage());
                    running.set(false);
                    break;
                } catch (MySQLBinlogExtractor.SchemaTimelineMissingException e) {
                    // 时序库给不出该位点的版本，且 fallback=FAIL_STOP：降级回查源库当前定义
                    // 等于退回"用现在的结构解释过去的事件"，用户明确要求不接受这种退化
                    logger.error("表结构时序库缺少该位点的版本，停止抽取: {}", e.getMessage());
                    writeExtractErrorStatus("E3022", e.getMessage());
                    running.set(false);
                    break;
                } catch (com.migration.extract.schema.DdlParseException e) {
                    // DDL 施加不了，且 fallback=FAIL_STOP：漏施加一条改列的 DDL，
                    // 该表之后的每个版本都是错的
                    logger.error("DDL 解析失败（表结构时序库），停止抽取: {}", e.getMessage());
                    writeExtractErrorStatus("E3023", e.getMessage());
                    running.set(false);
                    break;
                } catch (PostgresWalExtractor.UnreconstructableValueException e) {
                    // PG 逻辑复制没把某列的值发过来（未变更的 TOAST），而这一列又是定位行必需的。
                    // 猜一个值写下去就是改错行/丢行，且不会有任何报错
                    logger.error("WAL 事件缺少必需的列值，停止抽取: {}", e.getMessage());
                    writeExtractErrorStatus("E3025", e.getMessage());
                    running.set(false);
                    break;
                } catch (MySQLBinlogExtractor.UnsupportedBinlogEventException e) {
                    // 不认识的事件类型：跳过去就是静默丢数据（源端开了压缩 binlog / PARTIAL_JSON
                    // 这类参数时会命中）。停下来上报，让人先确认源端配置
                    logger.error("遇到不支持的 binlog 事件类型，停止抽取: {}", e.getMessage());
                    writeExtractErrorStatus("E3020", e.getMessage());
                    running.set(false);
                    break;
                } catch (XaTransactionBuffer.XaQuotaExceededException e) {
                    // XA 缓冲配额突破：继续跑下去要么把分支丢掉（源库已提交的数据永久不到目标库），
                    // 要么把磁盘撑爆。停下来上报，让人先处置源库的未决分支
                    logger.error("XA 事务缓冲超限，停止抽取: {}", e.getMessage());
                    writeExtractErrorStatus("E3018", e.getMessage());
                    running.set(false);
                    break;
                } catch (Exception e) {
                    logger.error("Error during file scanning", e);
                }
            }
        } finally {
            if (watcher != null) watcher.close();
        }

        closeCurrentThlWriter();
        closeExtractor();
        saveProgress();
        // 进程退出时确保恢复 capture
        if (backpressureController != null) {
            backpressureController.forceResume();
        }
        logger.info("Continuous Extract stopped");
    }

    public void stop() {
        running.set(false);
    }

    /**
     * 检测 THL 输出目录中待处理的文件数量，超过高水位时向 capture 发送暂停信号。
     * 待处理文件 = .thl 文件总数 - increment 已处理文件数（近似用文件数估算）。
     */
    private void applyBackpressureIfNeeded() {
        if (backpressureController == null) return;

        try {
            File outputDirFile = new File(outputDir);
            if (!outputDirFile.exists()) return;

            File[] thlFiles = outputDirFile.listFiles((dir, name) ->
                    name.endsWith(".thl") && !name.startsWith("."));
            int pendingCount = thlFiles != null ? thlFiles.length : 0;

            backpressureController.checkAndApplyBackpressure(pendingCount);
        } catch (Exception e) {
            logger.debug("背压检测异常: {}", e.getMessage());
        }
    }

    /**
     * 写入真实吞吐/积压指标到 {@code binlog_output/}，供 agent 的 collectMetrics 采集：
     * <ul>
     *   <li>{@code capture_rate}：抽取吞吐率 events/sec（RATE_WINDOW_MS 窗口内真实计数折算，非随机数）；</li>
     *   <li>{@code capture_queue_depth}：未抽取完的 .cap 文件数（source→extract 积压）；</li>
     *   <li>{@code extract_queue_depth}：待 increment 消费的 .thl 文件数（extract→apply 积压）。</li>
     * </ul>
     */
    private void writeThroughputAndQueueMetrics(int eventsThisRound) {
        try {
            // 吞吐率：窗口内累计事件数，满窗才折算落盘
            rateWindowEvents += Math.max(0, eventsThisRound);
            long now = System.currentTimeMillis();
            long elapsed = now - rateWindowStartMs;
            if (elapsed >= RATE_WINDOW_MS) {
                long ratePerSec = elapsed > 0 ? (rateWindowEvents * 1000L) / elapsed : 0;
                writeLongMetric("capture_rate", ratePerSec);
                rateWindowStartMs = now;
                rateWindowEvents = 0;
            }

            // cap 积压：尚未标记 completed 的 .cap 文件数
            long capBacklog = 0;
            File inputDirFile = new File(inputDir);
            File[] capFiles = inputDirFile.listFiles((dir, name) ->
                    name.startsWith("binlog_") && name.endsWith(".cap"));
            if (capFiles != null) {
                for (File f : capFiles) {
                    FileProgress p = fileProgressMap.get(f.getName());
                    if (p == null || !p.completed) capBacklog++;
                }
            }
            writeLongMetric("capture_queue_depth", capBacklog);

            // thl 积压：输出目录 .thl 文件数（与背压同口径）
            File outputDirFile = new File(outputDir);
            File[] thlFiles = outputDirFile.listFiles((dir, name) ->
                    name.endsWith(".thl") && !name.startsWith("."));
            writeLongMetric("extract_queue_depth", thlFiles != null ? thlFiles.length : 0);

            // 未决 XA 分支：已 prepare、等源库给决议的分支数与最老分支的等待时长。
            // 这两个数不为 0 是正常的（源库的分布式事务还没提交），但一直涨就说明源端有卡住的分支
            if (mysqlExtractor != null) {
                writeLongMetric("xa_pending_branches", mysqlExtractor.xaPendingBranchCount());
                writeLongMetric("xa_pending_oldest_ms", mysqlExtractor.xaOldestPendingAgeMs());
            }
        } catch (Exception e) {
            logger.debug("写入吞吐/积压指标失败: {}", e.getMessage());
        }
    }

    /**
     * 写 {@code binlog_output/error_status}（格式与 capture / increment 端一致），
     * agent 轮询到即把任务上报 FAILED，而不是让 extract 无声地空转。
     */
    private void writeExtractErrorStatus(String errorCode, String message) {
        try {
            File dir = new File(inputDir);
            if (!dir.exists()) dir.mkdirs();
            com.migration.common.io.AtomicFileWriter.writeStringQuietly(
                    new File(dir, "error_status"),
                    System.currentTimeMillis() + "|" + errorCode + "|-1|"
                            + message.replace("|", "/") + "|extract\n");
        } catch (Exception e) {
            logger.warn("写入 extract 错误状态文件失败: {}", e.getMessage());
        }
    }

    /** 原子写入单值指标文件到 binlog_output/（agent 侧按行读取 long）。 */
    private void writeLongMetric(String fileName, long value) {
        File dir = new File(inputDir);
        if (!dir.exists() && !dir.mkdirs()) return;
        File tmp = new File(dir, fileName + ".tmp");
        File dst = new File(dir, fileName);
        try (java.io.FileWriter w = new java.io.FileWriter(tmp, false)) {
            w.write(Long.toString(value));
        } catch (IOException e) {
            logger.debug("写指标 {} 失败: {}", fileName, e.getMessage());
            return;
        }
        // rename 原子替换，避免 agent 读到半截内容
        if (!tmp.renameTo(dst)) {
            tmp.delete();
        }
    }

    private int scanAndProcessFiles() throws Exception {
        File inputDirFile = new File(inputDir);
        if (!inputDirFile.exists()) {
            return 0;
        }

        File[] binlogFiles = inputDirFile.listFiles((dir, name) ->
                name.startsWith("binlog_") && name.endsWith(".cap"));

        if (binlogFiles == null || binlogFiles.length == 0) {
            return 0;
        }

        Arrays.sort(binlogFiles, Comparator.comparing(File::getName));

        // 判定"capture 不会再写这个文件了"用**最后修改时间**，不用文件名。
        // 文件名是 binlog_<时间戳>_<序号>.cap，而 capture 重启后序号从 0000 重来：
        // 同一秒内轮转+重启就会产出一个名字比现存文件更小的**活跃**文件，按名字排序会
        // 把它当成旧文件，读完即标完成，之后 capture 追加的内容再也不会被抽取 —— 静默丢数据。
        long newestModified = 0;
        for (File f : binlogFiles) {
            newestModified = Math.max(newestModified, f.lastModified());
        }

        int totalEvents = 0;
        for (File binlogFile : binlogFiles) {
            if (!running.get()) break;
            totalEvents += processFileIncremental(binlogFile, binlogFile.lastModified() >= newestModified);
        }
        // 一轮扫描写完，把缓冲区交给内核。
        //
        // THLFileWriter 改成攒批 flush 之后（原来每条一次 flush，把 BufferedOutputStream
        // 完全废掉），"写了就一定可见"不再自动成立：低流量链路下最后几条事件可能一直
        // 躺在缓冲区里，直到下一条事件到来才被顺带刷出去——那可能是几分钟以后。
        // 本仓库在"capture 位点低流量不落盘"上已经栽过同一类问题。
        // 压在扫描周期边界上刷：可见性粒度与改动前的轮询周期一致，而 syscall 数
        // 从"每事件一次"降到"每轮一次"。
        if (currentThlWriter != null) {
            try {
                currentThlWriter.flush();
            } catch (IOException e) {
                logger.warn("THL 刷盘失败: {}", e.getMessage());
            }
        }
        return totalEvents;
    }

    /** .cap 逐行解密器（惰性初始化：未开加密时不做 KDF）。 */
    private com.migration.common.security.CapLineCipher capCipher;
    /** initialize 收到的配置，capCipher 要用它取加密开关与口令。 */
    private Properties taskProps = new Properties();

    private com.migration.common.security.CapLineCipher capCipher() {
        if (capCipher == null) {
            capCipher = new com.migration.common.security.CapLineCipher(taskProps);
        }
        return capCipher;
    }

    private int processFileIncremental(File binlogFile, boolean isNewest) throws Exception {
        FileProgress progress = fileProgressMap.get(binlogFile.getName());

        if (progress == null) {
            progress = new FileProgress(binlogFile.getName());
            fileProgressMap.put(binlogFile.getName(), progress);
            logger.info("Detected new binlog file: {}", binlogFile.getName());
        }

        if (progress.completed) return 0;

        int totalLinesInFile = countLines(binlogFile);
        if (totalLinesInFile <= progress.linesRead) {
            // 已经读完，且 capture 不会再往它里面写了 —— 就是处理完了。
            //
            // 旧判据（"连续 3 轮字节数没变"）实际上**永远不成立**：没有新行时上面这一句就返回了，
            // 有新行时字节数必然也变了，fileStoppedGrowing 恒为 false。实测约 100 个真实任务的
            // .extract_progress 里 completed 全是 false，于是 cleanupCompletedCapFiles() 从未生效
            //（.cap 无限堆积），而且每轮扫描都要对每个 .cap 整文件 countLines 一遍。
            markCompletedIfSettled(binlogFile, progress, isNewest);
            return 0;
        }

        int newLines = totalLinesInFile - progress.linesRead;
        logger.info("Processing binlog file: {} ({} new lines, already read: {}, total: {})",
                binlogFile.getName(), newLines, progress.linesRead, totalLinesInFile);

        // 使用类级别 currentThlWriter 统一写入，按50MB大小轮转文件
        int newEventCount = readAndExtractNewLines(binlogFile, progress, progress.linesRead, totalLinesInFile);

        progress.lastFileSize = binlogFile.length();
        markCompletedIfSettled(binlogFile, progress, isNewest);

        logger.info("Processed binlog file: {} -> {} new events", binlogFile.getName(), newEventCount);
        if (newEventCount > 0) {
            saveExtractorSeqno();
        }
        saveProgress();
        return newEventCount;
    }

    private void writeHeartbeatIfNeeded() {
        long now = System.currentTimeMillis();
        long idleMs = now - lastRealEventTime;

        if (idleMs < HEARTBEAT_IDLE_THRESHOLD_MS) {
            return;
        }

        if (now - lastHeartbeatTime < HEARTBEAT_INTERVAL_MS) {
            return;
        }

        try {
            ensureThlWriter();

            long seqno = getNextHeartbeatSeqno();

            THLEvent heartbeat = new THLEvent();
            heartbeat.setSeqno(seqno);
            heartbeat.setType(THLEvent.HEARTBEAT_EVENT);
            heartbeat.setSourceTstamp(new Timestamp(now));
            heartbeat.setEventId("heartbeat:" + seqno);
            heartbeat.setSourceId(captureType);
            heartbeat.addMetadata("event_type", "HEARTBEAT");
            heartbeat.addMetadata("heartbeat_timestamp", now);
            // 这条心跳的时间戳是**本机时钟**，不是源端时钟：它只能证明 extract 还活着，
            // 不能用来算延迟。下游拿它做 `now - sourceTstamp` 恒得 ≈0 ——
            // capture 死掉时 extract 空转、每秒造一条，面板就会显示"延迟极低"而实际一条数据都没动。
            // 打上标记，increment 见到只推进位点与活性、不刷 rto_metric。
            heartbeat.addMetadata(SYNTHETIC_HEARTBEAT, Boolean.TRUE);

            currentThlWriter.writeEvent(heartbeat);
            // 心跳的唯一作用就是让下游立刻看到"extract 还活着"，
            // 它必须绕过攒批——躺在缓冲区里的心跳等于没有心跳。
            currentThlWriter.flush();

            lastHeartbeatTime = now;

            logger.debug("Heartbeat event written: seqno={}, sourceTstamp={}", seqno, heartbeat.getSourceTstamp());
        } catch (Exception e) {
            logger.warn("Failed to write heartbeat event: {}", e.getMessage());
            closeCurrentThlWriter();
        }
    }

    private long getNextHeartbeatSeqno() {
        if (mysqlExtractor != null) {
            heartbeatSeqno = mysqlExtractor.getCurrentSeqno();
            mysqlExtractor.incrementSeqnoForHeartbeat();
            return heartbeatSeqno;
        } else if (pgExtractor != null) {
            heartbeatSeqno = pgExtractor.getCurrentSeqno();
            pgExtractor.incrementSeqnoForHeartbeat();
            return heartbeatSeqno;
        } else if (oracleExtractor != null) {
            heartbeatSeqno = oracleExtractor.getCurrentSeqno();
            oracleExtractor.incrementSeqnoForHeartbeat();
            return heartbeatSeqno;
        }
        return ++heartbeatSeqno;
    }

    private void closeCurrentThlWriter() {
        if (currentThlWriter != null) {
            try {
                currentThlWriter.close();
            } catch (Exception e) {
                logger.warn("Error closing current THL writer: {}", e.getMessage());
            }
            currentThlWriter = null;
        }
    }

    /**
     * 文件里<b>已写完整</b>的行数（不含末尾那条还没写完的）。
     *
     * <p>capture 是一边写一边被读的：一条记录写进 {@code BufferedWriter} 时，若跨过 8192 字符的
     * 缓冲边界，它会分两次落到文件上。读取方正好夹在两次之间扫到文件，就会看到半条记录 ——
     * 而 {@code readLine()} 对"文件末尾没有换行符"和"一行正常结束"给出的结果一模一样。
     * 把半行当完整事件消费掉，前半截会被当成合法事件写进 THL（字段够数时下游根本发现不了），
     * 后半截在下一轮变成字段数不足的孤行被丢掉 —— 两头都是静默的。
     *
     * <p>所以以换行符为准：文件不以换行结束时，末行一律留到下一轮再读。
     * （对照 THL 那一跳：分帧格式里半条记录是能被显式识别的，见 {@code THLFileReader}。）
     */
    private int countLines(File file) throws IOException {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while (reader.readLine() != null) count++;
        }
        if (count > 0 && !endsWithNewline(file)) {
            count--;
        }
        return count;
    }

    /**
     * 已读完、且 capture 不会再写它的 .cap 文件，标记为处理完成。
     *
     * @param isNewest 是否是目录里最后被修改的那个 .cap —— capture 只往它里面追加
     */
    private void markCompletedIfSettled(File binlogFile, FileProgress progress, boolean isNewest) {
        if (progress.completed || isNewest) {
            return;
        }
        // 再等一个静默期才收口：标完成之后这个文件既不再读也可能被清理掉，
        // 判断错一次就是永久丢掉那段事件，宁可晚一点
        if (System.currentTimeMillis() - binlogFile.lastModified() < capSettleMs) {
            return;
        }
        // 正在收集 XA 分支时不标 completed、也不清理 .cap：崩溃重启要从分支起点把这些行重读一遍，
        // 文件被当成"已处理完"或直接删掉，重读就无从谈起
        if (mysqlExtractor != null && mysqlExtractor.isXaBranchActive()) {
            return;
        }
        progress.completed = true;
        logger.info("Binlog file {} 已读完且不再有写入，标记为处理完成", binlogFile.getName());
        cleanupCompletedCapFiles();
        saveProgress();
    }

    /** 文件最后一个字节是否是换行符（空文件按"未结束"处理）。 */
    private boolean endsWithNewline(File file) throws IOException {
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "r")) {
            long len = raf.length();
            if (len == 0) {
                return false;
            }
            raf.seek(len - 1);
            return raf.read() == '\n';
        }
    }

    /** 创建 THL 文件写入器（根据加密配置自动选择） */
    private THLFileWriter createThlWriter(String filePath) throws IOException {
        if (thlEncryptionService != null && thlEncryptionService.isEnabled()) {
            return new EncryptedTHLFileWriter(filePath, thlEncryptionService);
        }
        return new THLFileWriter(filePath);
    }

    /**
     * 多行事件拆成"每行一条"时<b>不能整份复制</b>的元数据 key：
     * 行集合（{@code rows_*}）与它们的单行投影（{@code row_data}/{@code row_data_before}，
     * 由 extractor 置为第 0 行），拆分时逐行重写；{@code multi_row} 拆完即不再成立。
     */
    private static final java.util.Set<String> PER_ROW_METADATA_KEYS = new java.util.HashSet<>(
            java.util.Arrays.asList("rows_data", "rows_data_before", "rows_typed", "rows_before_typed",
                    "row_data", "row_data_before", "multi_row"));

    /**
     * 把一个多行事件拆成"每行一条"的 THL 事件；不是多行事件则返回 null（调用方原样写出）。
     *
     * <p><b>逐行元数据必须逐行切</b>。此前拆分时只排除了 {@code rows_data}/{@code multi_row}，
     * 其余 key 整份复制——于是拆出来的 N 个事件<b>每一个都带着全部 N 行</b>的
     * {@code rows_typed}，而增量端的类型化值管道读的正是它：N 行的源事件产生
     * N 个 THL 事件 × 每个 N 条 SQL = <b>N² 次目标写入</b>（实测 500 行的一批变更产生
     * 62104 次写入，放大 124 倍，也是增量吞吐只有二三十行每秒的真正原因）。
     * 前镜像 {@code row_data_before} 同理：整份复制时每个拆出事件拿到的都是第 0 行的前镜像，
     * 多行 UPDATE 在文本路径与订阅端会全部指向同一行。
     */
    static java.util.List<THLEvent> splitMultiRowEvent(THLEvent event) {
        Boolean multiRow = (Boolean) event.getMetadata().get("multi_row");
        if (multiRow == null || !multiRow) {
            return null;
        }
        Object rowsDataRaw = event.getMetadata().get("rows_data");
        if (!(rowsDataRaw instanceof java.util.List)) {
            return null;
        }
        java.util.List<?> rowsData = (java.util.List<?>) rowsDataRaw;
        if (rowsData.size() <= 1) {
            return null;
        }

        int rowCount = rowsData.size();
        java.util.List<?> rowsBefore = rowListOf(event, "rows_data_before", rowCount);
        java.util.List<?> rowsTyped = rowListOf(event, "rows_typed", rowCount);
        java.util.List<?> rowsBeforeTyped = rowListOf(event, "rows_before_typed", rowCount);

        java.util.List<THLEvent> out = new java.util.ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            THLEvent rowEvent = new THLEvent();
            rowEvent.setSeqno(event.getSeqno() + i);
            rowEvent.setEventId(event.getEventId() + "_" + i);
            rowEvent.setSourceId(event.getSourceId());
            rowEvent.setSourceTstamp(event.getSourceTstamp());

            for (java.util.Map.Entry<String, Object> entry : event.getMetadata().entrySet()) {
                if (!PER_ROW_METADATA_KEYS.contains(entry.getKey())) {
                    rowEvent.addMetadata(entry.getKey(), entry.getValue());
                }
            }
            rowEvent.addMetadata("row_data", rowsData.get(i));
            if (rowsBefore != null) {
                rowEvent.addMetadata("row_data_before", rowsBefore.get(i));
            }
            // 类型化值管道读的是复数 key，切片后仍用原 key 传单元素列表（下游无需改动）；
            // 行数对不上时整体不下发，让下游回退文本路径，而不是拿着错行的值往下走
            if (rowsTyped != null) {
                rowEvent.addMetadata("rows_typed", singletonRow(rowsTyped.get(i)));
            }
            if (rowsBeforeTyped != null) {
                rowEvent.addMetadata("rows_before_typed", singletonRow(rowsBeforeTyped.get(i)));
            }
            out.add(rowEvent);
        }
        return out;
    }

    /**
     * 取出可按行切片的行集合元数据；不是列表或行数与 {@code rows_data} 不一致时返回 null
     * （对不齐就整体不下发，避免把第 j 行的值安到第 i 行上）。
     */
    private static java.util.List<?> rowListOf(THLEvent event, String key, int expectedRows) {
        Object v = event.getMetadata().get(key);
        if (!(v instanceof java.util.List)) {
            return null;
        }
        java.util.List<?> list = (java.util.List<?>) v;
        if (list.size() != expectedRows) {
            logger.warn("多行事件 {} 行数 {} 与 rows_data 行数 {} 不一致，本次不下发该元数据 (seqno={})",
                    key, list.size(), expectedRows, event.getSeqno());
            return null;
        }
        return list;
    }

    /** 单行切片仍以列表形态下发，保持下游 {@code rows_typed} 的读取方式不变。 */
    private static java.util.ArrayList<Object> singletonRow(Object row) {
        java.util.ArrayList<Object> one = new java.util.ArrayList<>(1);
        one.add(row);
        return one;
    }

    /**
     * @param maxLines 本轮最多读到第几行（即 {@link #countLines} 数出的完整行数）。
     *                 读的时候 capture 可能又追加了内容，末行未必写完 —— 以进入本轮时数出的
     *                 完整行数为准，多出来的留到下一轮，避免消费半行（见 {@link #countLines}）
     */
    private int readAndExtractNewLines(File binlogFile, FileProgress progress,
                                       int skipLines, int maxLines) throws Exception {
        int[] eventCount = {0};
        int currentLine = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(binlogFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                currentLine++;
                if (currentLine > maxLines) break;
                if (currentLine <= skipLines) continue;
                if (line.trim().isEmpty()) {
                    progress.linesRead++;
                    continue;
                }

                // .cap 行可能是密文（capture 侧逐行加密）。解密只看行首标记，
                // 不看本任务的开关——同一个文件里明文行与密文行可以共存
                // （开启加密的那次轮转之前写的行就是明文），断点因此不受影响。
                line = capCipher().decryptLine(line);

                byte[] eventBytes = line.getBytes("UTF-8");
                THLEvent event = extractor.extract(eventBytes);
                if (event != null) {
                    eventCount[0] += writeExtractedEvent(event);
                }
                // XA 事务：本行若是 XA COMMIT，整个分支在这里被重放成一个普通事务下发；
                // 绝大多数行上这是一次空调用
                if (mysqlExtractor != null) {
                    mysqlExtractor.drainXaReplay(ev -> eventCount[0] += writeExtractedEvent(ev));
                }
                // .cap 读取进度的落盘要压在分支起点：分支还在收集时崩溃，重启得从 XA START
                // 重新收集一遍（收集中的 .part 落盘文件启动时一律删除）。这里只冻结"要写进
                // 进度文件的那一份"，内存里的读取位置照常前进，不影响本进程继续往下读。
                if (mysqlExtractor != null) {
                    if (mysqlExtractor.isXaBranchActive()) {
                        if (heldProgressSnapshot == null) {
                            heldProgressSnapshot = serializeProgress();
                        }
                    } else {
                        heldProgressSnapshot = null;
                    }
                }
                progress.linesRead++;
            }
        }
        return eventCount[0];
    }

    /** 把一个抽取出来的事件写进 THL（必要时先按行拆分、按大小轮转文件），返回实际写出的事件数。 */
    private int writeExtractedEvent(THLEvent event) throws Exception {
        ensureThlWriter();
        checkAndRotateThlFile();

        java.util.List<THLEvent> rowEvents = splitMultiRowEvent(event);
        if (rowEvents != null) {
            int written = 0;
            for (THLEvent rowEvent : rowEvents) {
                currentThlWriter.writeEvent(rowEvent);
                written++;
                checkAndRotateThlFile();
            }
            return written;
        }
        currentThlWriter.writeEvent(event);
        return 1;
    }

    /** 确保 currentThlWriter 可用，若为空则创建新THL文件 */
    private void ensureThlWriter() throws IOException {
        if (currentThlWriter == null || currentThlFile == null) {
            createNewThlFile();
        }
    }

    /** 检查当前THL文件大小，超过50MB则轮转到新文件 */
    private void checkAndRotateThlFile() throws IOException {
        if (currentThlFile != null && currentThlFile.length() >= THL_FILE_MAX_SIZE_BYTES) {
            logger.info("THL文件 {} 达到{}MB，轮转到新文件",
                    currentThlFile.getName(), currentThlFile.length() / (1024 * 1024));
            createNewThlFile();
        }
    }

    /** 创建新的THL文件并初始化writer */
    private void createNewThlFile() throws IOException {
        closeCurrentThlWriter();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());
        String fileName = String.format("binlog_%s_%04d.thl", timestamp, globalThlIndex++);
        currentThlFile = new File(outputDir, fileName);
        currentThlWriter = createThlWriter(currentThlFile.getAbsolutePath());
        logger.info("Created new THL file: {}", currentThlFile.getName());
    }

    private void loadProgress() {
        File recordFile = new File(progressRecordFile);
        if (!recordFile.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(recordFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split("\\|");
                if (parts.length >= 4) {
                    FileProgress progress = new FileProgress(parts[0]);
                    progress.linesRead = Integer.parseInt(parts[1]);
                    progress.lastFileSize = Long.parseLong(parts[2]);
                    progress.completed = Boolean.parseBoolean(parts[3]);
                    fileProgressMap.put(parts[0], progress);
                }
            }
            logger.info("Loaded {} file progress records", fileProgressMap.size());
        } catch (IOException e) {
            logger.warn("Error loading extract progress", e);
        }
    }

    private void saveProgress() {
        File recordFile = new File(progressRecordFile);
        File parentDir = recordFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        // 正在收集 XA 分支时写的是"分支开始那一刻"的快照：崩溃重启后会从 XA START 重新读，
        // 把整段分支重新收集完整。写实时进度就会让重启后从分支中间接着读，拼出半个事务。
        String content = heldProgressSnapshot != null ? heldProgressSnapshot : serializeProgress();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(recordFile))) {
            writer.write(content);
        } catch (IOException e) {
            logger.warn("Error saving extract progress", e);
        }
    }

    private String serializeProgress() {
        StringBuilder sb = new StringBuilder();
        for (FileProgress progress : fileProgressMap.values()) {
            sb.append(progress.fileName).append('|').append(progress.linesRead).append('|')
                    .append(progress.lastFileSize).append('|').append(progress.completed)
                    .append(System.lineSeparator());
        }
        return sb.toString();
    }

    private static class FileProgress {
        String fileName;
        int linesRead;
        long lastFileSize;
        boolean completed;

        FileProgress(String fileName) {
            this.fileName = fileName;
            this.linesRead = 0;
            this.lastFileSize = 0;
            this.completed = false;
        }
    }

    private void closeExtractor() {
        if (mysqlExtractor != null) {
            mysqlExtractor.close();
        } else if (pgExtractor != null) {
            pgExtractor.close();
        } else if (oracleExtractor != null) {
            oracleExtractor.close();
        }
    }

    private void saveExtractorSeqno() {
        if (mysqlExtractor != null) {
            mysqlExtractor.saveSeqno();
        } else if (pgExtractor != null) {
            pgExtractor.saveSeqno();
        } else if (oracleExtractor != null) {
            oracleExtractor.saveSeqno();
        }
    }

    /**
     * 清理已完全处理的cap文件，保留最近 capRetentionCount 个已处理文件作为安全余量。
     * 只删除 fileProgressMap 中标记为 completed=true 的文件。
     */
    private void cleanupCompletedCapFiles() {
        try {
            File inputDirFile = new File(inputDir);
            if (!inputDirFile.exists() || !inputDirFile.isDirectory()) return;

            File[] capFiles = inputDirFile.listFiles((dir, name) ->
                    name.startsWith("binlog_") && name.endsWith(".cap"));
            if (capFiles == null || capFiles.length == 0) return;

            // 按文件名排序
            Arrays.sort(capFiles, Comparator.comparing(File::getName));

            // 收集已处理完成的文件
            List<File> completedFiles = new ArrayList<>();
            for (File f : capFiles) {
                FileProgress progress = fileProgressMap.get(f.getName());
                if (progress != null && progress.completed) {
                    completedFiles.add(f);
                }
            }

            // 保留最近 capRetentionCount 个已处理文件，删除其余的
            if (completedFiles.size() <= capRetentionCount) {
                return;
            }

            int toDelete = completedFiles.size() - capRetentionCount;
            for (int i = 0; i < toDelete; i++) {
                File f = completedFiles.get(i);
                if (f.delete()) {
                    fileProgressMap.remove(f.getName());
                    logger.info("已清理已处理的cap文件: {}", f.getName());
                } else {
                    logger.warn("清理cap文件失败: {}", f.getName());
                }
            }
            saveProgress();
        } catch (Exception e) {
            logger.warn("清理已处理cap文件时异常: {}", e.getMessage());
        }
    }
}
