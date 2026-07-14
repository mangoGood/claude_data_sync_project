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
        // 解密 config.properties 中的加密口令（ENC: 前缀）；历史明文配置无前缀，原样通过。
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

        String taskId = props.getProperty("task.id", System.getProperty("task.id", "unknown"));
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
        this.inputDir = props.getProperty("extract.input.dir",
                "files/" + props.getProperty("task.id", "unknown") + "/binlog_output");
        this.outputDir = props.getProperty("extract.output.dir",
                "files/" + props.getProperty("task.id", "unknown") + "/thl_output");
        this.scanInterval = Long.parseLong(props.getProperty("extract.scan.interval", "3000"));
        this.watchEnabled = Boolean.parseBoolean(props.getProperty("extract.watch.enabled", "true"));
        this.watchFallbackMs = Long.parseLong(props.getProperty("extract.watch.fallback.ms", "1000"));
        this.progressRecordFile = outputDir + "/.extract_progress";
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
        } catch (Exception e) {
            logger.debug("写入吞吐/积压指标失败: {}", e.getMessage());
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

        int totalEvents = 0;
        for (File binlogFile : binlogFiles) {
            if (!running.get()) break;
            totalEvents += processFileIncremental(binlogFile);
        }
        return totalEvents;
    }

    private int processFileIncremental(File binlogFile) throws Exception {
        FileProgress progress = fileProgressMap.get(binlogFile.getName());

        if (progress == null) {
            progress = new FileProgress(binlogFile.getName());
            fileProgressMap.put(binlogFile.getName(), progress);
            logger.info("Detected new binlog file: {}", binlogFile.getName());
        }

        if (progress.completed) return 0;

        int totalLinesInFile = countLines(binlogFile);
        if (totalLinesInFile <= progress.linesRead) return 0;

        int newLines = totalLinesInFile - progress.linesRead;
        logger.info("Processing binlog file: {} ({} new lines, already read: {}, total: {})",
                binlogFile.getName(), newLines, progress.linesRead, totalLinesInFile);

        // 使用类级别 currentThlWriter 统一写入，按50MB大小轮转文件
        int newEventCount = readAndExtractNewLines(binlogFile, progress, progress.linesRead);

        long currentSize = binlogFile.length();
        boolean fileStoppedGrowing = (currentSize == progress.lastFileSize && currentSize > 0);
        progress.lastFileSize = currentSize;

        if (fileStoppedGrowing) {
            progress.stableCheckCount++;
        } else {
            progress.stableCheckCount = 0;
        }

        if (progress.stableCheckCount >= 3) {
            progress.completed = true;
            logger.info("Binlog file {} appears complete, marking as completed", binlogFile.getName());
            // 文件处理完成，清理旧的已完成cap文件
            cleanupCompletedCapFiles();
        }

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

            currentThlWriter.writeEvent(heartbeat);

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

    private int countLines(File file) throws IOException {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while (reader.readLine() != null) count++;
        }
        return count;
    }

    /** 创建 THL 文件写入器（根据加密配置自动选择） */
    private THLFileWriter createThlWriter(String filePath) throws IOException {
        if (thlEncryptionService != null && thlEncryptionService.isEnabled()) {
            return new EncryptedTHLFileWriter(filePath, thlEncryptionService);
        }
        return new THLFileWriter(filePath);
    }

    private int readAndExtractNewLines(File binlogFile,
                                        FileProgress progress, int skipLines) throws Exception {
        int eventCount = 0;
        int currentLine = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(binlogFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                currentLine++;
                if (currentLine <= skipLines) continue;
                if (line.trim().isEmpty()) {
                    progress.linesRead++;
                    continue;
                }

                byte[] eventBytes = line.getBytes("UTF-8");
                THLEvent event = extractor.extract(eventBytes);
                if (event != null) {
                    ensureThlWriter();
                    checkAndRotateThlFile();

                    Boolean multiRow = (Boolean) event.getMetadata().get("multi_row");
                    if (multiRow != null && multiRow) {
                        @SuppressWarnings("unchecked")
                        java.util.List<String> rowsData = (java.util.List<String>) event.getMetadata().get("rows_data");
                        if (rowsData != null && rowsData.size() > 1) {
                            for (int i = 0; i < rowsData.size(); i++) {
                                THLEvent rowEvent = new THLEvent();
                                rowEvent.setSeqno(event.getSeqno() + i);
                                rowEvent.setEventId(event.getEventId() + "_" + i);
                                rowEvent.setSourceId(event.getSourceId());
                                rowEvent.setSourceTstamp(event.getSourceTstamp());
                                
                                for (java.util.Map.Entry<String, Object> entry : event.getMetadata().entrySet()) {
                                    String key = entry.getKey();
                                    if (!"rows_data".equals(key) && !"multi_row".equals(key)) {
                                        rowEvent.addMetadata(key, entry.getValue());
                                    }
                                }
                                rowEvent.addMetadata("row_data", rowsData.get(i));
                                
                                currentThlWriter.writeEvent(rowEvent);
                                eventCount++;
                                checkAndRotateThlFile();
                            }
                            progress.linesRead++;
                            continue;
                        }
                    }
                    currentThlWriter.writeEvent(event);
                    eventCount++;
                }
                progress.linesRead++;
            }
        }
        return eventCount;
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

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(recordFile))) {
            for (FileProgress progress : fileProgressMap.values()) {
                writer.write(progress.fileName + "|" + progress.linesRead + "|" +
                        progress.lastFileSize + "|" + progress.completed);
                writer.newLine();
            }
        } catch (IOException e) {
            logger.warn("Error saving extract progress", e);
        }
    }

    private static class FileProgress {
        String fileName;
        int linesRead;
        long lastFileSize;
        int stableCheckCount;
        boolean completed;

        FileProgress(String fileName) {
            this.fileName = fileName;
            this.linesRead = 0;
            this.lastFileSize = 0;
            this.stableCheckCount = 0;
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
