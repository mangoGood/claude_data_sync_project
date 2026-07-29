package com.migration.subscribe;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.migration.common.MdcUtil;
import com.migration.subscribe.security.DataMaskingService;
import com.migration.thl.EncryptedTHLFileReader;
import com.migration.thl.THLEvent;
import com.migration.thl.THLFileReader;
import com.migration.thl.crypto.ThlEncryptionService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ContinuousSubscribeMain {

    private static final Logger logger = LoggerFactory.getLogger(ContinuousSubscribeMain.class);

    private String thlDirectory;
    private String kafkaBootstrapServers;
    private String kafkaTopicPrefix;
    private String kafkaTopicStrategy;
    private String subscribeFormat;
    private String taskId;
    private String sourceType;
    private long scanInterval;

    private KafkaProducer<String, String> kafkaProducer;
    private final Gson gson = new Gson();
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * 每个 THL 文件已订阅到的最后 seqno。
     *
     * <p>历史实现对"已读完"的文件写 -1 覆盖掉真实 seqno，一旦该文件重新变成最新文件
     * （新文件被清理/轮转边界）就只能整文件回退重读、把几十万条事件重投一遍。
     * 现在 seqno 与"是否读完"({@link #completedFiles}) 分开记，读完的文件即便重新变成
     * 最新文件也只从上次 seqno 之后续读。
     */
    private final Map<String, Long> fileSeqno = new LinkedHashMap<>();
    /** 已读到 EOF 且不再是最新文件的 THL 文件名（不会再被扫描）。 */
    private final Set<String> completedFiles = new LinkedHashSet<>();
    private String progressFile;

    private final AtomicLong totalEventsSent = new AtomicLong(0);
    private final AtomicLong totalBytesSent = new AtomicLong(0);
    private final AtomicLong sendErrors = new AtomicLong(0);

    private volatile long lastRtoMs = -1;
    private volatile long lastRtoReportTime = 0;
    private static final long RTO_REPORT_INTERVAL_MS = 3000;

    private final Map<String, Long> topicOffsetMap = new ConcurrentHashMap<>();

    /** 数据脱敏服务 */
    private DataMaskingService dataMaskingService;

    /** THL 加密服务 */
    private ThlEncryptionService thlEncryptionService;

    public static void main(String[] args) {
        logger.info("=== 数据订阅服务启动 ===");

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
                logger.error("加载配置文件失败: {}", configPath, e);
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
                    logger.error("加载默认配置失败", e);
                    System.exit(1);
                }
            }
        }
        // 解密 config.properties 中的加密口令（ENC: 前缀）；历史明文配置无前缀，原样通过。
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

        String taskId = props.getProperty("task.id", System.getProperty("task.id", "unknown"));

        MdcUtil.setTaskId(taskId);
        MdcUtil.setProcessName("subscribe");

        try {
            ContinuousSubscribeMain main = new ContinuousSubscribeMain();
            main.initialize(props);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("收到关闭信号，停止订阅服务...");
                main.stop();
                MdcUtil.clear();
            }));

            main.start();
        } catch (Exception e) {
            logger.error("数据订阅服务异常退出", e);
            System.exit(1);
        }
    }

    public void initialize(Properties props) throws Exception {
        this.taskId = props.getProperty("task.id", "unknown");
        this.thlDirectory = props.getProperty("subscribe.thl.dir",
                "files/" + taskId + "/thl_output");
        this.kafkaBootstrapServers = props.getProperty("subscribe.kafka.bootstrap.servers",
                "localhost:9092");
        this.kafkaTopicPrefix = props.getProperty("subscribe.kafka.topic.prefix", "cdc");
        this.kafkaTopicStrategy = props.getProperty("subscribe.kafka.topic.strategy", "TABLE");
        this.subscribeFormat = props.getProperty("subscribe.format", "DEBEZIUM_JSON");
        this.sourceType = props.getProperty("source.db.type", "mysql");
        this.scanInterval = Long.parseLong(props.getProperty("subscribe.scan.interval", "3000"));

        // 初始化数据脱敏服务
        this.dataMaskingService = new DataMaskingService(props);
        if (dataMaskingService.isEnabled()) {
            logger.info("数据脱敏已启用");
        }

        // 初始化 THL 加密服务
        this.thlEncryptionService = new ThlEncryptionService(props);
        if (thlEncryptionService.isEnabled()) {
            logger.info("THL 文件加密已启用（subscribe 读取端）");
        }

        File checkpointDir = new File("./files/" + taskId + "/checkpoint");
        if (!checkpointDir.exists()) {
            checkpointDir.mkdirs();
        }

        initKafkaProducer();

        // 订阅续传只认 .subscribe_progress（每个 THL 文件读到哪个 seqno）。
        // 曾经还有一个 checkpoint/subscribe_checkpoint 文件被读进 lastSentSeqno，但从未参与过跳过判断，
        // 留着只会让人误以为续传靠它——已移除，避免排查续传问题时被带偏。
        progressFile = "./files/" + taskId + "/checkpoint/.subscribe_progress";
        loadProgress();

        logger.info("数据订阅服务初始化完成 - thlDir: {}, kafka: {}, topicPrefix: {}, strategy: {}, format: {}, 已记录 {} 个THL文件位点",
                thlDirectory, kafkaBootstrapServers, kafkaTopicPrefix, kafkaTopicStrategy, subscribeFormat, fileSeqno.size());
    }

    private void initKafkaProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=all + 幂等生产者：订阅位点一旦推进就再也不会重读这段 THL，因此"写进 Kafka"必须是
        // 真的落到全部 ISR。acks=1 时 leader 刚确认就崩溃会静默丢消息，而位点已经推进 —— 永久丢数据。
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        this.kafkaProducer = new KafkaProducer<>(producerProps);
        logger.info("Kafka生产者初始化完成, bootstrapServers: {} (acks=all, 幂等)", kafkaBootstrapServers);
    }

    public void start() {
        logger.info("启动THL文件订阅到Kafka...");

        while (running.get()) {
            try {
                touchLiveness();
                scanAndProcessThlFiles();
                touchLiveness();
                Thread.sleep(scanInterval);
            } catch (InterruptedException e) {
                logger.info("订阅线程被中断");
                break;
            } catch (RuntimeException e) {
                logger.error("THL文件处理致命错误, 服务关闭", e);
                throw e;
            } catch (Exception e) {
                logger.error("THL文件扫描异常", e);
            }
        }

        close();
        logger.info("数据订阅服务已停止, 总发送事件数: {}", totalEventsSent.get());
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

        Arrays.sort(thlFiles, Comparator.comparing(File::getName));

        String latestFileName = thlFiles[thlFiles.length - 1].getName();

        for (File thlFile : thlFiles) {
            if (!running.get()) break;

            boolean isLatest = thlFile.getName().equals(latestFileName);
            // 读完且已不是最新文件的，不会再有新事件追加，直接跳过；
            // 若它又变回最新文件（更新的文件被清理），则继续从已记录的 seqno 之后续读，
            // 而不是回退到文件开头把整个文件重投一遍。
            if (completedFiles.contains(thlFile.getName())) {
                if (!isLatest) {
                    continue;
                }
                completedFiles.remove(thlFile.getName());
            }

            processThlFile(thlFile, isLatest);
        }
    }

    /** 每处理这么多事件就 flush + 落一次位点，既限制崩溃后的重投量，也限制未确认消息的窗口。 */
    private static final int CHECKPOINT_EVERY_EVENTS = 500;

    private void processThlFile(File thlFile, boolean isLatestFile) {
        String fileName = thlFile.getName();

        logger.info("处理订阅THL文件: {}", fileName);
        int eventCount = 0;
        int sinceCheckpoint = 0;
        long startSeqno = fileSeqno.getOrDefault(fileName, -1L);
        long fileLastSeqno = startSeqno;
        // 是否自然读到文件末尾。中途因停机而 break 出来的不算读完——否则会把只读了一半的
        // 非最新文件标成"已完成"，重启后整个跳过，剩下的事件永远进不了 Kafka。
        boolean reachedEof = false;

        try (THLFileReader reader = createThlReader(thlFile.getAbsolutePath())) {
            THLEvent event;
            while (true) {
                if (!running.get()) break;
                if ((event = reader.readEvent()) == null) {
                    reachedEof = true;
                    break;
                }

                touchLiveness();

                // 跳过本文件中已经投递过的事件（同一文件内 seqno 单调递增）
                if (event.getSeqno() <= startSeqno) {
                    continue;
                }

                if (event.getType() == THLEvent.HEARTBEAT_EVENT) {
                    fileLastSeqno = event.getSeqno();
                    reportRto(event);
                    continue;
                }

                CdcEvent cdcEvent = convertToCdcEvent(event);
                if (cdcEvent != null) {
                    sendToKafka(cdcEvent);
                    eventCount++;
                    sinceCheckpoint++;
                }

                fileLastSeqno = event.getSeqno();
                reportRto(event);

                if (eventCount % 100 == 0) {
                    logger.info("已订阅 {} 个事件, 最后seqno: {}", eventCount, fileLastSeqno);
                }

                // 中途落位点：必须先 flush 确认消息真的进了 Kafka 再推进，否则崩溃即丢
                if (sinceCheckpoint >= CHECKPOINT_EVERY_EVENTS) {
                    if (commitProgress(fileName, fileLastSeqno, false)) {
                        startSeqno = fileLastSeqno;
                        sinceCheckpoint = 0;
                    } else {
                        // 有消息没能确认落盘：不推进位点，退出本轮，下次从旧位点重投这一段
                        logger.error("订阅位点未推进（存在未确认的 Kafka 消息），将从 seqno {} 重投", startSeqno);
                        return;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("处理THL文件异常: {}", fileName, e);
            throw new RuntimeException("Fatal error processing THL file: " + fileName, e);
        }

        if (eventCount > 0) {
            logger.info("THL文件处理完成: {} -> {} 个事件已发送到Kafka, 最后seqno: {}", fileName, eventCount, fileLastSeqno);
        }

        // 只有"自然读到 EOF 且已不是最新文件"才算这个文件彻底完结、可以永久跳过
        boolean completed = reachedEof && !isLatestFile;
        // 没读到新事件、且完成状态也没变化时不必反复 flush + 重写位点文件
        // （空闲时每 3s 一轮扫描，否则会白白刷盘）
        boolean newlyCompleted = completed && !completedFiles.contains(fileName);
        if (fileLastSeqno > startSeqno || newlyCompleted) {
            commitProgress(fileName, fileLastSeqno, completed);
        }
    }

    /**
     * 推进订阅位点：**先** flush 等所有已发消息拿到 broker 确认，确认全部成功才落盘位点。
     *
     * <p>这是订阅链路"崩溃即丢数据"的根因修复：此前位点是异步发送后立刻落盘的，
     * KafkaProducer 缓冲区里还没发出去的那一批（linger 5ms / 16KB 攒批）在 SIGKILL 时全部丢失，
     * 而位点已经写成"这段已订阅完"，重启后直接跳过——源库有、Kafka 永远没有。
     *
     * @return true 表示位点已安全推进；false 表示有发送失败，调用方不应把位点当已提交
     */
    private boolean commitProgress(String fileName, long seqno, boolean completed) {
        long errsBefore = sendErrors.get();
        try {
            kafkaProducer.flush();
        } catch (Exception e) {
            logger.error("flush Kafka 失败，订阅位点不推进: {}", e.getMessage());
            return false;
        }
        if (sendErrors.get() != errsBefore) {
            logger.error("flush 后发现 {} 条消息发送失败，订阅位点不推进", sendErrors.get() - errsBefore);
            return false;
        }
        fileSeqno.put(fileName, seqno);
        if (completed) {
            completedFiles.add(fileName);
        }
        saveProgress();
        return true;
    }

    /**
     * 活性心跳：主循环每一轮、以及处理大文件的逐事件过程中都改写
     * {@code binlog_output/subscribe_liveness}（限流 2s 一次）。
     *
     * <p>为什么不能复用 {@code subscribe_rto_ms}：那个文件只在"碰到带源时间戳的事件"时才写，
     * 空闲时段本就长时间不更新，拿它判僵死会误杀；而订阅进程被冻结（SIGSTOP）时进程仍 alive，
     * 只有一个"活着就一定在刷"的文件才能把假活识别出来。
     */
    private volatile long lastLivenessWriteMs = 0;

    private void touchLiveness() {
        long now = System.currentTimeMillis();
        if (now - lastLivenessWriteMs < 2000) {
            return;
        }
        lastLivenessWriteMs = now;
        File dir = new File("./files/" + taskId + "/binlog_output");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        try (BufferedWriter w = new BufferedWriter(new FileWriter(new File(dir, "subscribe_liveness"), false))) {
            w.write(String.valueOf(now));
        } catch (IOException e) {
            logger.debug("写 subscribe_liveness 失败: {}", e.getMessage());
        }
    }

    private CdcEvent convertToCdcEvent(THLEvent thlEvent) {
        Map<String, Object> metadata = thlEvent.getMetadata();
        if (metadata == null || metadata.isEmpty()) {
            logger.debug("THLEvent seqno={} 无metadata, data={}", thlEvent.getSeqno(), 
                thlEvent.getData() != null ? new String(thlEvent.getData(), StandardCharsets.UTF_8).substring(0, Math.min(100, thlEvent.getData().length)) : "null");
            return null;
        }

        String operation = metadata.get("operation") != null ? metadata.get("operation").toString() : null;
        if (operation == null || "COMMIT".equals(operation) || "ROTATE".equals(operation) || "HEARTBEAT".equals(operation)) {
            return null;
        }

        logger.info("转换THLEvent seqno={}, operation={}, metadata keys={}", thlEvent.getSeqno(), operation, metadata.keySet());

        CdcEvent cdcEvent = new CdcEvent();
        cdcEvent.seqno = thlEvent.getSeqno();
        cdcEvent.sourceTstamp = thlEvent.getSourceTstamp() != null ? thlEvent.getSourceTstamp().getTime() : System.currentTimeMillis();
        cdcEvent.localEnqueueTstamp = thlEvent.getLocalEnqueueTstamp() != null ? thlEvent.getLocalEnqueueTstamp().getTime() : System.currentTimeMillis();

        // Map operation to Debezium format
        switch (operation) {
            case "INSERT":
                cdcEvent.operation = "c";
                break;
            case "UPDATE":
                cdcEvent.operation = "u";
                break;
            case "DELETE":
                cdcEvent.operation = "d";
                break;
            default:
                return null;
        }

        // Extract database and table from metadata
        Object dbName = metadata.get("database_name");
        Object tableName = metadata.get("table_name");
        if (dbName != null) {
            cdcEvent.database = dbName.toString();
        } else {
            cdcEvent.database = thlEvent.getShardId() != null ? thlEvent.getShardId() : "unknown";
        }
        if (tableName != null) {
            cdcEvent.table = tableName.toString();
        } else {
            cdcEvent.table = "unknown";
        }

        // 列名必须按「本次事件实际带了哪些列」来取，不能一律用整表列名 column_names。
        //
        // Oracle LogMiner 的 UPDATE 只给变更列（SET 了哪些给哪些），PG 部分列 INSERT 同理，
        // 而 column_names 始终是整表列。用整表列名去和"只有变更列"的值列表按位置拉链，
        // 结果是整体错位——实测 Oracle 源 UPDATE 订阅出来是 {"ID":"upd-253","GRP":1000253}：
        // 主键 ID 被写成了 VAL 的值，真正的主键干脆丢了，下游拿到的是废数据。
        // 增量应用侧（TypedDmlConverter/THLToSqlConverter）早就按 *_column_names 对齐，
        // 订阅侧此前一直没跟上。
        String[] allColumns = splitColumns(metadata.get("column_names"));
        String[] afterColumns = splitColumns(metadata.get(
                "INSERT".equals(operation) ? "insert_column_names" : "update_column_names"));
        if (afterColumns == null) {
            afterColumns = allColumns;
        }
        String[] beforeColumns = splitColumns(metadata.get("update_before_column_names"));
        if (beforeColumns == null) {
            beforeColumns = allColumns;
        }
        // DELETE 的 row_data 装的是前像（整行），列名用整表列
        if ("DELETE".equals(operation)) {
            afterColumns = allColumns;
        }

        // Extract row data from metadata
        Object rowData = metadata.get("row_data");
        if (rowData != null) {
            cdcEvent.afterData = mapRowIfPossible(rowData.toString(), afterColumns);
        }

        // 前像的 key 是 row_data_before（各 extractor 统一如此）。此处曾写成 before_row_data，
        // 那个 key 没有任何 extractor 会产生：MySQL/TiDB 靠下面 rows_data_before 复数兜底
        // 侥幸能出 before，PG / Oracle 只发单数 key，于是 UPDATE/DELETE 的 before 恒为 null。
        Object beforeRowData = metadata.get("row_data_before");
        if (beforeRowData != null) {
            cdcEvent.beforeData = mapRowIfPossible(beforeRowData.toString(), beforeColumns);
        }

        // Also try rows_data (plural form) if row_data is null
        if (cdcEvent.afterData == null) {
            Object rowsData = metadata.get("rows_data");
            if (rowsData != null) {
                cdcEvent.afterData = mapRowIfPossible(firstOfList(rowsData.toString()), afterColumns);
            }
        }

        if (cdcEvent.beforeData == null) {
            Object rowsDataBefore = metadata.get("rows_data_before");
            if (rowsDataBefore != null) {
                cdcEvent.beforeData = mapRowIfPossible(firstOfList(rowsDataBefore.toString()), beforeColumns);
            }
        }

        // If no row_data, try to extract from data field (fallback)
        if (cdcEvent.afterData == null && thlEvent.getData() != null && thlEvent.getData().length > 0) {
            String rawData = new String(thlEvent.getData(), StandardCharsets.UTF_8).trim();
            if (!rawData.isEmpty()) {
                String[] lines = rawData.split("\n");
                for (String line : lines) {
                    if (line.trim().isEmpty()) continue;
                    parseCdcLine(line, cdcEvent);
                    if (cdcEvent.operation != null && cdcEvent.afterData != null) {
                        break;
                    }
                }
            }
        }

        // UPDATE 的后像补全：Oracle LogMiner（以及部分列更新的 PG）只给变更列，
        // 后像里连主键都没有，下游根本认不出改的是哪一行。而"未变更的列"在前像里的值
        // 按定义就等于变更后的值，所以 前像 ⊕ 变更列 = 完整后像，语义上是准确的补全，
        // 也才符合 Debezium 里 after 表示"变更后整行"的约定。
        if ("UPDATE".equals(operation)) {
            String merged = mergeBeforeIntoAfter(cdcEvent.beforeData, cdcEvent.afterData);
            if (merged != null) {
                cdcEvent.afterData = merged;
            }
        }

        cdcEvent.key = extractKey(cdcEvent, metadata);
        cdcEvent.rawData = cdcEvent.afterData != null ? cdcEvent.afterData : "";

        return cdcEvent;
    }

    /** 前像 ⊕ 后像（后像覆盖同名列）→ 完整后像 JSON；任一不是 JSON 对象时返回 null（不动原值）。 */
    private String mergeBeforeIntoAfter(String beforeJson, String afterJson) {
        if (beforeJson == null || afterJson == null) return null;
        if (!beforeJson.trim().startsWith("{") || !afterJson.trim().startsWith("{")) return null;
        try {
            Type t = new TypeToken<Map<String, Object>>() {}.getType();
            Map<String, Object> before = gson.fromJson(beforeJson, t);
            Map<String, Object> after = gson.fromJson(afterJson, t);
            if (before == null || after == null) return null;
            Map<String, Object> merged = new LinkedHashMap<>(before);
            merged.putAll(after);
            return gson.toJson(merged);
        } catch (Exception e) {
            logger.debug("合并 UPDATE 前后像失败: {}", e.getMessage());
            return null;
        }
    }

    private void parseCdcLine(String line, CdcEvent cdcEvent) {
        char FIELD_SEP = '\001';
        String[] fields = line.split(String.valueOf(FIELD_SEP));

        if (fields.length < 1) return;

        String eventType = fields[0].trim();

        switch (eventType) {
            case "INSERT":
            case "WRITE_ROWS":
                cdcEvent.operation = "c";
                break;
            case "UPDATE":
            case "UPDATE_ROWS":
                cdcEvent.operation = "u";
                break;
            case "DELETE":
            case "DELETE_ROWS":
                cdcEvent.operation = "d";
                break;
            default:
                return;
        }

        if (fields.length >= 2) {
            String dbTable = fields[1].trim();
            int dotIdx = dbTable.lastIndexOf('.');
            if (dotIdx > 0) {
                cdcEvent.database = dbTable.substring(0, dotIdx);
                cdcEvent.table = dbTable.substring(dotIdx + 1);
            } else {
                cdcEvent.database = dbTable;
                cdcEvent.table = dbTable;
            }
        }

        if (fields.length >= 3) {
            cdcEvent.afterData = fields[2].trim();
        }
        if (fields.length >= 4) {
            cdcEvent.beforeData = fields[3].trim();
        }

        cdcEvent.key = extractKey(cdcEvent, null);
    }

    private String extractDatabaseFromMetadata(THLEvent event) {
        Object db = event.getMetadata("database");
        if (db != null) return db.toString();
        Object shard = event.getMetadata("shardId");
        if (shard != null) return shard.toString();
        return "unknown";
    }

    /**
     * Kafka 消息 key：同一行的所有变更必须落到同一个 key，否则多分区下同一行的
     * INSERT/UPDATE/DELETE 会散到不同分区，下游消费顺序错乱、最终状态就是错的。
     *
     * <p>因此优先用**主键列的值**构造 key（主键列名来自 {@code primary_keys} 元数据），
     * 前像后像都找。此前是"取 afterData 的第一个字段"——对 Oracle UPDATE（后像只含变更列）
     * 取到的是 VAL 而不是主键，同一行的 INSERT 与 UPDATE 因此 key 不同。
     */
    private String extractKey(CdcEvent cdcEvent, Map<String, Object> metadata) {
        String prefix = cdcEvent.database + "." + cdcEvent.table + ".";

        String[] pkCols = metadata != null ? splitColumns(metadata.get("primary_keys")) : null;
        if (pkCols != null) {
            String pkPart = pkValues(cdcEvent.afterData, pkCols);
            if (pkPart == null) {
                pkPart = pkValues(cdcEvent.beforeData, pkCols);
            }
            if (pkPart != null) {
                return prefix + pkPart;
            }
        }

        // 无主键元数据时退回旧行为（取后像第一个字段），再退回 seqno（至少保证 key 唯一）
        if (cdcEvent.afterData != null && !cdcEvent.afterData.isEmpty()) {
            String[] cols = cdcEvent.afterData.split(",");
            if (cols.length > 0) {
                return prefix + cols[0].replaceAll("[^a-zA-Z0-9_]", "");
            }
        }
        return prefix + cdcEvent.seqno;
    }

    /** 从已映射成 JSON 的行里取主键列的值拼成 key 片段；缺任一主键列返回 null。 */
    private String pkValues(String jsonRow, String[] pkCols) {
        if (jsonRow == null || !jsonRow.trim().startsWith("{")) return null;
        Map<String, Object> row;
        try {
            row = gson.fromJson(jsonRow, new TypeToken<Map<String, Object>>() {}.getType());
        } catch (Exception e) {
            return null;
        }
        if (row == null) return null;
        StringBuilder sb = new StringBuilder();
        for (String pk : pkCols) {
            String col = pk.trim();
            Object v = row.containsKey(col) ? row.get(col) : caseInsensitiveGet(row, col);
            if (v == null) return null;
            if (sb.length() > 0) sb.append('_');
            sb.append(v);
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    private Object caseInsensitiveGet(Map<String, Object> row, String col) {
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getKey().equalsIgnoreCase(col)) return e.getValue();
        }
        return null;
    }

    /** 创建 THL 文件读取器（根据加密配置自动选择） */
    private THLFileReader createThlReader(String filePath) throws IOException {
        if (thlEncryptionService != null && thlEncryptionService.isEnabled()) {
            return new EncryptedTHLFileReader(filePath, thlEncryptionService);
        }
        return new THLFileReader(filePath);
    }

    private void sendToKafka(CdcEvent cdcEvent) {
        String topic = resolveTopic(cdcEvent);
        String messageKey = cdcEvent.key;
        String messageValue;

        if ("SIMPLE_JSON".equals(subscribeFormat)) {
            messageValue = buildSimpleJson(cdcEvent);
        } else {
            messageValue = buildDebeziumJson(cdcEvent);
        }

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageKey, messageValue);
            Future<RecordMetadata> future = kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    sendErrors.incrementAndGet();
                    logger.error("发送事件到Kafka失败, topic {}: {}", topic, exception.getMessage());
                } else {
                    topicOffsetMap.put(topic, metadata.offset());
                }
            });

            totalEventsSent.incrementAndGet();
            totalBytesSent.addAndGet(messageValue.length());

            if (totalEventsSent.get() % 1000 == 0) {
                logger.info("Kafka发送统计: 总事件数={}, 字节数={}, 错误数={}",
                        totalEventsSent.get(), totalBytesSent.get(), sendErrors.get());
            }
        } catch (Exception e) {
            sendErrors.incrementAndGet();
            logger.error("发送事件到Kafka异常: {}", e.getMessage());
        }
    }

    private String resolveTopic(CdcEvent cdcEvent) {
        switch (kafkaTopicStrategy.toUpperCase()) {
            case "TABLE":
                return kafkaTopicPrefix + "." + taskId + "." + cdcEvent.database + "." + cdcEvent.table;
            case "TASK":
                return kafkaTopicPrefix + "." + taskId;
            case "GLOBAL":
                return kafkaTopicPrefix + ".events";
            default:
                return kafkaTopicPrefix + "." + taskId + "." + cdcEvent.database + "." + cdcEvent.table;
        }
    }

    private String buildDebeziumJson(CdcEvent cdcEvent) {
        Map<String, Object> beforeMap = parseDataToMap(cdcEvent.beforeData);
        Map<String, Object> afterMap = parseDataToMap(cdcEvent.afterData);

        // 应用数据脱敏
        if (dataMaskingService != null && dataMaskingService.isEnabled()) {
            beforeMap = dataMaskingService.mask(beforeMap);
            afterMap = dataMaskingService.mask(afterMap);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("before", beforeMap);
        payload.put("after", afterMap);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("version", "1.0.0");
        source.put("connector", sourceType);
        source.put("db", cdcEvent.database);
        source.put("table", cdcEvent.table);
        source.put("ts_ms", cdcEvent.sourceTstamp);
        source.put("seqno", cdcEvent.seqno);
        payload.put("source", source);

        payload.put("op", cdcEvent.operation);
        payload.put("ts_ms", cdcEvent.localEnqueueTstamp);

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("payload", payload);

        return gson.toJson(envelope);
    }

    private String buildSimpleJson(CdcEvent cdcEvent) {
        Map<String, Object> beforeMap = parseDataToMap(cdcEvent.beforeData);
        Map<String, Object> afterMap = parseDataToMap(cdcEvent.afterData);

        // 应用数据脱敏
        if (dataMaskingService != null && dataMaskingService.isEnabled()) {
            beforeMap = dataMaskingService.mask(beforeMap);
            afterMap = dataMaskingService.mask(afterMap);
        }

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("op", cdcEvent.operation);
        message.put("db", cdcEvent.database);
        message.put("table", cdcEvent.table);
        message.put("before", beforeMap);
        message.put("after", afterMap);
        message.put("sourceTs", cdcEvent.sourceTstamp);
        message.put("seqno", cdcEvent.seqno);

        return gson.toJson(message);
    }

    private Map<String, Object> parseDataToMap(String data) {
        if (data == null || data.isEmpty()) return null;
        // If data is JSON format, parse it directly
        if (data.trim().startsWith("{")) {
            try {
                return gson.fromJson(data, new TypeToken<Map<String, Object>>() {}.getType());
            } catch (Exception e) {
                logger.debug("JSON解析失败: {}", data.substring(0, Math.min(100, data.length())));
            }
        }
        // Fallback: try key=value format
        Map<String, Object> map = new LinkedHashMap<>();
        String[] pairs = data.split(",");
        for (String pair : pairs) {
            int eqIdx = pair.indexOf('=');
            if (eqIdx > 0) {
                String key = pair.substring(0, eqIdx).trim();
                String value = pair.substring(eqIdx + 1).trim();
                map.put(key, value);
            }
        }
        return map.isEmpty() ? null : map;
    }

    /** 逗号分隔的列名元数据 → 数组；缺失或空串返回 null。 */
    private String[] splitColumns(Object metaValue) {
        if (metaValue == null) return null;
        String s = metaValue.toString().trim();
        if (s.isEmpty()) return null;
        return s.split(",");
    }

    /** {@code rows_data} 之类的复数形式可能是 List 的 toString，取第一行。 */
    private String firstOfList(String s) {
        if (s != null && s.startsWith("[")) {
            return s.substring(1, s.length() - 1).trim();
        }
        return s;
    }

    private String mapRowIfPossible(String rowStr, String[] columnNames) {
        return columnNames != null ? mapRowToColumns(rowStr, columnNames) : rowStr;
    }

    /**
     * Maps SQL value list (e.g., 'value1',123,'value2') to column names,
     * producing a JSON string like {"col1":"value1","col2":123,"col3":"value2"}
     *
     * <p>列数与值数不一致时**不做截断拉链**——那样只会安静地产出错位的假数据（把 VAL 的值
     * 写进 ID）。宁可退回原始字符串，让下游一眼看出是未解析的原文。
     */
    private String mapRowToColumns(String rowData, String[] columnNames) {
        if (rowData == null || columnNames == null || columnNames.length == 0) return rowData;
        List<String> values = parseSqlValueList(rowData);
        if (values.size() != columnNames.length) {
            logger.warn("列数({})与值数({})不一致，不做列名映射以免错位: {}",
                    columnNames.length, values.size(),
                    rowData.length() > 120 ? rowData.substring(0, 120) + "..." : rowData);
            return rowData;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < Math.min(columnNames.length, values.size()); i++) {
            String colName = columnNames[i].trim();
            String value = values.get(i);
            // Try to parse as number
            if (value != null && !value.equals("null")) {
                try {
                    if (value.contains(".")) {
                        map.put(colName, Double.parseDouble(value));
                    } else {
                        map.put(colName, Long.parseLong(value));
                    }
                } catch (NumberFormatException e) {
                    map.put(colName, value);
                }
            } else {
                map.put(colName, null);
            }
        }
        return gson.toJson(map);
    }

    /**
     * Parses SQL value list like 'value1',123,'value2' into individual values
     */
    private List<String> parseSqlValueList(String data) {
        List<String> values = new ArrayList<>();
        if (data == null || data.isEmpty()) return values;
        
        int i = 0;
        while (i < data.length()) {
            // Skip whitespace and commas
            while (i < data.length() && (data.charAt(i) == ',' || Character.isWhitespace(data.charAt(i)))) {
                i++;
            }
            if (i >= data.length()) break;

            if (data.charAt(i) == '\'') {
                // Quoted string value
                StringBuilder sb = new StringBuilder();
                i++; // skip opening quote
                while (i < data.length()) {
                    if (data.charAt(i) == '\\' && i + 1 < data.length()) {
                        // Escape sequence
                        char next = data.charAt(i + 1);
                        if (next == '\'' || next == '\\') {
                            sb.append(next);
                            i += 2;
                        } else {
                            sb.append(data.charAt(i));
                            i++;
                        }
                    } else if (data.charAt(i) == '\'') {
                        i++; // skip closing quote
                        break;
                    } else {
                        sb.append(data.charAt(i));
                        i++;
                    }
                }
                values.add(sb.toString());
            } else if (data.startsWith("null", i)) {
                values.add("null");
                i += 4;
            } else if (data.startsWith("0x", i) || data.startsWith("0X", i)) {
                // Hex value
                StringBuilder sb = new StringBuilder();
                sb.append(data.charAt(i));
                i++;
                sb.append(data.charAt(i));
                i++;
                while (i < data.length() && Character.isLetterOrDigit(data.charAt(i))) {
                    sb.append(data.charAt(i));
                    i++;
                }
                values.add(sb.toString());
            } else {
                // Unquoted value (number or identifier)
                StringBuilder sb = new StringBuilder();
                while (i < data.length() && data.charAt(i) != ',' && !Character.isWhitespace(data.charAt(i))) {
                    sb.append(data.charAt(i));
                    i++;
                }
                values.add(sb.toString());
            }
        }
        return values;
    }

    private void reportRto(THLEvent event) {
        if (event.getSourceTstamp() != null) {
            long now = System.currentTimeMillis();
            long rtoMs = now - event.getSourceTstamp().getTime();
            if (rtoMs >= 0 && now - lastRtoReportTime > RTO_REPORT_INTERVAL_MS) {
                lastRtoMs = rtoMs;
                lastRtoReportTime = now;
                writeRtoMetric(rtoMs);
            }
        }
    }

    private void writeRtoMetric(long rtoMs) {
        String metricPath = "./files/" + taskId + "/metrics/subscribe_rto_ms";
        try {
            File metricDir = new File(metricPath).getParentFile();
            if (!metricDir.exists()) metricDir.mkdirs();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(metricPath))) {
                writer.write(String.valueOf(rtoMs));
            }
        } catch (IOException e) {
            logger.debug("写入RTO指标失败: {}", e.getMessage());
        }
    }

    /**
     * 进度文件格式：{@code 文件名=seqno|done}（done 为 0/1）。
     * 兼容旧格式 {@code 文件名=seqno}（-1 表示"整个文件已读完"，无 seqno 信息）。
     */
    private void loadProgress() {
        File file = new File(progressFile);
        if (!file.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length != 2) continue;
                String name = parts[0];
                String[] vals = parts[1].trim().split("\\|");
                try {
                    long seqno = Long.parseLong(vals[0].trim());
                    if (vals.length >= 2) {
                        fileSeqno.put(name, seqno);
                        if ("1".equals(vals[1].trim())) {
                            completedFiles.add(name);
                        }
                    } else if (seqno == -1L) {
                        // 旧格式的"已读完"只记了一个 -1，没留 seqno。标记为已完成即可（它当时不是
                        // 最新文件，正常不会再被扫）。位置记 -1 而不是 MAX_VALUE：万一它又变回最新
                        // 文件，宁可整文件重读产生重复（下游按 seqno 幂等吸收），也不能直接跳过丢数据。
                        completedFiles.add(name);
                        fileSeqno.put(name, -1L);
                    } else {
                        fileSeqno.put(name, seqno);
                    }
                } catch (NumberFormatException e) {
                    logger.warn("无效的进度条目: {}", line);
                }
            }
        } catch (IOException e) {
            logger.warn("加载订阅进度失败: {}", e.getMessage());
        }
    }

    /** 位点落盘：先写临时文件再原子 rename，避免崩在写一半时留下截断的进度文件。 */
    private void saveProgress() {
        File target = new File(progressFile);
        File tmp = new File(progressFile + ".tmp");
        try {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
                for (Map.Entry<String, Long> entry : fileSeqno.entrySet()) {
                    writer.write(entry.getKey() + "=" + entry.getValue()
                            + "|" + (completedFiles.contains(entry.getKey()) ? "1" : "0"));
                    writer.newLine();
                }
            }
            if (!tmp.renameTo(target)) {
                java.nio.file.Files.move(tmp.toPath(), target.toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            logger.warn("保存订阅进度失败: {}", e.getMessage());
        }
    }

    private void close() {
        if (kafkaProducer != null) {
            try {
                kafkaProducer.flush();
                kafkaProducer.close(java.time.Duration.ofSeconds(10));
            } catch (Exception e) {
                logger.warn("关闭Kafka生产者异常: {}", e.getMessage());
            }
        }
        saveProgress();
    }

    public long getTotalEventsSent() {
        return totalEventsSent.get();
    }

    public long getSendErrors() {
        return sendErrors.get();
    }

    public long getLastRtoMs() {
        return lastRtoMs;
    }

    private static class CdcEvent {
        long seqno;
        String operation;
        String database;
        String table;
        String key;
        String beforeData;
        String afterData;
        String rawData;
        long sourceTstamp;
        long localEnqueueTstamp;
    }
}
