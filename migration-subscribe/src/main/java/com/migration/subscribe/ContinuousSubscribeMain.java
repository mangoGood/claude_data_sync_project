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
    private final Map<String, Long> processedFiles = new LinkedHashMap<>();
    private String progressFile;

    private long lastSentSeqno = 0;
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

        String checkpointPath = "./files/" + taskId + "/checkpoint/subscribe_checkpoint";
        File checkpointDir = new File(checkpointPath).getParentFile();
        if (!checkpointDir.exists()) {
            checkpointDir.mkdirs();
        }

        File seqnoFile = new File(checkpointPath);
        if (seqnoFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(seqnoFile))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) {
                    lastSentSeqno = Long.parseLong(line.trim());
                }
            }
        }

        initKafkaProducer();

        progressFile = "./files/" + taskId + "/checkpoint/.subscribe_progress";
        loadProgress();

        logger.info("数据订阅服务初始化完成 - thlDir: {}, kafka: {}, topicPrefix: {}, strategy: {}, format: {}, lastSeqno: {}",
                thlDirectory, kafkaBootstrapServers, kafkaTopicPrefix, kafkaTopicStrategy, subscribeFormat, lastSentSeqno);
    }

    private void initKafkaProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        this.kafkaProducer = new KafkaProducer<>(producerProps);
        logger.info("Kafka生产者初始化完成, bootstrapServers: {}", kafkaBootstrapServers);
    }

    public void start() {
        logger.info("启动THL文件订阅到Kafka...");

        while (running.get()) {
            try {
                scanAndProcessThlFiles();
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

            Long lastProcessedSeqno = processedFiles.get(thlFile.getName());
            if (lastProcessedSeqno != null && lastProcessedSeqno == -1) {
                if (!thlFile.getName().equals(latestFileName)) {
                    continue;
                }
                processedFiles.remove(thlFile.getName());
            }

            processThlFile(thlFile, thlFile.getName().equals(latestFileName));
        }
    }

    private void processThlFile(File thlFile, boolean isLatestFile) {
        String fileName = thlFile.getName();
        Long lastProcessedSeqno = processedFiles.get(fileName);

        if (lastProcessedSeqno != null && lastProcessedSeqno == -1) {
            return;
        }

        logger.info("处理订阅THL文件: {}", fileName);
        int eventCount = 0;
        long fileLastSeqno = 0;

        try (THLFileReader reader = createThlReader(thlFile.getAbsolutePath())) {
            THLEvent event;
            while ((event = reader.readEvent()) != null) {
                if (!running.get()) break;

                // Skip events already processed in this file based on file-level progress
                Long fileProgress = processedFiles.get(fileName);
                if (fileProgress != null && event.getSeqno() <= fileProgress && fileProgress != -1) {
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
                }

                fileLastSeqno = event.getSeqno();
                reportRto(event);

                if (eventCount % 100 == 0) {
                    logger.info("已订阅 {} 个事件, 最后seqno: {}", eventCount, fileLastSeqno);
                }
            }
        } catch (Exception e) {
            logger.error("处理THL文件异常: {}", fileName, e);
            throw new RuntimeException("Fatal error processing THL file: " + fileName, e);
        }

        if (eventCount > 0) {
            logger.info("THL文件处理完成: {} -> {} 个事件已发送到Kafka, 最后seqno: {}", fileName, eventCount, fileLastSeqno);
        }

        if (isLatestFile) {
            processedFiles.put(fileName, fileLastSeqno);
        } else {
            processedFiles.put(fileName, -1L);
        }
        saveProgress();
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

        // Extract column names for structured data mapping
        String[] columnNames = null;
        Object columnNamesObj = metadata.get("column_names");
        if (columnNamesObj != null) {
            String columnsStr = columnNamesObj.toString();
            columnNames = columnsStr.split(",");
        }

        // Extract row data from metadata
        Object rowData = metadata.get("row_data");
        if (rowData != null) {
            String rowStr = rowData.toString();
            if (columnNames != null) {
                cdcEvent.afterData = mapRowToColumns(rowStr, columnNames);
            } else {
                cdcEvent.afterData = rowStr;
            }
        }

        Object beforeRowData = metadata.get("before_row_data");
        if (beforeRowData != null) {
            String beforeRowStr = beforeRowData.toString();
            if (columnNames != null) {
                cdcEvent.beforeData = mapRowToColumns(beforeRowStr, columnNames);
            } else {
                cdcEvent.beforeData = beforeRowStr;
            }
        }

        // Also try rows_data (plural form) if row_data is null
        if (cdcEvent.afterData == null) {
            Object rowsData = metadata.get("rows_data");
            if (rowsData != null) {
                String rowsStr = rowsData.toString();
                // rows_data may be a List, take first element
                if (rowsStr.startsWith("[")) {
                    rowsStr = rowsStr.substring(1, rowsStr.length() - 1).trim();
                }
                if (columnNames != null) {
                    cdcEvent.afterData = mapRowToColumns(rowsStr, columnNames);
                } else {
                    cdcEvent.afterData = rowsStr;
                }
            }
        }

        if (cdcEvent.beforeData == null) {
            Object rowsDataBefore = metadata.get("rows_data_before");
            if (rowsDataBefore != null) {
                String beforeRowsStr = rowsDataBefore.toString();
                if (beforeRowsStr.startsWith("[")) {
                    beforeRowsStr = beforeRowsStr.substring(1, beforeRowsStr.length() - 1).trim();
                }
                if (columnNames != null) {
                    cdcEvent.beforeData = mapRowToColumns(beforeRowsStr, columnNames);
                } else {
                    cdcEvent.beforeData = beforeRowsStr;
                }
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

        cdcEvent.key = extractKey(cdcEvent);
        cdcEvent.rawData = cdcEvent.afterData != null ? cdcEvent.afterData : "";

        return cdcEvent;
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

        cdcEvent.key = extractKey(cdcEvent);
    }

    private String extractDatabaseFromMetadata(THLEvent event) {
        Object db = event.getMetadata("database");
        if (db != null) return db.toString();
        Object shard = event.getMetadata("shardId");
        if (shard != null) return shard.toString();
        return "unknown";
    }

    private String extractKey(CdcEvent cdcEvent) {
        if (cdcEvent.afterData != null && !cdcEvent.afterData.isEmpty()) {
            String[] cols = cdcEvent.afterData.split(",");
            if (cols.length > 0) {
                return cdcEvent.database + "." + cdcEvent.table + "." + cols[0].replaceAll("[^a-zA-Z0-9_]", "");
            }
        }
        return cdcEvent.database + "." + cdcEvent.table + "." + cdcEvent.seqno;
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

    /**
     * Maps SQL value list (e.g., 'value1',123,'value2') to column names,
     * producing a JSON string like {"col1":"value1","col2":123,"col3":"value2"}
     */
    private String mapRowToColumns(String rowData, String[] columnNames) {
        if (rowData == null || columnNames == null || columnNames.length == 0) return rowData;
        List<String> values = parseSqlValueList(rowData);
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

    private void saveCheckpoint(THLEvent event) {
        String checkpointPath = "./files/" + taskId + "/checkpoint/subscribe_checkpoint";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(checkpointPath))) {
            writer.write(String.valueOf(event.getSeqno()));
        } catch (IOException e) {
            logger.warn("保存订阅checkpoint失败: {}", e.getMessage());
        }
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

    private void loadProgress() {
        File file = new File(progressFile);
        if (!file.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    try {
                        processedFiles.put(parts[0], Long.parseLong(parts[1].trim()));
                    } catch (NumberFormatException e) {
                        logger.warn("无效的进度条目: {}", line);
                    }
                }
            }
        } catch (IOException e) {
            logger.warn("加载订阅进度失败: {}", e.getMessage());
        }
    }

    private void saveProgress() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(progressFile))) {
            for (Map.Entry<String, Long> entry : processedFiles.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
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
