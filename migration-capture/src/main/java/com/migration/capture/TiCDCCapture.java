package com.migration.capture;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.migration.common.AbstractCapture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TiDB 增量捕获：消费 TiCDC changefeed 投递到 Kafka 的 canal-json 变更流，落成与
 * {@link MySQLBinlogCapture} 同格式的 {@code binlog_*.cap} 记录，交给下游 extract 转 THL。
 *
 * <p>TiDB 不对外提供 MySQL binlog dump 协议，官方增量出口是 TiCDC。因此本类不像
 * MySQLBinlogCapture 那样直连源库拉日志，而是：
 * <ol>
 *   <li>通过 TiCDC OpenAPI v2 确保任务专属 changefeed 存在（sink = Kafka，协议 canal-json，
 *       单分区保序，start_ts 取全量开始前记录的 TSO checkpoint）；</li>
 *   <li>用独立 consumer group 消费该 topic，把每条变更写成一行 {@code .cap} 记录。</li>
 * </ol>
 *
 * <p>记录格式与 MySQL 侧保持一致（{@code \001} 分隔），只是各字段含义按 TiDB 语义填充：
 * <pre>
 *   eventType \001 file \001 commitTs \001 timestampMs \001 serverId \001 kafkaOffset \001 canal-json
 * </pre>
 * {@code file} 恒为 checkpoint 里的文件名（TiDB 的 {@code SHOW MASTER STATUS} 返回
 * {@code tidb-binlog}），{@code commitTs} 是 TSO——两者合起来让 extract 端沿用
 * “文件名相同则比位点”的断点续传判定，且 TSO 单调递增天然可比。
 *
 * <p>位移提交采用“先落盘再提交”（at-least-once）：重启后最多重复少量已写记录，
 * 由增量应用侧的幂等语义（INSERT ... ON DUPLICATE KEY UPDATE）吸收。
 */
public class TiCDCCapture extends AbstractCapture<byte[]> {

    private static final char FIELD_SEP = '\001';
    private static final char RECORD_SEP = '\n';

    /** TSO 高 46 位是物理时间（毫秒），低 18 位是逻辑计数。 */
    private static final int TSO_PHYSICAL_SHIFT = 18;

    private static final long POSITION_SAVE_INTERVAL_MS = 5000;
    private static final long RPO_REPORT_INTERVAL_MS = 3000;
    private static final long HEARTBEAT_WRITE_INTERVAL_MS = 5000;

    private final Gson gson = new Gson();

    private String taskId;
    private String outputDir;
    private long maxEventsPerFile = 10000;
    private long serverId;

    private String cdcApiUrl;
    private String changefeedId;
    private String topic;
    private String sinkBootstrap;
    private String consumeBootstrap;
    private long startTs;
    private boolean manageChangefeed;
    private boolean forceReplicate;
    private String positionFileName;

    private Set<String> syncedDatabases = new LinkedHashSet<>();
    private Set<String> syncedTables = new LinkedHashSet<>();

    private KafkaConsumer<String, String> consumer;
    private Thread consumeThread;

    private BufferedWriter writer;
    private final AtomicLong eventCounter = new AtomicLong(0);
    private final AtomicLong fileCounter = new AtomicLong(0);
    private long currentFileEvents = 0;

    private volatile long currentCommitTs = 0;
    private volatile long lastRpoMs = -1;
    private volatile long lastSourceEventTs = -1;
    private long lastPositionSaveTime = 0;
    private long lastRpoReportTime = 0;
    private long lastHeartbeatWriteTime = 0;

    private String backpressureSignalPath;
    private volatile boolean backpressurePaused = false;

    @Override
    protected void doInitialize() throws Exception {
        taskId = props.getProperty("task.id", "unknown");
        outputDir = props.getProperty("capture.output.dir", "binlog_output");
        maxEventsPerFile = Long.parseLong(props.getProperty("capture.max.events.per.file", "10000"));
        serverId = Long.parseLong(props.getProperty("capture.server.id", "65535"));

        cdcApiUrl = trimTrailingSlash(props.getProperty("capture.ticdc.api.url", "http://127.0.0.1:18300"));
        changefeedId = props.getProperty("capture.ticdc.changefeed.id", defaultChangefeedId(taskId));
        topic = props.getProperty("capture.ticdc.topic", defaultTopic(taskId));
        sinkBootstrap = props.getProperty("capture.ticdc.kafka.sink.bootstrap", "synctask-kafka:9092");
        consumeBootstrap = props.getProperty("capture.ticdc.kafka.bootstrap", "localhost:29092");
        manageChangefeed = Boolean.parseBoolean(props.getProperty("capture.ticdc.manage.changefeed", "true"));
        forceReplicate = Boolean.parseBoolean(props.getProperty("capture.ticdc.force.replicate", "false"));

        // changefeed 起点 TSO：全量开始前 agent 用 SHOW MASTER STATUS 记录的位点（TiDB 返回 TSO）
        startTs = Long.parseLong(props.getProperty("capture.ticdc.start.ts",
                props.getProperty("capture.binlog.position", "0")));
        positionFileName = props.getProperty("capture.binlog.file", "tidb-binlog");
        if (positionFileName == null || positionFileName.isEmpty()) {
            positionFileName = "tidb-binlog";
        }
        // 已消费到的位点优先于 checkpoint 起点：进程重启后不必从全量起点重放整段
        long resumedTs = loadResumeTs();
        if (resumedTs > startTs) {
            logger.info("从上次捕获位点恢复: commitTs={}（checkpoint 起点 {}）", resumedTs, startTs);
            startTs = resumedTs;
        }

        backpressureSignalPath = "files/" + taskId + "/backpressure.signal";

        parseSyncObjects();

        logger.info("TiCDC Capture 初始化完成 - cdcApi={} changefeed={} topic={} sinkBootstrap={} "
                        + "consumeBootstrap={} startTs={} outputDir={} syncedDatabases={} syncedTables={}",
                cdcApiUrl, changefeedId, topic, sinkBootstrap, consumeBootstrap, startTs, outputDir,
                syncedDatabases, syncedTables);
    }

    /** changefeed id 只允许字母数字与中划线，任务 id 里的其它字符统一折成中划线。 */
    static String defaultChangefeedId(String taskId) {
        String sanitized = taskId == null ? "" : taskId.replaceAll("[^a-zA-Z0-9]+", "-");
        sanitized = sanitized.replaceAll("^-+", "").replaceAll("-+$", "");
        if (sanitized.isEmpty()) {
            sanitized = "unknown";
        }
        return "sync-" + sanitized;
    }

    /** Kafka topic 名不接受中划线以外的特殊字符以及过长的名字，统一按 changefeed id 派生。 */
    static String defaultTopic(String taskId) {
        return "ticdc-" + defaultChangefeedId(taskId).substring("sync-".length());
    }

    private static String trimTrailingSlash(String url) {
        if (url == null) return "";
        String s = url.trim();
        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    /**
     * 解析同步对象，构建库/表过滤集合。与 MySQLBinlogCapture 的口径一致：
     * 优先 {@code migration.included.tables}（表级），否则 {@code migration.included.databases}（库级）。
     * 这里的集合同时用于 TiCDC changefeed 的 filter rules，从源头就不投递无关表的变更。
     */
    private void parseSyncObjects() {
        String tables = props.getProperty("migration.included.tables", "");
        if (tables != null && !tables.isEmpty()) {
            for (String t : tables.split(",")) {
                String trimmed = t.trim();
                if (trimmed.isEmpty()) continue;
                syncedTables.add(trimmed);
                int dot = trimmed.indexOf('.');
                if (dot > 0) {
                    syncedDatabases.add(trimmed.substring(0, dot));
                }
            }
        }
        String databases = props.getProperty("migration.included.databases", "");
        if (databases != null && !databases.isEmpty()) {
            for (String db : databases.split(",")) {
                String trimmed = db.trim();
                if (!trimmed.isEmpty()) {
                    syncedDatabases.add(trimmed);
                }
            }
        }
        // 库级同步：整库放行（新表也要同步），不能按表清单收敛
        String dbLevelDatabases = props.getProperty("sync.db.level.databases", "");
        if (Boolean.parseBoolean(props.getProperty("sync.db.level", "false"))
                && dbLevelDatabases != null && !dbLevelDatabases.isEmpty()) {
            for (String db : dbLevelDatabases.split(",")) {
                String trimmed = db.trim();
                if (!trimmed.isEmpty()) {
                    syncedDatabases.add(trimmed);
                    syncedTables.removeIf(t -> t.startsWith(trimmed + "."));
                }
            }
        }
    }

    /** TiCDC filter rules：表级下发到表，库级下发 {@code db.*}，都没有则全放行。 */
    java.util.List<String> buildFilterRules() {
        java.util.List<String> rules = new java.util.ArrayList<>();
        Set<String> dbCovered = new HashSet<>();
        String dbLevelDatabases = props == null ? "" : props.getProperty("sync.db.level.databases", "");
        if (dbLevelDatabases != null && !dbLevelDatabases.isEmpty()) {
            for (String db : dbLevelDatabases.split(",")) {
                String trimmed = db.trim();
                if (!trimmed.isEmpty()) {
                    rules.add(trimmed + ".*");
                    dbCovered.add(trimmed);
                }
            }
        }
        for (String t : syncedTables) {
            int dot = t.indexOf('.');
            if (dot > 0 && dbCovered.contains(t.substring(0, dot))) continue;
            rules.add(t);
        }
        if (rules.isEmpty()) {
            for (String db : syncedDatabases) {
                rules.add(db + ".*");
            }
        }
        if (rules.isEmpty()) {
            rules.add("*.*");
        }
        return rules;
    }

    @Override
    protected void doStart() throws Exception {
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }
        openNewOutputFile();

        if (manageChangefeed) {
            ensureChangefeed();
        } else {
            logger.info("capture.ticdc.manage.changefeed=false，跳过 changefeed 自管理，直接消费 topic={}", topic);
        }

        consumer = new KafkaConsumer<>(buildConsumerProps());
        consumer.subscribe(Collections.singletonList(topic));

        consumeThread = new Thread(this::consumeLoop, "ticdc-consumer-" + taskId);
        consumeThread.setDaemon(true);
        consumeThread.start();
        logger.info("TiCDC 变更流消费已启动: topic={}, group={}", topic, consumerGroupId());
    }

    private String consumerGroupId() {
        return "ticdc-capture-" + defaultChangefeedId(taskId);
    }

    private Properties buildConsumerProps() {
        Properties cp = new Properties();
        cp.put("bootstrap.servers", consumeBootstrap);
        cp.put("group.id", consumerGroupId());
        cp.put("key.deserializer", StringDeserializer.class.getName());
        cp.put("value.deserializer", StringDeserializer.class.getName());
        // 位移只在记录落盘后手工提交，保证崩溃时重复而非丢失
        cp.put("enable.auto.commit", "false");
        cp.put("auto.offset.reset", "earliest");
        cp.put("max.poll.records", props.getProperty("capture.ticdc.max.poll.records", "500"));
        return cp;
    }

    private void consumeLoop() {
        try {
            while (running) {
                checkBackpressureSignal();
                if (backpressurePaused) {
                    Thread.sleep(500);
                    continue;
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    maybeSavePosition();
                    continue;
                }
                boolean batchComplete = true;
                for (ConsumerRecord<String, String> record : records) {
                    if (!running) {
                        batchComplete = false;
                        break;
                    }
                    handleRecord(record);
                }
                writer.flush();
                // 只有整批都落盘了才提交位移：停机时中断的半批不提交，下次启动重新消费
                // （重复由增量应用的幂等语义吸收，丢失则无从补偿）
                if (batchComplete) {
                    consumer.commitSync();
                }
                maybeSavePosition();
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            logger.info("TiCDC 消费线程收到唤醒信号，准备退出");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("TiCDC 变更流消费异常: {}", e.getMessage(), e);
        }
    }

    private void handleRecord(ConsumerRecord<String, String> record) throws IOException {
        String value = record.value();
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        JsonObject msg;
        try {
            msg = gson.fromJson(value, JsonObject.class);
        } catch (Exception e) {
            logger.warn("跳过无法解析的 canal-json 消息 (offset={}): {}", record.offset(), e.getMessage());
            return;
        }
        if (msg == null) {
            return;
        }

        String type = optString(msg, "type", "");
        boolean isDdl = msg.has("isDdl") && !msg.get("isDdl").isJsonNull() && msg.get("isDdl").getAsBoolean();
        long commitTs = readTidbTs(msg);
        long sourceTsMs = commitTs > 0 ? (commitTs >> TSO_PHYSICAL_SHIFT) : optLong(msg, "es", 0);

        if ("TIDB_WATERMARK".equalsIgnoreCase(type)) {
            // watermark 表示“该时刻之前的变更已全部投递”，用于低流量时推进位点并刷新 RPO
            if (commitTs > currentCommitTs) {
                currentCommitTs = commitTs;
            }
            if (sourceTsMs > 0) {
                lastSourceEventTs = sourceTsMs;
                lastRpoMs = System.currentTimeMillis() - sourceTsMs;
                writeRpoMetric(lastRpoMs);
            }
            // TiCDC 每秒都会推进 resolved ts，逐条落盘会让 .cap 里心跳记录淹没真实变更；
            // 与 MySQL 侧一致，按固定间隔写一条即可（心跳只用于 RPO/延迟观测）
            long now = System.currentTimeMillis();
            if (now - lastHeartbeatWriteTime >= HEARTBEAT_WRITE_INTERVAL_MS) {
                lastHeartbeatWriteTime = now;
                writeRecord("SYNC_HEARTBEAT", commitTs, sourceTsMs, record.offset(), "");
            }
            return;
        }

        String database = optString(msg, "database", "");
        String table = optString(msg, "table", "");
        if (!isDdl && !shouldCapture(database, table)) {
            return;
        }

        if (commitTs > currentCommitTs) {
            currentCommitTs = commitTs;
        }
        if (sourceTsMs > 0) {
            lastSourceEventTs = sourceTsMs;
            long now = System.currentTimeMillis();
            long rpo = now - sourceTsMs;
            if (rpo >= 0 && (eventCounter.get() % 100 == 0 || now - lastRpoReportTime > RPO_REPORT_INTERVAL_MS)) {
                lastRpoMs = rpo;
                lastRpoReportTime = now;
                writeRpoMetric(rpo);
            }
        }

        String eventType;
        if (isDdl) {
            eventType = "TICDC_DDL";
        } else if ("INSERT".equalsIgnoreCase(type)) {
            eventType = "TICDC_INSERT";
        } else if ("UPDATE".equalsIgnoreCase(type)) {
            eventType = "TICDC_UPDATE";
        } else if ("DELETE".equalsIgnoreCase(type)) {
            eventType = "TICDC_DELETE";
        } else {
            logger.debug("忽略非数据类 canal-json 消息: type={}", type);
            return;
        }

        writeRecord(eventType, commitTs, sourceTsMs, record.offset(), value);
    }

    /** 表过滤：表级清单优先，其次库级；都没配则全量捕获。 */
    private boolean shouldCapture(String database, String table) {
        if (syncedDatabases.isEmpty() && syncedTables.isEmpty()) {
            return true;
        }
        if (!syncedTables.isEmpty()) {
            if (syncedTables.contains(database + "." + table)) {
                return true;
            }
            // 库级同步的库（表清单里没有该库的条目）整库放行
            return syncedDatabases.contains(database) && !hasTableRuleFor(database);
        }
        return syncedDatabases.contains(database);
    }

    private boolean hasTableRuleFor(String database) {
        String prefix = database + ".";
        for (String t : syncedTables) {
            if (t.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private synchronized void writeRecord(String eventType, long commitTs, long timestampMs,
                                          long offset, String payload) throws IOException {
        String sanitized = payload == null ? ""
                : payload.replace("\r", " ").replace("\n", " ").replace(String.valueOf(FIELD_SEP), " ");

        StringBuilder sb = new StringBuilder();
        sb.append(eventType).append(FIELD_SEP);
        sb.append(positionFileName).append(FIELD_SEP);
        sb.append(commitTs).append(FIELD_SEP);
        sb.append(timestampMs).append(FIELD_SEP);
        sb.append(serverId).append(FIELD_SEP);
        // Kafka 位移做事件唯一标识后缀：同一事务多行共享 commitTs，只靠 commitTs 会撞 eventId
        sb.append(offset).append(FIELD_SEP);
        sb.append(sanitized);
        sb.append(RECORD_SEP);

        writer.write(sb.toString());
        writer.flush();

        long count = eventCounter.incrementAndGet();
        currentFileEvents++;
        if (currentFileEvents >= maxEventsPerFile) {
            rotateOutputFile();
        }
        if (count % 1000 == 0) {
            logger.info("已捕获 {} 条 TiCDC 变更, 当前位点: {}:{}", count, positionFileName, currentCommitTs);
            savePosition();
            lastPositionSaveTime = System.currentTimeMillis();
        }
    }

    private void maybeSavePosition() {
        long now = System.currentTimeMillis();
        if (now - lastPositionSaveTime >= POSITION_SAVE_INTERVAL_MS) {
            savePosition();
            lastPositionSaveTime = now;
        }
    }

    private synchronized void openNewOutputFile() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());
        // 文件名沿用 binlog_ 前缀：extract 侧按该前缀 + .cap 后缀扫描输入目录
        String fileName = String.format("binlog_%s_%04d.cap", timestamp, fileCounter.get());
        File outputFile = new File(outputDir, fileName);
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));
        currentFileEvents = 0;
        logger.info("打开新的捕获输出文件: {}", outputFile.getAbsolutePath());
    }

    private synchronized void rotateOutputFile() throws IOException {
        fileCounter.incrementAndGet();
        openNewOutputFile();
    }

    private void checkBackpressureSignal() {
        if (backpressureSignalPath == null) return;
        File signalFile = new File(backpressureSignalPath);
        if (!signalFile.exists()) {
            backpressurePaused = false;
            return;
        }
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(signalFile))) {
            String firstLine = reader.readLine();
            boolean shouldPause = firstLine != null && "PAUSE".equalsIgnoreCase(firstLine.trim());
            if (shouldPause != backpressurePaused) {
                backpressurePaused = shouldPause;
                logger.info(shouldPause ? "收到背压暂停信号，暂停 TiCDC 变更消费" : "收到背压恢复信号，恢复 TiCDC 变更消费");
            }
        } catch (IOException e) {
            logger.debug("读取背压信号文件失败: {}", e.getMessage());
        }
    }

    private void savePosition() {
        if (currentCommitTs <= 0) return;
        File positionFile = new File(outputDir, "capture_position.properties");
        Properties posProps = new Properties();
        posProps.setProperty("binlog.file", positionFileName);
        posProps.setProperty("binlog.position", String.valueOf(currentCommitTs));
        posProps.setProperty("ticdc.commit.ts", String.valueOf(currentCommitTs));
        posProps.setProperty("last.update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        try (FileOutputStream fos = new FileOutputStream(positionFile)) {
            posProps.store(fos, "TiCDC capture position for task: " + taskId);
        } catch (IOException e) {
            logger.warn("保存捕获位点失败: {}", e.getMessage());
        }
    }

    private long loadResumeTs() {
        File positionFile = new File(outputDir, "capture_position.properties");
        if (!positionFile.exists()) return 0;
        Properties posProps = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream(positionFile)) {
            posProps.load(fis);
            return Long.parseLong(posProps.getProperty("ticdc.commit.ts", "0"));
        } catch (Exception e) {
            logger.warn("读取历史捕获位点失败，按 checkpoint 起点开始: {}", e.getMessage());
            return 0;
        }
    }

    private void writeRpoMetric(long rpoMs) {
        File dir = new File(outputDir == null ? "binlog_output" : outputDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File metricFile = new File(dir, "rpo_metric");
        try (java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.FileWriter(metricFile, false))) {
            pw.println(System.currentTimeMillis() + "|" + rpoMs + "|" + lastSourceEventTs);
        } catch (IOException e) {
            logger.warn("写入RPO指标失败: {}", e.getMessage());
        }
    }

    // ==================== TiCDC OpenAPI ====================

    /**
     * 确保 changefeed 存在且处于运行态：不存在则按 checkpoint TSO 创建，
     * 存在但被停止/失败则恢复。任何一步失败都直接抛出——changefeed 不通等于增量没有数据源。
     */
    private void ensureChangefeed() throws Exception {
        JsonObject existing = getChangefeed();
        if (existing == null) {
            createChangefeed();
            return;
        }
        String state = optString(existing, "state", "");
        logger.info("changefeed {} 已存在, state={}", changefeedId, state);
        if ("stopped".equalsIgnoreCase(state) || "failed".equalsIgnoreCase(state) || "error".equalsIgnoreCase(state)) {
            logger.info("changefeed {} 处于 {} 态，尝试恢复", changefeedId, state);
            httpRequest("POST", cdcApiUrl + "/api/v2/changefeeds/" + changefeedId + "/resume", "{}");
        }
    }

    private JsonObject getChangefeed() {
        try {
            String body = httpRequest("GET", cdcApiUrl + "/api/v2/changefeeds/" + changefeedId, null);
            return gson.fromJson(body, JsonObject.class);
        } catch (Exception e) {
            logger.debug("查询 changefeed {} 失败（视为不存在）: {}", changefeedId, e.getMessage());
            return null;
        }
    }

    private void createChangefeed() throws Exception {
        JsonObject filter = new JsonObject();
        com.google.gson.JsonArray rules = new com.google.gson.JsonArray();
        for (String rule : buildFilterRules()) {
            rules.add(rule);
        }
        filter.add("rules", rules);

        JsonObject replicaConfig = new JsonObject();
        replicaConfig.add("filter", filter);
        replicaConfig.addProperty("force_replicate", forceReplicate);

        JsonObject body = new JsonObject();
        body.addProperty("changefeed_id", changefeedId);
        body.addProperty("sink_uri", buildSinkUri());
        if (startTs > 0) {
            body.addProperty("start_ts", startTs);
        }
        body.addProperty("force_replicate", forceReplicate);
        body.add("replica_config", replicaConfig);

        String payload = gson.toJson(body);
        logger.info("创建 TiCDC changefeed: {}", payload.replaceAll("password=[^&\"]*", "password=***"));
        httpRequest("POST", cdcApiUrl + "/api/v2/changefeeds", payload);
        logger.info("changefeed {} 创建成功, startTs={}", changefeedId, startTs);
    }

    String buildSinkUri() {
        // partition-num=1：canal-json 按分区保序，多分区会打乱同表事件的先后关系
        // enable-tidb-extension=true：消息里带 _tidb.commitTs（TSO 位点）与周期性 watermark
        return "kafka://" + sinkBootstrap + "/" + topic
                + "?protocol=canal-json&partition-num=1&replication-factor=1"
                + "&max-message-bytes=10485760&enable-tidb-extension=true";
    }

    private String httpRequest(String method, String urlStr, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        try {
            conn.setRequestMethod(method);
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(30000);
            conn.setRequestProperty("Accept", "application/json");
            if (body != null) {
                conn.setDoOutput(true);
                conn.setRequestProperty("Content-Type", "application/json");
                byte[] payload = body.getBytes(StandardCharsets.UTF_8);
                conn.getOutputStream().write(payload);
                conn.getOutputStream().flush();
            }
            int code = conn.getResponseCode();
            String response = readStream(code >= 400 ? conn.getErrorStream() : conn.getInputStream());
            if (code >= 400) {
                throw new IOException("TiCDC API " + method + " " + urlStr + " 返回 " + code + ": " + response);
            }
            return response;
        } finally {
            conn.disconnect();
        }
    }

    private String readStream(java.io.InputStream in) throws IOException {
        if (in == null) return "";
        try (java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) > 0) {
                bos.write(buf, 0, n);
            }
            return bos.toString(StandardCharsets.UTF_8);
        }
    }

    // ==================== JSON 小工具 ====================

    private static String optString(JsonObject obj, String key, String def) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return def;
        try {
            return obj.get(key).getAsString();
        } catch (Exception e) {
            return def;
        }
    }

    private static long optLong(JsonObject obj, String key, long def) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return def;
        try {
            return obj.get(key).getAsLong();
        } catch (Exception e) {
            return def;
        }
    }

    /** 取 canal-json 的 TiDB 扩展位点：数据/DDL 消息是 commitTs，watermark 消息是 watermarkTs。 */
    private static long readTidbTs(JsonObject msg) {
        if (msg == null || !msg.has("_tidb") || msg.get("_tidb").isJsonNull()) return 0;
        try {
            JsonObject ext = msg.getAsJsonObject("_tidb");
            long commitTs = optLong(ext, "commitTs", 0);
            return commitTs > 0 ? commitTs : optLong(ext, "watermarkTs", 0);
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (consumer != null) {
            consumer.wakeup();
        }
        if (consumeThread != null) {
            consumeThread.join(5000);
        }
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                logger.debug("关闭 Kafka consumer 异常: {}", e.getMessage());
            }
        }
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        savePosition();
        logger.info("TiCDC 捕获已停止, 总事件数: {}, 最后位点: {}, 最后RPO: {}ms",
                eventCounter.get(), currentCommitTs, lastRpoMs);
    }

    public long getEventCount() {
        return eventCounter.get();
    }

    public long getCurrentCommitTs() {
        return currentCommitTs;
    }
}
