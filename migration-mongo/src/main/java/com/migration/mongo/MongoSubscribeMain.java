package com.migration.mongo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.changestream.OperationType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MongoDB 副本集/分片集群 → Kafka 数据订阅（独立子进程，由 agent 的 MongoSubscribeTask 拉起）。
 *
 * <p>SQL 侧的订阅链路是 capture → extract → subscribe 三进程（binlog/WAL/redo → THL → Kafka）；
 * MongoDB 没有可落成 THL 的物理日志，变更出口就是 Change Streams，因此订阅是**单进程**：
 * change stream 事件直接转成与 SQL 侧完全一致的消息格式投递 Kafka。
 *
 * <p>与全量/增量同步一样，订阅**不做全量**——它只投递任务创建之后发生的变更（首次启动时
 * 从当前流位点开始），这与 MySQL 订阅从当前 binlog 位点开始的语义一致。
 *
 * <p>不丢数据的关键约定（与 ContinuousSubscribeMain 相同的教训）：resume token **必须**在
 * {@code producer.flush()} 确认全部消息落到 Kafka 之后才推进。异步 send 之后立刻落位点的话，
 * producer 缓冲区里那批还没发出去的消息在 SIGKILL 时全丢，而位点已经推进——源库有、Kafka 永远没有。
 */
public final class MongoSubscribeMain {

    private static final Logger logger = LoggerFactory.getLogger(MongoSubscribeMain.class);
    /**
     * serializeNulls：显式 null 字段必须原样出现在消息里。
     *
     * <p>Gson 默认丢弃 null 值，于是源端 {@code {"t_null": null}} 到了下游变成"这个字段不存在"——
     * "字段为 null" 和 "字段不存在" 在文档模型里是两件事（后者意味着从未设置过），
     * 下游据此做 upsert 会漏掉"把某字段置空"这个语义。
     */
    private static final Gson gson = new com.google.gson.GsonBuilder().serializeNulls().create();

    /** 攒够这么多事件就 flush + 落一次位点，限制崩溃后的重投量与未确认消息窗口。 */
    private static final int CHECKPOINT_EVERY_EVENTS = 500;
    private static final long CHECKPOINT_INTERVAL_MS = 5000;
    /** BSON 扩展 JSON 输出用 relaxed 模式：数字/日期落成普通 JSON 标量，下游好消费。 */
    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder()
            .outputMode(org.bson.json.JsonMode.RELAXED).build();

    private final String taskId;
    private final Properties props;
    private final Map<String, List<String>> syncObjects;
    private final Path progressPath;
    private final Path tokenPath;

    private final String kafkaBootstrapServers;
    private final String topicPrefix;
    private final String topicStrategy;
    private final String subscribeFormat;

    private KafkaProducer<String, String> producer;

    private final AtomicLong totalEventsSent = new AtomicLong();
    private final AtomicLong sendErrors = new AtomicLong();
    private volatile String phase = "SUBSCRIBE";
    private volatile String error;
    private volatile long lastRtoMs = -1;
    private long lastRtoReportTime = 0;
    private static final long RTO_REPORT_INTERVAL_MS = 3000;
    private long lastLivenessWriteMs = 0;

    MongoSubscribeMain(String taskId, Properties props, Map<String, List<String>> syncObjects) {
        this.taskId = taskId;
        this.props = props;
        this.syncObjects = syncObjects;
        this.progressPath = Paths.get("files", taskId, "mongo_progress.json");
        this.tokenPath = Paths.get("files", taskId, "checkpoint", "mongo_subscribe_token.json");
        this.kafkaBootstrapServers = props.getProperty("subscribe.kafka.bootstrap.servers", "localhost:9092");
        this.topicPrefix = props.getProperty("subscribe.kafka.topic.prefix", "cdc");
        this.topicStrategy = props.getProperty("subscribe.kafka.topic.strategy", "TABLE");
        this.subscribeFormat = props.getProperty("subscribe.format", "DEBEZIUM_JSON");
    }

    void run() throws Exception {
        logger.info("Mongo 订阅启动: taskId={}, kafka={}, topicPrefix={}, strategy={}, format={}, syncObjects={}",
                taskId, kafkaBootstrapServers, topicPrefix, topicStrategy, subscribeFormat, syncObjects.keySet());

        initProducer();
        try (MongoClient source = MongoClients.create(clientSettings())) {
            discoverDatabasesIfEmpty(source);
            BsonDocument resumeToken = loadResumeToken();
            runChangeStream(source, resumeToken);
        } finally {
            closeProducer();
        }
    }

    private void initProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=all + 幂等：位点一旦推进就再也不会重读这段变更流，"写进 Kafka"必须是真的落到全部 ISR
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        this.producer = new KafkaProducer<>(p);
        logger.info("Kafka 生产者初始化完成: {} (acks=all, 幂等)", kafkaBootstrapServers);
    }

    /** 订阅任务未指定同步对象（_all）时，订阅源实例上全部非系统库。 */
    private void discoverDatabasesIfEmpty(MongoClient source) {
        if (!syncObjects.isEmpty()) {
            return;
        }
        for (String db : source.listDatabaseNames()) {
            if (!MongoSyncMain.SYSTEM_DATABASES.contains(db)) {
                syncObjects.put(db, new ArrayList<>());
            }
        }
        logger.info("订阅任务未指定同步对象，自动订阅全部非系统库: {}", syncObjects.keySet());
    }

    private void runChangeStream(MongoClient source, BsonDocument resumeToken) throws Exception {
        ChangeStreamIterable<Document> stream = source.watch()
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                // 前像是"尽力而为"：只有开启了 changeStreamPreAndPostImages 的集合（MongoDB 6.0+）
                // 才有；未开启时 before 为 null，不影响 after / 主键 key，也不会报错。
                .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
                .maxAwaitTime(1, TimeUnit.SECONDS);
        if (resumeToken != null) {
            stream = stream.resumeAfter(resumeToken);
            logger.info("从 checkpoint 续订：resumeAfter 已有位点");
        } else {
            logger.info("无 checkpoint，从当前流位点开始订阅（订阅任务不投递存量数据）");
        }

        long lastCheckpoint = System.currentTimeMillis();
        int sinceCheckpoint = 0;
        // 已 send 但还没被 flush 确认的那批事件对应的位点：确认前绝不落盘
        BsonDocument pendingToken = null;

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = stream.cursor()) {
            if (resumeToken == null) {
                // 首次启动必须**立刻**把起始位点落盘：否则进程在首次周期性落点（5s / 500 条）之前
                // 崩溃，重启后又是"没有 checkpoint"，只能再从**当时的**最新位点起步，
                // 这两次起步之间源库的写入就永久漏掉了（订阅没有全量兜底可以补）。
                BsonDocument initial = cursor.getResumeToken();
                if (initial != null) {
                    saveResumeToken(initial);
                    pendingToken = initial;
                    logger.info("首次启动的起始位点已立即落盘，崩溃重启从该位点续订而非跳到最新");
                }
            }
            while (true) {
                touchLiveness();
                ChangeStreamDocument<Document> event = cursor.tryNext();
                long now = System.currentTimeMillis();

                if (event != null) {
                    if (sendIfSelected(event)) {
                        sinceCheckpoint++;
                    }
                    reportRto(event, now);
                }

                BsonDocument cursorToken = cursor.getResumeToken();
                if (cursorToken != null) {
                    pendingToken = cursorToken;
                }

                if (pendingToken != null
                        && (sinceCheckpoint >= CHECKPOINT_EVERY_EVENTS
                            || now - lastCheckpoint >= CHECKPOINT_INTERVAL_MS)) {
                    if (commitProgress(pendingToken)) {
                        sinceCheckpoint = 0;
                        lastCheckpoint = now;
                    } else {
                        // 有消息没能确认落盘：位点不推进，下次 resume 会从旧位点重投这一段
                        // （下游按 _id 幂等吸收）。这里不退出，让 producer 的重试继续跑。
                        logger.error("订阅位点未推进（存在未确认的 Kafka 消息），将从上个位点重投");
                        lastCheckpoint = now;
                    }
                }
            }
        }
    }

    /** 命中同步对象则转换并投递；返回是否真的投递了。 */
    private boolean sendIfSelected(ChangeStreamDocument<Document> event) {
        if (event.getNamespace() == null) {
            return false; // dropDatabase / invalidate 等无 ns 事件
        }
        String db = event.getNamespace().getDatabaseName();
        String coll = event.getNamespace().getCollectionName();
        if (!isSelected(db, coll)) {
            return false;
        }
        OperationType op = event.getOperationType();
        String debeziumOp;
        switch (op) {
            case INSERT:  debeziumOp = "c"; break;
            case UPDATE:
            case REPLACE: debeziumOp = "u"; break;
            case DELETE:  debeziumOp = "d"; break;
            default:
                logger.debug("跳过非 DML 事件: {} on {}.{}", op, db, coll);
                return false;
        }

        String after = event.getFullDocument() != null ? event.getFullDocument().toJson(RELAXED) : null;
        String before = event.getFullDocumentBeforeChange() != null
                ? event.getFullDocumentBeforeChange().toJson(RELAXED) : null;
        // DELETE 没有 fullDocument，documentKey（_id）就是下游识别被删文档的唯一依据
        if (before == null && "d".equals(debeziumOp) && event.getDocumentKey() != null) {
            before = event.getDocumentKey().toJson(RELAXED);
        }

        String key = documentKeyValue(event);
        long seqno = seqnoOf(event);
        long sourceTs = event.getClusterTime() != null ? event.getClusterTime().getTime() * 1000L
                : System.currentTimeMillis();

        String topic = resolveTopic(db, coll);
        String messageKey = db + "." + coll + "." + key;
        String docKey = event.getDocumentKey() != null ? event.getDocumentKey().toJson(RELAXED) : null;
        com.google.gson.JsonElement updateDesc = updateDescriptionJson(event);
        String value = "SIMPLE_JSON".equals(subscribeFormat)
                ? buildSimpleJson(debeziumOp, db, coll, before, after, sourceTs, seqno, docKey, updateDesc)
                : buildDebeziumJson(debeziumOp, db, coll, before, after, sourceTs, seqno, docKey, updateDesc);

        try {
            producer.send(new ProducerRecord<>(topic, messageKey, value), (meta, ex) -> {
                if (ex != null) {
                    sendErrors.incrementAndGet();
                    logger.error("发送事件到 Kafka 失败, topic {}: {}", topic, ex.getMessage());
                }
            });
            totalEventsSent.incrementAndGet();
            return true;
        } catch (Exception e) {
            sendErrors.incrementAndGet();
            logger.error("发送事件到 Kafka 异常: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 推进订阅位点：**先** flush 等所有已发消息拿到 broker 确认，全部成功才落盘位点。
     *
     * @return true 表示位点已安全推进
     */
    private boolean commitProgress(BsonDocument token) {
        long errsBefore = sendErrors.get();
        try {
            producer.flush();
        } catch (Exception e) {
            logger.error("flush Kafka 失败，订阅位点不推进: {}", e.getMessage());
            return false;
        }
        if (sendErrors.get() != errsBefore) {
            logger.error("flush 后发现 {} 条消息发送失败，订阅位点不推进", sendErrors.get() - errsBefore);
            return false;
        }
        saveResumeToken(token);
        writeProgress();
        return true;
    }

    /**
     * Kafka 消息 key：同一文档的所有变更必须落到同一个 key，否则多分区下同一文档的
     * insert/update/delete 会散到不同分区，下游消费顺序错乱、最终状态就是错的。
     * documentKey（即 _id）在 insert/update/replace/delete 上都有，是唯一可靠的选择。
     */
    private String documentKeyValue(ChangeStreamDocument<Document> event) {
        BsonDocument dk = event.getDocumentKey();
        if (dk == null || !dk.containsKey("_id")) {
            return String.valueOf(seqnoOf(event));
        }
        org.bson.BsonValue id = dk.get("_id");
        if (id.isObjectId()) {
            return id.asObjectId().getValue().toHexString();
        }
        if (id.isString()) {
            return id.asString().getValue();
        }
        if (id.isNumber()) {
            return String.valueOf(id.asNumber().longValue());
        }
        return id.toString();
    }

    /**
     * 事件序号：用 oplog 的 clusterTime（BsonTimestamp = 秒 + 同秒内递增序）拼成一个 long。
     * 这是 MongoDB 里天然单调递增且跨重启稳定的量，正好对应 SQL 侧订阅消息里的 seqno 语义。
     */
    private long seqnoOf(ChangeStreamDocument<Document> event) {
        BsonTimestamp ts = event.getClusterTime();
        if (ts == null) {
            return 0L;
        }
        return ((long) ts.getTime() << 32) | (ts.getInc() & 0xFFFFFFFFL);
    }

    private String resolveTopic(String db, String coll) {
        switch (topicStrategy.toUpperCase()) {
            case "TASK":   return topicPrefix + "." + taskId;
            case "GLOBAL": return topicPrefix + ".events";
            case "TABLE":
            default:       return topicPrefix + "." + taskId + "." + db + "." + coll;
        }
    }

    /**
     * UPDATE 的**精确增量**：change stream 自带的 updatedFields / removedFields。
     *
     * <p>这不是锦上添花，而是订阅链路"不丢"的唯一保证。{@code fullDocument: UPDATE_LOOKUP} 拿到的
     * after 是**事件读出时再去查一次**的结果，不是这次更新当时的后像：同一文档被连续快速更新两次时，
     * 第一条事件查到的往往已经是第二次更新后的值 —— 中间那次更新的取值在整条流里再也找不到。
     * 实测 2 分钟高频写入丢了 22 条 UPDATE 的中间值（最终状态照样收敛，光比最终状态发现不了）。
     *
     * <p>updatedFields 则是"这次更新到底改了哪些字段成什么值"的原始记录，与查询时机无关，
     * 因此每一次更新在流里都有精确、完整的表达（Debezium 的 MongoDB 连接器同样暴露此字段）。
     * after 仍然保留，作为"变更后整篇文档"的便利视图（尽力而为）。
     */
    private com.google.gson.JsonElement updateDescriptionJson(ChangeStreamDocument<Document> event) {
        if (event.getUpdateDescription() == null) {
            return null;
        }
        com.google.gson.JsonObject ud = new com.google.gson.JsonObject();
        BsonDocument updated = event.getUpdateDescription().getUpdatedFields();
        ud.add("updatedFields", updated != null ? parseJson(updated.toJson(RELAXED))
                : com.google.gson.JsonNull.INSTANCE);
        com.google.gson.JsonArray removed = new com.google.gson.JsonArray();
        if (event.getUpdateDescription().getRemovedFields() != null) {
            for (String f : event.getUpdateDescription().getRemovedFields()) {
                removed.add(f);
            }
        }
        ud.add("removedFields", removed);
        return ud;
    }

    private String buildDebeziumJson(String op, String db, String coll, String before, String after,
                                     long sourceTs, long seqno, String docKey,
                                     com.google.gson.JsonElement updateDesc) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("before", parseJson(before));
        payload.put("after", parseJson(after));
        // documentKey 恒存在（增删改都有），是事件唯一稳定的归属标识：
        // UPDATE 的 fullDocument 查询可能因文档已被删除而为 null，那时只剩它能说明改的是哪一篇。
        if (docKey != null) {
            payload.put("documentKey", parseJson(docKey));
        }
        if (updateDesc != null) {
            payload.put("updateDescription", updateDesc);
        }

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("version", "1.0.0");
        source.put("connector", "mongodb");
        source.put("db", db);
        source.put("table", coll);
        source.put("ts_ms", sourceTs);
        source.put("seqno", seqno);
        payload.put("source", source);

        payload.put("op", op);
        payload.put("ts_ms", System.currentTimeMillis());

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("payload", payload);
        return gson.toJson(envelope);
    }

    private String buildSimpleJson(String op, String db, String coll, String before, String after,
                                   long sourceTs, long seqno, String docKey,
                                   com.google.gson.JsonElement updateDesc) {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("op", op);
        message.put("db", db);
        message.put("table", coll);
        message.put("before", parseJson(before));
        message.put("after", parseJson(after));
        if (docKey != null) {
            message.put("documentKey", parseJson(docKey));
        }
        if (updateDesc != null) {
            message.put("updateDescription", updateDesc);
        }
        message.put("sourceTs", sourceTs);
        message.put("seqno", seqno);
        return gson.toJson(message);
    }

    /**
     * BSON 扩展 JSON 文本 → Gson 语法树（**不是** Map）。
     *
     * <p>必须走 JsonParser 而不是 {@code fromJson(json, Map.class)}：后者把所有数字都读成
     * {@code Double}，Int64 的 9223372036854775807 会变成 9.223372036854776E18 —— 精度当场丢掉，
     * 大整数主键、金额、雪花 ID 到了下游就是错的。JsonParser 产出的数字是 LazilyParsedNumber，
     * 保留原始字面量，再序列化出去与源端逐字符一致。
     */
    private com.google.gson.JsonElement parseJson(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return com.google.gson.JsonParser.parseString(json);
        } catch (Exception e) {
            logger.debug("解析文档 JSON 失败: {}", e.getMessage());
            return null;
        }
    }

    /** 与同步链路同一套选择语义：整库（空清单）接受该库全部集合，集合级只接受清单内。 */
    private boolean isSelected(String db, String coll) {
        List<String> colls = syncObjects.get(db);
        if (colls == null || coll.startsWith("system.")) {
            return false;
        }
        if (com.migration.common.bidi.BidiConstants.MARKER_TABLE.equals(coll)) {
            return false; // 防回环标记是链路自己的簿记数据，不是业务变更
        }
        return colls.isEmpty() || colls.contains(coll);
    }

    // ==================== checkpoint / 进度 / 活性 ====================

    private BsonDocument loadResumeToken() {
        try {
            if (!tokenPath.toFile().exists()) {
                return null;
            }
            String json = new String(Files.readAllBytes(tokenPath), StandardCharsets.UTF_8);
            return json.trim().isEmpty() ? null : BsonDocument.parse(json);
        } catch (Exception e) {
            logger.warn("读取订阅 resume token 失败，将从当前位点开始: {}", e.getMessage());
            return null;
        }
    }

    private void saveResumeToken(BsonDocument token) {
        try {
            Files.createDirectories(tokenPath.getParent());
            Path tmp = tokenPath.resolveSibling(tokenPath.getFileName() + ".tmp");
            Files.write(tmp, token.toJson().getBytes(StandardCharsets.UTF_8));
            Files.move(tmp, tokenPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            logger.warn("持久化订阅 resume token 失败: {}", e.getMessage());
        }
    }

    void writeProgress() {
        try {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("phase", phase);
            p.put("eventsSent", totalEventsSent.get());
            p.put("sendErrors", sendErrors.get());
            if (error != null) {
                p.put("error", error);
            }
            p.put("updatedAt", System.currentTimeMillis());
            Files.createDirectories(progressPath.getParent());
            Path tmp = progressPath.resolveSibling(progressPath.getFileName() + ".tmp");
            Files.write(tmp, gson.toJson(p).getBytes(StandardCharsets.UTF_8));
            Files.move(tmp, progressPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            logger.debug("写进度文件失败: {}", e.getMessage());
        }
    }

    void failProgress(String message) {
        this.error = message;
        this.phase = "FAILED";
        writeProgress();
    }

    /**
     * 活性心跳：主循环每一轮都刷（限流 2s）。
     *
     * <p>不能拿 {@code subscribe_rto_ms} 或进度里的事件数当活性信号——空闲时段本就不更新，
     * 拿它判僵死会误杀；而进程被 SIGSTOP 冻结时进程仍 alive，只有"活着就一定在刷"的文件
     * 才能把假活识别出来。
     */
    private void touchLiveness() {
        long now = System.currentTimeMillis();
        if (now - lastLivenessWriteMs < 2000) {
            return;
        }
        lastLivenessWriteMs = now;
        File dir = new File("files/" + taskId + "/binlog_output");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        try (BufferedWriter w = new BufferedWriter(new FileWriter(new File(dir, "subscribe_liveness"), false))) {
            w.write(String.valueOf(now));
        } catch (IOException e) {
            logger.debug("写 subscribe_liveness 失败: {}", e.getMessage());
        }
    }

    private void reportRto(ChangeStreamDocument<Document> event, long now) {
        if (event.getClusterTime() == null) {
            return;
        }
        long rtoMs = now - event.getClusterTime().getTime() * 1000L;
        if (rtoMs >= 0 && now - lastRtoReportTime > RTO_REPORT_INTERVAL_MS) {
            lastRtoMs = rtoMs;
            lastRtoReportTime = now;
            String metricPath = "files/" + taskId + "/metrics/subscribe_rto_ms";
            try {
                File metricDir = new File(metricPath).getParentFile();
                if (!metricDir.exists()) {
                    metricDir.mkdirs();
                }
                try (BufferedWriter w = new BufferedWriter(new FileWriter(metricPath))) {
                    w.write(String.valueOf(rtoMs));
                }
            } catch (IOException e) {
                logger.debug("写入 RTO 指标失败: {}", e.getMessage());
            }
        }
    }

    private void closeProducer() {
        if (producer == null) {
            return;
        }
        try {
            producer.flush();
            producer.close(java.time.Duration.ofSeconds(10));
        } catch (Exception e) {
            logger.warn("关闭 Kafka 生产者异常: {}", e.getMessage());
        }
    }

    private com.mongodb.MongoClientSettings clientSettings() {
        return MongoSyncMain.buildClientSettings(props, "source");
    }

    long getTotalEventsSent() {
        return totalEventsSent.get();
    }

    long getLastRtoMs() {
        return lastRtoMs;
    }
}
