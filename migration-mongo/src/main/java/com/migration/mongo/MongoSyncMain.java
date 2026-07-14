package com.migration.mongo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
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

/**
 * MongoDB 副本集 → 副本集 数据同步（独立子进程，由 agent 拉起）。
 *
 * <p>与 SQL 侧的 capture/extract/increment 三进程管线不同，Mongo 同步是单进程两阶段：
 * <ol>
 *   <li><b>全量</b>：逐集合复制索引 + 数据（bulkWrite ReplaceOne upsert，幂等可重跑）；</li>
 *   <li><b>增量</b>（migration.mode=fullAndIncre）：Change Streams 订阅（这正是要求副本集的原因——
 *       change stream 仅在副本集/分片集群可用），resume token 持久化到 checkpoint 文件，
 *       进程重启后 resumeAfter 断点续传。</li>
 * </ol>
 *
 * <p>全量与增量的衔接：全量开始前先打开 change stream 记录起始 resume token，全量完成后从该
 * token 重放期间的写入——重放事件用 upsert/delete 应用，与全量数据天然幂等收敛，不丢不重。
 *
 * <p>进度通过 files/{taskId}/mongo_progress.json（原子写）暴露给 agent 的 MongoSyncTask 轮询上报。
 */
public final class MongoSyncMain {

    private static final Logger logger = LoggerFactory.getLogger(MongoSyncMain.class);
    private static final Gson gson = new Gson();

    private static final int FULL_BATCH_SIZE = 1000;
    private static final int TOKEN_FLUSH_EVERY_EVENTS = 100;
    private static final long TOKEN_FLUSH_INTERVAL_MS = 5000;

    private final String taskId;
    private final Properties props;
    /** 同步对象：db -> 集合清单；空清单 = 整库（dbLevel） */
    private final Map<String, List<String>> syncObjects;
    private final Path progressPath;
    private final Path tokenPath;

    // 进度状态
    private volatile String phase = "FULL";
    private volatile int totalCollections;
    private volatile int completedCollections;
    private volatile String currentCollection = "";
    private volatile long copiedDocs;
    private volatile long incrEvents;
    private volatile String error;

    private MongoSyncMain(String taskId, Properties props) {
        this.taskId = taskId;
        this.props = props;
        this.syncObjects = parseSyncObjects(props.getProperty("migration.sync.objects", ""));
        this.progressPath = Paths.get("files", taskId, "mongo_progress.json");
        this.tokenPath = Paths.get("files", taskId, "checkpoint", "mongo_resume_token.json");
    }

    public static void main(String[] args) throws Exception {
        String taskId = System.getProperty("task.id", "");
        for (int i = 0; i < args.length - 1; i++) {
            if ("--task-id".equals(args[i])) {
                taskId = args[i + 1];
            }
        }
        if (taskId.isEmpty()) {
            System.err.println("task.id is required (-Dtask.id or --task-id)");
            System.exit(1);
        }

        Properties props = new Properties();
        File configFile = new File("files/" + taskId + "/config.properties");
        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }

        MongoSyncMain sync = new MongoSyncMain(taskId, props);
        try {
            sync.run();
        } catch (Exception e) {
            logger.error("Mongo 同步失败", e);
            sync.error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            sync.phase = "FAILED";
            sync.writeProgress();
            System.exit(1);
        }
    }

    private void run() throws Exception {
        boolean fullAndIncre = "fullAndIncre".equals(props.getProperty("migration.mode", "full"));
        logger.info("Mongo 同步启动: taskId={}, mode={}, syncObjects={}",
                taskId, fullAndIncre ? "fullAndIncre" : "full", syncObjects.keySet());

        try (MongoClient source = MongoClients.create(clientSettings("source"));
             MongoClient target = MongoClients.create(clientSettings("target"))) {

            // 增量起点：优先用 checkpoint 里的 token（断点续传）；否则在全量前取当前流位点，
            // 保证全量期间源库的写入会在增量阶段被重放（upsert 幂等，不丢不重）。
            BsonDocument resumeToken = fullAndIncre ? loadResumeToken() : null;
            boolean resumedFromCheckpoint = resumeToken != null;
            if (fullAndIncre && resumeToken == null) {
                resumeToken = currentStreamToken(source);
                logger.info("已记录增量起始位点（全量开始前）");
            }

            // 断点续传（进程重启）时跳过全量：数据已在目标，增量从 token 续传即可
            if (!resumedFromCheckpoint) {
                runFullCopy(source, target);
            } else {
                logger.info("检测到增量 checkpoint，跳过全量，直接从断点续传增量");
            }

            if (!fullAndIncre) {
                phase = "DONE";
                writeProgress();
                logger.info("仅全量任务完成");
                return;
            }

            phase = "INCREMENT";
            writeProgress();
            runChangeStream(source, target, resumeToken);
        }
    }

    // ==================== 全量 ====================

    private void runFullCopy(MongoClient source, MongoClient target) {
        Map<String, List<String>> resolved = resolveCollections(source);
        totalCollections = resolved.values().stream().mapToInt(List::size).sum();
        writeProgress();
        logger.info("全量开始: {} 个集合", totalCollections);

        for (Map.Entry<String, List<String>> e : resolved.entrySet()) {
            String dbName = e.getKey();
            MongoDatabase srcDb = source.getDatabase(dbName);
            MongoDatabase tgtDb = target.getDatabase(dbName);
            for (String collName : e.getValue()) {
                currentCollection = dbName + "." + collName;
                writeProgress();
                copyCollection(srcDb.getCollection(collName), tgtDb.getCollection(collName));
                completedCollections++;
                writeProgress();
                logger.info("集合 {} 全量完成 ({}/{})", currentCollection, completedCollections, totalCollections);
            }
        }
        logger.info("全量完成: {} 个集合, 共 {} 文档", totalCollections, copiedDocs);
    }

    private void copyCollection(MongoCollection<Document> src, MongoCollection<Document> tgt) {
        // 索引先行（跳过默认 _id_），保证数据落入后即有约束/查询性能
        for (Document idx : src.listIndexes()) {
            String name = idx.getString("name");
            if ("_id_".equals(name)) {
                continue;
            }
            try {
                Document keys = (Document) idx.get("key");
                com.mongodb.client.model.IndexOptions opts = new com.mongodb.client.model.IndexOptions().name(name);
                if (Boolean.TRUE.equals(idx.getBoolean("unique", false))) {
                    opts.unique(true);
                }
                if (Boolean.TRUE.equals(idx.getBoolean("sparse", false))) {
                    opts.sparse(true);
                }
                tgt.createIndex(keys, opts);
            } catch (Exception ex) {
                logger.warn("复制索引 {} 失败（继续数据复制）: {}", name, ex.getMessage());
            }
        }

        List<WriteModel<Document>> batch = new ArrayList<>(FULL_BATCH_SIZE);
        ReplaceOptions upsert = new ReplaceOptions().upsert(true);
        try (MongoCursor<Document> cursor = src.find().batchSize(FULL_BATCH_SIZE).iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                batch.add(new ReplaceOneModel<>(new Document("_id", doc.get("_id")), doc, upsert));
                if (batch.size() >= FULL_BATCH_SIZE) {
                    tgt.bulkWrite(batch, new com.mongodb.client.model.BulkWriteOptions().ordered(false));
                    copiedDocs += batch.size();
                    batch.clear();
                    writeProgress();
                }
            }
            if (!batch.isEmpty()) {
                tgt.bulkWrite(batch, new com.mongodb.client.model.BulkWriteOptions().ordered(false));
                copiedDocs += batch.size();
            }
        }
    }

    // ==================== 增量（Change Streams） ====================

    /** 打开一次 deployment 级 change stream，取当前位点 token（无事件也能拿 postBatchResumeToken）。 */
    private BsonDocument currentStreamToken(MongoClient source) {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                     source.watch().maxAwaitTime(500, TimeUnit.MILLISECONDS).cursor()) {
            cursor.tryNext();
            return cursor.getResumeToken();
        }
    }

    private void runChangeStream(MongoClient source, MongoClient target, BsonDocument resumeToken) throws Exception {
        logger.info("增量启动（Change Streams），resumeToken={}", resumeToken != null ? "已有" : "无（从当前）");
        ChangeStreamIterable<Document> stream = source.watch()
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .maxAwaitTime(1, TimeUnit.SECONDS);
        if (resumeToken != null) {
            stream = stream.resumeAfter(resumeToken);
        }

        long lastFlush = System.currentTimeMillis();
        int sinceFlush = 0;
        ReplaceOptions upsert = new ReplaceOptions().upsert(true);

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = stream.cursor()) {
            while (true) {
                ChangeStreamDocument<Document> event = cursor.tryNext();
                long now = System.currentTimeMillis();

                if (event != null) {
                    applyEvent(target, event, upsert);
                    incrEvents++;
                    sinceFlush++;
                }

                // resume token 周期性持久化（含空闲时的 postBatchResumeToken，推进断点避免重放过多）
                if (sinceFlush >= TOKEN_FLUSH_EVERY_EVENTS || now - lastFlush >= TOKEN_FLUSH_INTERVAL_MS) {
                    BsonDocument token = cursor.getResumeToken();
                    if (token != null) {
                        saveResumeToken(token);
                    }
                    writeProgress();
                    lastFlush = now;
                    sinceFlush = 0;
                }
            }
        }
    }

    private void applyEvent(MongoClient target, ChangeStreamDocument<Document> event, ReplaceOptions upsert) {
        if (event.getNamespace() == null) {
            return; // dropDatabase / invalidate 等无 ns 事件
        }
        String db = event.getNamespace().getDatabaseName();
        String coll = event.getNamespace().getCollectionName();
        if (!isSelected(db, coll)) {
            return;
        }

        MongoCollection<Document> tgt = target.getDatabase(db).getCollection(coll);
        OperationType op = event.getOperationType();
        try {
            switch (op) {
                case INSERT:
                case REPLACE:
                case UPDATE: {
                    // 统一用 fullDocument upsert：幂等、天然处理乱序/重放；
                    // UPDATE 且 fullDocument 为 null = 文档在 lookup 前已被删除，交给后续 DELETE 事件
                    Document full = event.getFullDocument();
                    if (full != null) {
                        tgt.replaceOne(new Document("_id", full.get("_id")), full, upsert);
                    }
                    break;
                }
                case DELETE: {
                    BsonDocument key = event.getDocumentKey();
                    if (key != null) {
                        tgt.deleteOne(key);
                    }
                    break;
                }
                case DROP:
                    tgt.drop();
                    logger.info("集合 {}.{} 已随源库 drop", db, coll);
                    break;
                default:
                    logger.debug("跳过不处理的事件类型: {} on {}.{}", op, db, coll);
            }
        } catch (Exception e) {
            // 单事件失败记日志继续（upsert/delete 幂等，绝大多数为暂时性错误，
            // 下轮 resume 重放可自愈；不因单事件卡死整个流）
            logger.error("应用增量事件失败: {} {}.{}: {}", op, db, coll, e.getMessage());
        }
    }

    // ==================== 同步对象 ====================

    private static Map<String, List<String>> parseSyncObjects(String json) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        if (json == null || json.isEmpty()) {
            return result;
        }
        try {
            Map<String, Object> raw = gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
            for (Map.Entry<String, Object> e : raw.entrySet()) {
                Object v = e.getValue();
                if (v instanceof Map) {
                    Map<?, ?> m = (Map<?, ?>) v;
                    if (Boolean.TRUE.equals(m.get("dbLevel"))) {
                        result.put(e.getKey(), new ArrayList<>()); // 空 = 整库
                    } else if (m.get("tables") instanceof List) {
                        List<String> colls = new ArrayList<>();
                        for (Object t : (List<?>) m.get("tables")) {
                            colls.add(String.valueOf(t));
                        }
                        result.put(e.getKey(), colls);
                    }
                } else if (v instanceof List) {
                    List<String> colls = new ArrayList<>();
                    for (Object t : (List<?>) v) {
                        colls.add(String.valueOf(t));
                    }
                    result.put(e.getKey(), colls);
                }
            }
        } catch (Exception ex) {
            logger.warn("解析 sync objects 失败: {}", ex.getMessage());
        }
        return result;
    }

    /** 全量阶段：整库（空清单）时枚举源库当前全部集合。 */
    private Map<String, List<String>> resolveCollections(MongoClient source) {
        Map<String, List<String>> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : syncObjects.entrySet()) {
            if (e.getValue().isEmpty()) {
                List<String> colls = new ArrayList<>();
                for (String c : source.getDatabase(e.getKey()).listCollectionNames()) {
                    if (!c.startsWith("system.")) {
                        colls.add(c);
                    }
                }
                resolved.put(e.getKey(), colls);
            } else {
                resolved.put(e.getKey(), e.getValue());
            }
        }
        return resolved;
    }

    /** 增量事件过滤：整库模式接受该库全部集合（含全量后新建的），集合级只接受清单内。 */
    private boolean isSelected(String db, String coll) {
        List<String> colls = syncObjects.get(db);
        if (colls == null) {
            return false;
        }
        if (coll.startsWith("system.")) {
            return false;
        }
        return colls.isEmpty() || colls.contains(coll);
    }

    // ==================== 连接 ====================

    private MongoClientSettings clientSettings(String prefix) {
        String host = props.getProperty(prefix + ".db.host", "localhost");
        String port = props.getProperty(prefix + ".db.port", "27017");
        String user = props.getProperty(prefix + ".db.username", "");
        // config.properties 落盘的口令是 ENC: 加密的，读出时解密（旧明文原样返回）
        String password = com.migration.common.crypto.CredentialCipher.decrypt(
                props.getProperty(prefix + ".db.password", ""));

        StringBuilder uri = new StringBuilder("mongodb://");
        if (!user.isEmpty()) {
            uri.append(urlEncode(user));
            if (!password.isEmpty()) {
                uri.append(':').append(urlEncode(password));
            }
            uri.append('@');
        }
        // directConnection：只连指定节点，不按副本集配置里的内部主机名（容器 hostname 等）
        // 重路由；Change Streams 与写入在直连 Primary 下均正常工作
        uri.append(host).append(':').append(port).append("/?authSource=admin&directConnection=true");
        return MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri.toString()))
                .build();
    }

    private static String urlEncode(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    // ==================== checkpoint / 进度 ====================

    private BsonDocument loadResumeToken() {
        try {
            File f = tokenPath.toFile();
            if (!f.exists()) {
                return null;
            }
            String json = new String(Files.readAllBytes(tokenPath), StandardCharsets.UTF_8);
            if (json.trim().isEmpty()) {
                return null;
            }
            return BsonDocument.parse(json);
        } catch (Exception e) {
            logger.warn("读取 resume token 失败，将从当前位点开始: {}", e.getMessage());
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
            logger.warn("持久化 resume token 失败: {}", e.getMessage());
        }
    }

    private void writeProgress() {
        try {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("phase", phase);
            p.put("totalCollections", totalCollections);
            p.put("completedCollections", completedCollections);
            p.put("currentCollection", currentCollection);
            p.put("copiedDocs", copiedDocs);
            p.put("incrEvents", incrEvents);
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
}
