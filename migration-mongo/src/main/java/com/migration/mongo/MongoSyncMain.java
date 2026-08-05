package com.migration.mongo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.migration.common.bidi.BidiConstants;
import com.migration.common.bidi.BidiLoopGuard;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
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

    /** 系统库：灾备/订阅自动发现同步对象时一律排除。 */
    static final List<String> SYSTEM_DATABASES = java.util.Arrays.asList("admin", "local", "config");

    private final String taskId;
    private final Properties props;
    /** 同步对象：db -> 集合清单；空清单 = 整库（dbLevel） */
    private final Map<String, List<String>> syncObjects;
    /** 列处理（列过滤/列名映射/附加列）；无配置时各集合短路走原始文档 */
    private final MongoDocumentProcessor processor;
    /** 聚合路由（汇聚多集合到一个 / 按字段拆分到多个集合）；未配置时 isActive()=false，走原 1:1 路径 */
    private final com.migration.common.route.DocumentRouter router;
    private final Path progressPath;
    private final Path tokenPath;
    /** 全量 bulkWrite 装载配置（与 SQL/ES/Redis 各链路共用同一组 migration.full.bulk.* 键）。 */
    private final com.migration.common.bulk.BulkLoadOptions bulkOptions;
    /** 全量一致性快照档位（migration.full.snapshot.mode）。 */
    private final String snapshotMode;
    /** 灾备任务（含双向的反向影子通道）：同步对象可留空由本进程在源实例上自动发现。 */
    private final boolean drTask;
    /**
     * 仅增量：跳过全量、从当前流位点起步。两种场景必须置位，否则会把数据反向灌回去：
     * <ul>
     *   <li>双向灾备的反向影子通道（DR_SHADOW）——反向全量会把灾备库整个搬回主库；</li>
     *   <li>主备倒换后的重启——新源就是原目标，全量等于把备库重新灌回原主库。</li>
     * </ul>
     */
    private final boolean incrementOnly;
    /** 双向灾备防回环：应用写入带 origin 标记事务，捕获侧见标记即跳过整个事务。 */
    private final boolean bidiEnabled;
    /** 本节点标识（写进标记文档，仅供观测/排障；跳过判定不依赖其取值）。 */
    private final String nodeId;

    // 进度状态
    private volatile String phase = "FULL";
    private volatile int totalCollections;
    private volatile int completedCollections;
    private volatile String currentCollection = "";
    private volatile long copiedDocs;
    private volatile long incrEvents;
    private volatile long skippedLoopEvents;
    private volatile String error;

    private MongoSyncMain(String taskId, Properties props) {
        this.taskId = taskId;
        this.props = props;
        this.syncObjects = parseSyncObjects(props.getProperty("migration.sync.objects", ""));
        this.processor = MongoDocumentProcessor.fromProperties(props);
        this.router = com.migration.common.route.DocumentRouter.fromProperties(props);
        this.progressPath = Paths.get("files", taskId, "mongo_progress.json");
        this.tokenPath = Paths.get("files", taskId, "checkpoint", "mongo_resume_token.json");
        this.bulkOptions = com.migration.common.bulk.BulkLoadOptions.from(props);
        this.snapshotMode = props.getProperty("migration.full.snapshot.mode", "GTID_ONLY").trim().toUpperCase();
        this.drTask = "DR".equals(props.getProperty("task.type", ""));
        this.incrementOnly = Boolean.parseBoolean(props.getProperty("migration.increment.only", "false"));
        this.bidiEnabled = com.migration.common.bidi.BidiConstants.isEnabled(props);
        this.nodeId = com.migration.common.bidi.BidiConstants.nodeId(props);
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

        // 单实例互斥 + 父进程看门狗：同一 taskId 只允许一个 mongo 引擎进程
        com.migration.common.proc.ChildProcessBootstrap.init(taskId, "mongo");

        Properties props = new Properties();
        File configFile = new File("files/" + taskId + "/config.properties");
        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }

        // 数据订阅（mongodb → Kafka）与同步（mongodb → mongodb）共用本 jar 的入口：
        // 二者都是"单进程 + Change Streams"，只是出口不同，分派放在这里比再拆一个模块/jar 划算。
        if ("SUBSCRIBE".equals(props.getProperty("task.type", ""))) {
            MongoSubscribeMain subscribe = new MongoSubscribeMain(taskId, props,
                    parseSyncObjects(props.getProperty("migration.sync.objects", "")));
            try {
                subscribe.run();
            } catch (Exception e) {
                logger.error("Mongo 订阅失败", e);
                subscribe.failProgress(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                System.exit(1);
            }
            return;
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
        // 仅增量（灾备反向通道 / 倒换后重启）必然含增量阶段，即便 migration.mode 缺省也不能当纯全量跑
        boolean fullAndIncre = incrementOnly
                || "fullAndIncre".equals(props.getProperty("migration.mode", "full"));
        logger.info("Mongo 同步启动: taskId={}, mode={}, syncObjects={}, dr={}, incrementOnly={}, bidi={}",
                taskId, fullAndIncre ? "fullAndIncre" : "full", syncObjects.keySet(),
                drTask, incrementOnly, bidiEnabled);
        if (!processor.isEmpty()) {
            logger.info("列处理已启用（列过滤/列名映射/附加列按集合生效）");
        }

        try (MongoClient source = MongoClients.create(clientSettings("source"));
             MongoClient target = MongoClients.create(clientSettings("target"))) {

            // 灾备任务允许不指定同步对象：在源实例上发现全部非系统库（整库同步）
            discoverDrDatabases(source);

            // resume token 只在"同一个源部署"上有意义：主备倒换后源已换成原目标实例，
            // 旧 token 里的时间戳/UUID 在新源的 oplog 上要么直接报错要么落到错误位置。
            // 因此 checkpoint 连同源部署标识一起落盘，不匹配即丢弃、从当前位点起步。
            String sourceId = deploymentId(source, "source");
            Checkpoint checkpoint = fullAndIncre ? loadCheckpoint(sourceId) : null;
            BsonDocument resumeToken = checkpoint != null ? checkpoint.token : null;
            boolean resumedFromCheckpoint = resumeToken != null;
            if (fullAndIncre && resumeToken == null) {
                resumeToken = currentStreamToken(source);
                logger.info("已记录增量起始位点（{}）", incrementOnly ? "仅增量，从当前位点起步" : "全量开始前");
                if (incrementOnly) {
                    // 仅增量模式必须**立刻**把起始位点落盘。否则进程在首次周期性 flush（5s）之前
                    // 崩溃，重启后又是"没有 checkpoint 的仅增量"，只能再从**当时的**最新位点起步，
                    // 这两次起步之间源库的写入就永久漏掉了——而仅增量模式没有全量兜底可以补回来。
                    // 实测：主备倒换窗口内杀掉引擎，新方向少了 628 行。
                    //
                    // 只在仅增量模式这么做：普通模式下"token 存在"意味着"全量已完成"，
                    // 提前落盘会让全量中途崩溃的进程重启后直接跳过全量，那才是真丢数据。
                    saveCheckpoint(resumeToken, null, sourceId);
                    logger.info("仅增量模式：起始位点已立即落盘，崩溃重启从该位点续传而非跳到最新");
                }
            }

            // 断点续传（进程重启）时跳过全量：数据已在目标，增量从 token 续传即可
            if (resumedFromCheckpoint) {
                logger.info("检测到增量 checkpoint，跳过全量，直接从断点续传增量");
            } else if (incrementOnly) {
                logger.info("仅增量模式：跳过全量（反向灾备通道/主备倒换后重启），从当前流位点开始捕获");
            } else {
                runFullCopy(source, target);
            }

            if (!fullAndIncre) {
                phase = "DONE";
                writeProgress();
                logger.info("仅全量任务完成");
                return;
            }

            phase = "INCREMENT";
            writeProgress();
            runChangeStream(source, target, resumeToken,
                    checkpoint != null ? checkpoint.markedTxnKey : null, sourceId);
        }
    }

    /**
     * 灾备任务未指定同步对象时，在源实例上发现全部非系统库并按整库同步登记。
     *
     * <p>与 SQL 灾备链路（agent ConfigService.discoverDrSyncObjects）同样的语义，但 Mongo 走
     * mongodb-driver 而非 JDBC，放在引擎侧做可以让 agent 不必额外引入 mongo 驱动。
     */
    private void discoverDrDatabases(MongoClient source) {
        if (!syncObjects.isEmpty() || !drTask) {
            return;
        }
        for (String db : source.listDatabaseNames()) {
            if (SYSTEM_DATABASES.contains(db)) {
                continue;
            }
            syncObjects.put(db, new ArrayList<>()); // 空清单 = 整库
        }
        logger.info("灾备任务自动发现同步对象（整库）: {}", syncObjects.keySet());
    }

    /** 源部署标识：主机端口 + 副本集名。倒换/换源后与 checkpoint 里记录的不一致，据此作废旧 token。 */
    private String deploymentId(MongoClient client, String prefix) {
        String setName = "";
        try {
            Document hello = client.getDatabase("admin").runCommand(new Document("hello", 1));
            Object sn = hello.get("setName");
            if (sn != null) {
                setName = String.valueOf(sn);
            }
        } catch (Exception e) {
            logger.warn("获取源部署信息失败（不影响同步，仅用于 checkpoint 归属判定）: {}", e.getMessage());
        }
        return props.getProperty(prefix + ".db.host", "") + ":"
                + props.getProperty(prefix + ".db.port", "") + "/" + setName;
    }

    // ==================== 全量 ====================

    private void runFullCopy(MongoClient source, MongoClient target) {
        Map<String, List<String>> resolved = resolveCollections(source);
        totalCollections = resolved.values().stream().mapToInt(List::size).sum();
        writeProgress();
        logger.info("全量开始: {} 个集合", totalCollections);

        // 一致性快照会话：CONSISTENT 且 minSnapshotHistoryWindowInSeconds 足够大时开出来，
        // 所有集合的读都走它 —— 各集合因此看到<b>同一个 clusterTime</b> 的库，而不是各自时刻的库。
        try (com.mongodb.client.ClientSession snapshotSession = beginFullSnapshot(source)) {
            for (Map.Entry<String, List<String>> e : resolved.entrySet()) {
                String dbName = e.getKey();
                MongoDatabase srcDb = source.getDatabase(dbName);
                MongoDatabase tgtDb = target.getDatabase(dbName);
                for (String collName : e.getValue()) {
                    currentCollection = dbName + "." + collName;
                    writeProgress();
                    if (router.matches(dbName, collName)) {
                        copyRoutedCollection(target, dbName, collName,
                                srcDb.getCollection(collName), snapshotSession);
                        completedCollections++;
                        writeProgress();
                        logger.info("集合 {} 全量完成（路由）({}/{})",
                                currentCollection, completedCollections, totalCollections);
                        continue;
                    }
                    copyCollection(dbName, collName, srcDb.getCollection(collName),
                            tgtDb.getCollection(collName), snapshotSession);
                    completedCollections++;
                    writeProgress();
                    logger.info("集合 {} 全量完成 ({}/{})", currentCollection, completedCollections, totalCollections);
                }
            }
        } catch (com.mongodb.MongoCommandException e) {
            if (e.getErrorCode() == SNAPSHOT_TOO_OLD || e.getErrorCode() == SNAPSHOT_UNAVAILABLE) {
                throw new RuntimeException("全量快照在搬运过程中失效（历史窗口不够长）。"
                        + "请调大 minSnapshotHistoryWindowInSeconds 后重跑，或把快照档位改为只记位点。原因: "
                        + e.getMessage(), e);
            }
            throw e;
        }
        logger.info("全量完成: {} 个集合, 共 {} 文档", totalCollections, copiedDocs);
    }

    /** MongoDB 快照相关错误码：历史版本已被回收 / 快照当前不可用。 */
    private static final int SNAPSHOT_TOO_OLD = 239;
    private static final int SNAPSHOT_UNAVAILABLE = 246;
    /** 真快照要求的最小历史窗口：默认只有 300s，撑不住一次正经全量。 */
    private static final long MIN_SNAPSHOT_WINDOW_SECONDS = 3600;

    /**
     * 建立全量一致性快照。
     *
     * <p>MongoDB 这边的"快照"是<b>快照会话</b>（{@code ClientSessionOptions.snapshot(true)}，
     * 5.0+）：会话在第一次读时钉住一个 clusterTime，之后该会话的所有读都在那个时间点上。
     * 与 MySQL 不同，它不需要任何锁；但历史版本受 {@code minSnapshotHistoryWindowInSeconds}
     * 限制（默认 300 秒），窗口内搬不完就会读失败。因此窗口太小时<b>降级为只记位点</b>，
     * 并把该调的参数写进日志——与 SQL 侧"快照是增强项、不能让全量起不来"的原则一致。
     *
     * @return 快照会话；未开启真快照时返回 null（调用方按不带 session 的原路径读）
     */
    private com.mongodb.client.ClientSession beginFullSnapshot(MongoClient source) {
        if ("NONE".equals(snapshotMode)) {
            return null;
        }
        String position = readClusterTime(source);
        com.mongodb.client.ClientSession session = null;
        String effectiveMode = "GTID_ONLY";
        if ("CONSISTENT".equals(snapshotMode)) {
            long window = readSnapshotHistoryWindowSeconds(source);
            if (window >= 0 && window < MIN_SNAPSHOT_WINDOW_SECONDS) {
                logger.warn("MongoDB 快照历史窗口仅 {}s，短于一次全量的常见耗时，搬到一半会失效；"
                                + "本次降级为只记位点。要用真快照请先调大："
                                + "db.adminCommand({{setParameter:1, minSnapshotHistoryWindowInSeconds:86400}})",
                        window);
            } else {
                try {
                    session = source.startSession(com.mongodb.ClientSessionOptions.builder()
                            .snapshot(true).build());
                    effectiveMode = "CONSISTENT";
                    if (window < 0) {
                        logger.warn("未能读取 minSnapshotHistoryWindowInSeconds（缺权限？），"
                                + "已按真快照启动；若中途报快照失效，请调大该参数或改用只记位点");
                    }
                } catch (Exception e) {
                    logger.warn("开启 MongoDB 快照会话失败（需要 5.0+ 副本集），降级为只记位点: {}", e.getMessage());
                }
            }
        }
        com.migration.common.snapshot.SnapshotPosition.write(taskId, effectiveMode, "mongodb", position);
        logger.info("全量快照: mode={}, clusterTime={}", effectiveMode, position);
        return session;
    }

    /** 当前 clusterTime（{@code hello} 回包的 operationTime），作为本次全量的位点。 */
    private String readClusterTime(MongoClient source) {
        try {
            Document reply = source.getDatabase("admin").runCommand(new Document("hello", 1));
            Object ts = reply.get("operationTime");
            if (ts instanceof org.bson.BsonTimestamp bt) {
                return "clusterTime:" + bt.getTime() + "." + bt.getInc();
            }
            if (ts != null) {
                return "clusterTime:" + ts;
            }
        } catch (Exception e) {
            logger.debug("读取 clusterTime 失败: {}", e.getMessage());
        }
        return "unknown";
    }

    /** 读 {@code minSnapshotHistoryWindowInSeconds}；读不到返回 -1（按"未知"处理，不降级）。 */
    private long readSnapshotHistoryWindowSeconds(MongoClient source) {
        try {
            Document reply = source.getDatabase("admin").runCommand(
                    new Document("getParameter", 1).append("minSnapshotHistoryWindowInSeconds", 1));
            Object v = reply.get("minSnapshotHistoryWindowInSeconds");
            if (v instanceof Number n) {
                return n.longValue();
            }
        } catch (Exception e) {
            logger.debug("读取 minSnapshotHistoryWindowInSeconds 失败: {}", e.getMessage());
        }
        return -1;
    }

    private void copyCollection(String db, String coll, MongoCollection<Document> src, MongoCollection<Document> tgt,
                                com.mongodb.client.ClientSession snapshotSession) {
        boolean active = processor.isActive(db, coll);
        // 索引先行（跳过默认 _id_），保证数据落入后即有约束/查询性能。
        // 列名映射时索引 key 字段也随之改写（源 note 上的唯一索引 → 目标 remark 上）。
        for (Document idx : src.listIndexes()) {
            String name = idx.getString("name");
            if ("_id_".equals(name)) {
                continue;
            }
            try {
                Document keys = (Document) idx.get("key");
                Document targetKeys = keys;
                if (active) {
                    targetKeys = new Document();
                    for (Map.Entry<String, Object> k : keys.entrySet()) {
                        targetKeys.put(processor.mapField(db, coll, k.getKey()), k.getValue());
                    }
                }
                com.mongodb.client.model.IndexOptions opts = new com.mongodb.client.model.IndexOptions().name(name);
                if (Boolean.TRUE.equals(idx.getBoolean("unique", false))) {
                    opts.unique(true);
                }
                if (Boolean.TRUE.equals(idx.getBoolean("sparse", false))) {
                    opts.sparse(true);
                }
                tgt.createIndex(targetKeys, opts);
            } catch (Exception ex) {
                logger.warn("复制索引 {} 失败（继续数据复制）: {}", name, ex.getMessage());
            }
        }

        // 目标集合为空才走 insertMany 快路径。用 countDocuments(limit 1) 而不是
        // estimatedDocumentCount()——后者读的是元数据估算值，非正常关闭后可能报 0，
        // 据此走 insert 会让续搬的每一条都撞重复键。
        boolean emptyTarget = tgt.countDocuments(new Document(),
                new com.mongodb.client.model.CountOptions().limit(1)) == 0;
        // 快照会话下的读会钉在同一个 clusterTime 上；无快照时是原来的即时读
        com.mongodb.client.FindIterable<Document> find = snapshotSession != null
                ? src.find(snapshotSession)
                : src.find();
        try (MongoBulkChannel channel = new MongoBulkChannel(tgt, bulkOptions, emptyTarget);
             MongoCursor<Document> cursor = find.batchSize(FULL_BATCH_SIZE).iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                if (active && processor.excluded(db, coll, doc)) {
                    continue; // 命中列过滤，跳过不同步
                }
                channel.add(active ? processor.transform(db, coll, doc) : doc);
                if (channel.isFull()) {
                    copiedDocs += channel.flush()[0];
                    writeProgress();
                }
            }
            if (!channel.isEmpty()) {
                copiedDocs += channel.flush()[0];
            }
            logger.info("集合 {}.{} 装载完成（{}）: {}", db, coll,
                    emptyTarget ? "insertMany 快路径" : "ReplaceOne upsert", channel.stats().summary());
        }
    }

    // ==================== 全量：聚合路由 ====================

    /**
     * 路由下的全量搬运：逐文档算落点后写目标集合。
     *
     * <p>与 1:1 路径分开写而不是在里面塞判断，是因为两件事根本不同：那条路径一个源集合对一个
     * 目标集合、可以走 insertMany 快路径；这里一条文档一个落点，只能逐条 upsert。
     *
     * <p>汇聚必须换 {@code _id}（{@code 来源标识|原_id}）：不换的话两个来源里 _id 相同的文档
     * 会互相覆盖，数据只会少、不会报错——与关系库"必须用复合主键"是同一件事。
     */
    private void copyRoutedCollection(MongoClient target, String db, String coll,
                                      MongoCollection<Document> src,
                                      com.mongodb.client.ClientSession snapshotSession) {
        boolean active = processor.isActive(db, coll);
        String shardKey = router.shardKeyField(db, coll);
        com.mongodb.client.model.ReplaceOptions upsert =
                new com.mongodb.client.model.ReplaceOptions().upsert(true);
        com.mongodb.client.FindIterable<Document> find = snapshotSession != null
                ? src.find(snapshotSession) : src.find();
        long copied = 0;
        try (MongoCursor<Document> cursor = find.batchSize(FULL_BATCH_SIZE).iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                if (active && processor.excluded(db, coll, doc)) {
                    continue;
                }
                Document out = active ? processor.transform(db, coll, doc) : new Document(doc);
                for (Document written : routeDocument(target, db, coll, out, shardKey, upsert)) {
                    copied += written == null ? 0 : 1;
                }
            }
        }
        copiedDocs += copied;
        writeProgress();
        logger.info("集合 {}.{} 路由装载完成: {} 文档", db, coll, copied);
    }

    /**
     * 把一条文档写到它的路由落点，返回实际写入的份数（广播时可能多份）。
     *
     * <p>拆分下分片键为空时按未路由策略处置：默认广播到每一片（宁可重复也不静默丢），
     * DEADLETTER 则整条不写。
     */
    private java.util.List<Document> routeDocument(MongoClient target, String db, String coll,
                                                   Document doc, String shardKey,
                                                   com.mongodb.client.model.ReplaceOptions upsert) {
        java.util.List<Document> written = new java.util.ArrayList<>();
        if (router.isMerge()) {
            com.migration.common.route.DocumentRouter.Target t = router.mergeTarget(db, coll, db);
            Document out = new Document(doc);
            out.put("_id", router.mergedId(db, coll, doc.get("_id")));
            router.mergeTags(db, coll).forEach(out::put);
            target.getDatabase(t.getDatabase()).getCollection(t.getName())
                    .replaceOne(new Document("_id", out.get("_id")), out, upsert);
            written.add(out);
            return written;
        }
        for (com.migration.common.route.DocumentRouter.Target t : shardTargetsOf(db, coll, doc, shardKey)) {
            target.getDatabase(t.getDatabase()).getCollection(t.getName())
                    .replaceOne(new Document("_id", doc.get("_id")), doc, upsert);
            written.add(doc);
        }
        return written;
    }

    /** 拆分：一条文档该进哪些分片（正常一片；分片键算不出时按未路由策略广播或丢弃）。 */
    private java.util.List<com.migration.common.route.DocumentRouter.Target> shardTargetsOf(
            String db, String coll, Document doc, String shardKey) {
        Object value = shardKey == null ? null : doc.get(shardKey);
        com.migration.common.route.DocumentRouter.Target t = router.shardOf(db, coll, value, db);
        if (t != null) {
            return java.util.List.of(t);
        }
        switch (router.unroutedPolicy(db, coll)) {
            case DEADLETTER:
                logger.warn("文档 {}.{} _id={} 的分片键为空/算不出分片，按 DEADLETTER 丢弃",
                        db, coll, doc.get("_id"));
                return java.util.List.of();
            case ERROR:
                throw new IllegalStateException("文档 " + db + "." + coll + " _id=" + doc.get("_id")
                        + " 的分片键算不出分片，按 ERROR 策略终止");
            default:
                return router.allShards(db, coll, db);
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

    private void runChangeStream(MongoClient source, MongoClient target, BsonDocument resumeToken,
                                 String resumedMarkedTxnKey, String sourceId) throws Exception {
        logger.info("增量启动（Change Streams），resumeToken={}, 防回环={}",
                resumeToken != null ? "已有" : "无（从当前）", bidiEnabled ? "开启" : "关闭");
        ChangeStreamIterable<Document> stream = source.watch()
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .maxAwaitTime(1, TimeUnit.SECONDS);
        if (resumeToken != null) {
            stream = stream.resumeAfter(resumeToken);
        }

        long lastFlush = System.currentTimeMillis();
        int sinceFlush = 0;
        ReplaceOptions upsert = new ReplaceOptions().upsert(true);

        // 防回环状态机：与 MySQL binlog / PG WAL 用的是同一个 BidiLoopGuard，
        // 只是"事务边界"在 Change Streams 里靠事件自带的 (lsid, txnNumber) 变化来识别。
        BidiLoopGuard guard = new BidiLoopGuard(bidiEnabled);
        String currentTxnKey = null;
        if (bidiEnabled && resumedMarkedTxnKey != null) {
            // 断点恰好落在"已读到标记、事务其余事件还没读完"的中间：恢复跳过态，
            // 否则该事务剩下的业务事件会被当成本地写入回传，形成回环。
            currentTxnKey = resumedMarkedTxnKey;
            guard.onOriginMarker();
            logger.info("恢复防回环跳过态：断点位于已打标事务 {} 中间", resumedMarkedTxnKey);
        }

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = stream.cursor()) {
            while (true) {
                ChangeStreamDocument<Document> event = cursor.tryNext();
                long now = System.currentTimeMillis();

                if (event != null) {
                    boolean propagate = true;
                    if (bidiEnabled) {
                        String txnKey = txnKeyOf(event);
                        if (!java.util.Objects.equals(txnKey, currentTxnKey)) {
                            currentTxnKey = txnKey;
                            guard.onTransactionBoundary();
                        }
                        if (isMarkerEvent(event)) {
                            // 标记本身永不传播；它的出现说明本事务是对端 apply 写进来的
                            guard.onOriginMarker();
                            propagate = false;
                        } else if (guard.shouldSkipReplicatedData()) {
                            propagate = false;
                            skippedLoopEvents++;
                        }
                    }

                    if (propagate) {
                        applyEvent(target, event, upsert);
                        incrEvents++;
                    }
                    sinceFlush++;
                }

                // resume token 周期性持久化（含空闲时的 postBatchResumeToken，推进断点避免重放过多）
                if (sinceFlush >= TOKEN_FLUSH_EVERY_EVENTS || now - lastFlush >= TOKEN_FLUSH_INTERVAL_MS) {
                    BsonDocument token = cursor.getResumeToken();
                    if (token != null) {
                        saveCheckpoint(token, guard.currentTxnMarked() ? currentTxnKey : null, sourceId);
                    }
                    writeProgress();
                    lastFlush = now;
                    sinceFlush = 0;
                }
            }
        }
    }

    /** 事务身份：同一事务内的所有 change stream 事件带同一对 (lsid, txnNumber)；非事务写入返回 null。 */
    private static String txnKeyOf(ChangeStreamDocument<Document> event) {
        BsonDocument lsid = event.getLsid();
        org.bson.BsonInt64 txnNumber = event.getTxnNumber();
        if (lsid == null || txnNumber == null) {
            return null;
        }
        return lsid.toJson() + "#" + txnNumber.getValue();
    }

    /** 是否是对端 apply 写入的 origin 标记事件（集合名即约定的标记集合）。 */
    private static boolean isMarkerEvent(ChangeStreamDocument<Document> event) {
        return event.getNamespace() != null
                && BidiConstants.MARKER_TABLE.equals(event.getNamespace().getCollectionName());
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

        OperationType op = event.getOperationType();
        try {
            if (op == OperationType.DROP) {
                // DDL 不能进事务，因此 drop 无法带 origin 标记。双向下对端会把这次 drop 回传一次，
                // 但对一个已经不存在的集合执行 drop 是 no-op 且不产生 oplog 事件，回环在一跳内自然终止。
                target.getDatabase(db).getCollection(coll).drop();
                logger.info("集合 {}.{} 已随源库 drop", db, coll);
                return;
            }
            if (router.matches(db, coll)) {
                applyRoutedDml(target, db, coll, event, upsert);
            } else if (bidiEnabled) {
                applyInMarkedTransaction(target, db, coll, event, upsert);
            } else {
                applyDml(target.getDatabase(db).getCollection(coll), db, coll, event, upsert, null);
            }
        } catch (Exception e) {
            // 单事件失败记日志继续（upsert/delete 幂等，绝大多数为暂时性错误，
            // 下轮 resume 重放可自愈；不因单事件卡死整个流）
            logger.error("应用增量事件失败: {} {}.{}: {}", op, db, coll, e.getMessage());
        }
    }

    /**
     * 路由下的增量应用。
     *
     * <p>两处必须与 1:1 路径不同，都是 change stream 拿不到前镜像逼出来的：
     * <ul>
     *   <li><b>DELETE 只带 _id</b>，没有文档内容 → 算不出分片键 → 只能<b>广播删</b>到每一片。
     *       删一个不存在的 _id 是 no-op，代价可接受；按某一片猜着删则会漏删，留下幽灵文档。</li>
     *   <li><b>UPDATE 改了分片键时</b>，新落点由新文档算得出，旧落点却算不出（没有前镜像）。
     *       做法是：先把这个 _id 从<b>其余各片</b>删掉，再写进新落点——等价于一次搬迁，
     *       且对"没改分片键"的普通更新也是安全的（其余片本来就没有这条）。</li>
     * </ul>
     *
     * <p>汇聚下 _id 已按来源重构，天然不会跨来源撞车，删改都按重构后的 _id 定位。
     */
    private void applyRoutedDml(MongoClient target, String db, String coll,
                                ChangeStreamDocument<Document> event,
                                ReplaceOptions upsert) {
        OperationType op = event.getOperationType();
        boolean active = processor.isActive(db, coll);
        if (op == OperationType.DELETE) {
            Document key = event.getDocumentKey() == null
                    ? null : Document.parse(event.getDocumentKey().toJson());
            Object id = key == null ? null : key.get("_id");
            if (id == null) {
                return;
            }
            if (router.isMerge()) {
                com.migration.common.route.DocumentRouter.Target t = router.mergeTarget(db, coll, db);
                target.getDatabase(t.getDatabase()).getCollection(t.getName())
                        .deleteOne(new Document("_id", router.mergedId(db, coll, id)));
                return;
            }
            for (com.migration.common.route.DocumentRouter.Target t : router.allShards(db, coll, db)) {
                target.getDatabase(t.getDatabase()).getCollection(t.getName())
                        .deleteOne(new Document("_id", id));
            }
            return;
        }

        Document full = event.getFullDocument();
        if (full == null) {
            // UPDATE 的 lookup 落空 = 文档已被删，后续 DELETE 事件会处理
            return;
        }
        if (active && processor.excluded(db, coll, full)) {
            // 列过滤把这条排除了：目标端不该再有它，按删处理（与关系库侧的语义一致）
            Object id = router.isMerge() ? router.mergedId(db, coll, full.get("_id")) : full.get("_id");
            if (router.isMerge()) {
                com.migration.common.route.DocumentRouter.Target t = router.mergeTarget(db, coll, db);
                target.getDatabase(t.getDatabase()).getCollection(t.getName())
                        .deleteOne(new Document("_id", id));
            } else {
                for (com.migration.common.route.DocumentRouter.Target t : router.allShards(db, coll, db)) {
                    target.getDatabase(t.getDatabase()).getCollection(t.getName())
                            .deleteOne(new Document("_id", id));
                }
            }
            return;
        }

        Document doc = active ? processor.transform(db, coll, full) : new Document(full);
        if (router.isMerge()) {
            routeDocument(target, db, coll, doc, null, upsert);
            return;
        }
        String shardKey = router.shardKeyField(db, coll);
        java.util.List<com.migration.common.route.DocumentRouter.Target> targets =
                shardTargetsOf(db, coll, doc, shardKey);
        java.util.Set<String> keep = new java.util.HashSet<>();
        for (com.migration.common.route.DocumentRouter.Target t : targets) {
            keep.add(t.toString());
        }
        // 先清其余片上的同 _id（分片键被改过时那份就是陈行），再写新落点
        for (com.migration.common.route.DocumentRouter.Target t : router.allShards(db, coll, db)) {
            if (!keep.contains(t.toString())) {
                target.getDatabase(t.getDatabase()).getCollection(t.getName())
                        .deleteOne(new Document("_id", doc.get("_id")));
            }
        }
        for (com.migration.common.route.DocumentRouter.Target t : targets) {
            target.getDatabase(t.getDatabase()).getCollection(t.getName())
                    .replaceOne(new Document("_id", doc.get("_id")), doc, upsert);
        }
    }

    /**
     * 双向灾备的应用写入：在一个多文档事务里**先**写 origin 标记、再写业务数据。
     *
     * <p>这两条写入原子提交进目标库的 oplog，对端的 change stream 会按顺序读到它们，且同一事务
     * 的事件带相同 (lsid, txnNumber)。对端捕获侧读到标记即判定整个事务"是复制来的"，跳过其业务
     * 事件不再回传 —— 与 MySQL binlog / PG WAL 侧完全同一套约定（标记必先于业务 DML）。
     *
     * <p>每个事件一个事务而不是攒批：批内任一事件写失败会整批回滚，重放/补偿逻辑复杂且容易在
     * 极端条件下丢事件；单事件事务在副本集上开销可接受，换来的是"失败只影响这一条"。
     */
    private void applyInMarkedTransaction(MongoClient target, String db, String coll,
                                          ChangeStreamDocument<Document> event, ReplaceOptions upsert) {
        MongoDatabase tgtDb = target.getDatabase(db);
        // 事务内隐式建集合要 MongoDB 4.4+，先在事务外确保集合存在，兼容 4.0/4.2 且只做一次
        ensureCollection(tgtDb, BidiConstants.MARKER_TABLE);
        ensureCollection(tgtDb, coll);

        try (ClientSession session = target.startSession()) {
            session.startTransaction();
            try {
                writeOriginMarker(tgtDb, session);
                applyDml(tgtDb.getCollection(coll), db, coll, event, upsert, session);
                session.commitTransaction();
            } catch (RuntimeException e) {
                try {
                    session.abortTransaction();
                } catch (Exception ignore) {
                    // abort 失败无需再处理：事务未提交，超时后自动回滚
                }
                throw e;
            }
        }
    }

    /** 应用一条 DML（session 为 null 时非事务写入，即单向同步的原有路径）。 */
    private void applyDml(MongoCollection<Document> tgt, String db, String coll,
                          ChangeStreamDocument<Document> event, ReplaceOptions upsert,
                          ClientSession session) {
        boolean active = processor.isActive(db, coll);
        switch (event.getOperationType()) {
            case INSERT:
            case REPLACE:
            case UPDATE: {
                // 统一用 fullDocument upsert：幂等、天然处理乱序/重放；
                // UPDATE 且 fullDocument 为 null = 文档在 lookup 前已被删除，交给后续 DELETE 事件
                Document full = event.getFullDocument();
                if (full == null) {
                    break;
                }
                if (active && processor.excluded(db, coll, full)) {
                    // 后镜像命中列过滤 → 目标端按 _id 删除（与 mysql/pg 的
                    // "UPDATE 后镜像被过滤转 DELETE" 语义一致，处理"原本符合、改后不符合"）
                    Document filter = new Document("_id", full.get("_id"));
                    if (session != null) {
                        tgt.deleteOne(session, filter);
                    } else {
                        tgt.deleteOne(filter);
                    }
                } else {
                    Document out = active ? processor.transform(db, coll, full) : full;
                    Document filter = new Document("_id", out.get("_id"));
                    if (session != null) {
                        tgt.replaceOne(session, filter, out, upsert);
                    } else {
                        tgt.replaceOne(filter, out, upsert);
                    }
                }
                break;
            }
            case DELETE: {
                BsonDocument key = event.getDocumentKey();
                if (key != null) {
                    if (session != null) {
                        tgt.deleteOne(session, key);
                    } else {
                        tgt.deleteOne(key);
                    }
                }
                break;
            }
            default:
                logger.debug("跳过不处理的事件类型: {} on {}.{}", event.getOperationType(), db, coll);
        }
    }

    /** origin 标记：单文档滚动更新，每个应用事务都产生一次行事件供对端识别。 */
    private void writeOriginMarker(MongoDatabase tgtDb, ClientSession session) {
        Document marker = new Document("_id", BidiConstants.MARKER_ROW_ID)
                .append("origin", nodeId)
                .append("taskId", taskId)
                .append("ts", System.currentTimeMillis());
        tgtDb.getCollection(BidiConstants.MARKER_TABLE).replaceOne(session,
                new Document("_id", BidiConstants.MARKER_ROW_ID), marker,
                new ReplaceOptions().upsert(true));
    }

    /** 已确保存在的集合（库.集合），避免每个事件都发一次 create 命令。 */
    private final java.util.Set<String> ensuredCollections = new java.util.HashSet<>();

    private void ensureCollection(MongoDatabase db, String coll) {
        String key = db.getName() + "." + coll;
        if (!ensuredCollections.add(key)) {
            return;
        }
        try {
            db.createCollection(coll);
        } catch (Exception e) {
            // 已存在（NamespaceExists）是正常路径，其它异常留给随后的写入去报
            logger.debug("确保集合 {} 存在: {}", key, e.getMessage());
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
                    if (!c.startsWith("system.") && !BidiConstants.MARKER_TABLE.equals(c)) {
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
        // 防回环标记集合是同步链路自己的簿记数据，任何方向都不作为业务数据传播
        if (BidiConstants.MARKER_TABLE.equals(coll)) {
            return false;
        }
        return colls.isEmpty() || colls.contains(coll);
    }

    // ==================== 连接 ====================

    private MongoClientSettings clientSettings(String prefix) {
        return buildClientSettings(props, prefix);
    }

    /** 连接设置（同步与订阅两条链路共用）。 */
    static MongoClientSettings buildClientSettings(Properties props, String prefix) {
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

    /**
     * 增量断点：resume token + 归属的源部署 + 断点所在的"已打标事务"。
     *
     * <p>后两项都是防数据错乱的必需项，不是锦上添花：
     * <ul>
     *   <li>{@code sourceId} 不匹配 = 主备倒换/换源了，旧 token 在新源 oplog 上无意义，必须丢弃；</li>
     *   <li>{@code markedTxnKey} 记录"断点落在某个对端写入事务的中间"，重启后据此继续跳过该事务
     *       剩余事件，否则这些事件会被当本地写入回传，双向下形成回环。</li>
     * </ul>
     */
    private static final class Checkpoint {
        final BsonDocument token;
        final String markedTxnKey;

        Checkpoint(BsonDocument token, String markedTxnKey) {
            this.token = token;
            this.markedTxnKey = markedTxnKey;
        }
    }

    private Checkpoint loadCheckpoint(String sourceId) {
        try {
            File f = tokenPath.toFile();
            if (!f.exists()) {
                return null;
            }
            String json = new String(Files.readAllBytes(tokenPath), StandardCharsets.UTF_8);
            if (json.trim().isEmpty()) {
                return null;
            }
            BsonDocument doc = BsonDocument.parse(json);
            // 旧格式：整个文件就是 resume token 本身（只有 _data）
            if (!doc.containsKey("resumeToken")) {
                return new Checkpoint(doc, null);
            }
            String savedSource = doc.containsKey("sourceId") ? doc.getString("sourceId").getValue() : null;
            if (savedSource != null && sourceId != null && !savedSource.equals(sourceId)) {
                logger.warn("checkpoint 归属的源部署（{}）与当前源（{}）不一致——判定为主备倒换/换源，"
                        + "丢弃旧 resume token，从当前位点开始捕获", savedSource, sourceId);
                return null;
            }
            BsonDocument token = doc.getDocument("resumeToken");
            String markedTxn = doc.containsKey("markedTxnKey") && !doc.isNull("markedTxnKey")
                    ? doc.getString("markedTxnKey").getValue() : null;
            return new Checkpoint(token, markedTxn);
        } catch (Exception e) {
            logger.warn("读取 resume token 失败，将从当前位点开始: {}", e.getMessage());
            return null;
        }
    }

    private void saveCheckpoint(BsonDocument token, String markedTxnKey, String sourceId) {
        try {
            BsonDocument doc = new BsonDocument();
            doc.put("resumeToken", token);
            doc.put("sourceId", new org.bson.BsonString(sourceId != null ? sourceId : ""));
            if (markedTxnKey != null) {
                doc.put("markedTxnKey", new org.bson.BsonString(markedTxnKey));
            }
            Files.createDirectories(tokenPath.getParent());
            Path tmp = tokenPath.resolveSibling(tokenPath.getFileName() + ".tmp");
            String json = doc.toJson();
            Files.write(tmp, json.getBytes(StandardCharsets.UTF_8));
            Files.move(tmp, tokenPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            saveUnifiedCheckpoint(json);
        } catch (Exception e) {
            logger.warn("持久化 resume token 失败: {}", e.getMessage());
        }
    }

    /**
     * 并行写一份统一位点，供 agent 上卷到元数据库、以及接管方回灌。
     *
     * <p>payload 直接存 resume token 文件的<b>原文</b>：token 是服务端的不透明结构，
     * 拆开重组毫无意义且容易失真，回灌时原样写回文件即可。
     * 同理它也<b>折不出可比标量</b>（{@code _data} 是编码过的十六进制串），
     * 因此 monotonicKey 记 UNKNOWN，单调守卫对这一行自动降级为不校验——
     * mongo 这条链路的防回退靠的是 resume token 本身在服务端的语义。
     */
    private void saveUnifiedCheckpoint(String checkpointJson) {
        try {
            java.util.Properties payload = new java.util.Properties();
            payload.setProperty("mongo.checkpoint.json", checkpointJson);
            payload.setProperty("carrier", "mongo");
            com.migration.common.position.LocalCheckpointStore.saveThrottled(
                    new com.migration.common.position.CheckpointRecord(
                            taskId,
                            com.migration.common.position.CheckpointRecord.Stage.CAPTURE,
                            "mongodb",
                            com.migration.common.position.CheckpointRecord.Kind.RESUME_TOKEN,
                            payload,
                            com.migration.common.position.MonotonicKey.UNKNOWN,
                            0L),
                    1000L, false);
        } catch (Exception e) {
            logger.debug("统一位点（mongo）落盘失败: {}", e.getMessage());
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
            if (bidiEnabled) {
                p.put("skippedLoopEvents", skippedLoopEvents);
            }
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
