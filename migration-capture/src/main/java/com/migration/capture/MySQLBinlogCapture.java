package com.migration.capture;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.migration.common.AbstractCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MySQLBinlogCapture extends AbstractCapture<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(MySQLBinlogCapture.class);

    private static final char FIELD_SEP = '\001';
    private static final char RECORD_SEP = '\n';
    private static final String HEARTBEAT_TABLE = "__sync_heartbeat";
    private static final long HEARTBEAT_INTERVAL_MS = 5000;

    private String host;
    private int port;
    private String user;
    private String password;
    private String binlogFile;
    private long binlogPosition;
    /** GTID 位点（capture.gtid.set，来自 checkpoint 的 gtid_executed 快照）；非空且启用时优先于 file+pos。 */
    private String gtidSet;
    private boolean gtidEnabled;
    private String outputDir;
    private String taskId;
    private long serverId;
    private String heartbeatDatabase;

    private BinaryLogClient client;
    private BufferedWriter writer;
    private final AtomicLong eventCounter = new AtomicLong(0);
    private final AtomicLong fileCounter = new AtomicLong(0);
    private long maxEventsPerFile = 10000;
    private long currentFileEvents = 0;

    private volatile String currentBinlogFile;
    private volatile long currentBinlogPosition;

    private Thread heartbeatThread;
    private Connection heartbeatConnection;
    private volatile long clockOffsetMs = 0;
    private volatile long lastRpoMs = -1;
    private volatile long lastHeartbeatSourceTs = -1;
    private volatile long lastSourceEventTs = -1;
    private volatile long lastRpoReportTime = 0;
    private static final long RPO_REPORT_INTERVAL_MS = 3000;
    private final Map<Long, String> tableIdToNameMap = new java.util.concurrent.ConcurrentHashMap<>();

    // 背压控制：extract 通过信号文件通知 capture 暂停/恢复
    private volatile boolean backpressurePaused = false;

    // ---- 双向同步/环路防护（active-active）----
    /** 启用后：识别带 origin 标记的复制事务并跳过其数据事件，防止 A→B→A 回环。 */
    private boolean bidirectionalEnabled;
    /** 前向单遍状态机：读标记表行事件即置位，遇 BEGIN/XID 复位；带标记事务的数据事件跳过。 */
    private com.migration.common.bidi.BidiLoopGuard loopGuard;
    private String backpressureSignalPath;

    /** 需要同步的数据库集合（为空表示不过滤，捕获所有） */
    private Set<String> syncedDatabases = new HashSet<>();
    /** 需要同步的表集合（格式：database.table，为空表示不过滤） */
    private Set<String> syncedTables = new HashSet<>();

    @Override
    protected void doInitialize() throws Exception {
        host = props.getProperty("source.db.host", "localhost");
        port = Integer.parseInt(props.getProperty("source.db.port", "3306"));
        user = props.getProperty("source.db.username", "root");
        password = props.getProperty("source.db.password", "");
        binlogFile = props.getProperty("capture.binlog.file", "");
        binlogPosition = Long.parseLong(props.getProperty("capture.binlog.position", "4"));
        // GTID 位点：gtid_executed 快照与 file+pos 在同一时刻记录（CheckpointManager），
        // 有 GTID 集且未显式关闭时优先按 GTID 自动定位——源端 HA 切换/binlog 文件名变化时
        // file+pos 失效而 GTID 仍然有效。gtid_executed 原文可能含换行，需归一化。
        gtidEnabled = Boolean.parseBoolean(props.getProperty("capture.gtid.enabled", "true"));
        gtidSet = props.getProperty("capture.gtid.set", "").replaceAll("\\s+", "");
        outputDir = props.getProperty("capture.output.dir", "binlog_output");
        taskId = props.getProperty("task.id", "unknown");
        maxEventsPerFile = Long.parseLong(props.getProperty("capture.max.events.per.file", "10000"));
        serverId = Long.parseLong(props.getProperty("capture.server.id", "65535"));
        bidirectionalEnabled = com.migration.common.bidi.BidiConstants.isEnabled(props);
        loopGuard = new com.migration.common.bidi.BidiLoopGuard(bidirectionalEnabled);
        backpressureSignalPath = "files/" + taskId + "/backpressure.signal";

        heartbeatDatabase = props.getProperty("source.db.database", "");
        if (heartbeatDatabase == null || heartbeatDatabase.isEmpty()) {
            String jdbcUrl = props.getProperty("source.db.jdbc.url", "");
            if (jdbcUrl != null && !jdbcUrl.isEmpty()) {
                String urlPart = jdbcUrl.contains("?") ? jdbcUrl.substring(0, jdbcUrl.indexOf('?')) : jdbcUrl;
                int lastSlash = urlPart.lastIndexOf('/');
                if (lastSlash > 0 && lastSlash < urlPart.length() - 1) {
                    String dbFromUrl = urlPart.substring(lastSlash + 1);
                    if (!dbFromUrl.isEmpty()) {
                        heartbeatDatabase = dbFromUrl;
                    }
                }
            }
        }
        if (heartbeatDatabase == null || heartbeatDatabase.isEmpty()) {
            String syncObjects = props.getProperty("migration.sync.objects", "");
            if (syncObjects == null || syncObjects.isEmpty()) {
                syncObjects = props.getProperty("sync.objects", "");
            }
            if (syncObjects != null && !syncObjects.isEmpty() && syncObjects.startsWith("{")) {
                int firstQuote = syncObjects.indexOf('"');
                int secondQuote = syncObjects.indexOf('"', firstQuote + 1);
                if (firstQuote >= 0 && secondQuote > firstQuote) {
                    heartbeatDatabase = syncObjects.substring(firstQuote + 1, secondQuote);
                }
            }
        }
        if (heartbeatDatabase == null || heartbeatDatabase.isEmpty()) {
            String tables = props.getProperty("source.db.tables", "");
            if (tables != null && !tables.isEmpty()) {
                String firstTable = tables.split(",")[0].trim();
                if (firstTable.contains(".")) {
                    heartbeatDatabase = firstTable.substring(0, firstTable.indexOf('.'));
                }
            }
        }

        if (binlogFile.isEmpty()) {
            binlogFile = null;
            binlogPosition = 0;
        }

        // 解析同步对象配置，构建数据库/表过滤集合
        parseSyncObjects();

        logger.info("Capture初始化完成 - host={}:{} user={} outputDir={} taskId={} binlogFile={} binlogPosition={} heartbeatDatabase={} syncedDatabases={} syncedTables={}",
                host, port, user, outputDir, taskId, binlogFile, binlogPosition, heartbeatDatabase,
                syncedDatabases, syncedTables);
    }

    /**
     * 解析同步对象配置，构建数据库和表过滤集合。
     * 配置格式: migration.sync.objects={"test_db1":{"tables":["users","orders"]}}
     * 也支持 migration.included.databases 和 migration.included.tables
     */
    private void parseSyncObjects() {
        // 方式1: 从 migration.sync.objects JSON 解析
        String syncObjectsJson = props.getProperty("migration.sync.objects", "");
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            syncObjectsJson = props.getProperty("sync.objects", "");
        }

        if (syncObjectsJson != null && !syncObjectsJson.isEmpty() && syncObjectsJson.startsWith("{")) {
            try {
                // 简单解析JSON: {"db1":{"tables":["t1","t2"]},"db2":{"tables":["t3"]}}
                String json = syncObjectsJson.replace("\\\"", "\"");
                // 提取所有 "dbname":{"tables":[...]} 模式
                java.util.regex.Pattern dbPattern = java.util.regex.Pattern.compile("\"([^\"]+)\"\\s*:\\s*\\{\\s*\"tables\"\\s*:\\s*\\[([^\\]]*)\\]");
                java.util.regex.Matcher matcher = dbPattern.matcher(json);
                while (matcher.find()) {
                    String dbName = matcher.group(1);
                    String tablesStr = matcher.group(2);
                    syncedDatabases.add(dbName);
                    // 解析表名列表
                    java.util.regex.Pattern tablePattern = java.util.regex.Pattern.compile("\"([^\"]+)\"");
                    java.util.regex.Matcher tableMatcher = tablePattern.matcher(tablesStr);
                    while (tableMatcher.find()) {
                        syncedTables.add(dbName + "." + tableMatcher.group(1));
                    }
                }
            } catch (Exception e) {
                logger.warn("解析sync.objects失败，将捕获所有数据库事件: {}", e.getMessage());
            }
        }

        // 方式2: 从 migration.included.databases 和 migration.included.tables 解析
        if (syncedDatabases.isEmpty()) {
            String databases = props.getProperty("migration.included.databases", "");
            if (databases != null && !databases.isEmpty()) {
                for (String db : databases.split(",")) {
                    String trimmed = db.trim();
                    if (!trimmed.isEmpty()) {
                        syncedDatabases.add(trimmed);
                    }
                }
            }
        }

        if (syncedTables.isEmpty()) {
            String tables = props.getProperty("migration.included.tables", "");
            if (tables != null && !tables.isEmpty()) {
                for (String table : tables.split(",")) {
                    String trimmed = table.trim();
                    if (!trimmed.isEmpty()) {
                        syncedTables.add(trimmed);
                        // 同时提取数据库名
                        if (trimmed.contains(".")) {
                            syncedDatabases.add(trimmed.substring(0, trimmed.indexOf('.')));
                        }
                    }
                }
            }
        }

        if (!syncedDatabases.isEmpty() || !syncedTables.isEmpty()) {
            logger.info("已启用binlog事件过滤 - 数据库: {}, 表: {}", syncedDatabases, syncedTables);
        } else {
            logger.info("未配置同步对象过滤，将捕获所有数据库事件");
        }
    }

    /**
     * 检查指定表是否在同步范围内。
     * @param dbName 数据库名
     * @param tableName 表名
     * @return true表示需要同步，false表示需要跳过
     */
    private boolean shouldCaptureTable(String dbName, String tableName) {
        // 未配置过滤则捕获所有
        if (syncedDatabases.isEmpty() && syncedTables.isEmpty()) {
            return true;
        }
        // 优先检查表级过滤
        if (!syncedTables.isEmpty()) {
            String fullTableName = dbName + "." + tableName;
            return syncedTables.contains(fullTableName);
        }
        // 仅数据库级过滤
        return syncedDatabases.contains(dbName);
    }

    /**
     * 检查数据事件（Write/Update/Delete）是否应该被捕获。
     * 通过tableId查找表名，再判断是否在同步范围内。
     */
    /** 取数据事件（Write/Update/Delete）对应的 {@code db.table} 全名；非数据事件或映射未知返回 null。 */
    private String dataEventTableName(EventData eventData) {
        long tableId = -1;
        if (eventData instanceof WriteRowsEventData) {
            tableId = ((WriteRowsEventData) eventData).getTableId();
        } else if (eventData instanceof UpdateRowsEventData) {
            tableId = ((UpdateRowsEventData) eventData).getTableId();
        } else if (eventData instanceof DeleteRowsEventData) {
            tableId = ((DeleteRowsEventData) eventData).getTableId();
        }
        return tableId < 0 ? null : tableIdToNameMap.get(tableId);
    }

    private boolean shouldCaptureDataEvent(EventData eventData) {
        long tableId = -1;
        if (eventData instanceof WriteRowsEventData) {
            tableId = ((WriteRowsEventData) eventData).getTableId();
        } else if (eventData instanceof UpdateRowsEventData) {
            tableId = ((UpdateRowsEventData) eventData).getTableId();
        } else if (eventData instanceof DeleteRowsEventData) {
            tableId = ((DeleteRowsEventData) eventData).getTableId();
        }

        if (tableId < 0) {
            return true; // 无法确定表，默认捕获
        }

        String fullTableName = tableIdToNameMap.get(tableId);
        if (fullTableName == null) {
            return true; // 表映射未知，默认捕获（可能是TableMap事件还未到达）
        }

        int dotIndex = fullTableName.indexOf('.');
        if (dotIndex < 0) {
            return true;
        }

        String dbName = fullTableName.substring(0, dotIndex);
        String tableName = fullTableName.substring(dotIndex + 1);
        return shouldCaptureTable(dbName, tableName);
    }

    @Override
    protected void doStart() throws Exception {
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        openNewOutputFile();

        client = new BinaryLogClient(host, port, user, password);
        client.setServerId(serverId);

        // 反序列化模式改造：
        // 1) 符号感知 TIME2：连接器默认实现丢负号（-100:00:00 → +924:00:00），用自定义行反序列化器修正；
        // 2) CHAR_AND_BINARY_AS_BYTE_ARRAY：binlog 协议不区分 VARBINARY/VARCHAR，默认按字符集解码
        //    会把二进制字节按字符串损坏——统一按 byte[] 交付（serializeValue 转 0x hex，
        //    extractor 端按列类型分别做 UTF-8 文本解码或保持二进制）。
        // 注意：兼容模式必须在注册自定义反序列化器之后设置，才能传播到它们。
        java.util.Map<Long, com.github.shyiko.mysql.binlog.event.TableMapEventData> tableMapShared =
                new java.util.HashMap<>();
        com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer eventDeserializer =
                new com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer();
        eventDeserializer.setEventDataDeserializer(com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP,
                new com.migration.capture.binlog.SignAwareRowsDeserializers.SharedTableMapDeserializer(tableMapShared));
        eventDeserializer.setEventDataDeserializer(com.github.shyiko.mysql.binlog.event.EventType.WRITE_ROWS,
                new com.migration.capture.binlog.SignAwareRowsDeserializers.Write(tableMapShared));
        eventDeserializer.setEventDataDeserializer(com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS,
                new com.migration.capture.binlog.SignAwareRowsDeserializers.Write(tableMapShared)
                        .setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(com.github.shyiko.mysql.binlog.event.EventType.UPDATE_ROWS,
                new com.migration.capture.binlog.SignAwareRowsDeserializers.Update(tableMapShared));
        eventDeserializer.setEventDataDeserializer(com.github.shyiko.mysql.binlog.event.EventType.EXT_UPDATE_ROWS,
                new com.migration.capture.binlog.SignAwareRowsDeserializers.Update(tableMapShared)
                        .setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(com.github.shyiko.mysql.binlog.event.EventType.DELETE_ROWS,
                new com.migration.capture.binlog.SignAwareRowsDeserializers.Delete(tableMapShared));
        eventDeserializer.setEventDataDeserializer(com.github.shyiko.mysql.binlog.event.EventType.EXT_DELETE_ROWS,
                new com.migration.capture.binlog.SignAwareRowsDeserializers.Delete(tableMapShared)
                        .setMayContainExtraInformation(true));
        eventDeserializer.setCompatibilityMode(
                com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        client.setEventDeserializer(eventDeserializer);
        if (gtidEnabled && gtidSet != null && !gtidSet.isEmpty()) {
            // GTID 自动定位：连接器向服务端声明已执行集，由服务端决定起始文件/位点；
            // 流式过程中连接器自动维护该集（client.getGtidSet()），断线重连也按最新集续传
            client.setGtidSet(gtidSet);
            logger.info("按 GTID 集开始捕获（自动定位）: {}", gtidSet);
        } else if (binlogFile != null && !binlogFile.isEmpty()) {
            client.setBinlogFilename(binlogFile);
            client.setBinlogPosition(binlogPosition);
            logger.info("从binlog位点开始捕获: {}:{}", binlogFile, binlogPosition);
        } else {
            logger.info("从最新binlog位点开始捕获");
        }

        client.registerEventListener(this::processEvent);

        client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                logger.info("已连接到MySQL binlog流");
            }

            @Override
            public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                logger.error("MySQL通信失败: {}", ex.getMessage());
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                logger.error("事件反序列化失败: {}", ex.getMessage());
            }

            @Override
            public void onDisconnect(BinaryLogClient client) {
                logger.info("已断开MySQL binlog流连接");
            }
        });

        initClockOffset();
        startHeartbeat();
        startBackpressureMonitor();

        client.connect();

        logger.info("Binlog捕获已启动, taskId: {}", taskId);
    }

    @Override
    protected void doStop() throws Exception {
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        if (heartbeatConnection != null) {
            try {
                heartbeatConnection.close();
            } catch (Exception e) {
                logger.warn("关闭心跳连接异常: {}", e.getMessage());
            }
        }

        if (client != null) {
            try {
                client.disconnect();
            } catch (Exception e) {
                logger.warn("断开binlog客户端异常: {}", e.getMessage());
            }
        }

        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (Exception e) {
                logger.warn("关闭写入器异常: {}", e.getMessage());
            }
        }

        savePosition();
        logger.info("Binlog捕获已停止, 总事件数: {}, 最后RPO: {}ms", eventCounter.get(), lastRpoMs);
    }

    /**
     * 检查背压信号文件，更新 backpressurePaused 状态。
     * extract 进程在 THL 积压时写入 PAUSE 信号，积压解除后写入 RESUME。
     */
    private void checkBackpressureSignal() {
        if (backpressureSignalPath == null) return;
        java.io.File signalFile = new java.io.File(backpressureSignalPath);
        if (!signalFile.exists()) {
            backpressurePaused = false;
            return;
        }
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(signalFile))) {
            String firstLine = reader.readLine();
            boolean shouldPause = firstLine != null && "PAUSE".equalsIgnoreCase(firstLine.trim());
            if (shouldPause != backpressurePaused) {
                backpressurePaused = shouldPause;
                if (shouldPause) {
                    logger.warn("收到背压暂停信号，暂停 binlog 事件处理");
                } else {
                    logger.info("收到背压恢复信号，恢复 binlog 事件处理");
                }
            }
        } catch (IOException e) {
            logger.debug("读取背压信号文件失败: {}", e.getMessage());
        }
    }

    /**
     * 启动后台线程定期检查背压信号，确保无事件时也能及时响应暂停/恢复。
     */
    private void startBackpressureMonitor() {
        Thread monitor = new Thread(() -> {
            while (running) {
                try {
                    checkBackpressureSignal();
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.debug("背压监控异常: {}", e.getMessage());
                }
            }
        }, "Backpressure-Monitor-" + taskId);
        monitor.setDaemon(true);
        monitor.start();
        logger.info("背压监控线程已启动, taskId={}", taskId);
    }

    private void processEvent(Event event) {
        if (!running) {
            return;
        }

        // 背压检查：如果 extract 发出暂停信号，则等待恢复
        if (backpressurePaused) {
            try {
                while (backpressurePaused && running) {
                    Thread.sleep(500);
                    checkBackpressureSignal();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        try {
            EventHeaderV4 header = (EventHeaderV4) event.getHeader();
            currentBinlogFile = client.getBinlogFilename();
            currentBinlogPosition = client.getBinlogPosition();

            String eventType = event.getHeader().getEventType().name();
            long timestamp = header.getTimestamp();
            long serverId = header.getServerId();

            EventData eventData = event.getData();
            if (eventData instanceof TableMapEventData) {
                TableMapEventData tableMap = (TableMapEventData) eventData;
                tableIdToNameMap.put(tableMap.getTableId(), tableMap.getDatabase() + "." + tableMap.getTable());
            }

            // 双向同步/环路防护：前向单遍事务标记状态机
            if (bidirectionalEnabled) {
                // 事务边界：BEGIN 开启新事务 / XID 提交，复位标记状态；双向模式下二者均不外传。
                if (eventData instanceof QueryEventData) {
                    String sql = ((QueryEventData) eventData).getSql();
                    if (sql != null && "BEGIN".equalsIgnoreCase(sql.trim())) {
                        loopGuard.onTransactionBoundary();
                    }
                    // 双向模式只复制 DML，不传播 DDL / 事务控制语句（BEGIN/COMMIT/CREATE/ALTER/DROP…）：
                    // DDL 无法用行标记打标（隐式提交，与 DML 不同事务），在 active-active 里会无限回环。
                    // schema 变更需各节点带外协调。此举也顺带滤掉 __sync_heartbeat/__sync_origin 建表 DDL。
                    return;
                } else if (eventData instanceof XidEventData) {
                    loopGuard.onTransactionBoundary();
                    return;
                }
                // origin 标记行事件：置位并丢弃（标记表不外传），后续本事务数据事件将被跳过
                String markerTable = com.migration.common.bidi.BidiConstants.MARKER_TABLE;
                String evTable = dataEventTableName(eventData);
                if (evTable != null && evTable.endsWith("." + markerTable)) {
                    loopGuard.onOriginMarker();
                    return;
                }
            }

            if (isHeartbeatEvent(eventData)) {
                long now = System.currentTimeMillis();
                lastRpoMs = now - timestamp + clockOffsetMs;
                lastHeartbeatSourceTs = timestamp;
                lastSourceEventTs = timestamp;
                writeRpoMetric(lastRpoMs);
                if (eventCounter.get() % 5000 == 0) {
                    logger.info("检测到RPO心跳: sourceTstamp={}, rpoMs={}, clockOffsetMs={}", timestamp, lastRpoMs, clockOffsetMs);
                }

                StringBuilder hbSb = new StringBuilder();
                hbSb.append("SYNC_HEARTBEAT").append(FIELD_SEP);
                hbSb.append(currentBinlogFile).append(FIELD_SEP);
                hbSb.append(currentBinlogPosition).append(FIELD_SEP);
                hbSb.append(timestamp).append(FIELD_SEP);
                hbSb.append(serverId).append(FIELD_SEP);
                hbSb.append(RECORD_SEP);
                writer.write(hbSb.toString());
                writer.flush();
                return;
            }

            boolean isDataEvent = (eventData instanceof WriteRowsEventData)
                    || (eventData instanceof UpdateRowsEventData)
                    || (eventData instanceof DeleteRowsEventData);
            if (isDataEvent) {
                // 双向防回环：当前事务带 origin 标记 → 是对端复制而来的写入，跳过、不回传
                if (loopGuard.shouldSkipReplicatedData()) {
                    return;
                }
                // 基于同步对象过滤：只捕获本任务相关的表事件，避免浪费网络和存储资源
                if (!shouldCaptureDataEvent(eventData)) {
                    return;
                }
                lastSourceEventTs = timestamp;
                long now = System.currentTimeMillis();
                long currentRpoMs = now - timestamp + clockOffsetMs;
                if (currentRpoMs >= 0 && (eventCounter.get() % 100 == 0 || now - lastRpoReportTime > RPO_REPORT_INTERVAL_MS)) {
                    lastRpoMs = currentRpoMs;
                    lastRpoReportTime = now;
                    writeRpoMetric(currentRpoMs);
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append(eventType).append(FIELD_SEP);
            sb.append(currentBinlogFile).append(FIELD_SEP);
            sb.append(currentBinlogPosition).append(FIELD_SEP);
            sb.append(timestamp).append(FIELD_SEP);
            sb.append(serverId).append(FIELD_SEP);

            String eventDataStr;
            if (eventData != null) {
                eventDataStr = serializeEventData(eventType, eventData);
            } else {
                eventDataStr = "";
            }
            eventDataStr = eventDataStr.replace("\n", " ").replace("\r", " ");
            sb.append(eventDataStr);

            sb.append(RECORD_SEP);

            writer.write(sb.toString());
            writer.flush();

            long count = eventCounter.incrementAndGet();
            currentFileEvents++;

            if (currentFileEvents >= maxEventsPerFile) {
                rotateOutputFile();
            }

            if (count % 1000 == 0) {
                logger.info("已捕获 {} 个事件, 当前位点: {}:{}", count, currentBinlogFile, currentBinlogPosition);
                savePosition();
            }
        } catch (Exception e) {
            logger.error("处理binlog事件异常: {}", e.getMessage(), e);
        }
    }

    private synchronized void openNewOutputFile() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());
        String fileName = String.format("binlog_%s_%04d.cap", timestamp, fileCounter.get());

        File outputFile = new File(outputDir, fileName);
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));
        currentFileEvents = 0;

        logger.info("打开新的捕获输出文件: {}", outputFile.getAbsolutePath());
    }

    private synchronized void rotateOutputFile() throws IOException {
        fileCounter.incrementAndGet();
        openNewOutputFile();
        logger.info("文件轮转, 已捕获 {} 个事件", maxEventsPerFile);
    }

    private String serializeEventData(String eventType, EventData eventData) {
        try {
            if (eventData instanceof WriteRowsEventData) {
                return serializeWriteRows((WriteRowsEventData) eventData);
            } else if (eventData instanceof UpdateRowsEventData) {
                return serializeUpdateRows((UpdateRowsEventData) eventData);
            } else if (eventData instanceof DeleteRowsEventData) {
                return serializeDeleteRows((DeleteRowsEventData) eventData);
            } else if (eventData instanceof TableMapEventData) {
                return eventData.toString();
            } else {
                return eventData.toString();
            }
        } catch (Exception e) {
            logger.warn("事件数据序列化失败, 回退到toString(): {}", e.getMessage());
            return eventData.toString();
        }
    }

    private String serializeWriteRows(WriteRowsEventData data) {
        StringBuilder sb = new StringBuilder();
        sb.append("WriteRowsEventData{tableId=").append(data.getTableId());
        sb.append(", includedColumns={").append(data.getIncludedColumns().toString()).append("}");
        sb.append(", rows=[");
        List<Serializable[]> rows = data.getRows();
        for (int r = 0; r < rows.size(); r++) {
            if (r > 0) sb.append(", ");
            sb.append("[");
            Serializable[] row = rows.get(r);
            for (int i = 0; i < row.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(serializeValue(row[i]));
            }
            sb.append("]");
        }
        sb.append("]}");
        return sb.toString();
    }

    private String serializeUpdateRows(UpdateRowsEventData data) {
        StringBuilder sb = new StringBuilder();
        sb.append("UpdateRowsEventData{tableId=").append(data.getTableId());
        sb.append(", includedColumnsBeforeUpdate={").append(data.getIncludedColumnsBeforeUpdate().toString()).append("}");
        sb.append(", includedColumns={").append(data.getIncludedColumns().toString()).append("}");
        sb.append(", rows=[");
        List<Map.Entry<Serializable[], Serializable[]>> rows = data.getRows();
        for (int r = 0; r < rows.size(); r++) {
            if (r > 0) sb.append(", ");
            Map.Entry<Serializable[], Serializable[]> entry = rows.get(r);
            sb.append("before=[");
            Serializable[] before = entry.getKey();
            for (int i = 0; i < before.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(serializeValue(before[i]));
            }
            sb.append("], after=[");
            Serializable[] after = entry.getValue();
            for (int i = 0; i < after.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(serializeValue(after[i]));
            }
            sb.append("]");
        }
        sb.append("]}");
        return sb.toString();
    }

    private String serializeDeleteRows(DeleteRowsEventData data) {
        StringBuilder sb = new StringBuilder();
        sb.append("DeleteRowsEventData{tableId=").append(data.getTableId());
        sb.append(", includedColumns={").append(data.getIncludedColumns().toString()).append("}");
        sb.append(", rows=[");
        List<Serializable[]> rows = data.getRows();
        for (int r = 0; r < rows.size(); r++) {
            if (r > 0) sb.append(", ");
            sb.append("[");
            Serializable[] row = rows.get(r);
            for (int i = 0; i < row.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(serializeValue(row[i]));
            }
            sb.append("]");
        }
        sb.append("]}");
        return sb.toString();
    }

    private String serializeValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[]) {
            return "0x" + bytesToHex((byte[]) value);
        }
        if (value instanceof java.sql.Timestamp) {
            java.sql.Timestamp ts = (java.sql.Timestamp) value;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
            return sdf.format(ts);
        }
        if (value instanceof java.sql.Date) {
            return value.toString();
        }
        if (value instanceof java.sql.Time) {
            // MySQL TIME 时长范围 ±838:59:59，不能用 HH:mm:ss 格式化（会对 24h 取模、丢符号）；
            // 直接从毫秒数（UTC 基准的午夜偏移）重建 HH:MM:SS，保留超过 24 小时与负号。
            long ms = ((java.sql.Time) value).getTime();
            boolean neg = ms < 0;
            long abs = Math.abs(ms);
            long h = abs / 3600000L;
            long m = (abs % 3600000L) / 60000L;
            long s = (abs % 60000L) / 1000L;
            return String.format("%s%02d:%02d:%02d", neg ? "-" : "", h, m, s);
        }
        if (value instanceof java.util.Date) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
            return sdf.format((java.util.Date) value);
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private void savePosition() {
        if (currentBinlogFile == null) return;

        File positionFile = new File(outputDir, "capture_position.properties");
        Properties posProps = new Properties();
        posProps.setProperty("binlog.file", currentBinlogFile);
        posProps.setProperty("binlog.position", String.valueOf(currentBinlogPosition));
        // GTID 连接模式下连接器持续维护已执行集，一并持久化（位点可视化/排障用）
        String currentGtidSet = client != null ? client.getGtidSet() : null;
        if (currentGtidSet != null && !currentGtidSet.isEmpty()) {
            posProps.setProperty("gtid.set", currentGtidSet.replaceAll("\\s+", ""));
        }
        posProps.setProperty("last.update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        try (FileOutputStream fos = new FileOutputStream(positionFile)) {
            posProps.store(fos, "Capture position for task: " + taskId);
        } catch (IOException e) {
            logger.warn("保存捕获位点失败: {}", e.getMessage());
        }
    }

    public String getCurrentBinlogFile() {
        return currentBinlogFile;
    }

    public long getCurrentBinlogPosition() {
        return currentBinlogPosition;
    }

    public long getEventCount() {
        return eventCounter.get();
    }

    public long getLastRpoMs() {
        return lastRpoMs;
    }

    private void initClockOffset() {
        try {
            String url = "jdbc:mysql://" + host + ":" + port + "/?useSSL=false&serverTimezone=UTC";
            try (Connection conn = DriverManager.getConnection(url, user, password);
                 Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery("SELECT UNIX_TIMESTAMP(NOW(3))*1000")) {
                if (rs.next()) {
                    long sourceDbTime = rs.getLong(1);
                    long localTime = System.currentTimeMillis();
                    clockOffsetMs = sourceDbTime - localTime;
                    logger.info("时钟偏移计算完成: sourceDbTime={}, localTime={}, offsetMs={}", sourceDbTime, localTime, clockOffsetMs);
                }
            }
        } catch (Exception e) {
            logger.warn("时钟偏移计算失败, 假设时钟已同步: {}", e.getMessage());
            clockOffsetMs = 0;
        }
    }

    private void startHeartbeat() {
        try {
            String db = (heartbeatDatabase != null && !heartbeatDatabase.isEmpty()) ? heartbeatDatabase : "mysql";
            String url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useSSL=false&serverTimezone=UTC";
            heartbeatConnection = DriverManager.getConnection(url, user, password);

            try (Statement stmt = heartbeatConnection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + HEARTBEAT_TABLE + " (id INT PRIMARY KEY, ts BIGINT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
                logger.info("心跳表已就绪: {}.{}", db, HEARTBEAT_TABLE);
            }
        } catch (Exception e) {
            logger.warn("初始化心跳表失败: {}", e.getMessage());
            return;
        }

        heartbeatThread = new Thread(() -> {
            logger.info("心跳线程已启动, 间隔={}ms", HEARTBEAT_INTERVAL_MS);
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                    if (!running) break;

                    try (Statement stmt = heartbeatConnection.createStatement()) {
                        stmt.execute("INSERT INTO " + HEARTBEAT_TABLE + " (id, ts) VALUES (1, UNIX_TIMESTAMP(NOW(3))*1000) " +
                                "ON DUPLICATE KEY UPDATE ts = UNIX_TIMESTAMP(NOW(3))*1000");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.warn("心跳写入失败: {}", e.getMessage());
                    try {
                        if (heartbeatConnection != null && !heartbeatConnection.isValid(2)) {
                            String db = (heartbeatDatabase != null && !heartbeatDatabase.isEmpty()) ? heartbeatDatabase : "mysql";
                            String url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useSSL=false&serverTimezone=UTC";
                            heartbeatConnection = DriverManager.getConnection(url, user, password);
                            logger.info("心跳连接已重新建立");
                        }
                    } catch (Exception ex) {
                        logger.warn("心跳连接重连失败: {}", ex.getMessage());
                    }
                }
            }
            logger.info("心跳线程已停止");
        }, "Heartbeat-" + taskId);
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private boolean isHeartbeatEvent(EventData eventData) {
        try {
            if (eventData instanceof UpdateRowsEventData) {
                UpdateRowsEventData data = (UpdateRowsEventData) eventData;
                String tableName = tableIdToNameMap.get(data.getTableId());
                return tableName != null && tableName.endsWith("." + HEARTBEAT_TABLE);
            } else if (eventData instanceof WriteRowsEventData) {
                WriteRowsEventData data = (WriteRowsEventData) eventData;
                String tableName = tableIdToNameMap.get(data.getTableId());
                return tableName != null && tableName.endsWith("." + HEARTBEAT_TABLE);
            }
        } catch (Exception e) {
            // ignore
        }
        return false;
    }

    private void writeRpoMetric(long rpoMs) {
        String metricsDir = outputDir;
        if (metricsDir == null) {
            metricsDir = "binlog_output";
        }
        File dir = new File(metricsDir);
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
}
