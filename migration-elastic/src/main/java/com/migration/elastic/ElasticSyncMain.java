package com.migration.elastic;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MySQL → Elasticsearch 数据同步（独立子进程，由 agent 拉起）。
 *
 * <p>与 SQL 侧的 capture/extract/increment 三进程管线不同，本模块是单进程两阶段
 * （模式与 migration-mongo 一致，完全不触碰既有管线）：
 * <ol>
 *   <li><b>全量</b>：逐表 JDBC 流式读取，_bulk 批量写入 ES（index 动作按 _id 覆盖，幂等可重跑）；
 *       索引名 = {库名}_{表名} 小写，文档 _id = 主键值（复合主键用 "_" 连接）；</li>
 *   <li><b>增量</b>（migration.mode=fullAndIncre）：直读 MySQL binlog（ROW 格式），
 *       INSERT/UPDATE → index 覆盖，DELETE → delete；binlog 位点持久化到 checkpoint 文件，
 *       进程重启后从位点续传（跳过全量）。</li>
 * </ol>
 *
 * <p>全量与增量的衔接：全量开始前记录 SHOW MASTER STATUS 位点，全量完成后从该位点重放——
 * 重放的 index/delete 与全量数据按 _id 幂等收敛，不丢不重。
 *
 * <p>无主键表：全量可同步（ES 自动生成 _id），但增量的 UPDATE/DELETE 无法定位文档，
 * 事件会被跳过并记警告——建议同步对象只勾选有主键的表。
 *
 * <p>进度通过 files/{taskId}/elastic_progress.json（原子写）暴露给 agent 的 ElasticSyncTask 轮询上报。
 */
public final class ElasticSyncMain {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSyncMain.class);
    private static final Gson gson = new Gson();

    private static final int FULL_BATCH_SIZE = 1000;
    private static final int POSITION_FLUSH_EVERY_EVENTS = 100;
    private static final long POSITION_FLUSH_INTERVAL_MS = 5000;

    private final String taskId;
    private final Properties props;
    /** 同步对象：db -> 表清单；空清单 = 整库（dbLevel） */
    private final Map<String, List<String>> syncObjects;
    private final Path progressPath;
    private final Path positionPath;

    /** 表元数据缓存：db.table -> meta（DDL 事件时失效） */
    private final Map<String, TableMeta> metaCache = new ConcurrentHashMap<>();

    // 进度状态
    private volatile String phase = "FULL";
    private volatile int totalTables;
    private volatile int completedTables;
    private volatile String currentTable = "";
    private volatile long copiedRows;
    private volatile long incrEvents;
    private volatile String error;

    private EsClient es;

    private ElasticSyncMain(String taskId, Properties props) {
        this.taskId = taskId;
        this.props = props;
        this.syncObjects = parseSyncObjects(props.getProperty("migration.sync.objects", ""));
        this.progressPath = Paths.get("files", taskId, "elastic_progress.json");
        this.positionPath = Paths.get("files", taskId, "checkpoint", "elastic_binlog_position.json");
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

        ElasticSyncMain sync = new ElasticSyncMain(taskId, props);
        try {
            sync.run();
        } catch (Exception e) {
            logger.error("Elastic 同步失败", e);
            sync.error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            sync.phase = "FAILED";
            sync.writeProgress();
            System.exit(1);
        }
    }

    private void run() throws Exception {
        boolean fullAndIncre = "fullAndIncre".equals(props.getProperty("migration.mode", "full"));
        logger.info("Elastic 同步启动: taskId={}, mode={}, syncObjects={}",
                taskId, fullAndIncre ? "fullAndIncre" : "full", syncObjects.keySet());

        es = new EsClient(
                props.getProperty("target.db.host", "localhost"),
                Integer.parseInt(props.getProperty("target.db.port", "9200")),
                props.getProperty("target.db.username", ""),
                com.migration.common.crypto.CredentialCipher.decrypt(props.getProperty("target.db.password", "")));
        logger.info("ES 连接成功: {}", es.info().getAsJsonObject("version").get("number").getAsString());

        // 增量起点：优先用 checkpoint 位点（断点续传，跳过全量）；否则全量前记录当前 master 位点
        BinlogPosition position = fullAndIncre ? loadPosition() : null;
        boolean resumedFromCheckpoint = position != null;

        try (Connection conn = openMysql()) {
            if (fullAndIncre && position == null) {
                position = currentMasterPosition(conn);
                logger.info("已记录增量起始位点（全量开始前）: {}:{}", position.file, position.position);
            }
            if (!resumedFromCheckpoint) {
                runFullCopy(conn);
            } else {
                logger.info("检测到 binlog checkpoint，跳过全量，直接从 {}:{} 续传增量", position.file, position.position);
            }
        }

        if (!fullAndIncre) {
            phase = "DONE";
            writeProgress();
            logger.info("仅全量任务完成");
            return;
        }

        phase = "INCREMENT";
        writeProgress();
        runBinlogStream(position);
    }

    // ==================== 全量 ====================

    private void runFullCopy(Connection conn) throws Exception {
        Map<String, List<String>> resolved = resolveTables(conn);
        totalTables = resolved.values().stream().mapToInt(List::size).sum();
        writeProgress();
        logger.info("全量开始: {} 个表", totalTables);

        for (Map.Entry<String, List<String>> e : resolved.entrySet()) {
            for (String table : e.getValue()) {
                currentTable = e.getKey() + "." + table;
                writeProgress();
                copyTable(conn, e.getKey(), table);
                completedTables++;
                writeProgress();
                logger.info("表 {} 全量完成 ({}/{})", currentTable, completedTables, totalTables);
            }
        }
        logger.info("全量完成: {} 个表, 共 {} 行", totalTables, copiedRows);
    }

    private void copyTable(Connection conn, String db, String table) throws Exception {
        TableMeta meta = tableMeta(conn, db, table);
        String index = indexName(db, table);
        es.createIndexIfAbsent(index);
        if (meta.pkIndexes.length == 0) {
            logger.warn("表 {}.{} 无主键：全量按自动 _id 写入，增量 UPDATE/DELETE 将被跳过", db, table);
        }

        List<String[]> batch = new ArrayList<>(FULL_BATCH_SIZE);
        // 流式读取：MySQL 驱动需 fetchSize=Integer.MIN_VALUE 才逐行拉取，避免大表 OOM
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM `" + db + "`.`" + table + "`")) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                int cols = rsMeta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    for (int i = 1; i <= cols; i++) {
                        doc.put(rsMeta.getColumnName(i), convertJdbcValue(rs.getObject(i)));
                    }
                    batch.add(EsClient.indexOp(index, docId(meta, doc), doc));
                    if (batch.size() >= FULL_BATCH_SIZE) {
                        flushBulk(batch);
                    }
                }
            }
        }
        flushBulk(batch);
    }

    private void flushBulk(List<String[]> batch) throws Exception {
        if (batch.isEmpty()) {
            return;
        }
        int failed = es.bulk(batch);
        if (failed > 0) {
            throw new RuntimeException("bulk 写入有 " + failed + " 条失败");
        }
        copiedRows += batch.size();
        batch.clear();
        writeProgress();
    }

    // ==================== 增量（binlog 直读） ====================

    private void runBinlogStream(BinlogPosition startPos) throws Exception {
        String host = props.getProperty("source.db.host", "localhost");
        int port = Integer.parseInt(props.getProperty("source.db.port", "3306"));
        String user = props.getProperty("source.db.username", "root");
        String password = com.migration.common.crypto.CredentialCipher.decrypt(
                props.getProperty("source.db.password", ""));

        logger.info("增量启动（binlog 直读），起始位点 {}:{}", startPos.file, startPos.position);

        // 元数据连接：binlog 行事件只有列序号，需要 information_schema 解析列名/类型/主键
        Connection metaConn = openMysql();

        BinaryLogClient client = new BinaryLogClient(host, port, user, password);
        // serverId 需在源库所有 replica 中唯一：与 capture 使用的号段（config capture.server.id）错开
        client.setServerId(50000 + Math.abs(taskId.hashCode() % 10000));
        client.setBinlogFilename(startPos.file);
        client.setBinlogPosition(startPos.position);

        final Map<Long, TableMapEventData> tableMap = new ConcurrentHashMap<>();
        final BinlogPosition current = new BinlogPosition(startPos.file, startPos.position);
        final long[] lastFlush = {System.currentTimeMillis()};
        final int[] sinceFlush = {0};

        client.registerEventListener(event -> {
            try {
                applyBinlogEvent(event, tableMap, metaConn, current);
            } catch (Exception e) {
                logger.error("应用 binlog 事件失败: {}", e.getMessage());
            }

            // 位点推进 + 周期性持久化（事件驱动即可：无事件时无新位点可存）
            if (event.getHeader() instanceof EventHeaderV4) {
                long next = ((EventHeaderV4) event.getHeader()).getNextPosition();
                if (next > 0) {
                    current.position = next;
                }
            }
            sinceFlush[0]++;
            long now = System.currentTimeMillis();
            if (sinceFlush[0] >= POSITION_FLUSH_EVERY_EVENTS || now - lastFlush[0] >= POSITION_FLUSH_INTERVAL_MS) {
                savePosition(current);
                writeProgress();
                lastFlush[0] = now;
                sinceFlush[0] = 0;
            }
        });

        // connect() 阻塞直至断开；通信失败由连接器自身的 keepalive 重连，进程级崩溃交给 ProcessGuard
        client.connect();
    }

    private void applyBinlogEvent(Event event, Map<Long, TableMapEventData> tableMap,
                                  Connection metaConn, BinlogPosition current) throws Exception {
        Object data = event.getData();
        if (data instanceof RotateEventData) {
            current.file = ((RotateEventData) data).getBinlogFilename();
            return;
        }
        if (data instanceof TableMapEventData) {
            TableMapEventData tm = (TableMapEventData) data;
            tableMap.put(tm.getTableId(), tm);
            return;
        }
        if (data instanceof QueryEventData) {
            // DDL：使列缓存失效（新表/改表下一次行事件时按需重查）；不向 ES 应用任何 DDL
            String sql = ((QueryEventData) data).getSql();
            if (sql != null && sql.toUpperCase().matches("(?s)\\s*(ALTER|CREATE|DROP|RENAME|TRUNCATE)\\s+TABLE.*")) {
                metaCache.clear();
                logger.info("检测到 DDL，已清空表元数据缓存: {}", sql.length() > 120 ? sql.substring(0, 120) : sql);
            }
            return;
        }

        EventType type = event.getHeader() instanceof EventHeaderV4
                ? ((EventHeaderV4) event.getHeader()).getEventType() : null;

        if (data instanceof WriteRowsEventData) {
            WriteRowsEventData w = (WriteRowsEventData) data;
            TableMapEventData tm = tableMap.get(w.getTableId());
            if (tm == null || !isSelected(tm.getDatabase(), tm.getTable())) {
                return;
            }
            TableMeta meta = tableMeta(metaConn, tm.getDatabase(), tm.getTable());
            List<String[]> ops = new ArrayList<>();
            for (Serializable[] row : w.getRows()) {
                Map<String, Object> doc = rowToDoc(meta, w.getIncludedColumns(), row);
                ops.add(EsClient.indexOp(indexName(tm.getDatabase(), tm.getTable()), docId(meta, doc), doc));
            }
            applyOps(ops);
        } else if (data instanceof UpdateRowsEventData) {
            UpdateRowsEventData u = (UpdateRowsEventData) data;
            TableMapEventData tm = tableMap.get(u.getTableId());
            if (tm == null || !isSelected(tm.getDatabase(), tm.getTable())) {
                return;
            }
            TableMeta meta = tableMeta(metaConn, tm.getDatabase(), tm.getTable());
            if (meta.pkIndexes.length == 0) {
                logger.warn("表 {}.{} 无主键，跳过 UPDATE 事件", tm.getDatabase(), tm.getTable());
                return;
            }
            String index = indexName(tm.getDatabase(), tm.getTable());
            List<String[]> ops = new ArrayList<>();
            for (Map.Entry<Serializable[], Serializable[]> pair : u.getRows()) {
                Map<String, Object> before = rowToDoc(meta, u.getIncludedColumnsBeforeUpdate(), pair.getKey());
                Map<String, Object> after = rowToDoc(meta, u.getIncludedColumns(), pair.getValue());
                String oldId = docId(meta, before);
                String newId = docId(meta, after);
                if (oldId != null && !oldId.equals(newId)) {
                    ops.add(EsClient.deleteOp(index, oldId)); // 主键变更：删旧建新
                }
                ops.add(EsClient.indexOp(index, newId, after));
            }
            applyOps(ops);
        } else if (data instanceof DeleteRowsEventData) {
            DeleteRowsEventData d = (DeleteRowsEventData) data;
            TableMapEventData tm = tableMap.get(d.getTableId());
            if (tm == null || !isSelected(tm.getDatabase(), tm.getTable())) {
                return;
            }
            TableMeta meta = tableMeta(metaConn, tm.getDatabase(), tm.getTable());
            if (meta.pkIndexes.length == 0) {
                logger.warn("表 {}.{} 无主键，跳过 DELETE 事件", tm.getDatabase(), tm.getTable());
                return;
            }
            String index = indexName(tm.getDatabase(), tm.getTable());
            List<String[]> ops = new ArrayList<>();
            for (Serializable[] row : d.getRows()) {
                Map<String, Object> doc = rowToDoc(meta, d.getIncludedColumns(), row);
                String id = docId(meta, doc);
                if (id != null) {
                    ops.add(EsClient.deleteOp(index, id));
                }
            }
            applyOps(ops);
        } else if (type != null && type == EventType.EXT_WRITE_ROWS) {
            logger.debug("未解析的行事件类型: {}", type);
        }
    }

    private void applyOps(List<String[]> ops) throws Exception {
        if (ops.isEmpty()) {
            return;
        }
        int failed = es.bulk(ops);
        if (failed > 0) {
            logger.error("增量 bulk 有 {} 条失败（幂等操作，可在后续事件收敛）", failed);
        }
        incrEvents += ops.size();
    }

    // ==================== 表元数据 / 行转文档 ====================

    private static final class TableMeta {
        final String[] columnNames;
        /** information_schema.COLUMNS.DATA_TYPE（小写，如 varchar/json/blob） */
        final String[] dataTypes;
        /** COLUMN_TYPE（含 unsigned / enum(...) 定义） */
        final String[] columnTypes;
        final int[] pkIndexes;

        TableMeta(String[] columnNames, String[] dataTypes, String[] columnTypes, int[] pkIndexes) {
            this.columnNames = columnNames;
            this.dataTypes = dataTypes;
            this.columnTypes = columnTypes;
            this.pkIndexes = pkIndexes;
        }
    }

    private TableMeta tableMeta(Connection conn, String db, String table) throws Exception {
        String key = db + "." + table;
        TableMeta cached = metaCache.get(key);
        if (cached != null) {
            return cached;
        }
        List<String> names = new ArrayList<>();
        List<String> dataTypes = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<Integer> pks = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY FROM information_schema.COLUMNS "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
            ps.setString(1, db);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    names.add(rs.getString(1));
                    dataTypes.add(rs.getString(2).toLowerCase());
                    columnTypes.add(rs.getString(3).toLowerCase());
                    if ("PRI".equalsIgnoreCase(rs.getString(4))) {
                        pks.add(i);
                    }
                    i++;
                }
            }
        }
        if (names.isEmpty()) {
            throw new RuntimeException("表 " + key + " 不存在或无列信息");
        }
        TableMeta meta = new TableMeta(
                names.toArray(new String[0]),
                dataTypes.toArray(new String[0]),
                columnTypes.toArray(new String[0]),
                pks.stream().mapToInt(Integer::intValue).toArray());
        metaCache.put(key, meta);
        return meta;
    }

    private Map<String, Object> rowToDoc(TableMeta meta, BitSet included, Serializable[] row) {
        Map<String, Object> doc = new LinkedHashMap<>();
        int rowIdx = 0;
        for (int col = included.nextSetBit(0); col >= 0 && rowIdx < row.length; col = included.nextSetBit(col + 1)) {
            if (col < meta.columnNames.length) {
                doc.put(meta.columnNames[col],
                        convertBinlogValue(meta.dataTypes[col], meta.columnTypes[col], row[rowIdx]));
            }
            rowIdx++;
        }
        return doc;
    }

    /** 文档 _id：主键值 join "_"；无主键返回 null（ES 自动生成）。 */
    private String docId(TableMeta meta, Map<String, Object> doc) {
        if (meta.pkIndexes.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int pk : meta.pkIndexes) {
            Object v = doc.get(meta.columnNames[pk]);
            if (sb.length() > 0) {
                sb.append('_');
            }
            sb.append(v);
        }
        return sb.toString();
    }

    // ==================== 值转换 ====================

    private static final ThreadLocal<SimpleDateFormat> ISO_DATETIME = ThreadLocal.withInitial(() -> {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        f.setTimeZone(TimeZone.getTimeZone("UTC"));
        return f;
    });

    /** JDBC 全量侧：驱动已按列类型给出 Java 对象，只需归一时间/二进制。 */
    private Object convertJdbcValue(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) v).toLocalDateTime().toString();
        }
        if (v instanceof java.sql.Date || v instanceof java.sql.Time) {
            return v.toString();
        }
        if (v instanceof java.time.LocalDateTime || v instanceof java.time.OffsetDateTime) {
            return v.toString();
        }
        if (v instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) v);
        }
        return v;
    }

    /** binlog 增量侧：连接器交付的原始类型 + 列类型信息 → 与全量一致的 JSON 值。 */
    private Object convertBinlogValue(String dataType, String columnType, Serializable v) {
        if (v == null) {
            return null;
        }
        if (v instanceof byte[]) {
            byte[] bytes = (byte[]) v;
            if ("json".equals(dataType)) {
                try {
                    return gson.fromJson(
                            com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary.parseAsString(bytes),
                            Object.class);
                } catch (Exception e) {
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            }
            // binlog 协议不区分文本与二进制字节：按列类型分流
            if (dataType.contains("blob") || dataType.contains("binary")) {
                return Base64.getEncoder().encodeToString(bytes);
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (v instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) v).toLocalDateTime().toString();
        }
        if (v instanceof java.util.Date) {
            if ("date".equals(dataType)) {
                SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                f.setTimeZone(TimeZone.getTimeZone("UTC"));
                return f.format((java.util.Date) v);
            }
            return ISO_DATETIME.get().format((java.util.Date) v);
        }
        if (v instanceof Integer && "enum".equals(dataType)) {
            String[] options = parseEnumOptions(columnType);
            int idx = (Integer) v;
            return idx >= 1 && idx <= options.length ? options[idx - 1] : v;
        }
        if (v instanceof Long && "set".equals(dataType)) {
            String[] options = parseEnumOptions(columnType);
            long mask = (Long) v;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < options.length; i++) {
                if ((mask & (1L << i)) != 0) {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(options[i]);
                }
            }
            return sb.toString();
        }
        // unsigned 整型修正：binlog 按带符号交付，负值需按位宽回绕
        if (columnType.contains("unsigned")) {
            if (v instanceof Integer && (Integer) v < 0) {
                return ((Integer) v) + 4294967296L;
            }
            if (v instanceof Long && (Long) v < 0) {
                return BigInteger.valueOf((Long) v).add(BigInteger.ONE.shiftLeft(64));
            }
        }
        if (v instanceof BitSet) {
            BitSet bits = (BitSet) v;
            long value = 0;
            for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
                value |= (1L << i);
            }
            return value;
        }
        return v;
    }

    /** 解析 enum('a','b')/set('a','b') 的选项列表。 */
    static String[] parseEnumOptions(String columnType) {
        int start = columnType.indexOf('(');
        int end = columnType.lastIndexOf(')');
        if (start < 0 || end <= start) {
            return new String[0];
        }
        String[] raw = columnType.substring(start + 1, end).split(",");
        String[] options = new String[raw.length];
        for (int i = 0; i < raw.length; i++) {
            options[i] = raw[i].trim().replaceAll("^'|'$", "");
        }
        return options;
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
                        result.put(e.getKey(), new ArrayList<>());
                    } else if (m.get("tables") instanceof List) {
                        List<String> tables = new ArrayList<>();
                        for (Object t : (List<?>) m.get("tables")) {
                            tables.add(String.valueOf(t));
                        }
                        result.put(e.getKey(), tables);
                    }
                } else if (v instanceof List) {
                    List<String> tables = new ArrayList<>();
                    for (Object t : (List<?>) v) {
                        tables.add(String.valueOf(t));
                    }
                    result.put(e.getKey(), tables);
                }
            }
        } catch (Exception ex) {
            logger.warn("解析 sync objects 失败: {}", ex.getMessage());
        }
        return result;
    }

    /** 全量阶段：整库（空清单）时枚举源库当前全部表。 */
    private Map<String, List<String>> resolveTables(Connection conn) throws Exception {
        Map<String, List<String>> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : syncObjects.entrySet()) {
            if (e.getValue().isEmpty()) {
                List<String> tables = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT TABLE_NAME FROM information_schema.TABLES "
                                + "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'")) {
                    ps.setString(1, e.getKey());
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            tables.add(rs.getString(1));
                        }
                    }
                }
                resolved.put(e.getKey(), tables);
            } else {
                resolved.put(e.getKey(), e.getValue());
            }
        }
        return resolved;
    }

    /** 增量事件过滤：整库模式接受该库全部表（含全量后新建的），表级只接受清单内。 */
    private boolean isSelected(String db, String table) {
        List<String> tables = syncObjects.get(db);
        if (tables == null) {
            return false;
        }
        return tables.isEmpty() || tables.contains(table);
    }

    /** ES 索引名：{db}_{table} 小写（ES 索引名不允许大写）。 */
    static String indexName(String db, String table) {
        return (db + "_" + table).toLowerCase();
    }

    // ==================== 连接 / checkpoint / 进度 ====================

    private Connection openMysql() throws Exception {
        String host = props.getProperty("source.db.host", "localhost");
        String port = props.getProperty("source.db.port", "3306");
        String user = props.getProperty("source.db.username", "root");
        String password = com.migration.common.crypto.CredentialCipher.decrypt(
                props.getProperty("source.db.password", ""));
        // tinyInt1isBit=false：TINYINT(1) 按数字交付（与 binlog 增量侧一致）。驱动默认把
        // TINYINT(1) 猜成 Boolean，会让全量建出 boolean mapping，而增量 binlog 给 1/0 数字，
        // 两侧口径不一致会导致增量写入被 ES 以类型冲突拒绝。
        String url = String.format("jdbc:mysql://%s:%s/?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
                + "&allowPublicKeyRetrieval=true&connectTimeout=15000&tinyInt1isBit=false", host, port);
        return DriverManager.getConnection(url, user, password);
    }

    private static final class BinlogPosition {
        volatile String file;
        volatile long position;

        BinlogPosition(String file, long position) {
            this.file = file;
            this.position = position;
        }
    }

    private BinlogPosition currentMasterPosition(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {
            if (rs.next()) {
                return new BinlogPosition(rs.getString("File"), rs.getLong("Position"));
            }
        }
        throw new RuntimeException("SHOW MASTER STATUS 无结果：源库未开启 binlog");
    }

    private BinlogPosition loadPosition() {
        try {
            File f = positionPath.toFile();
            if (!f.exists()) {
                return null;
            }
            String json = new String(Files.readAllBytes(positionPath), StandardCharsets.UTF_8);
            Map<String, Object> m = gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
            String file = (String) m.get("file");
            Number pos = (Number) m.get("position");
            if (file == null || pos == null) {
                return null;
            }
            return new BinlogPosition(file, pos.longValue());
        } catch (Exception e) {
            logger.warn("读取 binlog 位点失败，将重新记录: {}", e.getMessage());
            return null;
        }
    }

    private void savePosition(BinlogPosition pos) {
        try {
            Files.createDirectories(positionPath.getParent());
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("file", pos.file);
            m.put("position", pos.position);
            Path tmp = positionPath.resolveSibling(positionPath.getFileName() + ".tmp");
            Files.write(tmp, gson.toJson(m).getBytes(StandardCharsets.UTF_8));
            Files.move(tmp, positionPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            logger.warn("持久化 binlog 位点失败: {}", e.getMessage());
        }
    }

    private void writeProgress() {
        try {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("phase", phase);
            p.put("totalTables", totalTables);
            p.put("completedTables", completedTables);
            p.put("currentTable", currentTable);
            p.put("copiedRows", copiedRows);
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
