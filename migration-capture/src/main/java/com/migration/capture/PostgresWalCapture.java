package com.migration.capture;

import com.migration.common.AbstractCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class PostgresWalCapture extends AbstractCapture<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(PostgresWalCapture.class);

    private static final char FIELD_SEP = '\001';
    private static final char RECORD_SEP = '\n';

    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private String startLsn;
    private String outputDir;
    private String taskId;
    private String slotName;
    private String publicationName;

    private Connection conn;
    private PGReplicationStream replicationStream;
    private BufferedWriter writer;
    private final AtomicLong eventCounter = new AtomicLong(0);
    private final AtomicLong fileCounter = new AtomicLong(0);
    private long maxEventsPerFile = 10000;
    private long currentFileEvents = 0;

    private volatile String currentLsn;
    private volatile long currentLsnNumeric;

    // 背压控制：extract 通过信号文件通知 capture 暂停/恢复
    private volatile boolean backpressurePaused = false;
    private String backpressureSignalPath;

    @Override
    protected void doInitialize() throws Exception {
        host = props.getProperty("source.db.host", "localhost");
        port = Integer.parseInt(props.getProperty("source.db.port", "5432"));
        database = props.getProperty("source.db.database", "postgres");
        user = props.getProperty("source.db.username", "postgres");
        password = props.getProperty("source.db.password", "");
        startLsn = props.getProperty("capture.wal.lsn", "");
        outputDir = props.getProperty("capture.output.dir", "binlog_output");
        taskId = props.getProperty("task.id", "unknown");
        maxEventsPerFile = Long.parseLong(props.getProperty("capture.max.events.per.file", "10000"));
        slotName = props.getProperty("capture.wal.slot.name", "migration_slot_" + taskId.replaceAll("[^a-z0-9_]", "_"));
        publicationName = props.getProperty("capture.wal.publication.name", "migration_pub_" + taskId.replaceAll("[^a-z0-9_]", "_"));

        backpressureSignalPath = "files/" + taskId + "/backpressure.signal";

        if (startLsn.isEmpty()) {
            startLsn = null;
        }

        logger.info("PostgreSQL WAL Capture initialized - host={}:{} database={} user={} outputDir={} taskId={} startLsn={} slotName={}",
                host, port, database, user, outputDir, taskId, startLsn, slotName);
    }

    @Override
    protected void doStart() throws Exception {
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        openNewOutputFile();

        Class.forName("org.postgresql.Driver");

        ensureReplicationSlot();
        ensurePublication();

        String url = String.format("jdbc:postgresql://%s:%d/%s?replication=database&stringtype=unspecified",
                host, port, database);
        Properties connProps = new Properties();
        connProps.setProperty("user", user);
        connProps.setProperty("password", password);
        connProps.setProperty("preferQueryMode", "simple");
        connProps.setProperty("assumeMinServerVersion", "14");

        conn = DriverManager.getConnection(url, connProps);
        conn.setAutoCommit(false);

        logger.info("Connected to PostgreSQL for logical replication on database: {}", database);

        PGConnection pgConn = conn.unwrap(PGConnection.class);

        org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder builder = pgConn
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", publicationName)
                .withStatusInterval(1000, java.util.concurrent.TimeUnit.MILLISECONDS);

        if (startLsn != null && !startLsn.isEmpty()) {
            try {
                LogSequenceNumber lsn = LogSequenceNumber.valueOf(startLsn);
                builder.withStartPosition(lsn);
                logger.info("Starting WAL replication from LSN: {}", startLsn);
            } catch (Exception e) {
                logger.warn("Failed to parse start LSN '{}', starting from current position: {}", startLsn, e.getMessage());
            }
        } else {
            logger.info("Starting WAL replication from current position");
        }

        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                replicationStream = builder.start();
                logger.info("WAL replication stream started on attempt {}", attempt);
                break;
            } catch (org.postgresql.util.PSQLException e) {
                if (e.getMessage() != null && e.getMessage().contains("is active for PID") && attempt < maxRetries) {
                    logger.warn("Replication slot is active (attempt {}/{}), will retry after cleanup: {}", 
                        attempt, maxRetries, e.getMessage());
                    try {
                        conn.close();
                    } catch (Exception ce) {
                        logger.warn("Error closing connection: {}", ce.getMessage());
                    }
                    Thread.sleep(2000);
                    ensureReplicationSlot();
                    String retryUrl = String.format("jdbc:postgresql://%s:%d/%s?replication=database&stringtype=unspecified",
                            host, port, database);
                    conn = DriverManager.getConnection(retryUrl, connProps);
                    conn.setAutoCommit(false);
                    PGConnection retryPgConn = conn.unwrap(PGConnection.class);
                    builder = retryPgConn
                            .getReplicationAPI()
                            .replicationStream()
                            .logical()
                            .withSlotName(slotName)
                            .withSlotOption("proto_version", 1)
                            .withSlotOption("publication_names", publicationName)
                            .withStatusInterval(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (startLsn != null && !startLsn.isEmpty()) {
                        try {
                            LogSequenceNumber lsn = LogSequenceNumber.valueOf(startLsn);
                            builder.withStartPosition(lsn);
                        } catch (Exception le) {
                            logger.warn("Failed to parse start LSN '{}': {}", startLsn, le.getMessage());
                        }
                    }
                } else {
                    throw e;
                }
            }
        }

        Thread replicateThread = new Thread(() -> {
            int consecutiveErrors = 0;
            long lastDataTime = System.currentTimeMillis();
            long lastLivenessCheck = System.currentTimeMillis();
            while (running) {
                try {
                    if (replicationStream == null) {
                        logger.warn("WAL replication stream is null, attempting to reconnect...");
                        reconnectReplication();
                        consecutiveErrors = 0;
                        lastDataTime = System.currentTimeMillis();
                    }

                    long now = System.currentTimeMillis();
                    if (now - lastLivenessCheck > 30000) {
                        lastLivenessCheck = now;
                        if (!isReplicationSlotActive()) {
                            logger.warn("Replication slot is not active in PG, forcing reconnect...");
                            replicationStream = null;
                            continue;
                        }
                    }

                    if (now - lastDataTime > 60000) {
                        logger.warn("No WAL data received for 60s, checking connection liveness...");
                        if (!isReplicationSlotActive()) {
                            logger.warn("Replication slot not active, forcing reconnect...");
                            replicationStream = null;
                            continue;
                        }
                        lastDataTime = now;
                    }

                    ByteBuffer msgBuffer = replicationStream.readPending();
                    if (msgBuffer != null) {
                        processWalMessage(msgBuffer);
                        consecutiveErrors = 0;
                        lastDataTime = System.currentTimeMillis();
                    } else {
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    consecutiveErrors++;
                    if (running) {
                        logger.error("Error in WAL replication stream (consecutive: {}): {}", consecutiveErrors, e.getMessage());
                        if (consecutiveErrors >= 5) {
                            logger.warn("Too many consecutive errors, forcing reconnect...");
                            try {
                                replicationStream = null;
                                reconnectReplication();
                                consecutiveErrors = 0;
                                lastDataTime = System.currentTimeMillis();
                            } catch (Exception re) {
                                logger.error("Failed to reconnect: {}", re.getMessage());
                            }
                        } else {
                            try {
                                Thread.sleep(1000 * consecutiveErrors);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }
            }
            try {
                if (replicationStream != null) replicationStream.close();
            } catch (Exception e) {
                logger.warn("Error closing replication stream: {}", e.getMessage());
            }
        }, "WAL-Replication-" + taskId);
        replicateThread.setDaemon(true);
        replicateThread.start();

        startBackpressureMonitor();

        logger.info("WAL replication stream started for slot: {}", slotName);
    }

    /**
     * 检查背压信号文件，更新 backpressurePaused 状态。
     * extract 进程在 THL 积压时写入 PAUSE 信号，积压解除后写入 RESUME。
     */
    private void checkBackpressureSignal() {
        if (backpressureSignalPath == null) return;
        File signalFile = new File(backpressureSignalPath);
        if (!signalFile.exists()) {
            backpressurePaused = false;
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(signalFile))) {
            String firstLine = reader.readLine();
            boolean shouldPause = firstLine != null && "PAUSE".equalsIgnoreCase(firstLine.trim());
            if (shouldPause != backpressurePaused) {
                backpressurePaused = shouldPause;
                if (shouldPause) {
                    logger.warn("收到背压暂停信号，暂停 WAL 事件处理");
                } else {
                    logger.info("收到背压恢复信号，恢复 WAL 事件处理");
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

    private boolean isReplicationSlotActive() {
        try {
            String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, database);
            try (Connection checkConn = DriverManager.getConnection(url, user, password)) {
                String connCatalog = checkConn.getCatalog();
                logger.info("Liveness check connected to: host={}:{} db={} connCatalog={}", host, port, database, connCatalog);
                try (Statement stmt = checkConn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT slot_name, active, active_pid, database FROM pg_replication_slots")) {
                    while (rs.next()) {
                        String sn = rs.getString("slot_name");
                        boolean act = rs.getBoolean("active");
                        int apid = rs.getInt("active_pid");
                        String db = rs.getString("database");
                        logger.info("  Found slot: name={} active={} activePid={} database={}", sn, act, apid, db);
                    }
                }
                try (Statement stmt = checkConn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT slot_name, active, active_pid FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                    if (rs.next()) {
                        boolean active = rs.getBoolean("active");
                        int activePid = rs.getInt("active_pid");
                        logger.info("Liveness check: slot '{}' active={}, activePid={}", slotName, active, activePid);
                        return active;
                    }
                    logger.warn("Liveness check: slot '{}' not found in pg_replication_slots", slotName);
                    return false;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to check replication slot liveness: {}", e.getMessage());
            return false;
        }
    }

    private synchronized void reconnectReplication() throws Exception {
        logger.info("Reconnecting WAL replication stream...");

        try {
            if (replicationStream != null) {
                try { replicationStream.close(); } catch (Exception e) { /* ignore */ }
            }
        } catch (Exception e) { /* ignore */ }

        try {
            if (conn != null && !conn.isClosed()) {
                try { conn.close(); } catch (Exception e) { /* ignore */ }
            }
        } catch (Exception e) { /* ignore */ }

        ensureReplicationSlot();

        String url = String.format("jdbc:postgresql://%s:%d/%s?replication=database&stringtype=unspecified",
                host, port, database);
        Properties connProps = new Properties();
        connProps.setProperty("user", user);
        connProps.setProperty("password", password);
        connProps.setProperty("preferQueryMode", "simple");
        connProps.setProperty("assumeMinServerVersion", "14");

        conn = DriverManager.getConnection(url, connProps);
        conn.setAutoCommit(false);

        PGConnection pgConn = conn.unwrap(PGConnection.class);

        org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder builder = pgConn
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", publicationName)
                .withStatusInterval(1000, java.util.concurrent.TimeUnit.MILLISECONDS);

        if (currentLsn != null && !currentLsn.isEmpty()) {
            try {
                LogSequenceNumber lsn = LogSequenceNumber.valueOf(currentLsn);
                builder.withStartPosition(lsn);
                logger.info("Reconnecting WAL replication from last known LSN: {}", currentLsn);
            } catch (Exception e) {
                logger.warn("Failed to parse current LSN '{}', starting from current position: {}", currentLsn, e.getMessage());
            }
        }

        replicationStream = builder.start();
        logger.info("WAL replication stream reconnected successfully from LSN: {}", currentLsn);
    }

    private void ensureReplicationSlot() throws Exception {
        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, database);
        try (Connection checkConn = DriverManager.getConnection(url, user, password);
             Statement stmt = checkConn.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                "SELECT slot_name, active, active_pid FROM pg_replication_slots WHERE slot_name = '" + slotName + "'");
            if (rs.next()) {
                boolean active = rs.getBoolean("active");
                int activePid = rs.getInt("active_pid");
                if (active && activePid > 0) {
                    logger.warn("Replication slot '{}' is active for PID {}, terminating backend", slotName, activePid);
                    try {
                        stmt.execute("SELECT pg_terminate_backend(" + activePid + ")");
                    } catch (Exception e) {
                        logger.warn("Failed to terminate backend PID {}: {}", activePid, e.getMessage());
                    }
                    Thread.sleep(500);
                    try {
                        stmt.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
                        logger.info("Dropped active replication slot '{}', will recreate", slotName);
                        stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
                        logger.info("Recreated replication slot '{}' with pgoutput plugin", slotName);
                    } catch (Exception e) {
                        logger.warn("Failed to drop/recreate slot '{}': {}", slotName, e.getMessage());
                    }
                } else {
                    logger.info("Replication slot '{}' already exists (inactive)", slotName);
                }
            } else {
                stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
                logger.info("Created replication slot '{}' with pgoutput plugin", slotName);
            }
        }
    }

    private void ensurePublication() throws Exception {
        String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, database);
        try (Connection checkConn = DriverManager.getConnection(url, user, password);
             Statement stmt = checkConn.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                "SELECT pubname FROM pg_publication WHERE pubname = '" + publicationName + "'");
            if (rs.next()) {
                logger.info("Publication '{}' already exists", publicationName);
            } else {
                stmt.execute("CREATE PUBLICATION \"" + publicationName + "\" FOR ALL TABLES");
                logger.info("Created publication '{}' for all tables", publicationName);
            }
        }
    }

    private void processWalMessage(ByteBuffer msgBuffer) {
        if (!running) return;

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
            LogSequenceNumber receiveLsn = replicationStream.getLastReceiveLSN();
            currentLsn = receiveLsn.asString();
            currentLsnNumeric = receiveLsn.asLong();

            byte[] data = new byte[msgBuffer.remaining()];
            msgBuffer.get(data);

            String walData = new String(data, StandardCharsets.UTF_8);

            String parsedEvent = parsePgoutputMessage(data);
            String eventType;
            String eventDataStr;

            if (parsedEvent != null && !parsedEvent.isEmpty()) {
                eventType = parseWalEventType(parsedEvent);
                if ("WAL_EVENT".equals(eventType)) {
                    eventType = parseWalEventType(walData);
                }
                eventDataStr = parsedEvent;
            } else {
                eventType = parseWalEventType(walData);
                eventDataStr = walData.replace("\n", " ").replace("\r", " ");
            }

            long timestamp = System.currentTimeMillis();
            long xid = 0;

            StringBuilder sb = new StringBuilder();
            sb.append(eventType).append(FIELD_SEP);
            sb.append(currentLsn).append(FIELD_SEP);
            sb.append(currentLsnNumeric).append(FIELD_SEP);
            sb.append(timestamp).append(FIELD_SEP);
            sb.append(xid).append(FIELD_SEP);
            sb.append(eventDataStr);
            sb.append(RECORD_SEP);

            writer.write(sb.toString());
            writer.flush();

            replicationStream.setAppliedLSN(receiveLsn);
            replicationStream.setFlushedLSN(receiveLsn);

            long count = eventCounter.incrementAndGet();
            currentFileEvents++;

            if (currentFileEvents >= maxEventsPerFile) {
                rotateOutputFile();
            }

            if (count % 1000 == 0) {
                logger.info("Captured {} WAL events, current LSN: {}", count, currentLsn);
                savePosition();
            }
        } catch (Exception e) {
            logger.error("Error processing WAL message: {}", e.getMessage(), e);
        }
    }

    private String parsePgoutputMessage(byte[] data) {
        if (data == null || data.length == 0) return "";

        try {
            int offset = 0;
            char msgType = (char) (data[offset] & 0xFF);
            offset++;

            switch (msgType) {
                case 'B':
                    return parseBeginMessage(data, offset);
                case 'C':
                    return parseCommitMessage(data, offset);
                case 'R':
                    return parseRelationMessage(data, offset);
                case 'I':
                    return parseInsertMessage(data, offset);
                case 'U':
                    return parseUpdateMessage(data, offset);
                case 'D':
                    return parseDeleteMessage(data, offset);
                case 'O':
                case 'T':
                    return "";
                default:
                    return "";
            }
        } catch (Exception e) {
            logger.debug("Error parsing pgoutput message: {}", e.getMessage());
            return "";
        }
    }

    private String parseBeginMessage(byte[] data, int offset) {
        try {
            long lsn = readInt64BE(data, offset); offset += 8;
            long commitTime = readInt64BE(data, offset); offset += 8;
            long xid = readInt32BE(data, offset);
            return "BEGIN lsn=" + lsn + " transaction_id:" + xid;
        } catch (Exception e) {
            return "BEGIN";
        }
    }

    private String parseCommitMessage(byte[] data, int offset) {
        try {
            int flags = readInt8(data, offset); offset++;
            long lsn = readInt64BE(data, offset); offset += 8;
            long endLsn = readInt64BE(data, offset); offset += 8;
            long commitTime = readInt64BE(data, offset); offset += 8;
            long xid = readInt32BE(data, offset);
            return "COMMIT transaction_id:" + xid;
        } catch (Exception e) {
            return "COMMIT";
        }
    }

    private String parseRelationMessage(byte[] data, int offset) {
        try {
            long relationId = readInt32BE(data, offset); offset += 4;
            String schema = readCString(data, offset);
            offset += schema.length() + 1;
            String table = readCString(data, offset);
            offset += table.length() + 1;

            logger.debug("Relation message: {}.{} (oid={})", schema, table, relationId);
            return "RELATION schema:" + schema + " table:" + table + " oid:" + relationId;
        } catch (Exception e) {
            return "";
        }
    }

    private String parseInsertMessage(byte[] data, int offset) {
        try {
            long relationId = readInt32BE(data, offset); offset += 4;
            char tupleType = (char) (data[offset] & 0xFF); offset++;

            String[] schemaTable = resolveRelationId(relationId);
            String schema = schemaTable[0];
            String table = schemaTable[1];

            List<String> columnNames = fetchTableColumns(schema, table);
            List<String> columnTypes = fetchTableColumnTypes(schema, table);
            List<String> pkColumns = fetchTablePrimaryKeys(schema, table);

            List<String> values = parseTupleData(data, offset, columnNames, columnTypes);

            StringBuilder sb = new StringBuilder();
            sb.append("schema:").append(schema).append(" table:").append(table);
            if (!pkColumns.isEmpty()) {
                sb.append(" primary_keys:").append(String.join(",", pkColumns));
            }
            sb.append(" new-tuple:{");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sb.append(",");
                String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                sb.append(colName).append(":").append(values.get(i) != null ? values.get(i) : "[null]");
            }
            sb.append("}");

            return sb.toString();
        } catch (Exception e) {
            logger.error("Error parsing INSERT message: {}", e.getMessage());
            return "INSERT relation_id=" + (offset > 0 ? "unknown" : "unknown");
        }
    }

    private String parseUpdateMessage(byte[] data, int offset) {
        try {
            long relationId = readInt32BE(data, offset); offset += 4;

            String[] schemaTable = resolveRelationId(relationId);
            String schema = schemaTable[0];
            String table = schemaTable[1];

            List<String> columnNames = fetchTableColumns(schema, table);
            List<String> columnTypes = fetchTableColumnTypes(schema, table);
            List<String> pkColumns = fetchTablePrimaryKeys(schema, table);

            List<String> oldValues = null;
            List<String> newValues = null;

            while (offset < data.length) {
                char tupleType = (char) (data[offset] & 0xFF); offset++;

                if (tupleType == 'K' || tupleType == 'O') {
                    oldValues = parseTupleData(data, offset, columnNames, columnTypes);
                    offset = advancePastTuple(data, offset);
                } else if (tupleType == 'N') {
                    newValues = parseTupleData(data, offset, columnNames, columnTypes);
                    offset = advancePastTuple(data, offset);
                } else {
                    break;
                }
            }

            if (newValues == null && oldValues != null) {
                newValues = oldValues;
                oldValues = null;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("schema:").append(schema).append(" table:").append(table);
            if (!pkColumns.isEmpty()) {
                sb.append(" primary_keys:").append(String.join(",", pkColumns));
            }

            if (oldValues != null) {
                sb.append(" old-tuple:{");
                for (int i = 0; i < oldValues.size(); i++) {
                    if (i > 0) sb.append(",");
                    String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                    sb.append(colName).append(":").append(oldValues.get(i) != null ? oldValues.get(i) : "[null]");
                }
                sb.append("}");
            }

            if (newValues != null) {
                sb.append(" new-tuple:{");
                for (int i = 0; i < newValues.size(); i++) {
                    if (i > 0) sb.append(",");
                    String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                    sb.append(colName).append(":").append(newValues.get(i) != null ? newValues.get(i) : "[null]");
                }
                sb.append("}");
            }

            return sb.toString();
        } catch (Exception e) {
            logger.error("Error parsing UPDATE message: {}", e.getMessage());
            return "UPDATE";
        }
    }

    private int advancePastTuple(byte[] data, int offset) {
        try {
            int numCols = readInt16BE(data, offset); offset += 2;
            for (int i = 0; i < numCols; i++) {
                if (offset >= data.length) break;
                char colFlag = (char) (data[offset] & 0xFF); offset++;
                if (colFlag == 't') {
                    int colLen = readInt32BE(data, offset); offset += 4;
                    offset += colLen;
                }
            }
        } catch (Exception e) {
            logger.debug("Error advancing past tuple: {}", e.getMessage());
        }
        return offset;
    }

    private String parseDeleteMessage(byte[] data, int offset) {
        try {
            long relationId = readInt32BE(data, offset); offset += 4;
            char tupleType = (char) (data[offset] & 0xFF); offset++;

            String[] schemaTable = resolveRelationId(relationId);
            String schema = schemaTable[0];
            String table = schemaTable[1];

            List<String> columnNames = fetchTableColumns(schema, table);
            List<String> columnTypes = fetchTableColumnTypes(schema, table);
            List<String> pkColumns = fetchTablePrimaryKeys(schema, table);

            List<String> oldValues = parseTupleData(data, offset, columnNames, columnTypes);

            StringBuilder sb = new StringBuilder();
            sb.append("schema:").append(schema).append(" table:").append(table);
            if (!pkColumns.isEmpty()) {
                sb.append(" primary_keys:").append(String.join(",", pkColumns));
            }
            sb.append(" old-tuple:{");
            for (int i = 0; i < oldValues.size(); i++) {
                if (i > 0) sb.append(",");
                String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                sb.append(colName).append(":").append(oldValues.get(i) != null ? oldValues.get(i) : "[null]");
            }
            sb.append("}");

            return sb.toString();
        } catch (Exception e) {
            logger.error("Error parsing DELETE message: {}", e.getMessage());
            return "DELETE";
        }
    }

    private List<String> parseTupleData(byte[] data, int offset, List<String> columnNames, List<String> columnTypes) {
        List<String> values = new ArrayList<>();
        try {
            int numCols = readInt16BE(data, offset); offset += 2;
            logger.debug("parseTupleData: numCols={}, dataLen={}, offset={}", numCols, data.length, offset);

            for (int i = 0; i < numCols; i++) {
                char colFlag = (char) (data[offset] & 0xFF); offset++;
                String colName = (i < columnNames.size()) ? columnNames.get(i) : "col" + i;
                logger.debug("  col[{}] name={} flag='{}' (0x{}) offset={}", i, colName, colFlag, Integer.toHexString(colFlag), offset);
                if (colFlag == 't') {
                    int colLen = readInt32BE(data, offset); offset += 4;
                    byte[] colData = new byte[colLen];
                    System.arraycopy(data, offset, colData, 0, colLen);
                    offset += colLen;

                    String colType = (i < columnTypes.size()) ? columnTypes.get(i) : "";
                    String value = formatColumnValue(colData, colType, colName);
                    logger.debug("  col[{}] value={}", i, value);
                    values.add(value);
                } else if (colFlag == 'n') {
                    logger.debug("  col[{}] NULL", i);
                    values.add(null);
                } else if (colFlag == 'u') {
                    logger.debug("  col[{}] UNCHANGED_TOAST", i);
                    values.add(null);
                } else {
                    logger.debug("  col[{}] UNKNOWN_FLAG={}", i, (int)colFlag);
                    values.add(null);
                }
            }
        } catch (Exception e) {
            logger.debug("Error parsing tuple data: {}", e.getMessage());
        }
        return values;
    }

    private String formatColumnValue(byte[] colData, String colType, String colName) {
        String strValue = new String(colData, StandardCharsets.UTF_8);
        if (strValue.isEmpty()) {
            return "NULL";
        }
        String lowerType = colType != null ? colType.toLowerCase() : "";
        if ("boolean".equalsIgnoreCase(lowerType)) {
            return strValue.equals("t") ? "true" : "false";
        }
        if (lowerType.contains("integer") || lowerType.contains("bigint") ||
            lowerType.contains("smallint") || lowerType.contains("serial") ||
            lowerType.contains("bigserial") || lowerType.equals("int") ||
            lowerType.equals("int4") || lowerType.equals("int8") ||
            lowerType.equals("int2") || lowerType.equals("oid")) {
            return strValue;
        }
        if (lowerType.contains("numeric") || lowerType.contains("decimal") ||
            lowerType.contains("real") || lowerType.contains("double") ||
            lowerType.contains("float") || lowerType.equals("float4") ||
            lowerType.equals("float8") || lowerType.equals("money")) {
            return strValue;
        }
        return "'" + strValue.replace("'", "''") + "'";
    }

    private final Map<Long, String[]> relationIdCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, List<String>> tableColumnsCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, List<String>> tableColumnTypesCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, List<String>> tablePrimaryKeysCache = new java.util.concurrent.ConcurrentHashMap<>();

    private String[] resolveRelationId(long relationId) {
        return relationIdCache.computeIfAbsent(relationId, id -> {
            String[] result = new String[]{"public", "unknown_" + id};
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     Statement stmt = qConn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT n.nspname, c.relname FROM pg_class c " +
                             "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = " + id)) {
                    if (rs.next()) {
                        result[0] = rs.getString(1);
                        result[1] = rs.getString(2);
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to resolve relation OID {}: {}", id, e.getMessage());
            }
            return result;
        });
    }

    private List<String> fetchTableColumns(String schema, String table) {
        String key = schema + "." + table;
        return tableColumnsCache.computeIfAbsent(key, k -> {
            List<String> columns = new ArrayList<>();
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     PreparedStatement stmt = qConn.prepareStatement(
                             "SELECT column_name FROM information_schema.columns " +
                             "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            columns.add(rs.getString(1));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch columns for {}.{}: {}", schema, table, e.getMessage());
            }
            return columns;
        });
    }

    private List<String> fetchTableColumnTypes(String schema, String table) {
        String key = schema + "." + table;
        return tableColumnTypesCache.computeIfAbsent(key, k -> {
            List<String> types = new ArrayList<>();
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     PreparedStatement stmt = qConn.prepareStatement(
                             "SELECT data_type FROM information_schema.columns " +
                             "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            types.add(rs.getString(1));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch column types for {}.{}: {}", schema, table, e.getMessage());
            }
            return types;
        });
    }

    private List<String> fetchTablePrimaryKeys(String schema, String table) {
        String key = schema + "." + table;
        return tablePrimaryKeysCache.computeIfAbsent(key, k -> {
            List<String> pkColumns = new ArrayList<>();
            try {
                String url = String.format("jdbc:postgresql://%s:%d/%s?stringtype=unspecified", host, port, database);
                try (Connection qConn = DriverManager.getConnection(url, user, password);
                     PreparedStatement stmt = qConn.prepareStatement(
                             "SELECT a.attname FROM pg_index i " +
                             "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                             "JOIN pg_class c ON c.oid = i.indrelid " +
                             "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                             "WHERE i.indisprimary AND n.nspname = ? AND c.relname = ? " +
                             "ORDER BY array_position(i.indkey, a.attnum)")) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            pkColumns.add(rs.getString(1));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to fetch primary keys for {}.{}: {}", schema, table, e.getMessage());
            }
            return pkColumns;
        });
    }

    private int readInt8(byte[] data, int offset) {
        return data[offset] & 0xFF;
    }

    private int readInt16BE(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private int readInt32BE(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24) |
               ((data[offset + 1] & 0xFF) << 16) |
               ((data[offset + 2] & 0xFF) << 8) |
               (data[offset + 3] & 0xFF);
    }

    private long readInt64BE(byte[] data, int offset) {
        return ((long) (data[offset] & 0xFF) << 56) |
               ((long) (data[offset + 1] & 0xFF) << 48) |
               ((long) (data[offset + 2] & 0xFF) << 40) |
               ((long) (data[offset + 3] & 0xFF) << 32) |
               ((long) (data[offset + 4] & 0xFF) << 24) |
               ((long) (data[offset + 5] & 0xFF) << 16) |
               ((long) (data[offset + 6] & 0xFF) << 8) |
               ((long) (data[offset + 7] & 0xFF));
    }

    private String readCString(byte[] data, int offset) {
        int end = offset;
        while (end < data.length && data[end] != 0) {
            end++;
        }
        return new String(data, offset, end - offset, StandardCharsets.UTF_8);
    }

    private String parseWalEventType(String walData) {
        if (walData == null || walData.isEmpty()) return "WAL_EVENT";
        if (walData.startsWith("BEGIN")) return "BEGIN";
        if (walData.startsWith("COMMIT")) return "COMMIT";
        if (walData.contains("\"I\"")) return "INSERT";
        if (walData.contains("\"U\"")) return "UPDATE";
        if (walData.contains("\"D\"")) return "DELETE";
        if (walData.startsWith("B")) return "BEGIN";
        if (walData.startsWith("C")) return "COMMIT";
        if (walData.startsWith("I")) return "INSERT";
        if (walData.startsWith("U")) return "UPDATE";
        if (walData.startsWith("D")) return "DELETE";
        return "WAL_EVENT";
    }

    private long parseLsnToLong(String lsn) {
        if (lsn == null || lsn.isEmpty()) return 0;
        String[] parts = lsn.split("/");
        if (parts.length == 2) {
            long segment = Long.parseLong(parts[0], 16);
            long offset = Long.parseLong(parts[1], 16);
            return (segment << 32) | offset;
        }
        return 0;
    }

    @Override
    protected void doStop() throws Exception {
        if (replicationStream != null) {
            try {
                replicationStream.close();
            } catch (Exception e) {
                logger.warn("Error closing replication stream: {}", e.getMessage());
            }
        }

        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing PostgreSQL connection: {}", e.getMessage());
            }
        }

        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (Exception e) {
                logger.warn("Error closing writer: {}", e.getMessage());
            }
        }

        savePosition();
        logger.info("PostgreSQL WAL capture stopped. Total events captured: {}", eventCounter.get());
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

        logger.info("Opened new WAL capture output file: {}", outputFile.getAbsolutePath());
    }

    private synchronized void rotateOutputFile() throws IOException {
        fileCounter.incrementAndGet();
        openNewOutputFile();
        logger.info("Rotated to new capture output file after {} events", maxEventsPerFile);
    }

    private void savePosition() {
        if (currentLsn == null) return;

        File positionFile = new File(outputDir, "capture_position.properties");
        Properties posProps = new Properties();
        posProps.setProperty("wal.lsn", currentLsn);
        posProps.setProperty("wal.lsn.numeric", String.valueOf(currentLsnNumeric));
        posProps.setProperty("last.update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        try (FileOutputStream fos = new FileOutputStream(positionFile)) {
            posProps.store(fos, "WAL Capture position for task: " + taskId);
        } catch (IOException e) {
            logger.warn("Failed to save WAL capture position: {}", e.getMessage());
        }
    }

    public String getCurrentLsn() {
        return currentLsn;
    }

    public long getCurrentLsnNumeric() {
        return currentLsnNumeric;
    }

    public long getEventCount() {
        return eventCounter.get();
    }
}
