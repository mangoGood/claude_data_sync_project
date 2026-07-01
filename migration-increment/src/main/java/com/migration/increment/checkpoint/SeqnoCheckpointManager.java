package com.migration.increment.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;

public class SeqnoCheckpointManager {

    private static final Logger logger = LoggerFactory.getLogger(SeqnoCheckpointManager.class);

    private static final String DB_URL_PREFIX = "jdbc:h2:file:";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";

    private String dbPath;
    private Connection connection;

    public SeqnoCheckpointManager(String dbPath) {
        this.dbPath = dbPath;
        initDatabase();
    }

    private void initDatabase() {
        try {
            File parentDir = new File(dbPath).getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }

            String url = DB_URL_PREFIX + dbPath + ";AUTO_SERVER=TRUE";
            try {
                connection = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
            } catch (SQLException e) {
                logger.warn("H2 database connection failed: {}, cleaning lock files and retrying", e.getMessage());
                cleanH2LockFiles(dbPath);
                try {
                    connection = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
                } catch (SQLException e2) {
                    logger.warn("H2 database connection still failed after cleaning lock files, trying without AUTO_SERVER");
                    String urlNoAutoServer = DB_URL_PREFIX + dbPath;
                    cleanH2LockFiles(dbPath);
                    connection = DriverManager.getConnection(urlNoAutoServer, DB_USER, DB_PASSWORD);
                }
            }

            String createTableSql = "CREATE TABLE IF NOT EXISTS checkpoint (" +
                    "id INT PRIMARY KEY, " +
                    "seqno BIGINT, " +
                    "binlog_file VARCHAR(255), " +
                    "binlog_position BIGINT, " +
                    "event_id VARCHAR(255), " +
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ")";

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createTableSql);
            }

            logger.info("Checkpoint database initialized: {}", dbPath);
        } catch (SQLException e) {
            logger.error("Failed to initialize checkpoint database", e);
            throw new RuntimeException("Failed to initialize checkpoint database", e);
        }
    }

    private void cleanH2LockFiles(String dbPath) {
        String[] extensions = {".lock.db", ".mv.db", ".trace.db"};
        for (String ext : extensions) {
            File lockFile = new File(dbPath + ext);
            if (lockFile.exists()) {
                logger.info("Deleting H2 lock file: {}", lockFile.getAbsolutePath());
                lockFile.delete();
            }
        }
    }

    public void saveCheckpoint(long seqno, String binlogFile, long binlogPosition, String eventId) {
        String sql = "MERGE INTO checkpoint (id, seqno, binlog_file, binlog_position, event_id, updated_at) " +
                "VALUES (1, ?, ?, ?, ?, CURRENT_TIMESTAMP())";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, seqno);
            stmt.setString(2, binlogFile);
            stmt.setLong(3, binlogPosition);
            stmt.setString(4, eventId);
            stmt.executeUpdate();

            logger.debug("Checkpoint saved: seqno={}, binlog={}:{}, eventId={}", seqno, binlogFile, binlogPosition, eventId);
        } catch (SQLException e) {
            logger.error("Failed to save checkpoint", e);
        }
    }

    public long loadSeqno() {
        String sql = "SELECT seqno FROM checkpoint WHERE id = 1";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long seqno = rs.getLong("seqno");
                logger.info("Loaded checkpoint seqno: {}", seqno);
                return seqno;
            }
        } catch (SQLException e) {
            logger.error("Failed to load checkpoint", e);
        }

        logger.info("No checkpoint found, starting from 0");
        return 0;
    }

    public CheckpointInfo loadCheckpoint() {
        String sql = "SELECT seqno, binlog_file, binlog_position, event_id FROM checkpoint WHERE id = 1";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                CheckpointInfo info = new CheckpointInfo();
                info.seqno = rs.getLong("seqno");
                info.binlogFile = rs.getString("binlog_file");
                info.binlogPosition = rs.getLong("binlog_position");
                info.eventId = rs.getString("event_id");
                logger.info("Loaded checkpoint: {}", info);
                return info;
            }
        } catch (SQLException e) {
            logger.error("Failed to load checkpoint", e);
        }

        return null;
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Checkpoint database connection closed");
            } catch (SQLException e) {
                logger.error("Failed to close checkpoint database connection", e);
            }
        }
    }

    public static class CheckpointInfo {
        public long seqno;
        public String binlogFile;
        public long binlogPosition;
        public String eventId;

        @Override
        public String toString() {
            return "CheckpointInfo{seqno=" + seqno + ", binlog=" + binlogFile + ":" + binlogPosition +
                    ", eventId='" + eventId + "'}";
        }
    }
}
