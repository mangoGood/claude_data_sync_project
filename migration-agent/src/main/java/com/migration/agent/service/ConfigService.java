package com.migration.agent.service;

import com.google.gson.Gson;
import com.migration.agent.model.TaskMessage;
import com.migration.agent.util.ConnectionStringParser;
import com.migration.agent.util.ConnectionStringParser.ConnectionInfo;
import com.migration.dialect.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConfigService {
        private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);
    private final Gson gson = new Gson();
    
    public void updateConfig(TaskMessage taskMessage) throws IOException {
        String taskId = taskMessage.getTaskId();
        logger.info("Updating config file for task: {}", taskId);
        
        File taskDir = new File("files/" + taskId);
        if (!taskDir.exists()) {
            boolean created = taskDir.mkdirs();
            logger.info("Task directory created: {}, success: {}", taskDir.getAbsolutePath(), created);
        }
        
        File checkpointDir = new File(taskDir, "checkpoint");
        if (!checkpointDir.exists()) {
            checkpointDir.mkdirs();
            logger.info("Checkpoint directory created: {}", checkpointDir.getAbsolutePath());
        }
        
        File logsDir = new File(taskDir, "logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
            logger.info("Logs directory created: {}", logsDir.getAbsolutePath());
        }
        
        File binlogOutputDir = new File(taskDir, "binlog_output");
        if (!binlogOutputDir.exists()) {
            binlogOutputDir.mkdirs();
            logger.info("Binlog output directory created: {}", binlogOutputDir.getAbsolutePath());
        }
        
        File sqlOutputDir = new File(taskDir, "sql_output");
        if (!sqlOutputDir.exists()) {
            sqlOutputDir.mkdirs();
            logger.info("SQL output directory created: {}", sqlOutputDir.getAbsolutePath());
        }

        File thlOutputDir = new File(taskDir, "thl_output");
        if (!thlOutputDir.exists()) {
            thlOutputDir.mkdirs();
            logger.info("THL output directory created: {}", thlOutputDir.getAbsolutePath());
        }
        
        Properties props = new Properties();
        
        File configFile = new File(taskDir, "config.properties");
        logger.info("Config file path: {}", configFile.getAbsolutePath());
        
        if (configFile.exists()) {
            try (InputStream input = new FileInputStream(configFile)) {
                props.load(input);
            }
            logger.info("Loaded existing config file");
        } else {
            logger.info("Creating new config file");
        }
        
        if (taskMessage.getSourceConnection() != null && !taskMessage.getSourceConnection().isEmpty()) {
            ConnectionInfo sourceInfo = ConnectionStringParser.parse(taskMessage.getSourceConnection());
            if (sourceInfo != null) {
                props.setProperty("source.db.host", sourceInfo.getHost());
                props.setProperty("source.db.port", String.valueOf(sourceInfo.getPort()));
                props.setProperty("source.db.database", sourceInfo.getDatabase());
                if (sourceInfo.getUsername() != null) {
                    props.setProperty("source.db.username", sourceInfo.getUsername());
                }
                if (sourceInfo.getPassword() != null) {
                    props.setProperty("source.db.password", sourceInfo.getPassword());
                }
                if (sourceInfo.getUsername() == null && taskMessage.getSource() != null) {
                    TaskMessage.DatabaseConfig source = taskMessage.getSource();
                    if (source.getUsername() != null) {
                        props.setProperty("source.db.username", source.getUsername());
                    }
                    if (source.getPassword() != null) {
                        props.setProperty("source.db.password", source.getPassword());
                    }
                }
                props.setProperty("source.host", sourceInfo.getHost());
                props.setProperty("source.port", String.valueOf(sourceInfo.getPort()));
                props.setProperty("source.user", sourceInfo.getUsername() != null ? sourceInfo.getUsername() : props.getProperty("source.db.username", ""));
                props.setProperty("source.password", sourceInfo.getPassword() != null ? sourceInfo.getPassword() : props.getProperty("source.db.password", ""));
                props.setProperty("source.type", sourceInfo.getType() != null ? sourceInfo.getType() : "mysql");
                logger.info("Source database config updated: {}:{}", sourceInfo.getHost(), sourceInfo.getPort());
            }
        } else if (taskMessage.getSource() != null) {
            TaskMessage.DatabaseConfig source = taskMessage.getSource();
            props.setProperty("source.db.host", source.getHost());
            props.setProperty("source.db.port", String.valueOf(source.getPort()));
            props.setProperty("source.db.database", source.getDatabase());
            if (source.getUsername() != null) {
                props.setProperty("source.db.username", source.getUsername());
            }
            if (source.getPassword() != null) {
                props.setProperty("source.db.password", source.getPassword());
            }
            logger.info("Source database config updated from DatabaseConfig: {}:{}", source.getHost(), source.getPort());
        }
        
        if (taskMessage.getTargetConnection() != null && !taskMessage.getTargetConnection().isEmpty()) {
            String targetType = taskMessage.getTargetType() != null ? taskMessage.getTargetType() : "mysql";
            if ("kafka".equalsIgnoreCase(targetType)) {
                // 订阅任务的 targetConnection 是 Kafka 连接串，不是数据库连接串
                props.setProperty("subscribe.kafka.bootstrap.servers", taskMessage.getTargetConnection());
                logger.info("Subscribe Kafka target config updated: {}", taskMessage.getTargetConnection());
            } else {
                ConnectionInfo targetInfo = ConnectionStringParser.parse(taskMessage.getTargetConnection());
                if (targetInfo != null) {
                    props.setProperty("target.db.host", targetInfo.getHost());
                    props.setProperty("target.db.port", String.valueOf(targetInfo.getPort()));
                    props.setProperty("target.db.database", targetInfo.getDatabase());
                    if (targetInfo.getUsername() != null) {
                        props.setProperty("target.db.username", targetInfo.getUsername());
                    }
                    if (targetInfo.getPassword() != null) {
                        props.setProperty("target.db.password", targetInfo.getPassword());
                    }
                    if (targetInfo.getUsername() == null && taskMessage.getTarget() != null) {
                        TaskMessage.DatabaseConfig target = taskMessage.getTarget();
                        if (target.getUsername() != null) {
                            props.setProperty("target.db.username", target.getUsername());
                        }
                        if (target.getPassword() != null) {
                            props.setProperty("target.db.password", target.getPassword());
                        }
                    }
                    props.setProperty("target.host", targetInfo.getHost());
                    props.setProperty("target.port", String.valueOf(targetInfo.getPort()));
                    props.setProperty("target.user", targetInfo.getUsername() != null ? targetInfo.getUsername() : props.getProperty("target.db.username", ""));
                    props.setProperty("target.password", targetInfo.getPassword() != null ? targetInfo.getPassword() : props.getProperty("target.db.password", ""));
                    props.setProperty("target.type", targetInfo.getType() != null ? targetInfo.getType() : "mysql");
                    logger.info("Target database config updated: {}:{}", targetInfo.getHost(), targetInfo.getPort());
                }
            }
        } else if (taskMessage.getTarget() != null) {
            TaskMessage.DatabaseConfig target = taskMessage.getTarget();
            props.setProperty("target.db.host", target.getHost());
            props.setProperty("target.db.port", String.valueOf(target.getPort()));
            props.setProperty("target.db.database", target.getDatabase());
            if (target.getUsername() != null) {
                props.setProperty("target.db.username", target.getUsername());
            }
            if (target.getPassword() != null) {
                props.setProperty("target.db.password", target.getPassword());
            }
            logger.info("Target database config updated from DatabaseConfig: {}:{}", target.getHost(), target.getPort());
        }
        
        if (taskMessage.getSyncObjects() != null && !taskMessage.getSyncObjects().isEmpty()) {
            String syncObjectsJson = gson.toJson(taskMessage.getSyncObjects());
            props.setProperty("migration.sync.objects", syncObjectsJson);
            logger.info("Sync objects config updated: {}", syncObjectsJson);
            
            StringBuilder includedDatabases = new StringBuilder();
            StringBuilder includedTables = new StringBuilder();
            
            for (Map.Entry<String, Object> dbEntry : taskMessage.getSyncObjects().entrySet()) {
                String dbName = dbEntry.getKey();
                if (includedDatabases.length() > 0) {
                    includedDatabases.append(",");
                }
                includedDatabases.append(dbName);
                
                Object value = dbEntry.getValue();
                if (value instanceof List) {
                    List<?> tables = (List<?>) value;
                    for (Object table : tables) {
                        if (includedTables.length() > 0) {
                            includedTables.append(",");
                        }
                        includedTables.append(dbName).append(".").append(table.toString());
                    }
                } else if (value instanceof Map) {
                    Map<?, ?> dbValue = (Map<?, ?>) value;
                    Object tablesObj = dbValue.get("tables");
                    if (tablesObj instanceof List) {
                        List<?> tables = (List<?>) tablesObj;
                        for (Object table : tables) {
                            if (includedTables.length() > 0) {
                                includedTables.append(",");
                            }
                            includedTables.append(dbName).append(".").append(table.toString());
                        }
                    }
                }
            }
            
            if (includedDatabases.length() > 0) {
                props.setProperty("migration.included.databases", includedDatabases.toString());
                logger.info("Included databases: {}", includedDatabases.toString());
            }
            if (includedTables.length() > 0) {
                props.setProperty("migration.included.tables", includedTables.toString());
                logger.info("Included tables: {}", includedTables.toString());
            }
        }
        
        if (taskMessage.getSourceDbName() != null && !taskMessage.getSourceDbName().isEmpty()) {
            props.setProperty("source.db.name", taskMessage.getSourceDbName());
            if (props.getProperty("source.db.database") == null || props.getProperty("source.db.database").isEmpty()) {
                props.setProperty("source.db.database", taskMessage.getSourceDbName());
            }
            logger.info("Source database name: {}", taskMessage.getSourceDbName());
        }

        if (taskMessage.getTargetDbName() != null && !taskMessage.getTargetDbName().isEmpty()) {
            props.setProperty("target.db.name", taskMessage.getTargetDbName());
            if (props.getProperty("target.db.database") == null || props.getProperty("target.db.database").isEmpty()) {
                props.setProperty("target.db.database", taskMessage.getTargetDbName());
            }
            logger.info("Target database name: {}", taskMessage.getTargetDbName());
        }

        String sourceType = taskMessage.getSourceType() != null ? taskMessage.getSourceType() : "mysql";
        String targetType = taskMessage.getTargetType() != null ? taskMessage.getTargetType() : "mysql";
        props.setProperty("source.db.type", sourceType);
        props.setProperty("target.db.type", targetType);
        logger.info("Source database type: {}, Target database type: {}", sourceType, targetType);

        // 源库连接：JDBC 驱动与 URL 由方言统一生成；capture.type 仍按源库类型决定（binlog/wal/redo）
        SqlDialect sourceDialect = SqlDialect.forType(sourceType);
        props.setProperty("source.db.jdbc.driver", sourceDialect.jdbcDriverClass());
        props.setProperty("source.db.jdbc.url", sourceDialect.jdbcUrl(
            props.getProperty("source.db.host"), props.getProperty("source.db.port"), props.getProperty("source.db.database")));
        if ("postgresql".equals(sourceType)) {
            props.setProperty("capture.type", "wal");
            logger.info("PostgreSQL source config: using WAL capture, JDBC driver: {}", sourceDialect.jdbcDriverClass());
        } else if ("oracle".equals(sourceType)) {
            props.setProperty("capture.type", "redo");
            logger.info("Oracle source config: using LogMiner redo capture, JDBC driver: {}", sourceDialect.jdbcDriverClass());
        } else {
            props.setProperty("capture.type", "binlog");
            logger.info("MySQL source config: using binlog capture, JDBC driver: {}", sourceDialect.jdbcDriverClass());
        }

        if ("kafka".equalsIgnoreCase(targetType)) {
            props.setProperty("target.db.type", "kafka");
            logger.info("Kafka target config: no JDBC driver needed for subscribe task");
        } else {
            // 目标库连接：驱动、引用字符由方言统一生成
            SqlDialect targetDialect = SqlDialect.forType(targetType);
            props.setProperty("target.db.jdbc.driver", targetDialect.jdbcDriverClass());
            props.setProperty("target.db.quote.char", targetDialect.quoteChar());
            if ("postgresql".equals(targetType)) {
                // PG 目标 schema 依赖请求（MySQL→PG 用源库名作 schema），URL 仍按 schema 专门构造
                String targetPgSchema = "public";
                if ("mysql".equals(sourceType)) {
                    String sourceDbName = props.getProperty("source.db.name", props.getProperty("source.db.database", ""));
                    if (!sourceDbName.isEmpty()) {
                        targetPgSchema = sourceDbName;
                    }
                }
                props.setProperty("target.db.schema", targetPgSchema);
                props.setProperty("target.db.jdbc.url", String.format("jdbc:postgresql://%s:%s/%s?currentSchema=%s&stringtype=unspecified",
                    props.getProperty("target.db.host"), props.getProperty("target.db.port"), props.getProperty("target.db.database"), targetPgSchema));
                logger.info("PostgreSQL target config: JDBC driver: {}, quote char: double-quote, schema: {}", targetDialect.jdbcDriverClass(), targetPgSchema);
            } else {
                props.setProperty("target.db.jdbc.url", targetDialect.jdbcUrl(
                    props.getProperty("target.db.host"), props.getProperty("target.db.port"), props.getProperty("target.db.database")));
                logger.info("MySQL target config: JDBC driver: {}, quote char: backtick", targetDialect.jdbcDriverClass());
            }
        }

        props.setProperty("task.id", taskId);

        String taskType = taskMessage.getTaskType();
        if ("DR".equals(taskType)) {
            props.setProperty("task.type", "DR");
            props.setProperty("migration.mode", "fullAndIncre");
            logger.info("DR task detected, setting task.type=DR, migration.mode=fullAndIncre");
        }

        if ("SUBSCRIBE".equals(taskType)) {
            props.setProperty("task.type", "SUBSCRIBE");
            props.setProperty("migration.mode", "subscribe");
            logger.info("SUBSCRIBE task detected, setting task.type=SUBSCRIBE, migration.mode=subscribe");
        }

        if (taskMessage.getKafkaBootstrapServers() != null && !taskMessage.getKafkaBootstrapServers().isEmpty()) {
            props.setProperty("subscribe.kafka.bootstrap.servers", taskMessage.getKafkaBootstrapServers());
            logger.info("Subscribe Kafka bootstrap servers: {}", taskMessage.getKafkaBootstrapServers());
        }
        if (taskMessage.getKafkaTopicPrefix() != null && !taskMessage.getKafkaTopicPrefix().isEmpty()) {
            props.setProperty("subscribe.kafka.topic.prefix", taskMessage.getKafkaTopicPrefix());
        }
        if (taskMessage.getKafkaTopicStrategy() != null && !taskMessage.getKafkaTopicStrategy().isEmpty()) {
            props.setProperty("subscribe.kafka.topic.strategy", taskMessage.getKafkaTopicStrategy());
        }
        if (taskMessage.getSubscribeFormat() != null && !taskMessage.getSubscribeFormat().isEmpty()) {
            props.setProperty("subscribe.format", taskMessage.getSubscribeFormat());
        }

        props.setProperty("subscribe.thl.dir", "files/" + taskId + "/thl_output");

        if ("DR".equals(taskType) && (taskMessage.getSyncObjects() == null || taskMessage.getSyncObjects().isEmpty())) {
            Map<String, Object> drSyncObjects = discoverDrSyncObjects(props);
            if (drSyncObjects != null && !drSyncObjects.isEmpty()) {
                String syncObjectsJson = gson.toJson(drSyncObjects);
                props.setProperty("migration.sync.objects", syncObjectsJson);
                taskMessage.setSyncObjects(drSyncObjects);
                logger.info("DR task sync objects auto-discovered: {}", syncObjectsJson);

                StringBuilder includedDatabases = new StringBuilder();
                StringBuilder includedTables = new StringBuilder();
                for (Map.Entry<String, Object> dbEntry : drSyncObjects.entrySet()) {
                    String dbName = dbEntry.getKey();
                    if (includedDatabases.length() > 0) includedDatabases.append(",");
                    includedDatabases.append(dbName);
                    if (dbEntry.getValue() instanceof List) {
                        List<?> tables = (List<?>) dbEntry.getValue();
                        for (Object table : tables) {
                            if (includedTables.length() > 0) includedTables.append(",");
                            includedTables.append(dbName).append(".").append(table.toString());
                        }
                    }
                }
                if (includedDatabases.length() > 0) {
                    props.setProperty("migration.included.databases", includedDatabases.toString());
                }
                if (includedTables.length() > 0) {
                    props.setProperty("migration.included.tables", includedTables.toString());
                }
            }
        }

        props.setProperty("capture.output.dir", "files/" + taskId + "/binlog_output");
        props.setProperty("extract.input.dir", "files/" + taskId + "/binlog_output");
        props.setProperty("extract.output.dir", "files/" + taskId + "/thl_output");
        props.setProperty("increment.thl.dir", "files/" + taskId + "/thl_output");
        props.setProperty("extract.continuous", "true");
        props.setProperty("extract.skip.before.checkpoint", "true");
        logger.info("Module paths configured for task: {} - capture.output.dir={}, extract.input.dir={}, extract.output.dir={}, increment.thl.dir={}",
                taskId, "files/" + taskId + "/binlog_output", "files/" + taskId + "/binlog_output",
                "files/" + taskId + "/thl_output", "files/" + taskId + "/thl_output");

        String checkpointDbPath = "./files/" + taskId + "/checkpoint/checkpoint";
        writeCheckpointToConfig(props, checkpointDbPath);

        props.setProperty("migration.record.checkpoint", "false");
        logger.info("Checkpoint recording disabled (already recorded by agent before startup)");

        long uniqueServerId = 1000 + Math.abs(taskId.hashCode() % 9000);
        props.setProperty("capture.server.id", String.valueOf(uniqueServerId));
        logger.info("Assigned unique server_id for capture: {}", uniqueServerId);

        try (OutputStream output = new FileOutputStream(configFile)) {
            props.store(output, "Updated by Migration Agent for task: " + taskId);
        }
        
        createLogbackConfig(taskDir, taskId);
        
        logger.info("Config file updated successfully for task: {}", taskId);
    }

    private void writeCheckpointToConfig(Properties props, String checkpointDbPath) {
        String url = "jdbc:h2:" + checkpointDbPath;
        String sourceType = props.getProperty("source.db.type", "mysql");

        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            String sql = "SELECT filename, position FROM checkpoint WHERE id = 1";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    String filename = rs.getString("filename");
                    long position = rs.getLong("position");
                    if (filename != null && !filename.isEmpty()) {
                        if ("postgresql".equals(sourceType)) {
                            props.setProperty("checkpoint.wal.lsn", filename);
                            props.setProperty("capture.wal.lsn", filename);
                            props.setProperty("checkpoint.wal.position", String.valueOf(position));
                            props.setProperty("capture.wal.position", String.valueOf(position));
                            logger.info("PostgreSQL Checkpoint written to config: LSN={}", filename);
                        } else if ("oracle".equals(sourceType)) {
                            props.setProperty("checkpoint.redo.scn", filename);
                            props.setProperty("capture.redo.scn", filename);
                            props.setProperty("checkpoint.redo.position", String.valueOf(position));
                            props.setProperty("capture.redo.position", String.valueOf(position));
                            logger.info("Oracle Checkpoint written to config: SCN={}", filename);
                        } else {
                            props.setProperty("checkpoint.binlog.file", filename);
                            props.setProperty("checkpoint.binlog.position", String.valueOf(position));
                            props.setProperty("capture.binlog.file", filename);
                            props.setProperty("capture.binlog.position", String.valueOf(position));
                            logger.info("MySQL Checkpoint written to config: {}:{}", filename, position);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("Could not read checkpoint for config (may not exist yet): {}", e.getMessage());
        }
    }
    
    private Map<String, Object> discoverDrSyncObjects(Properties props) {
        Map<String, Object> syncObjects = new java.util.LinkedHashMap<>();
        String sourceType = props.getProperty("source.db.type", "mysql");
        String driver = props.getProperty("source.db.jdbc.driver");
        String url = props.getProperty("source.db.jdbc.url");
        String username = props.getProperty("source.db.username");
        String password = props.getProperty("source.db.password");

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            logger.error("JDBC driver not found: {}", driver, e);
            return syncObjects;
        }

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            if ("postgresql".equals(sourceType)) {
                String schema = props.getProperty("target.db.schema", "public");
                String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, schema);
                    try (ResultSet rs = stmt.executeQuery()) {
                        List<String> tables = new java.util.ArrayList<>();
                        while (rs.next()) {
                            tables.add(rs.getString("table_name"));
                        }
                        if (!tables.isEmpty()) {
                            syncObjects.put(props.getProperty("source.db.database", schema), tables);
                        }
                    }
                }
            } else if ("oracle".equals(sourceType)) {
                // Oracle: 查询当前用户拥有的所有表，按 OWNER 分组
                String owner = props.getProperty("source.db.username", "").toUpperCase();
                String sql = "SELECT OWNER, TABLE_NAME FROM ALL_TABLES WHERE OWNER = ? " +
                        "AND TABLE_NAME NOT LIKE 'BIN$%' AND TABLE_NAME NOT LIKE 'DR$%' " +
                        "ORDER BY TABLE_NAME";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, owner);
                    try (ResultSet rs = stmt.executeQuery()) {
                        List<String> tables = new java.util.ArrayList<>();
                        while (rs.next()) {
                            tables.add(rs.getString("TABLE_NAME"));
                        }
                        if (!tables.isEmpty()) {
                            syncObjects.put(owner, tables);
                        }
                    }
                }
            } else {
                String sql = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys') AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME";
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        String schema = rs.getString("TABLE_SCHEMA");
                        String table = rs.getString("TABLE_NAME");
                        @SuppressWarnings("unchecked")
                        List<String> tables = (List<String>) syncObjects.get(schema);
                        if (tables == null) {
                            tables = new java.util.ArrayList<>();
                            syncObjects.put(schema, tables);
                        }
                        tables.add(table);
                    }
                }
            }
            logger.info("Discovered DR sync objects: {}", syncObjects);
        } catch (SQLException e) {
            logger.error("Failed to discover DR sync objects from source database", e);
        }

        return syncObjects;
    }

    private void createLogbackConfig(File taskDir, String taskId) throws IOException {
        File logbackFile = new File(taskDir, "logback.xml");
        
        String logbackContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<configuration>\n" +
            "    <property name=\"LOG_PATH\" value=\"files/" + taskId + "/logs\"/>\n" +
            "    <property name=\"LOG_FILE\" value=\"migration\"/>\n" +
            "\n" +
            "    <appender name=\"CONSOLE\" class=\"ch.qos.logback.core.ConsoleAppender\">\n" +
            "        <encoder>\n" +
            "            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>\n" +
            "            <charset>UTF-8</charset>\n" +
            "        </encoder>\n" +
            "    </appender>\n" +
            "\n" +
            "    <appender name=\"FILE\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
            "        <file>${LOG_PATH}/${LOG_FILE}.log</file>\n" +
            "        <encoder>\n" +
            "            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>\n" +
            "            <charset>UTF-8</charset>\n" +
            "        </encoder>\n" +
            "        <rollingPolicy class=\"ch.qos.logback.core.rolling.TimeBasedRollingPolicy\">\n" +
            "            <fileNamePattern>${LOG_PATH}/${LOG_FILE}.%d{yyyy-MM-dd}.%i.log</fileNamePattern>\n" +
            "            <timeBasedFileNamingAndTriggeringPolicy class=\"ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP\">\n" +
            "                <maxFileSize>100MB</maxFileSize>\n" +
            "            </timeBasedFileNamingAndTriggeringPolicy>\n" +
            "            <maxHistory>30</maxHistory>\n" +
            "            <totalSizeCap>10GB</totalSizeCap>\n" +
            "        </rollingPolicy>\n" +
            "    </appender>\n" +
            "\n" +
            "    <root level=\"INFO\">\n" +
            "        <appender-ref ref=\"CONSOLE\"/>\n" +
            "        <appender-ref ref=\"FILE\"/>\n" +
            "    </root>\n" +
            "\n" +
            "    <logger name=\"com.migration\" level=\"DEBUG\"/>\n" +
            "</configuration>";
        
        try (OutputStream output = new FileOutputStream(logbackFile)) {
            output.write(logbackContent.getBytes(StandardCharsets.UTF_8));
        }
        
        logger.info("Logback config created for task: {}", taskId);
    }
}