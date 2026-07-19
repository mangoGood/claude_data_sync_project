package com.migration.agent.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class TaskMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String taskId;
    private String taskName;
    private Long userId;
    private DatabaseConfig source;
    private DatabaseConfig target;
    private String migrationMode;
    private String messageType;
    private String sourceConnection;
    private String targetConnection;
    private LocalDateTime createdAt;
    private String currentStatus;
    private Map<String, Object> syncObjects;
    private String sourceDbName;
    private String targetDbName;
    private String sourceType;
    private String targetType;
    private String taskType;
    private String drMode;
    private String kafkaBootstrapServers;
    private String kafkaTopicPrefix;
    private String kafkaTopicStrategy;
    private String subscribeFormat;
    private Boolean fanoutEnabled;
    private List<DatabaseConfig> targetConnections;
    /** 人工裁决要跳过的增量事件 seqno（逗号分隔），随 resume 消息下发，写入 increment.skip.seqnos */
    private String skipSeqnos;
    /** 人工裁决要跳过的增量事件 eventId（binlog文件:位点，逗号分隔）——跨重启稳定的首选身份，写入 increment.skip.event.ids */
    private String skipEventIds;

    public static class DatabaseConfig {
        private String host;
        private int port;
        private String database;
        private String username;
        private String password;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public DatabaseConfig getSource() {
        return source;
    }

    public void setSource(DatabaseConfig source) {
        this.source = source;
    }

    public DatabaseConfig getTarget() {
        return target;
    }

    public void setTarget(DatabaseConfig target) {
        this.target = target;
    }

    public String getMigrationMode() {
        return migrationMode;
    }

    public void setMigrationMode(String migrationMode) {
        this.migrationMode = migrationMode;
    }

    public String getSourceConnection() {
        return sourceConnection;
    }

    public void setSourceConnection(String sourceConnection) {
        this.sourceConnection = sourceConnection;
    }

    public String getTargetConnection() {
        return targetConnection;
    }

    public void setTargetConnection(String targetConnection) {
        this.targetConnection = targetConnection;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(String currentStatus) {
        this.currentStatus = currentStatus;
    }

    public Map<String, Object> getSyncObjects() {
        return syncObjects;
    }

    public void setSyncObjects(Map<String, Object> syncObjects) {
        this.syncObjects = syncObjects;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    public String getTargetDbName() {
        return targetDbName;
    }

    public void setTargetDbName(String targetDbName) {
        this.targetDbName = targetDbName;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getDrMode() {
        return drMode;
    }

    public void setDrMode(String drMode) {
        this.drMode = drMode;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaTopicPrefix() {
        return kafkaTopicPrefix;
    }

    public void setKafkaTopicPrefix(String kafkaTopicPrefix) {
        this.kafkaTopicPrefix = kafkaTopicPrefix;
    }

    public String getKafkaTopicStrategy() {
        return kafkaTopicStrategy;
    }

    public void setKafkaTopicStrategy(String kafkaTopicStrategy) {
        this.kafkaTopicStrategy = kafkaTopicStrategy;
    }

    public String getSubscribeFormat() {
        return subscribeFormat;
    }

    public void setSubscribeFormat(String subscribeFormat) {
        this.subscribeFormat = subscribeFormat;
    }

    public Boolean getFanoutEnabled() {
        return fanoutEnabled;
    }

    public void setFanoutEnabled(Boolean fanoutEnabled) {
        this.fanoutEnabled = fanoutEnabled;
    }

    public List<DatabaseConfig> getTargetConnections() {
        return targetConnections;
    }

    public void setTargetConnections(List<DatabaseConfig> targetConnections) {
        this.targetConnections = targetConnections;
    }

    public String getSkipSeqnos() {
        return skipSeqnos;
    }

    public void setSkipSeqnos(String skipSeqnos) {
        this.skipSeqnos = skipSeqnos;
    }

    public String getSkipEventIds() {
        return skipEventIds;
    }

    public void setSkipEventIds(String skipEventIds) {
        this.skipEventIds = skipEventIds;
    }
}