package com.migration.agent.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class RecoveryTask implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String taskId;
    private String taskName;
    private Long userId;
    private String sourceConnection;
    private String targetConnection;
    private String migrationMode;
    private String status;
    private int progress;
    private LocalDateTime createdAt;
    private String syncObjects;
    private String sourceDbName;
    private String sourceType;
    private String targetType;
    private String taskType;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
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

    public String getMigrationMode() {
        return migrationMode;
    }

    public void setMigrationMode(String migrationMode) {
        this.migrationMode = migrationMode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getSyncObjects() {
        return syncObjects;
    }

    public void setSyncObjects(String syncObjects) {
        this.syncObjects = syncObjects;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
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

    public TaskMessage toTaskMessage() {
        TaskMessage message = new TaskMessage();
        message.setTaskId(this.taskId);
        message.setTaskName(this.taskName);
        message.setUserId(this.userId);
        message.setSourceConnection(this.sourceConnection);
        message.setTargetConnection(this.targetConnection);
        message.setMigrationMode(this.migrationMode);
        message.setCreatedAt(this.createdAt);
        message.setSourceDbName(this.sourceDbName);
        message.setSourceType(this.sourceType != null ? this.sourceType : "mysql");
        message.setTargetType(this.targetType != null ? this.targetType : "mysql");
        message.setTaskType(this.taskType != null ? this.taskType : "SYNC");
        if (this.syncObjects != null && !this.syncObjects.isEmpty()) {
            try {
                com.google.gson.Gson gson = new com.google.gson.Gson();
                java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<java.util.Map<String, Object>>(){}.getType();
                java.util.Map<String, Object> syncObjectsMap = gson.fromJson(this.syncObjects, type);
                message.setSyncObjects(syncObjectsMap);
            } catch (Exception e) {
                // ignore parse error
            }
        }
        return message;
    }
}
