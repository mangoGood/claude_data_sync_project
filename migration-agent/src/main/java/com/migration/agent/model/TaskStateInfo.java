package com.migration.agent.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class TaskStateInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String taskId;
    private String taskName;
    private Long userId;
    private String migrationMode;
    private String sourceConnection;
    private String targetConnection;
    private String sourceType;
    private String targetType;
    private LocalDateTime createdAt;
    private String status;
    private int progress;
    private String lastProcessedTable;
    private long lastUpdated;
    
    public TaskStateInfo() {
    }
    
    public TaskStateInfo(String taskId) {
        this.taskId = taskId;
        this.lastUpdated = System.currentTimeMillis();
    }
    
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

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
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
    
    public String getLastProcessedTable() {
        return lastProcessedTable;
    }
    
    public void setLastProcessedTable(String lastProcessedTable) {
        this.lastProcessedTable = lastProcessedTable;
    }
    
    public long getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
