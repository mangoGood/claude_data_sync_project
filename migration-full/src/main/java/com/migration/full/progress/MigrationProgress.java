package com.migration.full.progress;

import java.time.LocalDateTime;

/**
 * 迁移进度模型类
 */
public class MigrationProgress {
    private String tableName;
    private long totalRows;
    private long migratedRows;
    private long lastMigratedId;
    private String status; // PENDING, IN_PROGRESS, COMPLETED, FAILED
    private LocalDateTime startTime;
    private LocalDateTime lastUpdateTime;
    private LocalDateTime completeTime;
    private String errorMessage;

    public MigrationProgress() {
    }

    public MigrationProgress(String tableName) {
        this.tableName = tableName;
        this.status = "PENDING";
        this.startTime = LocalDateTime.now();
        this.lastUpdateTime = LocalDateTime.now();
    }

    public MigrationProgress(String tableName, long totalRows) {
        this.tableName = tableName;
        this.totalRows = totalRows;
        this.status = "PENDING";
        this.startTime = LocalDateTime.now();
        this.lastUpdateTime = LocalDateTime.now();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    public long getMigratedRows() {
        return migratedRows;
    }

    public void setMigratedRows(long migratedRows) {
        this.migratedRows = migratedRows;
    }

    public long getLastMigratedId() {
        return lastMigratedId;
    }

    public void setLastMigratedId(long lastMigratedId) {
        this.lastMigratedId = lastMigratedId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(LocalDateTime lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public LocalDateTime getCompleteTime() {
        return completeTime;
    }

    public void setCompleteTime(LocalDateTime completeTime) {
        this.completeTime = completeTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * 获取进度百分比
     */
    public double getProgressPercentage() {
        if (totalRows == 0) {
            return 0.0;
        }
        return (double) migratedRows / totalRows * 100;
    }

    /**
     * 更新进度
     */
    public void updateProgress(long migratedRows, long lastMigratedId) {
        this.migratedRows = migratedRows;
        this.lastMigratedId = lastMigratedId;
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 标记为进行中
     */
    public void markInProgress() {
        this.status = "IN_PROGRESS";
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 标记为已完成
     */
    public void markCompleted() {
        this.status = "COMPLETED";
        this.completeTime = LocalDateTime.now();
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 标记为失败
     */
    public void markFailed(String errorMessage) {
        this.status = "FAILED";
        this.errorMessage = errorMessage;
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 重置进度状态
     */
    public void reset() {
        this.migratedRows = 0;
        this.lastMigratedId = 0;
        this.status = "PENDING";
        this.completeTime = null;
        this.errorMessage = null;
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 更新最后更新时间
     */
    public void updateLastUpdateTime() {
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 标记为进行中
     */
    public void markAsInProgress() {
        this.status = "IN_PROGRESS";
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 标记为已完成
     */
    public void markAsCompleted() {
        this.status = "COMPLETED";
        this.completeTime = LocalDateTime.now();
        this.lastUpdateTime = LocalDateTime.now();
    }

    /**
     * 标记为失败
     */
    public void markAsFailed(String errorMessage) {
        this.status = "FAILED";
        this.errorMessage = errorMessage;
        this.lastUpdateTime = LocalDateTime.now();
    }

    @Override
    public String toString() {
        return String.format("MigrationProgress{table='%s', status='%s', progress=%.2f%% (%d/%d)}",
                tableName, status, getProgressPercentage(), migratedRows, totalRows);
    }
}
