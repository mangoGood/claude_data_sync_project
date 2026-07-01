package com.synctask.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskStatusUpdate {
    @JsonProperty("task_id")
    private String taskId;
    
    private String status;
    private Integer progress;
    private Long userId;
    
    @JsonProperty("migration_mode")
    private String migrationMode;
    
    @JsonProperty("total_tables")
    private Integer totalTables;
    
    @JsonProperty("completed_tables")
    private Integer completedTables;
    
    @JsonProperty("current_table")
    private String currentTable;
    
    @JsonProperty("current_table_progress")
    private Integer currentTableProgress;
    
    @JsonProperty("current_table_rows")
    private Long currentTableRows;
    
    @JsonProperty("current_table_total_rows")
    private Long currentTableTotalRows;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("error_code")
    private String errorCode;

    @JsonProperty("rpo_ms")
    private Long rpoMs;

    @JsonProperty("rto_ms")
    private Long rtoMs;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getProgress() {
        return progress;
    }

    public void setProgress(Integer progress) {
        this.progress = progress;
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

    public Integer getTotalTables() {
        return totalTables;
    }

    public void setTotalTables(Integer totalTables) {
        this.totalTables = totalTables;
    }

    public Integer getCompletedTables() {
        return completedTables;
    }

    public void setCompletedTables(Integer completedTables) {
        this.completedTables = completedTables;
    }

    public String getCurrentTable() {
        return currentTable;
    }

    public void setCurrentTable(String currentTable) {
        this.currentTable = currentTable;
    }

    public Integer getCurrentTableProgress() {
        return currentTableProgress;
    }

    public void setCurrentTableProgress(Integer currentTableProgress) {
        this.currentTableProgress = currentTableProgress;
    }

    public Long getCurrentTableRows() {
        return currentTableRows;
    }

    public void setCurrentTableRows(Long currentTableRows) {
        this.currentTableRows = currentTableRows;
    }

    public Long getCurrentTableTotalRows() {
        return currentTableTotalRows;
    }

    public void setCurrentTableTotalRows(Long currentTableTotalRows) {
        this.currentTableTotalRows = currentTableTotalRows;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public Long getRpoMs() {
        return rpoMs;
    }

    public void setRpoMs(Long rpoMs) {
        this.rpoMs = rpoMs;
    }

    public Long getRtoMs() {
        return rtoMs;
    }

    public void setRtoMs(Long rtoMs) {
        this.rtoMs = rtoMs;
    }
}
