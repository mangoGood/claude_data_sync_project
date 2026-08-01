package com.migration.agent.model;

public class TaskStatusMessage {
    private String taskId;
    private String status;
    private String message;
    private int progress;
    private long timestamp;
    
    private Integer totalTables;
    private Integer completedTables;
    private String currentTable;
    private Integer currentTableProgress;
    private Long currentTableRows;
    private Long currentTableTotalRows;
    private String errorCode;
    private Long rpoMs;
    private Long rtoMs;

    // ===== SLA 闭环指标（P2-4）：随状态一起上报，落到 workflows 表供告警规则读取 =====
    /** 绝对复制延迟：源库当前时刻 − 已应用事件的源端时刻（源库空闲时 RPO 恒为 0，这个不会）。 */
    private Long replicationLagMs;
    /** capture 重启后重放的字节数。 */
    private Long captureReplayBytes;
    /** 近 10 分钟（crashloop.window.ms）子进程重启次数。 */
    private Integer restartCount10m;
    /** 双向冲突裁决累计条数。 */
    private Long conflictCount;
    /** 死信累计条数。 */
    private Long deadletterCount;
    /** 任务工作目录占用字节数。 */
    private Long diskUsageBytes;

    public TaskStatusMessage() {
        this.timestamp = System.currentTimeMillis();
    }

    public Long getReplicationLagMs() { return replicationLagMs; }

    public void setReplicationLagMs(Long replicationLagMs) { this.replicationLagMs = replicationLagMs; }

    public Long getCaptureReplayBytes() { return captureReplayBytes; }

    public void setCaptureReplayBytes(Long captureReplayBytes) { this.captureReplayBytes = captureReplayBytes; }

    public Integer getRestartCount10m() { return restartCount10m; }

    public void setRestartCount10m(Integer restartCount10m) { this.restartCount10m = restartCount10m; }

    public Long getConflictCount() { return conflictCount; }

    public void setConflictCount(Long conflictCount) { this.conflictCount = conflictCount; }

    public Long getDeadletterCount() { return deadletterCount; }

    public void setDeadletterCount(Long deadletterCount) { this.deadletterCount = deadletterCount; }

    public Long getDiskUsageBytes() { return diskUsageBytes; }

    public void setDiskUsageBytes(Long diskUsageBytes) { this.diskUsageBytes = diskUsageBytes; }

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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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