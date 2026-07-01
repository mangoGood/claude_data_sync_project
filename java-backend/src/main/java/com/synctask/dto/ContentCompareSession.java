package com.synctask.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ContentCompareSession {

    private String sessionId;
    private String workflowId;
    private String sourceConnection;
    private String targetConnection;
    private String sourceType;
    private String targetType;
    private List<TableCompareTask> tables;
    private String status;
    private long createdAt;

    public ContentCompareSession() {
        this.sessionId = UUID.randomUUID().toString();
        this.tables = new ArrayList<>();
        this.status = "RUNNING";
        this.createdAt = System.currentTimeMillis();
    }

    public static class TableCompareTask {
        private String sourceTable;
        private String targetTable;
        private String sourceDb;
        private String targetDb;
        private String primaryKeyColumn;
        private Boolean checksumMatch;
        private Long sourceRowCount;
        private Long targetRowCount;
        private List<ColumnMeta> columns;
        private CompareCursor cursor;
        private List<DataDiff> diffs;
        private int totalDiffsFound;
        private boolean scanCompleted;
        private String status;

        public TableCompareTask() {
            this.diffs = new ArrayList<>();
            this.totalDiffsFound = 0;
            this.scanCompleted = false;
            this.status = "PENDING";
        }

        public String getSourceTable() { return sourceTable; }
        public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }
        public String getTargetTable() { return targetTable; }
        public void setTargetTable(String targetTable) { this.targetTable = targetTable; }
        public String getSourceDb() { return sourceDb; }
        public void setSourceDb(String sourceDb) { this.sourceDb = sourceDb; }
        public String getTargetDb() { return targetDb; }
        public void setTargetDb(String targetDb) { this.targetDb = targetDb; }
        public String getPrimaryKeyColumn() { return primaryKeyColumn; }
        public void setPrimaryKeyColumn(String primaryKeyColumn) { this.primaryKeyColumn = primaryKeyColumn; }
        public Boolean getChecksumMatch() { return checksumMatch; }
        public void setChecksumMatch(Boolean checksumMatch) { this.checksumMatch = checksumMatch; }
        public Long getSourceRowCount() { return sourceRowCount; }
        public void setSourceRowCount(Long sourceRowCount) { this.sourceRowCount = sourceRowCount; }
        public Long getTargetRowCount() { return targetRowCount; }
        public void setTargetRowCount(Long targetRowCount) { this.targetRowCount = targetRowCount; }
        public List<ColumnMeta> getColumns() { return columns; }
        public void setColumns(List<ColumnMeta> columns) { this.columns = columns; }
        public CompareCursor getCursor() { return cursor; }
        public void setCursor(CompareCursor cursor) { this.cursor = cursor; }
        public List<DataDiff> getDiffs() { return diffs; }
        public void setDiffs(List<DataDiff> diffs) { this.diffs = diffs; }
        public int getTotalDiffsFound() { return totalDiffsFound; }
        public void setTotalDiffsFound(int totalDiffsFound) { this.totalDiffsFound = totalDiffsFound; }
        public boolean isScanCompleted() { return scanCompleted; }
        public void setScanCompleted(boolean scanCompleted) { this.scanCompleted = scanCompleted; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }

    public static class ColumnMeta {
        private String name;
        private String type;

        public ColumnMeta(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
    }

    public static class CompareCursor {
        private Object lastProcessedPk;
        private int currentBucket;
        private int totalBuckets;
        private long scannedRows;
        private long totalRows;

        public CompareCursor() {
            this.lastProcessedPk = null;
            this.currentBucket = 0;
            this.totalBuckets = 100;
            this.scannedRows = 0;
            this.totalRows = 0;
        }

        public Object getLastProcessedPk() { return lastProcessedPk; }
        public void setLastProcessedPk(Object lastProcessedPk) { this.lastProcessedPk = lastProcessedPk; }
        public int getCurrentBucket() { return currentBucket; }
        public void setCurrentBucket(int currentBucket) { this.currentBucket = currentBucket; }
        public int getTotalBuckets() { return totalBuckets; }
        public void setTotalBuckets(int totalBuckets) { this.totalBuckets = totalBuckets; }
        public long getScannedRows() { return scannedRows; }
        public void setScannedRows(long scannedRows) { this.scannedRows = scannedRows; }
        public long getTotalRows() { return totalRows; }
        public void setTotalRows(long totalRows) { this.totalRows = totalRows; }
    }

    public static class DataDiff {
        private Object primaryKeyValue;
        private String diffType;
        private List<String> diffFields;
        private String sourceData;
        private String targetData;

        public DataDiff() {
            this.diffFields = new ArrayList<>();
        }

        public Object getPrimaryKeyValue() { return primaryKeyValue; }
        public void setPrimaryKeyValue(Object primaryKeyValue) { this.primaryKeyValue = primaryKeyValue; }
        public String getDiffType() { return diffType; }
        public void setDiffType(String diffType) { this.diffType = diffType; }
        public List<String> getDiffFields() { return diffFields; }
        public void setDiffFields(List<String> diffFields) { this.diffFields = diffFields; }
        public String getSourceData() { return sourceData; }
        public void setSourceData(String sourceData) { this.sourceData = sourceData; }
        public String getTargetData() { return targetData; }
        public void setTargetData(String targetData) { this.targetData = targetData; }
    }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public String getSourceConnection() { return sourceConnection; }
    public void setSourceConnection(String sourceConnection) { this.sourceConnection = sourceConnection; }
    public String getTargetConnection() { return targetConnection; }
    public void setTargetConnection(String targetConnection) { this.targetConnection = targetConnection; }
    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }
    public String getTargetType() { return targetType; }
    public void setTargetType(String targetType) { this.targetType = targetType; }
    public List<TableCompareTask> getTables() { return tables; }
    public void setTables(List<TableCompareTask> tables) { this.tables = tables; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
}
