package com.migration.increment;

public class SqlStatement {

    private final int id;
    private final long seqno;
    private final String sql;
    private final OperationType operationType;
    private final String database;
    private final String ddlDatabase;
    private final String tableName;
    private final String primaryKeyValue;
    private final String conflictKey;
    private final boolean isTransactionBoundary;

    public enum OperationType {
        INSERT,
        UPDATE,
        DELETE,
        DDL,
        DCL,
        COMMIT,
        OTHER
    }

    private SqlStatement(Builder builder) {
        this.id = builder.id;
        this.seqno = builder.seqno;
        this.sql = builder.sql;
        this.operationType = builder.operationType;
        this.database = builder.database;
        this.ddlDatabase = builder.ddlDatabase;
        this.tableName = builder.tableName;
        this.primaryKeyValue = builder.primaryKeyValue;
        this.conflictKey = builder.conflictKey != null ? builder.conflictKey : buildConflictKey();
        this.isTransactionBoundary = builder.isTransactionBoundary;
    }

    private String buildConflictKey() {
        if (isTransactionBoundary) {
            return null;
        }
        if (tableName != null && primaryKeyValue != null) {
            String db = database != null ? database : "";
            return db + "." + tableName + ":" + primaryKeyValue;
        }
        if (tableName != null) {
            String db = database != null ? database : "";
            return db + "." + tableName + ":*";
        }
        return null;
    }

    public boolean hasConflictWith(SqlStatement other) {
        if (this.conflictKey == null || other.conflictKey == null) {
            return false;
        }
        return this.conflictKey.equals(other.conflictKey);
    }

    public int getId() { return id; }
    public long getSeqno() { return seqno; }
    public String getSql() { return sql; }
    public OperationType getOperationType() { return operationType; }
    public String getDatabase() { return database; }
    public String getDdlDatabase() { return ddlDatabase; }
    public String getTableName() { return tableName; }
    public String getPrimaryKeyValue() { return primaryKeyValue; }
    public String getConflictKey() { return conflictKey; }
    public boolean isTransactionBoundary() { return isTransactionBoundary; }

    public boolean isBarrier() {
        return operationType == OperationType.DDL
                || operationType == OperationType.DCL
                || operationType == OperationType.COMMIT
                || isTransactionBoundary;
    }

    public boolean isDml() {
        return operationType == OperationType.INSERT
                || operationType == OperationType.UPDATE
                || operationType == OperationType.DELETE;
    }

    @Override
    public String toString() {
        return String.format("SqlStatement{id=%d, seqno=%d, type=%s, conflictKey=%s, sql='%.80s'}",
                id, seqno, operationType, conflictKey,
                sql != null && sql.length() > 80 ? sql.substring(0, 80) + "..." : sql);
    }

    public static class Builder {
        private int id;
        private long seqno;
        private String sql;
        private OperationType operationType = OperationType.OTHER;
        private String database;
        private String ddlDatabase;
        private String tableName;
        private String primaryKeyValue;
        private String conflictKey;
        private boolean isTransactionBoundary;

        public Builder id(int id) { this.id = id; return this; }
        public Builder seqno(long seqno) { this.seqno = seqno; return this; }
        public Builder sql(String sql) { this.sql = sql; return this; }
        public Builder operationType(OperationType operationType) { this.operationType = operationType; return this; }
        public Builder database(String database) { this.database = database; return this; }
        public Builder ddlDatabase(String ddlDatabase) { this.ddlDatabase = ddlDatabase; return this; }
        public Builder tableName(String tableName) { this.tableName = tableName; return this; }
        public Builder primaryKeyValue(String primaryKeyValue) { this.primaryKeyValue = primaryKeyValue; return this; }
        public Builder conflictKey(String conflictKey) { this.conflictKey = conflictKey; return this; }
        public Builder isTransactionBoundary(boolean isTransactionBoundary) { this.isTransactionBoundary = isTransactionBoundary; return this; }

        public SqlStatement build() { return new SqlStatement(this); }
    }
}
