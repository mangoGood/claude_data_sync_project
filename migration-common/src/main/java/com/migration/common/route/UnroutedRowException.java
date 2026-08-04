package com.migration.common.route;

/**
 * 一行数据算不出分片，且规则的未路由策略为 {@code ERROR}。
 *
 * <p>受检异常在四条链路里都要层层改签名，这里用非受检——但<b>调用方不得吞掉</b>：
 * 它的语义是"这行不知道该写到哪，继续跑就是丢数据"，正确处置是 fail-stop。
 */
public class UnroutedRowException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String sourceDb;
    private final String sourceTable;
    private final String shardKey;

    public UnroutedRowException(String sourceDb, String sourceTable, String shardKey, Object value) {
        super("表 " + sourceDb + "." + sourceTable + " 的分片键 " + shardKey
                + " 值 [" + value + "] 算不出分片（未路由策略=ERROR）");
        this.sourceDb = sourceDb;
        this.sourceTable = sourceTable;
        this.shardKey = shardKey;
    }

    public String getSourceDb() {
        return sourceDb;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getShardKey() {
        return shardKey;
    }
}
