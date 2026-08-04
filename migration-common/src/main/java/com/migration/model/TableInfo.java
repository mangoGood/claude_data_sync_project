package com.migration.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 表信息类
 */
public class TableInfo {
    private String tableName;
    /** 目标表名（表名映射，仅表级同步配置）；null = 与源表同名 */
    private String targetTableName;
    private String createSql;
    private List<ColumnInfo> columns;
    /** 源库名；多库/汇聚场景下进度 key 与来源标识列取值都要用它（单库任务可为 null） */
    private String sourceDatabase;
    /** 路由（汇聚/拆分）指定的目标库；null = 用目标连接所在库 */
    private String targetDatabase;
    /** 汇聚来源标识列 → 值：建表时作为列追加并并入主键，写数时按序补在列表尾部 */
    private java.util.Map<String, String> mergeTagValues;
    /** 幂等装载：INSERT 换成 upsert，崩溃续传不清目标表（汇聚必须开，否则会清掉其它源的数据） */
    private boolean upsertLoad;
    /** 汇聚主键策略为 COMPOSITE_SOURCE：建表时把来源标识列并入主键 */
    private boolean mergeCompositePk;
    /** 拆分：分片键列名；null = 不拆分 */
    private String shardKeyColumn;
    /** 拆分：全部分片落点（预建目标表、按行路由都用它）；空 = 不拆分 */
    private java.util.List<com.migration.common.route.RouteTarget> routeTargets;

    public TableInfo() {
        this.columns = new ArrayList<>();
    }

    public TableInfo(String tableName, String createSql) {
        this.tableName = tableName;
        this.createSql = createSql;
        this.columns = new ArrayList<>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /** 目标端表名：配置了表名映射时返回映射名，否则与源表同名。 */
    public String getTargetTableName() {
        return (targetTableName != null && !targetTableName.isEmpty()) ? targetTableName : tableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getCreateSql() {
        return createSql;
    }

    public void setCreateSql(String createSql) {
        this.createSql = createSql;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
    }

    public void addColumn(ColumnInfo column) {
        this.columns.add(column);
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    /** 路由指定的目标库；未路由返回 null（调用方用目标连接所在库）。 */
    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    /** 汇聚来源标识列 → 值；未汇聚返回空 map。 */
    public java.util.Map<String, String> getMergeTagValues() {
        return mergeTagValues == null ? java.util.Collections.emptyMap() : mergeTagValues;
    }

    public void setMergeTagValues(java.util.Map<String, String> mergeTagValues) {
        this.mergeTagValues = mergeTagValues;
    }

    public boolean isUpsertLoad() {
        return upsertLoad;
    }

    public void setUpsertLoad(boolean upsertLoad) {
        this.upsertLoad = upsertLoad;
    }

    public boolean isMergeCompositePk() {
        return mergeCompositePk;
    }

    public void setMergeCompositePk(boolean mergeCompositePk) {
        this.mergeCompositePk = mergeCompositePk;
    }

    /** 拆分的分片键列名；未拆分返回 null。 */
    public String getShardKeyColumn() {
        return shardKeyColumn;
    }

    public void setShardKeyColumn(String shardKeyColumn) {
        this.shardKeyColumn = shardKeyColumn;
    }

    /** 拆分的全部分片落点；未拆分返回空列表。 */
    public java.util.List<com.migration.common.route.RouteTarget> getRouteTargets() {
        return routeTargets == null ? java.util.Collections.emptyList() : routeTargets;
    }

    public void setRouteTargets(java.util.List<com.migration.common.route.RouteTarget> routeTargets) {
        this.routeTargets = routeTargets;
    }

    /** 是否为拆分表（有分片键且有落点）。 */
    public boolean isSplitRouted() {
        return shardKeyColumn != null && !shardKeyColumn.isEmpty() && !getRouteTargets().isEmpty();
    }

    /**
     * 断点进度的 key。默认就是源表名（保持既有任务的续传状态不失效）；
     * 汇聚下必须带上源库名——多个源库的同名分表（shard_db_1.order_001 与 shard_db_2.order_001）
     * 共用一个 key 会让进度互相覆盖，续传直接错位。
     */
    public String getProgressKey() {
        if (upsertLoad && sourceDatabase != null && !sourceDatabase.isEmpty()) {
            return sourceDatabase + "." + tableName;
        }
        return tableName;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns.size() +
                '}';
    }
}
