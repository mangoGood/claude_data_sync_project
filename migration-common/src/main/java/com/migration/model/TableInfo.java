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

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns.size() +
                '}';
    }
}
