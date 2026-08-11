package com.migration.model;

/**
 * 列信息类
 */
public class ColumnInfo {
    private String columnName;
    private String dataType;
    private int columnSize;
    private boolean nullable;
    private String defaultValue;
    private boolean primaryKey;
    private boolean autoIncrement;
    private int decimalDigits;
    /**
     * 生成列（MySQL 的 STORED / VIRTUAL GENERATED）。
     *
     * <p>这类列的值由目标库按表达式自己算，显式写入会被拒绝（MySQL 3105），
     * 所以它们不参与数据搬运。
     */
    private boolean generated;

    public ColumnInfo() {
    }

    public ColumnInfo(String columnName, String dataType) {
        this.columnName = columnName;
        this.dataType = dataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(int decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    public boolean isGenerated() {
        return generated;
    }

    public void setGenerated(boolean generated) {
        this.generated = generated;
    }

    @Override
    public String toString() {
        return "ColumnInfo{" +
                "columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", nullable=" + nullable +
                ", primaryKey=" + primaryKey +
                '}';
    }
}
