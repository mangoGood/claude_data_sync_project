package com.migration.extract.schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * 一张表在<b>某一刻</b>的结构——表结构时序库的版本内容。
 *
 * <p>时序库解决的是"用现在的结构解释过去的事件"这个错位：链路有延迟时源库做了
 * {@code ALTER TABLE}，按当前定义解析积压中的行事件会让整行的值与列错位，写进目标库的是
 * 合法值、看不出异常。详见 {@code markdown/SCHEMA_TIMELINE_DESIGN_20260811.md}。
 *
 * <p>本类只描述结构本身，不带位点——位点由时序库的版本条目携带，两者分开是为了让
 * {@code SHOW CREATE TABLE} 解析出来的基线与 DDL 施加出来的后续版本是同一种东西。
 *
 * <p>对下游的契约是六项 THL 元数据（{@code column_names} / {@code mysql_column_types} /
 * {@code mysql_column_full_types} / {@code primary_keys} / {@code enum_set_values} /
 * {@code generated_columns}），本类的投影方法必须与
 * {@code MySQLBinlogExtractor} 今天从 {@code information_schema} 查出来的口径逐字一致，
 * 否则切换时会有暗差。
 */
public class TableSchema {

    private String database;
    private String table;

    /** 列，严格按 {@code ORDINAL_POSITION} 顺序——行事件的值就是按这个顺序排的。 */
    private List<ColumnSchema> columns = new ArrayList<>();

    /** 主键列，按 {@code seq_in_index} 顺序；无主键为空。 */
    private List<String> primaryKey = new ArrayList<>();

    /** 唯一索引：索引名 → 列清单（不含主键）。用于与目标端结构对比，不参与解析。 */
    private Map<String, List<String>> uniqueIndexes = new LinkedHashMap<>();

    /** 表级默认字符集 / 排序规则。 */
    private String charset;
    private String collation;

    /**
     * 这份结构是不是"推不出来的"（如 {@code CREATE TABLE ... AS SELECT}、DDL 解析失败）。
     * 置位后该表退出时序库，解析回落到查 {@code information_schema} 的旧路径并告警。
     */
    private boolean unusable;
    private String unusableReason;

    public TableSchema() {
    }

    public TableSchema(String database, String table) {
        this.database = database;
        this.table = table;
    }

    // ---- 对下游的六项元数据投影（口径必须与 information_schema 查出来的一致）----

    public List<String> columnNames() {
        List<String> names = new ArrayList<>(columns.size());
        for (ColumnSchema c : columns) {
            names.add(c.getName());
        }
        return names;
    }

    /** 对应 {@code DATA_TYPE}。 */
    public List<String> dataTypes() {
        List<String> types = new ArrayList<>(columns.size());
        for (ColumnSchema c : columns) {
            types.add(c.getTypeName());
        }
        return types;
    }

    /** 对应 {@code COLUMN_TYPE}，按源库版本口径渲染。 */
    public List<String> columnTypes(TypeRenderMode mode) {
        List<String> types = new ArrayList<>(columns.size());
        for (ColumnSchema c : columns) {
            types.add(c.columnType(mode));
        }
        return types;
    }

    /** 列名 → enum/set 取值表；没有 enum/set 列时返回空表。 */
    public Map<String, List<String>> enumSetValues() {
        Map<String, List<String>> out = new LinkedHashMap<>();
        for (ColumnSchema c : columns) {
            if (c.isEnumOrSet() && !c.getEnumValues().isEmpty()) {
                out.put(c.getName(), new ArrayList<>(c.getEnumValues()));
            }
        }
        return out;
    }

    /** 生成列（STORED / VIRTUAL）列名——apply 端拼 DML 时要按名字剔除，否则目标库报 3105。 */
    public List<String> generatedColumns() {
        List<String> out = new ArrayList<>();
        for (ColumnSchema c : columns) {
            if (c.isGenerated()) {
                out.add(c.getName());
            }
        }
        return out;
    }

    // ---- 查找 ----

    /** 按列名查（MySQL 列名不区分大小写），找不到返回 -1。 */
    public int indexOfColumn(String name) {
        if (name == null) {
            return -1;
        }
        for (int i = 0; i < columns.size(); i++) {
            if (name.equalsIgnoreCase(columns.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }

    public ColumnSchema findColumn(String name) {
        int i = indexOfColumn(name);
        return i < 0 ? null : columns.get(i);
    }

    public TableSchema copy() {
        TableSchema t = new TableSchema(database, table);
        for (ColumnSchema c : columns) {
            t.columns.add(c.copy());
        }
        t.primaryKey = new ArrayList<>(primaryKey);
        for (Map.Entry<String, List<String>> e : uniqueIndexes.entrySet()) {
            t.uniqueIndexes.put(e.getKey(), new ArrayList<>(e.getValue()));
        }
        t.charset = charset;
        t.collation = collation;
        t.unusable = unusable;
        t.unusableReason = unusableReason;
        return t;
    }

    /** {@code db.table} 小写——时序库与各类缓存统一用它做 key。 */
    public String key() {
        return key(database, table);
    }

    public static String key(String database, String table) {
        return ((database == null ? "" : database) + "." + (table == null ? "" : table))
                .toLowerCase(Locale.ROOT);
    }

    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }

    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }

    public List<ColumnSchema> getColumns() { return columns; }
    public void setColumns(List<ColumnSchema> columns) {
        this.columns = columns == null ? new ArrayList<>() : new ArrayList<>(columns);
    }

    public List<String> getPrimaryKey() { return primaryKey; }
    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey == null ? new ArrayList<>() : new ArrayList<>(primaryKey);
    }

    public Map<String, List<String>> getUniqueIndexes() { return uniqueIndexes; }
    public void setUniqueIndexes(Map<String, List<String>> uniqueIndexes) {
        this.uniqueIndexes = uniqueIndexes == null ? new LinkedHashMap<>() : new LinkedHashMap<>(uniqueIndexes);
    }

    public String getCharset() { return charset; }
    public void setCharset(String charset) { this.charset = charset; }

    public String getCollation() { return collation; }
    public void setCollation(String collation) { this.collation = collation; }

    public boolean isUnusable() { return unusable; }
    public String getUnusableReason() { return unusableReason; }

    public void markUnusable(String reason) {
        this.unusable = true;
        this.unusableReason = reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableSchema)) return false;
        TableSchema that = (TableSchema) o;
        return unusable == that.unusable
                && Objects.equals(database, that.database) && Objects.equals(table, that.table)
                && Objects.equals(columns, that.columns)
                && Objects.equals(primaryKey, that.primaryKey)
                && Objects.equals(uniqueIndexes, that.uniqueIndexes)
                && Objects.equals(charset, that.charset) && Objects.equals(collation, that.collation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, columns, primaryKey, uniqueIndexes, charset, collation, unusable);
    }

    @Override
    public String toString() {
        return key() + columnNames();
    }
}
