package com.migration.extract.schema;

/**
 * 一条 DDL 对某一张表造成的结构变化——{@link DdlApplier} 的产出，时序库的输入。
 *
 * <p>一条 DDL 可以产生多条：{@code RENAME TABLE a TO b} 是 a 的 {@link Kind#DROPPED}
 * 加 b 的 {@link Kind#CREATED}；{@code DROP TABLE a, b} 是两条 DROPPED。
 */
public class SchemaChange {

    public enum Kind {
        /** 新表（CREATE TABLE，或 RENAME 的目标名）。 */
        CREATED,
        /** 结构变了（ALTER / CREATE INDEX / DROP INDEX）。 */
        ALTERED,
        /** 表没了（DROP TABLE，或 RENAME 的源名）。 */
        DROPPED
    }

    private final Kind kind;
    private final String database;
    private final String table;
    /** 变化<b>之后</b>的结构；DROPPED 时为 null。 */
    private final TableSchema schema;
    /** RENAME 时的另一端（CREATED 记源名，DROPPED 记目标名），其余为 null。 */
    private final String renameCounterpart;

    private SchemaChange(Kind kind, String database, String table,
                         TableSchema schema, String renameCounterpart) {
        this.kind = kind;
        this.database = database;
        this.table = table;
        this.schema = schema;
        this.renameCounterpart = renameCounterpart;
    }

    public static SchemaChange created(TableSchema schema) {
        return new SchemaChange(Kind.CREATED, schema.getDatabase(), schema.getTable(), schema, null);
    }

    public static SchemaChange renamedTo(TableSchema schema, String fromQualified) {
        return new SchemaChange(Kind.CREATED, schema.getDatabase(), schema.getTable(), schema, fromQualified);
    }

    public static SchemaChange altered(TableSchema schema) {
        return new SchemaChange(Kind.ALTERED, schema.getDatabase(), schema.getTable(), schema, null);
    }

    public static SchemaChange dropped(String database, String table) {
        return new SchemaChange(Kind.DROPPED, database, table, null, null);
    }

    public static SchemaChange renamedFrom(String database, String table, String toQualified) {
        return new SchemaChange(Kind.DROPPED, database, table, null, toQualified);
    }

    public Kind getKind() { return kind; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public TableSchema getSchema() { return schema; }
    public String getRenameCounterpart() { return renameCounterpart; }

    public String key() {
        return TableSchema.key(database, table);
    }

    @Override
    public String toString() {
        return kind + " " + key() + (renameCounterpart == null ? "" : " (rename ↔ " + renameCounterpart + ")");
    }
}
