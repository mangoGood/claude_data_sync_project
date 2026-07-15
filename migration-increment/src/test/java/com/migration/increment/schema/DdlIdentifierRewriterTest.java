package com.migration.increment.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link DdlIdentifierRewriter} 单元测试：库名映射改写的正确性（含引号、字符串字面量不误伤等）。
 */
@DisplayName("DDL 库名改写测试")
class DdlIdentifierRewriterTest {

    /** test1 -> test2 的映射；其它库名原样返回。 */
    private final Function<String, String> map = db -> "test1".equals(db) ? "test2" : db;

    @Test
    @DisplayName("CREATE TABLE 限定库名改写 test1.t5 -> test2.t5")
    void createTableQualified() {
        assertEquals("CREATE TABLE test2.t5 (id INT PRIMARY KEY)",
                DdlIdentifierRewriter.rewriteSchema("CREATE TABLE test1.t5 (id INT PRIMARY KEY)", map));
    }

    @Test
    @DisplayName("反引号库名改写 `test1`.`t5` -> `test2`.`t5`")
    void backtickQualified() {
        assertEquals("CREATE TABLE `test2`.`t5` (id INT)",
                DdlIdentifierRewriter.rewriteSchema("CREATE TABLE `test1`.`t5` (id INT)", map));
    }

    @Test
    @DisplayName("USE 语句改写 USE test1 -> USE test2")
    void useStatement() {
        assertEquals("USE test2", DdlIdentifierRewriter.rewriteSchema("USE test1", map));
        assertEquals("USE `test2`", DdlIdentifierRewriter.rewriteSchema("USE `test1`", map));
    }

    @Test
    @DisplayName("ALTER / DROP / TRUNCATE 限定库名改写")
    void alterDropTruncate() {
        assertEquals("ALTER TABLE test2.t5 ADD COLUMN c INT",
                DdlIdentifierRewriter.rewriteSchema("ALTER TABLE test1.t5 ADD COLUMN c INT", map));
        assertEquals("DROP TABLE test2.t5",
                DdlIdentifierRewriter.rewriteSchema("DROP TABLE test1.t5", map));
        assertEquals("TRUNCATE TABLE test2.t5",
                DdlIdentifierRewriter.rewriteSchema("TRUNCATE TABLE test1.t5", map));
    }

    @Test
    @DisplayName("字符串字面量里的 'test1.' 不被误改")
    void stringLiteralNotTouched() {
        String sql = "INSERT INTO test1.t (id, note) VALUES (1, 'from test1.oldtable')";
        assertEquals("INSERT INTO test2.t (id, note) VALUES (1, 'from test1.oldtable')",
                DdlIdentifierRewriter.rewriteSchema(sql, map));
    }

    @Test
    @DisplayName("无库限定符时原样返回（依赖目标连接默认库）")
    void unqualifiedUnchanged() {
        assertEquals("CREATE TABLE t5 (id INT)",
                DdlIdentifierRewriter.rewriteSchema("CREATE TABLE t5 (id INT)", map));
    }

    @Test
    @DisplayName("库名未命中映射时原样返回")
    void noMappingUnchanged() {
        assertEquals("CREATE TABLE other.t5 (id INT)",
                DdlIdentifierRewriter.rewriteSchema("CREATE TABLE other.t5 (id INT)", map));
    }

    @Test
    @DisplayName("RENAME TABLE 多个限定名各自改写")
    void renameMultiple() {
        assertEquals("RENAME TABLE test2.a TO test2.b",
                DdlIdentifierRewriter.rewriteSchema("RENAME TABLE test1.a TO test1.b", map));
    }

    @Test
    @DisplayName("FK REFERENCES 里的限定库名改写")
    void foreignKeyReference() {
        String sql = "CREATE TABLE test1.child (id INT, pid INT, FOREIGN KEY (pid) REFERENCES test1.parent(id))";
        assertEquals("CREATE TABLE test2.child (id INT, pid INT, FOREIGN KEY (pid) REFERENCES test2.parent(id))",
                DdlIdentifierRewriter.rewriteSchema(sql, map));
    }

    @Test
    @DisplayName("表名映射预留能力：schema.table 同时改库名与表名")
    void tableMappingExtension() {
        Map<String, String> tbl = Map.of("test1.t5", "t5_new");
        String out = DdlIdentifierRewriter.rewrite("CREATE TABLE test1.t5 (id INT)", map,
                (db, t) -> tbl.getOrDefault(db + "." + t, t));
        assertEquals("CREATE TABLE test2.t5_new (id INT)", out);
    }

    @Test
    @DisplayName("null / 空输入健壮返回")
    void nullSafe() {
        assertEquals(null, DdlIdentifierRewriter.rewriteSchema(null, map));
        assertEquals("", DdlIdentifierRewriter.rewriteSchema("", map));
    }

    // ==== 表名映射（表级同步）：非限定表名走 defaultSchema 上下文 ====

    /** test1 库下 t1 -> t1_new 的表映射；其它原样。 */
    private final java.util.function.BiFunction<String, String, String> tblMap =
            (db, t) -> Map.of("test1.t1", "t1_new").getOrDefault(db + "." + t, t);

    @Test
    @DisplayName("非限定 CREATE/ALTER/DROP/TRUNCATE 表名按 defaultSchema 映射")
    void unqualifiedTableMapping() {
        assertEquals("CREATE TABLE t1_new (id INT PRIMARY KEY)",
                DdlIdentifierRewriter.rewrite("CREATE TABLE t1 (id INT PRIMARY KEY)", map, tblMap, "test1"));
        assertEquals("CREATE TABLE IF NOT EXISTS `t1_new` (id INT)",
                DdlIdentifierRewriter.rewrite("CREATE TABLE IF NOT EXISTS `t1` (id INT)", map, tblMap, "test1"));
        assertEquals("ALTER TABLE t1_new ADD COLUMN c INT",
                DdlIdentifierRewriter.rewrite("ALTER TABLE t1 ADD COLUMN c INT", map, tblMap, "test1"));
        assertEquals("DROP TABLE IF EXISTS t1_new",
                DdlIdentifierRewriter.rewrite("DROP TABLE IF EXISTS t1", map, tblMap, "test1"));
        assertEquals("TRUNCATE TABLE t1_new",
                DdlIdentifierRewriter.rewrite("TRUNCATE TABLE t1", map, tblMap, "test1"));
        assertEquals("TRUNCATE t1_new",
                DdlIdentifierRewriter.rewrite("TRUNCATE t1", map, tblMap, "test1"));
    }

    @Test
    @DisplayName("非限定表名：defaultSchema 不匹配映射库时不改")
    void unqualifiedWrongSchemaUnchanged() {
        assertEquals("CREATE TABLE t1 (id INT)",
                DdlIdentifierRewriter.rewrite("CREATE TABLE t1 (id INT)", map, tblMap, "otherdb"));
        assertEquals("ALTER TABLE t2 ADD COLUMN c INT",
                DdlIdentifierRewriter.rewrite("ALTER TABLE t2 ADD COLUMN c INT", map, tblMap, "test1"));
    }

    @Test
    @DisplayName("限定名 test1.t1 同时改库名与表名（defaultSchema 版本）")
    void qualifiedTableMappingWithDefaultSchema() {
        assertEquals("ALTER TABLE test2.t1_new ADD COLUMN c INT",
                DdlIdentifierRewriter.rewrite("ALTER TABLE test1.t1 ADD COLUMN c INT", map, tblMap, "test1"));
        // 限定名的映射 key 用限定库名而非 defaultSchema：other.t1 不命中 test1.t1
        assertEquals("ALTER TABLE other.t1 ADD COLUMN c INT",
                DdlIdentifierRewriter.rewrite("ALTER TABLE other.t1 ADD COLUMN c INT", map, tblMap, "test1"));
    }

    @Test
    @DisplayName("DROP TABLE 多表清单逐个映射")
    void dropMultipleTables() {
        assertEquals("DROP TABLE t1_new, t2",
                DdlIdentifierRewriter.rewrite("DROP TABLE t1, t2", map, tblMap, "test1"));
    }

    @Test
    @DisplayName("RENAME TABLE 非限定名两侧均按映射改写")
    void renameUnqualified() {
        assertEquals("RENAME TABLE t1_new TO t9",
                DdlIdentifierRewriter.rewrite("RENAME TABLE t1 TO t9", map, tblMap, "test1"));
    }

    @Test
    @DisplayName("FK REFERENCES 非限定表名映射")
    void referencesUnqualified() {
        assertEquals("CREATE TABLE child (id INT, pid INT, FOREIGN KEY (pid) REFERENCES t1_new(id))",
                DdlIdentifierRewriter.rewrite(
                        "CREATE TABLE child (id INT, pid INT, FOREIGN KEY (pid) REFERENCES t1(id))",
                        map, tblMap, "test1"));
    }

    @Test
    @DisplayName("字符串字面量里的表名不被误改")
    void tableNameInStringLiteralNotTouched() {
        assertEquals("ALTER TABLE t1_new COMMENT = 'ALTER TABLE t1 backup'",
                DdlIdentifierRewriter.rewrite(
                        "ALTER TABLE t1 COMMENT = 'ALTER TABLE t1 backup'",
                        map, tblMap, "test1"));
    }

    @Test
    @DisplayName("列名与表名同名不误改（非表名位置的标识符不动）")
    void columnNamedLikeTableNotTouched() {
        assertEquals("ALTER TABLE t1_new ADD COLUMN t1 INT",
                DdlIdentifierRewriter.rewrite("ALTER TABLE t1 ADD COLUMN t1 INT", map, tblMap, "test1"));
    }

    @Test
    @DisplayName("客户端注释前缀 + 全小写限定名：库名与表名都改写（回归任务 9e1e602e）")
    void leadingCommentLowercaseQualifiedAlter() {
        String in = "/* ApplicationName=DBeaver 25.2.4 - SQLEditor <Script-4.sql> */ "
                + "alter table test1.t1 add column name2 varchar(20)";
        String out = DdlIdentifierRewriter.rewrite(in, map, tblMap, "test1");
        assertEquals("/* ApplicationName=DBeaver 25.2.4 - SQLEditor <Script-4.sql> */ "
                + "alter table test2.t1_new add column name2 varchar(20)", out);
    }
}
