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
}
