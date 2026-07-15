package com.migration.extract;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DdlDatabaseAnltrExtractor#extractAffectedTables} 及其正则兜底
 * {@link DdlDatabaseExtractor#extractAffectedTables} 的单元测试。
 *
 * <p>该解析用于增量抽取在 DDL 后失效列元数据缓存：解析出表级 DDL 影响的
 * {@code database.table} 键，让下一个数据事件重新读取 information_schema。
 */
@DisplayName("DDL 受影响表解析测试")
class DdlDatabaseAnltrExtractorTest {

    @Nested
    @DisplayName("ANTLR 主解析路径")
    class AntlrPath {

        @Test
        @DisplayName("限定库名的 ALTER TABLE 返回 database.table")
        void alterTableQualified() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "ALTER TABLE test1.t5 ADD COLUMN name VARCHAR(30)", null));
        }

        @Test
        @DisplayName("未限定库名的 ALTER TABLE 用 defaultDatabase 补全")
        void alterTableUnqualifiedUsesDefault() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "ALTER TABLE t5 ADD COLUMN name VARCHAR(30)", "test1"));
        }

        @Test
        @DisplayName("反引号包裹的库名/表名被正确剥离")
        void backtickQuoted() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "ALTER TABLE `test1`.`t5` ADD COLUMN `name` VARCHAR(30)", null));
        }

        @Test
        @DisplayName("DROP TABLE IF EXISTS 返回受影响表")
        void dropTable() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables("DROP TABLE IF EXISTS t5", "test1"));
        }

        @Test
        @DisplayName("DROP TABLE 多表逐个返回，未限定名用 default 补全")
        void dropMultipleTables() {
            assertEquals(List.of("test1.t1", "test1.t2", "db2.t3"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "DROP TABLE IF EXISTS test1.t1, t2, db2.t3", "test1"));
        }

        @Test
        @DisplayName("DROP TABLE ... CASCADE 尾缀不被当作表名")
        void dropMultipleTablesWithCascade() {
            assertEquals(List.of("a.t1", "a.t2"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables("DROP TABLE a.t1, a.t2 CASCADE", null));
        }

        @Test
        @DisplayName("TRUNCATE TABLE 返回受影响表")
        void truncateTable() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables("TRUNCATE TABLE test1.t5", null));
        }

        @Test
        @DisplayName("CREATE TABLE IF NOT EXISTS 返回受影响表")
        void createTable() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "CREATE TABLE IF NOT EXISTS test1.t5 (id INT PRIMARY KEY)", null));
        }

        @Test
        @DisplayName("RENAME 单对返回旧名与新名")
        void renameSinglePair() {
            assertEquals(List.of("test1.t5", "test1.t6"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "RENAME TABLE test1.t5 TO test1.t6", null));
        }

        @Test
        @DisplayName("RENAME 多对返回全部旧名与新名")
        void renameMultiPair() {
            assertEquals(List.of("a.t1", "a.t2", "b.t3", "b.t4"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "RENAME TABLE a.t1 TO a.t2, b.t3 TO b.t4", null));
        }

        @Test
        @DisplayName("RENAME 未限定的新名用 defaultDatabase 补全")
        void renameUnqualifiedTargetUsesDefault() {
            assertEquals(List.of("a.t1", "cur.t2"),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(
                            "RENAME TABLE a.t1 TO t2", "cur"));
        }

        @Test
        @DisplayName("库级 DDL 不影响列布局，返回空")
        void databaseDdlReturnsEmpty() {
            assertTrue(DdlDatabaseAnltrExtractor.extractAffectedTables(
                    "CREATE DATABASE test1", null).isEmpty());
            assertTrue(DdlDatabaseAnltrExtractor.extractAffectedTables(
                    "DROP DATABASE test1", null).isEmpty());
        }

        @Test
        @DisplayName("索引 DDL 不改变列布局，返回空")
        void indexDdlReturnsEmpty() {
            assertTrue(DdlDatabaseAnltrExtractor.extractAffectedTables(
                    "CREATE INDEX idx ON test1.t5 (id)", null).isEmpty());
            assertTrue(DdlDatabaseAnltrExtractor.extractAffectedTables(
                    "DROP INDEX idx ON test1.t5", null).isEmpty());
        }

        @Test
        @DisplayName("DML 语句返回空")
        void dmlReturnsEmpty() {
            assertTrue(DdlDatabaseAnltrExtractor.extractAffectedTables(
                    "INSERT INTO test1.t5 VALUES (1, 'x')", "test1").isEmpty());
        }

        @Test
        @DisplayName("null 与空串返回空列表")
        void nullOrEmptyReturnsEmpty() {
            assertEquals(Collections.emptyList(),
                    DdlDatabaseAnltrExtractor.extractAffectedTables(null, "test1"));
            assertEquals(Collections.emptyList(),
                    DdlDatabaseAnltrExtractor.extractAffectedTables("   ", "test1"));
        }
    }

    @Nested
    @DisplayName("正则兜底路径")
    class RegexFallback {

        @Test
        @DisplayName("限定库名的 ALTER TABLE")
        void alterTableQualified() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseExtractor.extractAffectedTables(
                            "ALTER TABLE test1.t5 ADD COLUMN name VARCHAR(30)", null));
        }

        @Test
        @DisplayName("未限定库名用 defaultDatabase 补全")
        void alterTableUnqualified() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseExtractor.extractAffectedTables("ALTER TABLE t5 ADD COLUMN c INT", "test1"));
        }

        @Test
        @DisplayName("反引号剥离")
        void backtickQuoted() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseExtractor.extractAffectedTables(
                            "ALTER TABLE `test1`.`t5` ADD COLUMN c INT", null));
        }

        @Test
        @DisplayName("CREATE / DROP / TRUNCATE TABLE")
        void createDropTruncate() {
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseExtractor.extractAffectedTables("CREATE TABLE test1.t5 (id INT)", null));
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseExtractor.extractAffectedTables("DROP TABLE IF EXISTS test1.t5", null));
            assertEquals(List.of("test1.t5"),
                    DdlDatabaseExtractor.extractAffectedTables("TRUNCATE test1.t5", null));
        }

        @Test
        @DisplayName("DROP TABLE 多表（含未限定名与 CASCADE 尾缀）")
        void dropMultipleTables() {
            assertEquals(List.of("test1.t1", "test1.t2", "db2.t3"),
                    DdlDatabaseExtractor.extractAffectedTables(
                            "DROP TABLE IF EXISTS test1.t1, t2, db2.t3", "test1"));
            assertEquals(List.of("a.t1", "a.t2"),
                    DdlDatabaseExtractor.extractAffectedTables("DROP TABLE a.t1, a.t2 CASCADE", null));
        }

        @Test
        @DisplayName("RENAME 多对返回全部旧名与新名")
        void renameMultiPair() {
            assertEquals(List.of("a.t1", "a.t2", "b.t3", "b.t4"),
                    DdlDatabaseExtractor.extractAffectedTables(
                            "RENAME TABLE a.t1 TO a.t2, b.t3 TO b.t4", null));
        }

        @Test
        @DisplayName("库级/索引/DML 返回空")
        void nonColumnAlteringReturnsEmpty() {
            assertTrue(DdlDatabaseExtractor.extractAffectedTables("CREATE DATABASE test1", null).isEmpty());
            assertTrue(DdlDatabaseExtractor.extractAffectedTables("CREATE INDEX idx ON test1.t5 (id)", null).isEmpty());
            assertTrue(DdlDatabaseExtractor.extractAffectedTables("INSERT INTO test1.t5 VALUES (1)", "test1").isEmpty());
        }
    }
}
