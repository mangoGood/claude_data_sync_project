package com.migration.extract.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DdlApplier} 单元测试：DDL 施加到结构模型的正确性。
 *
 * <p>这一层错了的后果是最隐蔽的一类——模型与源库差一列，之后每一个行事件都按错的列布局解析，
 * 写进目标库的是合法值、看不出异常。所以断言全部落在"施加之后的列清单/顺序/类型/主键"上，
 * 而不是"有没有抛异常"。
 */
@DisplayName("DDL 施加到表结构模型")
class DdlApplierTest {

    private final CreateTableParser parser = new CreateTableParser();
    private DdlApplier applier = new DdlApplier();

    /** 内存里的一份"时序库当前版本"，模拟阶段 3 的查询接口。 */
    private final Map<String, TableSchema> store = new HashMap<>();

    private final DdlApplier.SchemaLookup lookup =
            (db, table) -> store.get(TableSchema.key(db, table));

    private void seed(String createSql) {
        TableSchema s = parser.parse(createSql, "db");
        store.put(s.key(), s);
    }

    /** 施加一条 DDL 并把结果写回 store（阶段 3 的时序库会做同样的事）。 */
    private List<SchemaChange> apply(String sql) {
        List<SchemaChange> changes = applier.apply(sql, "db", lookup);
        for (SchemaChange c : changes) {
            if (c.getKind() == SchemaChange.Kind.DROPPED) {
                store.remove(c.key());
            } else {
                store.put(c.key(), c.getSchema());
            }
        }
        return changes;
    }

    private TableSchema table(String qualified) {
        return store.get(qualified.toLowerCase());
    }

    @Nested
    @DisplayName("列的增删改")
    class Columns {

        @Test
        @DisplayName("ADD COLUMN 默认追加到末尾")
        void addColumnAtEnd() {
            seed("CREATE TABLE t (a int, b int)");
            apply("ALTER TABLE t ADD COLUMN c varchar(10) NOT NULL DEFAULT 'x'");

            TableSchema s = table("db.t");
            assertEquals(List.of("a", "b", "c"), s.columnNames());
            assertEquals("varchar(10)", s.findColumn("c").columnType(TypeRenderMode.NO_DISPLAY_WIDTH));
            assertFalse(s.findColumn("c").isNullable());
            assertEquals("'x'", s.findColumn("c").getDefaultExpr());
        }

        @Test
        @DisplayName("FIRST / AFTER 决定插入位置——列顺序就是行事件里值的顺序，错一位全错")
        void addColumnWithPosition() {
            seed("CREATE TABLE t (a int, b int, c int)");
            apply("ALTER TABLE t ADD COLUMN head int FIRST");
            assertEquals(List.of("head", "a", "b", "c"), table("db.t").columnNames());

            apply("ALTER TABLE t ADD COLUMN mid int AFTER `a`");
            assertEquals(List.of("head", "a", "mid", "b", "c"), table("db.t").columnNames());
        }

        @Test
        @DisplayName("DROP COLUMN 同时把它从主键与唯一索引里摘掉")
        void dropColumn() {
            seed("CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a,b), UNIQUE KEY uk (b,c))");
            apply("ALTER TABLE t DROP COLUMN b");

            TableSchema s = table("db.t");
            assertEquals(List.of("a", "c"), s.columnNames());
            assertEquals(List.of("a"), s.getPrimaryKey());
            assertEquals(List.of("c"), s.getUniqueIndexes().get("uk"));
        }

        @Test
        @DisplayName("MODIFY 改类型但不动位置")
        void modifyColumn() {
            seed("CREATE TABLE t (a int, b int, c int)");
            apply("ALTER TABLE t MODIFY COLUMN b bigint unsigned NOT NULL");

            TableSchema s = table("db.t");
            assertEquals(List.of("a", "b", "c"), s.columnNames());
            assertEquals("bigint unsigned",
                    s.findColumn("b").columnType(TypeRenderMode.NO_DISPLAY_WIDTH));
            assertFalse(s.findColumn("b").isNullable());
        }

        @Test
        @DisplayName("MODIFY 带 AFTER 时要先摘下来再插——下标在移除之后算")
        void modifyColumnWithPosition() {
            seed("CREATE TABLE t (a int, b int, c int)");
            apply("ALTER TABLE t MODIFY COLUMN a bigint AFTER c");
            assertEquals(List.of("b", "c", "a"), table("db.t").columnNames());
        }

        @Test
        @DisplayName("CHANGE 同时改名与改型，主键里的列名也跟着改")
        void changeColumn() {
            seed("CREATE TABLE t (id int, old_name varchar(10), PRIMARY KEY (old_name))");
            apply("ALTER TABLE t CHANGE COLUMN old_name new_name varchar(64) NOT NULL");

            TableSchema s = table("db.t");
            assertEquals(List.of("id", "new_name"), s.columnNames());
            assertEquals("varchar(64)",
                    s.findColumn("new_name").columnType(TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals(List.of("new_name"), s.getPrimaryKey());
        }

        @Test
        @DisplayName("RENAME COLUMN 只改名字，类型与位置不动")
        void renameColumn() {
            seed("CREATE TABLE t (a int, b varchar(8), UNIQUE KEY uk (b))");
            apply("ALTER TABLE t RENAME COLUMN b TO bb");

            TableSchema s = table("db.t");
            assertEquals(List.of("a", "bb"), s.columnNames());
            assertEquals("varchar(8)", s.findColumn("bb").columnType(TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals(List.of("bb"), s.getUniqueIndexes().get("uk"));
        }

        @Test
        @DisplayName("ALTER COLUMN SET/DROP DEFAULT")
        void alterColumnDefault() {
            seed("CREATE TABLE t (a int DEFAULT 1)");
            apply("ALTER TABLE t ALTER COLUMN a SET DEFAULT 9");
            assertEquals("9", table("db.t").findColumn("a").getDefaultExpr());

            apply("ALTER TABLE t ALTER COLUMN a DROP DEFAULT");
            assertNull(table("db.t").findColumn("a").getDefaultExpr());
        }
    }

    @Nested
    @DisplayName("多子句按序施加")
    class MultiClause {

        @Test
        @DisplayName("一条语句里的多个子句按书写顺序作用在中间态上")
        void inOrder() {
            seed("CREATE TABLE t (a int, b int, c int)");
            apply("ALTER TABLE t ADD COLUMN d int, DROP COLUMN b, MODIFY COLUMN c bigint");

            TableSchema s = table("db.t");
            assertEquals(List.of("a", "c", "d"), s.columnNames());
            assertEquals("bigint", s.findColumn("c").columnType(TypeRenderMode.NO_DISPLAY_WIDTH));
        }

        @Test
        @DisplayName("先 CHANGE 改名、后面的子句按新名字操作——顺序敏感的典型")
        void renameThenOperateOnNewName() {
            seed("CREATE TABLE t (a int, b int)");
            apply("ALTER TABLE t CHANGE COLUMN b bb int, ADD COLUMN c int AFTER bb");
            assertEquals(List.of("a", "bb", "c"), table("db.t").columnNames());
        }

        @Test
        @DisplayName("ALGORITHM/LOCK 子句不影响列布局，也不能让整条语句解析失败")
        void algorithmAndLockClauses() {
            seed("CREATE TABLE t (a int)");
            apply("ALTER TABLE t ADD COLUMN b int, ALGORITHM=INPLACE, LOCK=NONE");
            assertEquals(List.of("a", "b"), table("db.t").columnNames());
        }

        @Test
        @DisplayName("ADD COLUMN (c1 ..., c2 ...) 括号批量形式")
        void addColumnList() {
            seed("CREATE TABLE t (a int)");
            apply("ALTER TABLE t ADD COLUMN (b int, c varchar(4))");
            assertEquals(List.of("a", "b", "c"), table("db.t").columnNames());
        }
    }

    @Nested
    @DisplayName("主键与唯一索引")
    class Keys {

        @Test
        @DisplayName("ADD / DROP PRIMARY KEY")
        void primaryKey() {
            seed("CREATE TABLE t (a int, b int)");
            apply("ALTER TABLE t ADD PRIMARY KEY (a, b)");
            assertEquals(List.of("a", "b"), table("db.t").getPrimaryKey());

            apply("ALTER TABLE t DROP PRIMARY KEY");
            assertEquals(List.of(), table("db.t").getPrimaryKey());
        }

        @Test
        @DisplayName("唯一索引的增删；普通索引不进模型")
        void uniqueIndexes() {
            seed("CREATE TABLE t (a int, b int)");
            apply("ALTER TABLE t ADD UNIQUE KEY uk_a (a)");
            assertEquals(List.of("a"), table("db.t").getUniqueIndexes().get("uk_a"));

            apply("ALTER TABLE t ADD KEY idx_b (b)");
            assertEquals(1, table("db.t").getUniqueIndexes().size(), "普通索引不该进模型");

            apply("ALTER TABLE t DROP INDEX uk_a");
            assertTrue(table("db.t").getUniqueIndexes().isEmpty());
        }

        @Test
        @DisplayName("CREATE UNIQUE INDEX / DROP INDEX 独立语句")
        void standaloneIndexStatements() {
            seed("CREATE TABLE t (a int, b int)");
            apply("CREATE UNIQUE INDEX uk_ab ON t (a, b)");
            assertEquals(List.of("a", "b"), table("db.t").getUniqueIndexes().get("uk_ab"));

            apply("CREATE INDEX idx_a ON t (a)");
            assertEquals(1, table("db.t").getUniqueIndexes().size());

            apply("DROP INDEX uk_ab ON t");
            assertTrue(table("db.t").getUniqueIndexes().isEmpty());
        }
    }

    @Nested
    @DisplayName("RENAME / DROP TABLE")
    class TableLevel {

        @Test
        @DisplayName("RENAME TABLE 把结构搬到新名字下，旧名字退出时序库")
        void renameTable() {
            seed("CREATE TABLE t (a int, b varchar(8))");
            apply("RENAME TABLE t TO t2");

            assertNull(table("db.t"));
            assertNotNull(table("db.t2"));
            assertEquals(List.of("a", "b"), table("db.t2").columnNames());
            assertEquals("t2", table("db.t2").getTable());
        }

        @Test
        @DisplayName("pt-osc 收尾的交换：t TO _t_old, _t_new TO t 必须从左到右")
        void ptOscSwap() {
            // 影子表带着新结构（多一列 c），原表是旧结构
            seed("CREATE TABLE t (a int, b int)");
            seed("CREATE TABLE _t_new (a int, b int, c int)");

            apply("RENAME TABLE `db`.`t` TO `db`.`_t_old`, `db`.`_t_new` TO `db`.`t`");

            // t 现在应该是影子表的新结构；顺序处理错了的话 t 会保持旧结构或直接消失
            assertNotNull(table("db.t"), "交换之后 t 必须存在");
            assertEquals(List.of("a", "b", "c"), table("db.t").columnNames(),
                    "t 应该拿到影子表的新结构");
            assertEquals(List.of("a", "b"), table("db._t_old").columnNames());
            assertNull(table("db._t_new"), "影子表名字应该已经让出去了");
        }

        @Test
        @DisplayName("三方交换 a→tmp, b→a, tmp→b 也要正确")
        void threeWaySwap() {
            seed("CREATE TABLE a (x int)");
            seed("CREATE TABLE b (y int, z int)");
            apply("RENAME TABLE a TO tmp, b TO a, tmp TO b");

            assertEquals(List.of("y", "z"), table("db.a").columnNames());
            assertEquals(List.of("x"), table("db.b").columnNames());
            assertNull(table("db.tmp"));
        }

        @Test
        @DisplayName("源表不在时序库里时，目标名必须退出时序库而不是留着旧版本")
        void renameFromUnknownTableEvictsTarget() {
            seed("CREATE TABLE t (a int, b int)");
            // _t_new 不在同步范围（时序库里没有），却改名成了 t
            apply("RENAME TABLE `db`.`_t_new` TO `db`.`t`");

            assertNull(table("db.t"),
                    "t 的结构已经变成未知的了，留着旧版本会让之后的事件按错的结构解析");
        }

        @Test
        @DisplayName("ALTER ... RENAME TO 与 DROP TABLE")
        void alterRenameAndDrop() {
            seed("CREATE TABLE t (a int)");
            apply("ALTER TABLE t RENAME TO t9");
            assertNull(table("db.t"));
            assertEquals(List.of("a"), table("db.t9").columnNames());

            apply("DROP TABLE t9");
            assertNull(table("db.t9"));
        }

        @Test
        @DisplayName("DROP TABLE 多表")
        void dropMultiple() {
            seed("CREATE TABLE a (x int)");
            seed("CREATE TABLE b (y int)");
            apply("DROP TABLE IF EXISTS a, b");
            assertNull(table("db.a"));
            assertNull(table("db.b"));
        }
    }

    @Nested
    @DisplayName("幂等与边界")
    class Idempotency {

        @Test
        @DisplayName("重复施加同一条 ADD COLUMN 不会加两列，但要计数")
        void addColumnTwice() {
            seed("CREATE TABLE t (a int)");
            apply("ALTER TABLE t ADD COLUMN b int");
            apply("ALTER TABLE t ADD COLUMN b int");

            assertEquals(List.of("a", "b"), table("db.t").columnNames());
            assertEquals(1, applier.getStats().addColumnAlreadyExists.sum(),
                    "幂等吸收必须被记下来——持续增长说明模型跟丢了，不能默默吞掉");
        }

        @Test
        @DisplayName("DROP 不存在的列同样幂等 + 计数")
        void dropMissingColumn() {
            seed("CREATE TABLE t (a int)");
            apply("ALTER TABLE t DROP COLUMN nope");
            assertEquals(List.of("a"), table("db.t").columnNames());
            assertEquals(1, applier.getStats().dropColumnMissing.sum());
        }

        @Test
        @DisplayName("不在时序库里的表，ALTER 不产生任何版本（不能凭空造一个残缺版本）")
        void alterUnknownTable() {
            List<SchemaChange> changes = apply("ALTER TABLE ghost ADD COLUMN a int");
            assertEquals(List.of(), changes);
            assertEquals(1, applier.getStats().tableUnknown.sum());
        }

        @Test
        @DisplayName("解析不了的 DDL 抛异常——绝不当作\"不影响结构\"跳过")
        void unparseableThrows() {
            seed("CREATE TABLE t (a int)");
            assertThrows(DdlParseException.class, () -> apply("ALTER TABLE t ADD COLUMN"));
            assertThrows(DdlParseException.class, () -> apply(""));
        }

        @Test
        @DisplayName("与结构无关的 DDL 返回空，不算失败")
        void unrelatedDdl() {
            assertEquals(List.of(), apply("CREATE DATABASE other"));
            assertEquals(List.of(), apply("TRUNCATE TABLE t"));
        }

        @Test
        @DisplayName("库限定名与默认库上下文")
        void databaseQualification() {
            TableSchema s = parser.parse("CREATE TABLE `other`.`t` (a int)", "db");
            store.put(s.key(), s);

            apply("ALTER TABLE `other`.`t` ADD COLUMN b int");
            assertEquals(List.of("a", "b"), table("other.t").columnNames());
            // 默认库下的同名表不存在，不该被误伤
            assertNull(table("db.t"));
        }
    }

    @Nested
    @DisplayName("语法缺口的处置：不改列的放过，改列的必须报出来")
    class UnknownClauses {

        @Test
        @DisplayName("分区操作不改列布局，放过")
        void partitionOperations() {
            seed("CREATE TABLE t (a int, ts datetime)");
            apply("ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES LESS THAN (100))");
            apply("ALTER TABLE t DROP PARTITION p1");
            apply("ALTER TABLE t TRUNCATE PARTITION p3");

            assertEquals(List.of("a", "ts"), table("db.t").columnNames());
            // DROP PARTITION 不能被当成 DROP COLUMN partition —— 那会污染幂等计数，
            // 让"模型跟丢了"这个真信号淹没在噪声里
            assertEquals(0, applier.getStats().dropColumnMissing.sum());
        }

        @Test
        @DisplayName("RENAME INDEX / ALTER INDEX 不改列布局，放过")
        void indexMetaOperations() {
            seed("CREATE TABLE t (a int, KEY idx (a))");
            apply("ALTER TABLE t RENAME INDEX idx TO idx2");
            apply("ALTER TABLE t ALTER INDEX idx2 INVISIBLE");
            assertEquals(List.of("a"), table("db.t").columnNames());
        }

        @Test
        @DisplayName("以 ADD/DROP/MODIFY 开头但形态没覆盖的子句必须抛，不能静默跳过")
        void unknownStructuralClauseThrows() {
            seed("CREATE TABLE t (a int)");
            // 一条改列的 ALTER 被静默跳过，该表之后的每个版本都是错的——宁可报解析失败
            assertThrows(DdlParseException.class,
                    () -> apply("ALTER TABLE t ADD SOMETHING WEIRD HERE THAT IS NOT A COLUMN DEF"));
        }

        @Test
        @DisplayName("前导版本注释不影响施加")
        void leadingVersionComment() {
            seed("CREATE TABLE t (a int)");
            apply("/*!80000 ALGORITHM=INSTANT */ ALTER TABLE t ADD COLUMN b int");
            assertEquals(List.of("a", "b"), table("db.t").columnNames());
        }
    }

    @Nested
    @DisplayName("字符集")
    class Charset {

        @Test
        @DisplayName("CONVERT TO CHARACTER SET 改表级与所有字符列的字符集")
        void convertCharset() {
            seed("CREATE TABLE t (a int, b varchar(10) CHARACTER SET latin1) DEFAULT CHARSET=latin1");
            apply("ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4");

            assertEquals("utf8mb4", table("db.t").getCharset());
            assertEquals("utf8mb4", table("db.t").findColumn("b").getCharset());
        }
    }

    @Nested
    @DisplayName("CREATE TABLE LIKE")
    class CreateLike {

        @Test
        @DisplayName("源表在时序库里时复制其结构")
        void copiesSourceSchema() {
            seed("CREATE TABLE src (a int, b varchar(8), PRIMARY KEY (a))");
            apply("CREATE TABLE dst LIKE src");

            TableSchema s = table("db.dst");
            assertNotNull(s);
            assertFalse(s.isUnusable());
            assertEquals(List.of("a", "b"), s.columnNames());
            assertEquals(List.of("a"), s.getPrimaryKey());
            assertEquals("dst", s.getTable());
        }

        @Test
        @DisplayName("源表不在时序库里时保持不可用，交给调用方降级")
        void unknownSourceStaysUnusable() {
            apply("CREATE TABLE dst LIKE nowhere");
            assertTrue(table("db.dst").isUnusable());
        }
    }
}
