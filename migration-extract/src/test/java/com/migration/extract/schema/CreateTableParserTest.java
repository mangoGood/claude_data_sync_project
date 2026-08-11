package com.migration.extract.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CreateTableParser} 的单元测试。
 *
 * <p>断言的是<b>与 {@code information_schema} 对齐的口径</b>，不是"能不能解析"——
 * 时序库产出的 {@code DATA_TYPE}/{@code COLUMN_TYPE} 要顶替今天从 information_schema
 * 查出来的那份，口径差一点点（少个 unsigned、tinyint(1) 变成 tinyint）下游就会按错的类型
 * 转换值，而且看不出异常。
 */
@DisplayName("CREATE TABLE → TableSchema")
class CreateTableParserTest {

    private final CreateTableParser parser = new CreateTableParser();

    private TableSchema parse(String sql) {
        return parser.parse(sql, "testdb");
    }

    private String fullType(TableSchema s, String column, TypeRenderMode mode) {
        ColumnSchema c = s.findColumn(column);
        assertNotNull(c, "找不到列 " + column);
        return c.columnType(mode);
    }

    @Nested
    @DisplayName("基本结构")
    class Basics {

        @Test
        @DisplayName("库名取自表名限定符，没有限定符时取默认库")
        void databaseResolution() {
            assertEquals("testdb", parse("CREATE TABLE `t1` (`id` int NOT NULL)").getDatabase());
            assertEquals("other", parse("CREATE TABLE `other`.`t1` (`id` int)").getDatabase());
            assertEquals("t1", parse("CREATE TABLE `other`.`t1` (`id` int)").getTable());
        }

        @Test
        @DisplayName("列顺序严格按声明顺序——行事件的值就是按这个顺序排的")
        void columnOrderIsDeclarationOrder() {
            TableSchema s = parse("CREATE TABLE t (c int, a varchar(10), b bigint)");
            assertEquals(List.of("c", "a", "b"), s.columnNames());
        }

        @Test
        @DisplayName("表级 PRIMARY KEY 与列上 PRIMARY KEY 都能识别，复合主键保序")
        void primaryKey() {
            assertEquals(List.of("id"),
                    parse("CREATE TABLE t (id int PRIMARY KEY, v int)").getPrimaryKey());
            assertEquals(List.of("a", "b"),
                    parse("CREATE TABLE t (a int, b int, PRIMARY KEY (`a`,`b`))").getPrimaryKey());
            // 前缀长度与排序方向不能污染列名
            assertEquals(List.of("a", "b"),
                    parse("CREATE TABLE t (a varchar(64), b int, PRIMARY KEY (`a`(16) ASC, `b` DESC))")
                            .getPrimaryKey());
        }

        @Test
        @DisplayName("唯一索引进模型，普通索引/外键不进")
        void indexes() {
            TableSchema s = parse("CREATE TABLE t ("
                    + "id int, email varchar(64), name varchar(32), org int,"
                    + "PRIMARY KEY (id),"
                    + "UNIQUE KEY `uk_email` (`email`),"
                    + "KEY `idx_name` (`name`),"
                    + "CONSTRAINT `fk_org` FOREIGN KEY (`org`) REFERENCES `orgs` (`id`) ON DELETE CASCADE"
                    + ")");
            assertEquals(1, s.getUniqueIndexes().size());
            assertEquals(List.of("email"), s.getUniqueIndexes().get("uk_email"));
        }

        @Test
        @DisplayName("表级 charset / collate 从表选项里取")
        void tableOptions() {
            TableSchema s = parse("CREATE TABLE `t` (`id` int) ENGINE=InnoDB "
                    + "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci");
            assertEquals("utf8mb4", s.getCharset());
            assertEquals("utf8mb4_0900_ai_ci", s.getCollation());
        }
    }

    @Nested
    @DisplayName("类型口径（对齐 information_schema）")
    class Types {

        @Test
        @DisplayName("DATA_TYPE 归一：INTEGER/DEC/NUMERIC/REAL/BOOL 各归各家")
        void dataTypeNormalization() {
            TableSchema s = parse("CREATE TABLE t ("
                    + "a INTEGER, b DEC(10,2), c NUMERIC(8,3), d FIXED(6,1),"
                    + "e REAL, f BOOL, g BOOLEAN, h MIDDLEINT, i INT8)");
            assertEquals(List.of("int", "decimal", "decimal", "decimal",
                    "double", "tinyint", "tinyint", "mediumint", "bigint"), s.dataTypes());
        }

        @Test
        @DisplayName("BOOL 必须渲染成 tinyint(1)——下游靠这个 (1) 区分布尔列")
        void boolIsTinyintOne() {
            TableSchema s = parse("CREATE TABLE t (flag BOOLEAN, n TINYINT)");
            assertEquals("tinyint(1)", fullType(s, "flag", TypeRenderMode.NO_DISPLAY_WIDTH));
            // 同一口径下普通 tinyint 不带宽度，两者才区分得开
            assertEquals("tinyint", fullType(s, "n", TypeRenderMode.NO_DISPLAY_WIDTH));
        }

        @Test
        @DisplayName("unsigned / zerofill 落进 COLUMN_TYPE，zerofill 隐含 unsigned")
        void unsignedAndZerofill() {
            TableSchema s = parse("CREATE TABLE t (a BIGINT UNSIGNED, b INT(5) ZEROFILL)");
            assertEquals("bigint unsigned", fullType(s, "a", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertTrue(s.findColumn("b").isUnsigned(), "ZEROFILL 隐含 UNSIGNED");
            assertEquals("int(5) unsigned zerofill", fullType(s, "b", TypeRenderMode.NO_DISPLAY_WIDTH));
        }

        @Test
        @DisplayName("8.0.19 起整数不回显宽度，5.7 要补出默认宽度")
        void displayWidthByVersion() {
            TableSchema s = parse("CREATE TABLE t (a INT, b BIGINT UNSIGNED, c TINYINT, d INT(11))");
            assertEquals("int", fullType(s, "a", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("int(11)", fullType(s, "a", TypeRenderMode.WITH_DISPLAY_WIDTH));
            assertEquals("bigint(20) unsigned", fullType(s, "b", TypeRenderMode.WITH_DISPLAY_WIDTH));
            assertEquals("tinyint(4)", fullType(s, "c", TypeRenderMode.WITH_DISPLAY_WIDTH));
            // 显式写了宽度，8.0 口径下照样不回显（tinyint(1) 除外）
            assertEquals("int", fullType(s, "d", TypeRenderMode.NO_DISPLAY_WIDTH));
        }

        @Test
        @DisplayName("decimal / bit / char 的默认长度按 MySQL 补齐")
        void defaultLengths() {
            TableSchema s = parse("CREATE TABLE t (a DECIMAL, b DECIMAL(20), c BIT, d CHAR)");
            assertEquals("decimal(10,0)", fullType(s, "a", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("decimal(20,0)", fullType(s, "b", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("bit(1)", fullType(s, "c", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("char(1)", fullType(s, "d", TypeRenderMode.NO_DISPLAY_WIDTH));
        }

        @Test
        @DisplayName("时间类型的小数秒精度保留")
        void fractionalSeconds() {
            TableSchema s = parse("CREATE TABLE t (a DATETIME(3), b TIMESTAMP(6), c TIME)");
            assertEquals("datetime(3)", fullType(s, "a", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("timestamp(6)", fullType(s, "b", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("time", fullType(s, "c", TypeRenderMode.NO_DISPLAY_WIDTH));
        }

        @Test
        @DisplayName("列级 charset / collate")
        void columnCharset() {
            TableSchema s = parse("CREATE TABLE t ("
                    + "a varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,"
                    + "b text CHARSET latin1)");
            assertEquals("utf8mb4", s.findColumn("a").getCharset());
            assertEquals("utf8mb4_bin", s.findColumn("a").getCollation());
            assertEquals("latin1", s.findColumn("b").getCharset());
        }
    }

    @Nested
    @DisplayName("enum / set 取值表")
    class EnumSet {

        @Test
        @DisplayName("取值按声明顺序——binlog 给的是序号，顺序错一位就全错")
        void valuesInOrder() {
            TableSchema s = parse("CREATE TABLE t (st ENUM('a','b','c'), tags SET('x','y'))");
            assertEquals(List.of("a", "b", "c"), s.findColumn("st").getEnumValues());
            assertEquals(List.of("x", "y"), s.findColumn("tags").getEnumValues());
            assertEquals(List.of("st", "tags"), List.copyOf(s.enumSetValues().keySet()));
        }

        @Test
        @DisplayName("取值里的引号与逗号不能把取值表切错")
        void quotedValues() {
            TableSchema s = parse("CREATE TABLE t (st ENUM('a,b','it''s','say\\'hi'))");
            assertEquals(List.of("a,b", "it's", "say'hi"), s.findColumn("st").getEnumValues());
        }

        @Test
        @DisplayName("COLUMN_TYPE 渲染回 enum('a','b') 形态")
        void renderBack() {
            TableSchema s = parse("CREATE TABLE t (st ENUM('a','b') CHARACTER SET utf8mb4)");
            assertEquals("enum('a','b')", fullType(s, "st", TypeRenderMode.NO_DISPLAY_WIDTH));
        }
    }

    @Nested
    @DisplayName("生成列与默认值")
    class GeneratedAndDefaults {

        @Test
        @DisplayName("STORED / VIRTUAL 生成列进 generated_columns")
        void generatedColumns() {
            TableSchema s = parse("CREATE TABLE t ("
                    + "a int, b int,"
                    + "s int GENERATED ALWAYS AS (`a` + `b`) STORED,"
                    + "v int GENERATED ALWAYS AS (`a` * 2) VIRTUAL,"
                    + "w int AS (`a` - 1))");
            assertEquals(List.of("s", "v", "w"), s.generatedColumns());
            assertTrue(s.findColumn("s").isGeneratedStored());
            assertFalse(s.findColumn("v").isGeneratedStored());
            assertEquals("`a` + `b`", s.findColumn("s").getGenerationExpr());
        }

        @Test
        @DisplayName("DEFAULT 只存原文：字面量 / 负数 / 函数 / 8.0 的括号表达式")
        void defaults() {
            TableSchema s = parse("CREATE TABLE t ("
                    + "a int DEFAULT 0,"
                    + "b int DEFAULT -1,"
                    + "c varchar(8) DEFAULT 'x',"
                    + "d timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "e json DEFAULT (JSON_OBJECT()),"
                    + "f bit(1) DEFAULT b'0',"
                    + "g int DEFAULT NULL)");
            assertEquals("0", s.findColumn("a").getDefaultExpr());
            assertEquals("-1", s.findColumn("b").getDefaultExpr());
            assertEquals("'x'", s.findColumn("c").getDefaultExpr());
            assertEquals("CURRENT_TIMESTAMP", s.findColumn("d").getDefaultExpr());
            assertEquals("CURRENT_TIMESTAMP", s.findColumn("d").getOnUpdate());
            assertEquals("(JSON_OBJECT())", s.findColumn("e").getDefaultExpr());
            assertEquals("b'0'", s.findColumn("f").getDefaultExpr());
            assertEquals("NULL", s.findColumn("g").getDefaultExpr());
        }

        @Test
        @DisplayName("NOT NULL / AUTO_INCREMENT / COMMENT")
        void otherAttributes() {
            TableSchema s = parse("CREATE TABLE t ("
                    + "id bigint NOT NULL AUTO_INCREMENT COMMENT '主键',"
                    + "v int NULL, PRIMARY KEY (id))");
            ColumnSchema id = s.findColumn("id");
            assertFalse(id.isNullable());
            assertTrue(id.isAutoIncrement());
            assertEquals("主键", id.getComment());
            assertTrue(s.findColumn("v").isNullable());
        }
    }

    @Nested
    @DisplayName("真实 SHOW CREATE TABLE 与边界形态")
    class RealWorld {

        @Test
        @DisplayName("SHOW CREATE TABLE 的多行原文（8.0）")
        void showCreateTable80() {
            String sql = "CREATE TABLE `orders` (\n"
                    + "  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"
                    + "  `user_id` int NOT NULL DEFAULT '0',\n"
                    + "  `amount` decimal(20,4) NOT NULL DEFAULT '0.0000',\n"
                    + "  `status` enum('new','paid','shipped') CHARACTER SET utf8mb4 "
                    + "COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'new',\n"
                    + "  `is_vip` tinyint(1) DEFAULT '0',\n"
                    + "  `payload` json DEFAULT NULL,\n"
                    + "  `total` decimal(20,4) GENERATED ALWAYS AS ((`amount` * 2)) STORED,\n"
                    + "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP "
                    + "ON UPDATE CURRENT_TIMESTAMP,\n"
                    + "  PRIMARY KEY (`id`),\n"
                    + "  UNIQUE KEY `uk_user_status` (`user_id`,`status`),\n"
                    + "  KEY `idx_created` (`created_at`)\n"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=1024 DEFAULT CHARSET=utf8mb4 "
                    + "COLLATE=utf8mb4_0900_ai_ci COMMENT='订单表'";
            TableSchema s = parse(sql);

            assertEquals(List.of("id", "user_id", "amount", "status", "is_vip",
                    "payload", "total", "created_at"), s.columnNames());
            assertEquals(List.of("bigint", "int", "decimal", "enum", "tinyint",
                    "json", "decimal", "timestamp"), s.dataTypes());
            assertEquals("bigint unsigned", fullType(s, "id", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("tinyint(1)", fullType(s, "is_vip", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals("enum('new','paid','shipped')",
                    fullType(s, "status", TypeRenderMode.NO_DISPLAY_WIDTH));
            assertEquals(List.of("id"), s.getPrimaryKey());
            assertEquals(List.of("user_id", "status"), s.getUniqueIndexes().get("uk_user_status"));
            assertEquals(List.of("total"), s.generatedColumns());
            assertEquals("utf8mb4", s.getCharset());
        }

        @Test
        @DisplayName("分区子句被版本注释包着，不影响列解析")
        void partitionedTable() {
            String sql = "CREATE TABLE `logs` (\n"
                    + "  `id` bigint NOT NULL,\n"
                    + "  `ts` datetime NOT NULL,\n"
                    + "  PRIMARY KEY (`id`,`ts`)\n"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n"
                    + "/*!50100 PARTITION BY RANGE (year(`ts`))\n"
                    + "(PARTITION p2025 VALUES LESS THAN (2026) ENGINE = InnoDB,\n"
                    + " PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */";
            TableSchema s = parse(sql);
            assertEquals(List.of("id", "ts"), s.columnNames());
            assertEquals(List.of("id", "ts"), s.getPrimaryKey());
        }

        @Test
        @DisplayName("列名是关键字（不带反引号）也要能解析")
        void keywordColumnNames() {
            TableSchema s = parse("CREATE TABLE t (`key` int, comment varchar(10), "
                    + "`value` int, year int)");
            assertEquals(List.of("key", "comment", "value", "year"), s.columnNames());
        }

        @Test
        @DisplayName("CREATE TABLE ... AS SELECT / LIKE 推不出结构，标记不可用而不是硬猜")
        void unusableForms() {
            TableSchema asSelect = parse("CREATE TABLE t2 AS SELECT * FROM t1");
            assertTrue(asSelect.isUnusable());
            assertEquals("t2", asSelect.getTable());

            TableSchema like = parse("CREATE TABLE `t3` LIKE `t1`");
            assertTrue(like.isUnusable());
            assertTrue(like.getUnusableReason().contains("t1"));
        }

        @Test
        @DisplayName("解析不了的形态抛 DdlParseException，不能静默返回半棵树")
        void parseFailureThrows() {
            assertThrows(DdlParseException.class, () -> parse("CREATE TABLE t (a int"));
            assertThrows(DdlParseException.class, () -> parse(""));
        }
    }

    @Nested
    @DisplayName("JSON 往返")
    class Serialization {

        @Test
        @DisplayName("序列化再读回来结构完全一致——时序库靠这个落盘与恢复")
        void roundTrip() {
            TableSchema s = parse("CREATE TABLE `t` (`id` bigint unsigned NOT NULL AUTO_INCREMENT,"
                    + "`st` enum('a','b') DEFAULT 'a',"
                    + "`g` int GENERATED ALWAYS AS (`id` + 1) VIRTUAL,"
                    + "PRIMARY KEY (`id`), UNIQUE KEY `uk` (`st`)) DEFAULT CHARSET=utf8mb4");
            TableSchema back = SchemaJson.fromJson(SchemaJson.toJson(s));
            assertEquals(s, back);
            assertEquals(s.columnNames(), back.columnNames());
            assertEquals(s.enumSetValues(), back.enumSetValues());
            assertEquals(s.generatedColumns(), back.generatedColumns());
        }

        @Test
        @DisplayName("jsonl 落盘要求单行")
        void singleLine() {
            String json = SchemaJson.toJson(parse("CREATE TABLE t (a int, b varchar(4))"));
            assertFalse(json.contains("\n"), "jsonl 一行一条，多行会让崩溃残留无法安全跳过");
        }
    }

    @Nested
    @DisplayName("版本号 → 显示宽度口径")
    class VersionMode {

        @Test
        void mapping() {
            assertEquals(TypeRenderMode.WITH_DISPLAY_WIDTH, TypeRenderMode.forServerVersion("5.7.41-log"));
            assertEquals(TypeRenderMode.WITH_DISPLAY_WIDTH, TypeRenderMode.forServerVersion("8.0.18"));
            assertEquals(TypeRenderMode.NO_DISPLAY_WIDTH, TypeRenderMode.forServerVersion("8.0.19"));
            assertEquals(TypeRenderMode.NO_DISPLAY_WIDTH, TypeRenderMode.forServerVersion("8.0.44"));
            assertEquals(TypeRenderMode.NO_DISPLAY_WIDTH, TypeRenderMode.forServerVersion("8.4.0"));
            // 读不到版本时按新库口径，别让解析不出来的版本号变成一堆假宽度
            assertEquals(TypeRenderMode.NO_DISPLAY_WIDTH, TypeRenderMode.forServerVersion(null));
        }
    }
}
