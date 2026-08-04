package com.migration.common.route;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MergeDdlRewriter} 单元测试：来源标识列追加、主键扩展（MySQL/PG 两种建表语句形态）、
 * 主键列解析（含 MySQL 前缀索引），以及无主键时的行为。
 */
@DisplayName("MergeDdlRewriter 汇聚建表改写")
class MergeDdlRewriterTest {

    private static final List<String> TAGS = Arrays.asList("_src_db", "_src_table");

    private static final String MYSQL_CREATE =
            "CREATE TABLE `order_001` (\n"
                    + "  `id` bigint NOT NULL,\n"
                    + "  `amount` decimal(10,2) DEFAULT NULL,\n"
                    + "  PRIMARY KEY (`id`)\n"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    private static final String PG_CREATE =
            "CREATE TABLE \"order_001\" (\n"
                    + "  \"id\" bigint NOT NULL,\n"
                    + "  \"amount\" numeric(10,2),\n"
                    + "  CONSTRAINT \"pk_order_001\" PRIMARY KEY (\"id\")\n"
                    + ")";

    @Test
    @DisplayName("MySQL：来源标识列追加在定义体末尾，且并入主键")
    void mysqlAppendAndExtend() {
        String sql = MergeDdlRewriter.appendTagColumns(MYSQL_CREATE, TAGS, false);
        assertTrue(sql.contains("`_src_db` VARCHAR(64) NOT NULL DEFAULT ''"));
        assertTrue(sql.contains("`_src_table` VARCHAR(64) NOT NULL DEFAULT ''"));
        // 追加在定义体内部，而不是跑到 ENGINE 之后
        assertTrue(sql.indexOf("_src_table") < sql.indexOf("ENGINE=InnoDB"));

        String extended = MergeDdlRewriter.extendPrimaryKey(sql, TAGS, false);
        assertTrue(extended.contains("PRIMARY KEY (`id`, `_src_db`, `_src_table`)"), extended);
    }

    @Test
    @DisplayName("PG：双引号标识符 + CONSTRAINT 形式的主键同样能扩展")
    void postgresAppendAndExtend() {
        String sql = MergeDdlRewriter.appendTagColumns(PG_CREATE, TAGS, true);
        assertTrue(sql.contains("\"_src_db\" VARCHAR(64) NOT NULL DEFAULT ''"));
        assertFalse(sql.contains("COMMENT"), "PG 列定义不能带 MySQL 的 COMMENT 子句");

        String extended = MergeDdlRewriter.extendPrimaryKey(sql, TAGS, true);
        assertTrue(extended.contains("PRIMARY KEY (\"id\", \"_src_db\", \"_src_table\")"), extended);
    }

    @Test
    @DisplayName("主键列解析：去引用符、去 MySQL 前缀长度、支持复合主键")
    void primaryKeyColumnParsing() {
        assertEquals(Collections.singletonList("id"), MergeDdlRewriter.primaryKeyColumns(MYSQL_CREATE));
        assertEquals(Collections.singletonList("id"), MergeDdlRewriter.primaryKeyColumns(PG_CREATE));

        String composite = "CREATE TABLE `t` (\n  `a` varchar(64) NOT NULL,\n  `b` int NOT NULL,\n"
                + "  PRIMARY KEY (`a`(10),`b`)\n) ENGINE=InnoDB";
        assertEquals(Arrays.asList("a", "b"), MergeDdlRewriter.primaryKeyColumns(composite));
    }

    @Test
    @DisplayName("无主键：hasPrimaryKey 为 false，主键扩展原样返回（由调用方拒绝汇聚）")
    void noPrimaryKey() {
        String noPk = "CREATE TABLE `t` (\n  `a` int DEFAULT NULL\n) ENGINE=InnoDB";
        assertFalse(MergeDdlRewriter.hasPrimaryKey(noPk));
        assertTrue(MergeDdlRewriter.primaryKeyColumns(noPk).isEmpty());
        String sql = MergeDdlRewriter.appendTagColumns(noPk, TAGS, false);
        assertEquals(sql, MergeDdlRewriter.extendPrimaryKey(sql, TAGS, false));
    }

    @Test
    @DisplayName("幂等：已在主键里的来源列不会重复加入")
    void extendIsIdempotent() {
        String once = MergeDdlRewriter.extendPrimaryKey(MYSQL_CREATE, TAGS, false);
        String twice = MergeDdlRewriter.extendPrimaryKey(once, TAGS, false);
        assertEquals(once, twice);
    }

    @Test
    @DisplayName("KEY/UNIQUE KEY 里的括号不会被误当成主键列表")
    void otherIndexesNotConfused() {
        String withIndexes = "CREATE TABLE `t` (\n  `id` bigint NOT NULL,\n  `k` varchar(32) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`) USING BTREE,\n  KEY `idx_k` (`k`)\n) ENGINE=InnoDB";
        assertEquals(Collections.singletonList("id"), MergeDdlRewriter.primaryKeyColumns(withIndexes));
        String extended = MergeDdlRewriter.extendPrimaryKey(withIndexes, TAGS, false);
        assertTrue(extended.contains("PRIMARY KEY (`id`, `_src_db`, `_src_table`) USING BTREE"), extended);
        assertTrue(extended.contains("KEY `idx_k` (`k`)"));
    }

    @Test
    @DisplayName("空来源列 / null 语句：原样返回，不做任何改写")
    void noOpCases() {
        assertEquals(MYSQL_CREATE, MergeDdlRewriter.appendTagColumns(MYSQL_CREATE, Collections.emptyList(), false));
        assertEquals(MYSQL_CREATE, MergeDdlRewriter.extendPrimaryKey(MYSQL_CREATE, null, false));
        assertEquals(null, MergeDdlRewriter.appendTagColumns(null, TAGS, false));
    }
}
