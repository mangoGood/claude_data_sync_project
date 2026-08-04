package com.migration.common.route;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SplitDdlRewriter} 单元测试：分片表名改写（含库名限定）与 AUTO_INCREMENT 剥离。
 */
@DisplayName("SplitDdlRewriter 拆分建表改写")
class SplitDdlRewriterTest {

    private static final String MYSQL_CREATE =
            "CREATE TABLE `order` (\n"
                    + "  `id` bigint NOT NULL AUTO_INCREMENT,\n"
                    + "  `user_id` bigint NOT NULL,\n"
                    + "  `note` varchar(64) DEFAULT NULL,\n"
                    + "  PRIMARY KEY (`id`)\n"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=1024 DEFAULT CHARSET=utf8mb4";

    @Test
    @DisplayName("改写到分片库表：CREATE TABLE `dw_0`.`order_3`")
    void retargetWithDatabase() {
        String sql = SplitDdlRewriter.retargetCreateTable(MYSQL_CREATE, "dw_0", "order_3", false);
        assertTrue(sql.startsWith("CREATE TABLE `dw_0`.`order_3` ("), sql);
        // 定义体不受影响
        assertTrue(sql.contains("`user_id` bigint NOT NULL"));
    }

    @Test
    @DisplayName("不给库名：只改表名，目标库由连接决定")
    void retargetWithoutDatabase() {
        String sql = SplitDdlRewriter.retargetCreateTable(MYSQL_CREATE, null, "order_3", false);
        assertTrue(sql.startsWith("CREATE TABLE `order_3` ("), sql);
    }

    @Test
    @DisplayName("PG：双引号标识符，且不加库名限定（一条连接跨不了库）")
    void retargetPostgres() {
        String pg = "CREATE TABLE \"order\" (\n  \"id\" bigint NOT NULL\n)";
        String sql = SplitDdlRewriter.retargetCreateTable(pg, "dw_0", "order_3", true);
        assertTrue(sql.startsWith("CREATE TABLE \"order_3\" ("), sql);
        assertFalse(sql.contains("dw_0"), sql);
    }

    @Test
    @DisplayName("源语句已带库名限定时同样能改写")
    void retargetQualifiedSource() {
        String qualified = "CREATE TABLE `app`.`order` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB";
        String sql = SplitDdlRewriter.retargetCreateTable(qualified, "dw_1", "order_5", false);
        assertTrue(sql.startsWith("CREATE TABLE `dw_1`.`order_5` ("), sql);
    }

    @Test
    @DisplayName("AUTO_INCREMENT：列属性与表选项都剥掉（各分片自增会撞号）")
    void stripAutoIncrement() {
        String sql = SplitDdlRewriter.stripAutoIncrement(MYSQL_CREATE);
        assertFalse(sql.toUpperCase().contains("AUTO_INCREMENT"), sql);
        // 列定义其余部分保持不变
        assertTrue(sql.contains("`id` bigint NOT NULL,"), sql);
        assertTrue(sql.contains("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"), sql);
    }

    @Test
    @DisplayName("没有 AUTO_INCREMENT 的语句原样返回")
    void stripIsNoOpWhenAbsent() {
        String plain = "CREATE TABLE `t` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB";
        assertEquals(plain, SplitDdlRewriter.stripAutoIncrement(plain));
    }

    @Test
    @DisplayName("非 CREATE TABLE 语句 / null：原样返回，不做半吊子改写")
    void noOpCases() {
        assertEquals("SELECT 1", SplitDdlRewriter.retargetCreateTable("SELECT 1", "d", "t", false));
        assertEquals(null, SplitDdlRewriter.retargetCreateTable(null, "d", "t", false));
        assertEquals(MYSQL_CREATE, SplitDdlRewriter.retargetCreateTable(MYSQL_CREATE, "d", "", false));
    }
}
