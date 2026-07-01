package com.migration.increment.schema;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

/**
 * OnlineDdlService 单元测试
 * 验证 gh-ost/pt-osc 影子表 DDL 识别和转换逻辑
 */
public class OnlineDdlServiceTest {

    private OnlineDdlService createService() {
        Properties props = new Properties();
        props.setProperty("online.ddl.enabled", "true");
        return new OnlineDdlService(props);
    }

    @Test
    public void testGhostShadowTableRename_skip() {
        OnlineDdlService service = createService();
        String sql = "RENAME TABLE `_users_ghost_` TO `users`";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "RENAME");
        assertTrue(result.shouldSkip(), "gh-ost RENAME 影子表应跳过");
        assertNull(result.getSql(), "跳过后 SQL 应为 null");
    }

    @Test
    public void testPtOscShadowTableRename_skip() {
        OnlineDdlService service = createService();
        String sql = "RENAME TABLE `_users_new` TO `users`";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "RENAME");
        assertTrue(result.shouldSkip(), "pt-osc RENAME 影子表应跳过");
    }

    @Test
    public void testNormalRename_passThrough() {
        OnlineDdlService service = createService();
        String sql = "RENAME TABLE `old_users` TO `new_users`";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "RENAME");
        assertEquals(OnlineDdlService.OnlineDdlResult.Action.PASS_THROUGH, result.getAction(),
                "非影子表 RENAME 应原样传递");
        assertEquals(sql, result.getSql());
    }

    @Test
    public void testGhostCreateShadowTable_convert() {
        OnlineDdlService service = createService();
        String sql = "CREATE TABLE `_users_ghost_` (`id` INT, `name` VARCHAR(100))";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "CREATE_TABLE");
        assertTrue(result.shouldConvert(), "gh-ost CREATE 影子表应转换为 ALTER");
        assertNotNull(result.getSql(), "转换后应有 SQL");
        assertTrue(result.getSql().toUpperCase().startsWith("ALTER TABLE"),
                "转换后 SQL 应为 ALTER TABLE");
        assertTrue(result.getSql().contains("users"), "目标表应为原表 users");
    }

    @Test
    public void testGhostShadowTableDml_skip() {
        OnlineDdlService service = createService();
        String sql = "INSERT INTO `_users_ghost_` (`id`, `name`) VALUES (1, 'test')";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "INSERT");
        assertTrue(result.shouldSkip(), "gh-ost 影子表 INSERT 应跳过");
    }

    @Test
    public void testGhostShadowTableAlter_skip() {
        OnlineDdlService service = createService();
        String sql = "ALTER TABLE `_users_ghost_` ADD INDEX idx_name (`name`)";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "ALTER");
        assertTrue(result.shouldSkip(), "gh-ost 影子表 ALTER 应跳过");
    }

    @Test
    public void testGhostShadowTableDrop_skip() {
        OnlineDdlService service = createService();
        String sql = "DROP TABLE `_users_ghost_`";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "DROP_TABLE");
        assertTrue(result.shouldSkip(), "gh-ost DROP 影子表应跳过");
    }

    @Test
    public void testNormalDdl_passThrough() {
        OnlineDdlService service = createService();
        String sql = "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "ALTER");
        assertEquals(OnlineDdlService.OnlineDdlResult.Action.PASS_THROUGH, result.getAction(),
                "非影子表 ALTER 应原样传递");
        assertEquals(sql, result.getSql());
    }

    @Test
    public void testDisabledService_passThrough() {
        Properties props = new Properties();
        props.setProperty("online.ddl.enabled", "false");
        OnlineDdlService service = new OnlineDdlService(props);

        String sql = "RENAME TABLE `_users_ghost_` TO `users`";
        OnlineDdlService.OnlineDdlResult result = service.process(sql, "RENAME");
        assertEquals(OnlineDdlService.OnlineDdlResult.Action.PASS_THROUGH, result.getAction(),
                "禁用时应原样传递");
    }

    @Test
    public void testStats() {
        OnlineDdlService service = createService();
        service.process("RENAME TABLE `_users_ghost_` TO `users`", "RENAME");
        service.process("CREATE TABLE `_orders_ghost_` (`id` INT)", "CREATE_TABLE");
        service.process("ALTER TABLE `users` ADD COLUMN `x` INT", "ALTER");

        java.util.Map<String, Object> stats = service.getStats();
        assertEquals(Boolean.TRUE, stats.get("enabled"));
        assertEquals(2L, stats.get("totalGhostDdlDetected"));
        assertEquals(1L, stats.get("totalGhostDdlConverted"));
        assertEquals(1L, stats.get("totalGhostDdlSkipped"));
    }
}
