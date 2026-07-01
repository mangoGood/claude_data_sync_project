package com.migration.increment.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SchemaMappingConfig} 单元测试。
 */
@DisplayName("SchemaMappingConfig 配置测试")
class SchemaMappingConfigTest {

    @Test
    @DisplayName("默认配置应为 AUTO_APPLY 且启用跨库转换")
    void defaultConfigShouldBeAutoApplyAndConvertEnabled() {
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(new Properties());
        assertEquals(SchemaMappingConfig.DdlApplyPolicy.AUTO_APPLY, config.getDdlApplyPolicy());
        assertTrue(config.isCrossDbTypeConvert());
    }

    @Test
    @DisplayName("应正确加载 DDL 应用策略")
    void shouldLoadDdlApplyPolicy() {
        Properties props = new Properties();
        props.setProperty("schema.ddl.apply.policy", "SKIP");
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(props);
        assertEquals(SchemaMappingConfig.DdlApplyPolicy.SKIP, config.getDdlApplyPolicy());
    }

    @Test
    @DisplayName("无效策略应回退到 AUTO_APPLY")
    void invalidPolicyShouldFallbackToAutoApply() {
        Properties props = new Properties();
        props.setProperty("schema.ddl.apply.policy", "INVALID");
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(props);
        assertEquals(SchemaMappingConfig.DdlApplyPolicy.AUTO_APPLY, config.getDdlApplyPolicy());
    }

    @Test
    @DisplayName("应正确加载数据库映射")
    void shouldLoadDatabaseMapping() {
        Properties props = new Properties();
        props.setProperty("schema.mapping.db.source_db", "target_db");
        props.setProperty("schema.mapping.db.db_a", "db_b");
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(props);

        assertEquals("target_db", config.mapDatabase("source_db"));
        assertEquals("db_b", config.mapDatabase("db_a"));
        // 未映射的数据库返回原值
        assertEquals("unmapped", config.mapDatabase("unmapped"));
    }

    @Test
    @DisplayName("应正确加载表映射")
    void shouldLoadTableMapping() {
        Properties props = new Properties();
        props.setProperty("schema.mapping.table.source_db.users", "target_db.accounts");
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(props);

        assertEquals("target_db.accounts", config.mapTable("source_db", "users"));
    }

    @Test
    @DisplayName("表映射未配置时应使用数据库映射推导")
    void tableMappingShouldFallbackToDatabaseMapping() {
        Properties props = new Properties();
        props.setProperty("schema.mapping.db.source_db", "target_db");
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(props);

        assertEquals("target_db.orders", config.mapTable("source_db", "orders"));
    }

    @Test
    @DisplayName("应正确加载跳过的 DDL 子类型")
    void shouldLoadSkippedDdlSubtypes() {
        Properties props = new Properties();
        props.setProperty("schema.ddl.skip.subtypes", "CREATE_DATABASE, DROP_DATABASE ,TRUNCATE");
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(props);

        assertTrue(config.shouldSkipDdlSubtype("CREATE_DATABASE"));
        assertTrue(config.shouldSkipDdlSubtype("DROP_DATABASE"));
        assertTrue(config.shouldSkipDdlSubtype("truncate"));
        assertFalse(config.shouldSkipDdlSubtype("CREATE_TABLE"));
    }

    @Test
    @DisplayName("跨库转换开关应可关闭")
    void crossDbConvertCanBeDisabled() {
        Properties props = new Properties();
        props.setProperty("schema.cross.db.type.convert", "false");
        SchemaMappingConfig config = SchemaMappingConfig.loadFromProperties(props);
        assertFalse(config.isCrossDbTypeConvert());
    }
}
