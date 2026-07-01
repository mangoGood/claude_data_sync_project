package com.migration.increment.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Schema 演进映射配置。
 *
 * <p>支持以下映射能力：
 * <ul>
 *   <li>数据库名映射：source_db → target_db</li>
 *   <li>表名映射：source_db.source_table → target_db.target_table</li>
 *   <li>DDL 应用策略：AUTO_APPLY / SKIP / MANUAL</li>
 *   <li>跨库类型转换开关</li>
 * </ul>
 *
 * <p>配置通过 Properties 加载，键约定：
 * <ul>
 *   <li>{@code schema.ddl.apply.policy} = AUTO_APPLY | SKIP | MANUAL（默认 AUTO_APPLY）</li>
 *   <li>{@code schema.cross.db.type.convert} = true|false（默认 true）</li>
 *   <li>{@code schema.mapping.db.<sourceDb>} = <targetDb></li>
 *   <li>{@code schema.mapping.table.<sourceDb>.<sourceTable>} = <targetDb>.<targetTable></li>
 *   <li>{@code schema.ddl.skip.subtypes} = CREATE_DATABASE,DROP_DATABASE（逗号分隔，跳过的 DDL 子类型）</li>
 * </ul>
 */
public class SchemaMappingConfig {

    public enum DdlApplyPolicy {
        /** 自动将 DDL 应用到目标库（默认） */
        AUTO_APPLY,
        /** 跳过所有 DDL，仅同步 DML */
        SKIP,
        /** 仅记录 DDL 到日志/文件，由人工应用 */
        MANUAL
    }

    private DdlApplyPolicy ddlApplyPolicy = DdlApplyPolicy.AUTO_APPLY;
    private boolean crossDbTypeConvert = true;

    /** sourceDb → targetDb */
    private final Map<String, String> databaseMapping = new LinkedHashMap<>();
    /** sourceDb.sourceTable → targetDb.targetTable */
    private final Map<String, String> tableMapping = new LinkedHashMap<>();
    /** 需跳过的 DDL 子类型（大写），如 CREATE_DATABASE */
    private final java.util.Set<String> skippedDdlSubtypes = new java.util.HashSet<>();

    public SchemaMappingConfig() {
    }

    /**
     * 从 Properties 加载配置。
     */
    public static SchemaMappingConfig loadFromProperties(Properties props) {
        SchemaMappingConfig config = new SchemaMappingConfig();

        String policy = props.getProperty("schema.ddl.apply.policy", "AUTO_APPLY");
        try {
            config.ddlApplyPolicy = DdlApplyPolicy.valueOf(policy.toUpperCase());
        } catch (IllegalArgumentException e) {
            config.ddlApplyPolicy = DdlApplyPolicy.AUTO_APPLY;
        }

        config.crossDbTypeConvert = Boolean.parseBoolean(
                props.getProperty("schema.cross.db.type.convert", "true"));

        // 加载数据库映射：schema.mapping.db.<sourceDb>=<targetDb>
        String dbPrefix = "schema.mapping.db.";
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(dbPrefix)) {
                String sourceDb = name.substring(dbPrefix.length());
                String targetDb = props.getProperty(name);
                if (sourceDb != null && targetDb != null && !sourceDb.isEmpty() && !targetDb.isEmpty()) {
                    config.databaseMapping.put(sourceDb, targetDb);
                }
            }
        }

        // 加载表映射：schema.mapping.table.<sourceDb>.<sourceTable>=<targetDb>.<targetTable>
        String tablePrefix = "schema.mapping.table.";
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(tablePrefix)) {
                String key = name.substring(tablePrefix.length());
                String value = props.getProperty(name);
                if (key != null && value != null && !key.isEmpty() && !value.isEmpty()) {
                    config.tableMapping.put(key, value);
                }
            }
        }

        // 加载跳过的 DDL 子类型
        String skipSubtypes = props.getProperty("schema.ddl.skip.subtypes", "");
        if (skipSubtypes != null && !skipSubtypes.isEmpty()) {
            for (String sub : skipSubtypes.split(",")) {
                String trimmed = sub.trim().toUpperCase();
                if (!trimmed.isEmpty()) {
                    config.skippedDdlSubtypes.add(trimmed);
                }
            }
        }

        return config;
    }

    public DdlApplyPolicy getDdlApplyPolicy() {
        return ddlApplyPolicy;
    }

    public boolean isCrossDbTypeConvert() {
        return crossDbTypeConvert;
    }

    public Map<String, String> getDatabaseMapping() {
        return Collections.unmodifiableMap(databaseMapping);
    }

    public Map<String, String> getTableMapping() {
        return Collections.unmodifiableMap(tableMapping);
    }

    public java.util.Set<String> getSkippedDdlSubtypes() {
        return Collections.unmodifiableSet(skippedDdlSubtypes);
    }

    /**
     * 映射数据库名，未配置则返回原值。
     */
    public String mapDatabase(String sourceDb) {
        if (sourceDb == null) return null;
        return databaseMapping.getOrDefault(sourceDb, sourceDb);
    }

    /**
     * 映射表名（含库名前缀），未配置则返回原值。
     *
     * @param sourceDb    源库
     * @param sourceTable 源表
     * @return 目标 "db.table" 或原值
     */
    public String mapTable(String sourceDb, String sourceTable) {
        if (sourceDb == null || sourceTable == null) {
            return (sourceDb == null ? "" : sourceDb) + "." + (sourceTable == null ? "" : sourceTable);
        }
        String key = sourceDb + "." + sourceTable;
        if (tableMapping.containsKey(key)) {
            return tableMapping.get(key);
        }
        String targetDb = mapDatabase(sourceDb);
        return targetDb + "." + sourceTable;
    }

    /**
     * 判断指定 DDL 子类型是否应跳过。
     */
    public boolean shouldSkipDdlSubtype(String ddlSubType) {
        if (ddlSubType == null) return false;
        return skippedDdlSubtypes.contains(ddlSubType.toUpperCase());
    }
}
