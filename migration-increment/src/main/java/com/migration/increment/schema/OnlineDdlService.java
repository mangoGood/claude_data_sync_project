package com.migration.increment.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 在线表结构变更同步服务（gh-ost/pt-osc 风格）
 *
 * <p>gh-ost 和 pt-osc 通过影子表 + 触发器/CDC 实现在线 DDL，会产生大量影子表 DDL：
 * <ul>
 *   <li>gh-ost: 创建 `_tbl_ghost_` 影子表，copy 数据后 RENAME 替换原表</li>
 *   <li>pt-osc: 创建 `_tbl_new` 影子表，copy 数据后 RENAME 替换原表</li>
 * </ul>
 *
 * <p>本服务识别这些影子表操作，将其转换为对目标库原表的直接 DDL 应用，
 * 避免在目标库重放完整的影子表流程（节省资源和时间）。
 *
 * <p>策略：
 * <ol>
 *   <li>识别 CREATE 影子表 → 转换为对原表的 ALTER TABLE</li>
 *   <li>识别 RENAME 影子表 TO 原表 → 跳过（目标库已通过 ALTER 完成变更）</li>
 *   <li>识别 INSERT INTO 影子表 → 跳过（数据由 binlog 同步保证）</li>
 *   <li>保留对原表的直接 DDL（如非 ghost 流程的 ALTER）</li>
 * </ol>
 */
public class OnlineDdlService {
    private static final Logger logger = LoggerFactory.getLogger(OnlineDdlService.class);

    // gh-ost 影子表命名：_tbl_ghost_ 或 _tbl_gho 或 _tbl_ghc
    private static final Pattern GHOST_SHADOW_PATTERN = Pattern.compile("_([a-zA-Z0-9_]+)_(?:ghost_|gho|ghc)$", Pattern.CASE_INSENSITIVE);
    // pt-osc 影子表命名：_tbl_new
    private static final Pattern PT_OSC_SHADOW_PATTERN = Pattern.compile("_([a-zA-Z0-9_]+)_new$", Pattern.CASE_INSENSITIVE);
    // RENAME 语句模式：RENAME TABLE `_tbl_ghost_` TO `tbl`
    private static final Pattern RENAME_PATTERN = Pattern.compile(
            "RENAME\\s+TABLE\\s+[`\\[]?([^\\s`,\\[\\]]+)[`\\]]?\\s+TO\\s+[`\\[]?([^\\s`,\\[\\]]+)[`\\]]?\\s*$",
            Pattern.CASE_INSENSITIVE);
    // CREATE TABLE 模式
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(
            "CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?[`\\[]?([^\\s`,\\[\\]\\(]+)[`\\]]?",
            Pattern.CASE_INSENSITIVE);
    // ALTER TABLE 模式
    private static final Pattern ALTER_TABLE_PATTERN = Pattern.compile(
            "ALTER\\s+TABLE\\s+[`\\[]?([^\\s`,\\[\\]]+)[`\\]]?",
            Pattern.CASE_INSENSITIVE);

    private final boolean enabled;
    private long totalGhostDdlDetected = 0;
    private long totalGhostDdlConverted = 0;
    private long totalGhostDdlSkipped = 0;

    public OnlineDdlService(Properties props) {
        this.enabled = Boolean.parseBoolean(props.getProperty("online.ddl.enabled", "true"));
        logger.info("OnlineDdlService 初始化 | enabled={}", enabled);
    }

    /**
     * 处理 DDL 事件，判断是否为 gh-ost/pt-osc 影子表操作。
     *
     * @param sql       原始 DDL SQL
     * @param ddlSubType DDL 子类型
     * @return 处理结果
     */
    public OnlineDdlResult process(String sql, String ddlSubType) {
        if (!enabled || sql == null || sql.trim().isEmpty()) {
            return OnlineDdlResult.passThrough(sql);
        }

        String trimmedSql = sql.trim();
        String upperSql = trimmedSql.toUpperCase();

        // 1. 检测 RENAME TABLE 影子表 → 原表（ghost 完成阶段）
        if (upperSql.startsWith("RENAME TABLE")) {
            Matcher m = RENAME_PATTERN.matcher(trimmedSql);
            if (m.matches()) {
                String fromTable = m.group(1).replace("`", "").replace("[", "").replace("]", "");
                String toTable = m.group(2).replace("`", "").replace("[", "").replace("]", "");
                if (isShadowTable(fromTable) && !isShadowTable(toTable)) {
                    totalGhostDdlDetected++;
                    totalGhostDdlSkipped++;
                    logger.info("gh-ost/pt-osc RENAME 影子表操作，跳过（目标库已通过ALTER完成变更）: {} -> {}",
                            fromTable, toTable);
                    return OnlineDdlResult.skip("gh-ost/pt-osc RENAME 影子表，目标库已通过 ALTER 完成变更");
                }
            }
        }

        // 2. 检测 CREATE 影子表（ghost 初始化阶段）
        if (upperSql.startsWith("CREATE TABLE")) {
            Matcher m = CREATE_TABLE_PATTERN.matcher(trimmedSql);
            if (m.find()) {
                String tableName = m.group(1).replace("`", "").replace("[", "").replace("]", "");
                if (isShadowTable(tableName)) {
                    totalGhostDdlDetected++;
                    // 提取原表名
                    String originalTable = extractOriginalTableName(tableName);
                    if (originalTable != null) {
                        // 转换为对原表的 ALTER TABLE（添加 ghost 表中新增的列/索引）
                        String convertedSql = convertCreateShadowToAlter(trimmedSql, tableName, originalTable);
                        if (convertedSql != null) {
                            totalGhostDdlConverted++;
                            logger.info("gh-ost/pt-osc CREATE 影子表转换为 ALTER 原表: {} -> {}",
                                    tableName, originalTable);
                            return OnlineDdlResult.convert(convertedSql, originalTable);
                        }
                    }
                    // 无法转换则跳过
                    totalGhostDdlSkipped++;
                    return OnlineDdlResult.skip("gh-ost/pt-osc CREATE 影子表，跳过");
                }
            }
        }

        // 3. 检测针对影子表的 INSERT/UPDATE/DELETE（数据 copy 阶段）
        if (upperSql.startsWith("INSERT INTO") || upperSql.startsWith("UPDATE") || upperSql.startsWith("DELETE FROM")) {
            String tableName = extractTableNameFromDml(trimmedSql);
            if (tableName != null && isShadowTable(tableName)) {
                totalGhostDdlDetected++;
                totalGhostDdlSkipped++;
                logger.debug("gh-ost/pt-osc 影子表 DML 操作，跳过: {}", tableName);
                return OnlineDdlResult.skip("gh-ost/pt-osc 影子表 DML，跳过");
            }
        }

        // 4. 检测针对影子表的 ALTER/INDEX（ghost 中间阶段）
        if (upperSql.startsWith("ALTER TABLE") || upperSql.contains("CREATE INDEX") || upperSql.contains("DROP INDEX")) {
            Matcher m = ALTER_TABLE_PATTERN.matcher(trimmedSql);
            if (m.find()) {
                String tableName = m.group(1).replace("`", "").replace("[", "").replace("]", "");
                if (isShadowTable(tableName)) {
                    totalGhostDdlDetected++;
                    totalGhostDdlSkipped++;
                    logger.info("gh-ost/pt-osc 影子表 ALTER/INDEX 操作，跳过: {}", tableName);
                    return OnlineDdlResult.skip("gh-ost/pt-osc 影子表 ALTER/INDEX，跳过");
                }
            }
        }

        // 5. 检测 DROP 影子表（ghost 清理阶段）
        if (upperSql.startsWith("DROP TABLE") || upperSql.startsWith("DROP INDEX")) {
            String tableName = extractTableNameFromDrop(trimmedSql);
            if (tableName != null && isShadowTable(tableName)) {
                totalGhostDdlDetected++;
                totalGhostDdlSkipped++;
                logger.info("gh-ost/pt-osc DROP 影子表操作，跳过: {}", tableName);
                return OnlineDdlResult.skip("gh-ost/pt-osc DROP 影子表，跳过");
            }
        }

        // 非影子表操作，原样传递
        return OnlineDdlResult.passThrough(sql);
    }

    /** 判断是否为影子表 */
    private boolean isShadowTable(String tableName) {
        return GHOST_SHADOW_PATTERN.matcher(tableName).find()
                || PT_OSC_SHADOW_PATTERN.matcher(tableName).find();
    }

    /** 从影子表名提取原表名 */
    private String extractOriginalTableName(String shadowTable) {
        Matcher ghostMatcher = GHOST_SHADOW_PATTERN.matcher(shadowTable);
        if (ghostMatcher.matches()) {
            return ghostMatcher.group(1);
        }
        Matcher ptMatcher = PT_OSC_SHADOW_PATTERN.matcher(shadowTable);
        if (ptMatcher.matches()) {
            return ptMatcher.group(1);
        }
        return null;
    }

    /**
     * 将 CREATE 影子表转换为对原表的 ALTER TABLE。
     * 简化策略：提取新增列定义，生成 ALTER TABLE ADD COLUMN。
     */
    private String convertCreateShadowToAlter(String createSql, String shadowTable, String originalTable) {
        try {
            // 提取列定义部分（括号内内容）
            int start = createSql.indexOf("(");
            int end = createSql.lastIndexOf(")");
            if (start < 0 || end < 0 || end <= start) return null;

            String columnsDef = createSql.substring(start + 1, end);
            // 简化：直接生成 ALTER TABLE 原表 ADD (列定义)
            // 实际生产中需要对比原表结构，只添加新列
            return String.format("ALTER TABLE `%s` ADD COLUMN (%s)", originalTable, columnsDef);
        } catch (Exception e) {
            logger.warn("转换 CREATE 影子表为 ALTER 失败: {}", e.getMessage());
            return null;
        }
    }

    private String extractTableNameFromDml(String sql) {
        Matcher insertMatcher = Pattern.compile("INSERT\\s+INTO\\s+[`\\[]?([^\\s`,\\[\\]]+)[`\\]]?", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (insertMatcher.find()) return insertMatcher.group(1).replace("`", "");
        Matcher updateMatcher = Pattern.compile("UPDATE\\s+[`\\[]?([^\\s`,\\[\\]]+)[`\\]]?", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (updateMatcher.find()) return updateMatcher.group(1).replace("`", "");
        Matcher deleteMatcher = Pattern.compile("DELETE\\s+FROM\\s+[`\\[]?([^\\s`,\\[\\]]+)[`\\]]?", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (deleteMatcher.find()) return deleteMatcher.group(1).replace("`", "");
        return null;
    }

    private String extractTableNameFromDrop(String sql) {
        Matcher dropMatcher = Pattern.compile("DROP\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?[`\\[]?([^\\s`,\\[\\]]+)[`\\]]?", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (dropMatcher.find()) return dropMatcher.group(1).replace("`", "");
        return null;
    }

    /** 获取统计信息 */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", enabled);
        stats.put("totalGhostDdlDetected", totalGhostDdlDetected);
        stats.put("totalGhostDdlConverted", totalGhostDdlConverted);
        stats.put("totalGhostDdlSkipped", totalGhostDdlSkipped);
        return stats;
    }

    /** 处理结果 */
    public static class OnlineDdlResult {
        public enum Action { PASS_THROUGH, CONVERT, SKIP }

        private final Action action;
        private final String sql;
        private final String reason;
        private final String targetTable;

        private OnlineDdlResult(Action action, String sql, String reason, String targetTable) {
            this.action = action;
            this.sql = sql;
            this.reason = reason;
            this.targetTable = targetTable;
        }

        static OnlineDdlResult passThrough(String sql) {
            return new OnlineDdlResult(Action.PASS_THROUGH, sql, null, null);
        }

        static OnlineDdlResult convert(String sql, String targetTable) {
            return new OnlineDdlResult(Action.CONVERT, sql, "转换为对原表的 ALTER", targetTable);
        }

        static OnlineDdlResult skip(String reason) {
            return new OnlineDdlResult(Action.SKIP, null, reason, null);
        }

        public Action getAction() { return action; }
        public String getSql() { return sql; }
        public String getReason() { return reason; }
        public String getTargetTable() { return targetTable; }
        public boolean shouldSkip() { return action == Action.SKIP; }
        public boolean shouldConvert() { return action == Action.CONVERT; }
    }
}
