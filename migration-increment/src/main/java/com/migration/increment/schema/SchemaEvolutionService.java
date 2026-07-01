package com.migration.increment.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Schema 演进服务：处理 binlog 中的 DDL 事件，自动应用到目标库。
 *
 * <p>核心职责：
 * <ol>
 *   <li>识别 DDL 事件类型（CREATE/ALTER/DROP/TRUNCATE 等）</li>
 *   <li>根据 {@link SchemaMappingConfig} 决策是否应用、如何映射</li>
 *   <li>跨库场景下通过 {@link DdlTranslator} 翻译 DDL</li>
 *   <li>在目标库事务中执行 DDL，失败时记录到待人工处理文件</li>
 *   <li>记录 Schema 演进日志，便于审计与回滚</li>
 * </ol>
 *
 * <p>使用方式：
 * <pre>
 * SchemaEvolutionService service = new SchemaEvolutionService(props, targetConnection);
 * service.applyDdl("CREATE TABLE t1 (id INT PRIMARY KEY)", "CREATE_TABLE", "db1");
 * </pre>
 */
public class SchemaEvolutionService {
    private static final Logger logger = LoggerFactory.getLogger(SchemaEvolutionService.class);

    private final SchemaMappingConfig mappingConfig;
    private final DdlTranslator translator;
    private final Connection targetConnection;
    private final String manualDdlLogPath;
    private final boolean sourceIsPostgresql;
    private final boolean targetIsPostgresql;
    private final OnlineDdlService onlineDdlService;

    /** Schema 演进统计 */
    private long totalDdlProcessed = 0;
    private long totalDdlApplied = 0;
    private long totalDdlSkipped = 0;
    private long totalDdlFailed = 0;
    private long totalDdlManual = 0;
    private final Map<String, Long> subtypeStats = new LinkedHashMap<>();

    public SchemaEvolutionService(Properties props, Connection targetConnection) {
        this.mappingConfig = SchemaMappingConfig.loadFromProperties(props);
        this.targetConnection = targetConnection;
        this.manualDdlLogPath = props.getProperty("schema.ddl.manual.log.path",
                "logs/manual_ddl.log");
        this.sourceIsPostgresql = "postgresql".equalsIgnoreCase(props.getProperty("source.db.type", "mysql"));
        this.targetIsPostgresql = "postgresql".equalsIgnoreCase(props.getProperty("target.db.type", "mysql"));

        DdlTranslator.Direction direction;
        if (sourceIsPostgresql && !targetIsPostgresql) {
            direction = DdlTranslator.Direction.POSTGRES_TO_MYSQL;
        } else if (!sourceIsPostgresql && targetIsPostgresql) {
            direction = DdlTranslator.Direction.MYSQL_TO_POSTGRES;
        } else {
            direction = DdlTranslator.Direction.SAME_ENGINE;
        }
        this.translator = new DdlTranslator(direction, mappingConfig);
        this.onlineDdlService = new OnlineDdlService(props);

        // 确保日志目录存在
        File logFile = new File(manualDdlLogPath);
        File logDir = logFile.getParentFile();
        if (logDir != null && !logDir.exists()) {
            logDir.mkdirs();
        }

        logger.info("SchemaEvolutionService 初始化完成 | 策略={} | 方向={} | 跨库转换={}",
                mappingConfig.getDdlApplyPolicy(), direction, mappingConfig.isCrossDbTypeConvert());
    }

    /**
     * 应用 DDL 到目标库。
     *
     * @param sql       原始 DDL SQL
     * @param ddlSubType DDL 子类型（如 CREATE_TABLE、ALTER_TABLE）
     * @param sourceDb  源数据库
     * @return ApplyResult 处理结果
     */
    public ApplyResult applyDdl(String sql, String ddlSubType, String sourceDb) {
        totalDdlProcessed++;
        if (ddlSubType != null) {
            subtypeStats.merge(ddlSubType.toUpperCase(), 1L, Long::sum);
        }

        // 策略检查
        if (mappingConfig.getDdlApplyPolicy() == SchemaMappingConfig.DdlApplyPolicy.SKIP) {
            totalDdlSkipped++;
            logger.info("DDL 被跳过（策略=SKIP）: subtype={} | sql={}", ddlSubType, truncate(sql));
            return ApplyResult.skipped("DDL 应用策略为 SKIP");
        }

        // 子类型跳过检查
        if (mappingConfig.shouldSkipDdlSubtype(ddlSubType)) {
            totalDdlSkipped++;
            logger.info("DDL 子类型被跳过: subtype={} | sql={}", ddlSubType, truncate(sql));
            return ApplyResult.skipped("DDL 子类型 " + ddlSubType + " 在跳过列表中");
        }

        // MANUAL 策略：仅记录
        if (mappingConfig.getDdlApplyPolicy() == SchemaMappingConfig.DdlApplyPolicy.MANUAL) {
            totalDdlManual++;
            logManualDdl(sql, ddlSubType, sourceDb, "MANUAL 策略，需人工应用");
            return ApplyResult.manual("DDL 应用策略为 MANUAL，已记录到日志");
        }

        // 在线 DDL 检测：识别 gh-ost/pt-osc 影子表操作
        OnlineDdlService.OnlineDdlResult onlineResult = onlineDdlService.process(sql, ddlSubType);
        if (onlineResult.shouldSkip()) {
            totalDdlSkipped++;
            logger.info("在线 DDL 影子表操作跳过: subtype={} | reason={}", ddlSubType, onlineResult.getReason());
            return ApplyResult.skipped(onlineResult.getReason());
        }
        if (onlineResult.shouldConvert()) {
            // 使用转换后的 SQL（对原表的 ALTER）
            sql = onlineResult.getSql();
            logger.info("在线 DDL 影子表转换为原表 ALTER: targetTable={}", onlineResult.getTargetTable());
        }

        // 翻译 DDL
        String targetSql;
        if (mappingConfig.isCrossDbTypeConvert()) {
            targetSql = translator.translate(sql, sourceDb);
            if (targetSql == null) {
                totalDdlManual++;
                logManualDdl(sql, ddlSubType, sourceDb, "DDL 自动转换失败，需人工处理");
                logger.warn("DDL 自动转换失败，记录到人工日志: subtype={} | sql={}", ddlSubType, truncate(sql));
                return ApplyResult.manual("DDL 自动转换失败，已记录到人工日志");
            }
        } else {
            targetSql = sql;
        }

        // 执行 DDL
        if (targetConnection == null) {
            totalDdlFailed++;
            logger.error("目标库连接为空，无法执行 DDL: {}", truncate(targetSql));
            return ApplyResult.failed("目标库连接为空");
        }

        try {
            executeDdl(targetSql, sourceDb);
            totalDdlApplied++;
            logger.info("DDL 应用成功: subtype={} | sql={}", ddlSubType, truncate(targetSql));
            return ApplyResult.applied(targetSql);
        } catch (SQLException e) {
            totalDdlFailed++;
            logManualDdl(sql, ddlSubType, sourceDb, "应用失败: " + e.getMessage() + " | 翻译后: " + targetSql);
            logger.error("DDL 应用失败: subtype={} | sql={} | error={}", ddlSubType, truncate(targetSql), e.getMessage());
            return ApplyResult.failed("DDL 执行失败: " + e.getMessage(), targetSql);
        }
    }

    /**
     * 在目标库执行 DDL，使用事务确保原子性。
     * 注意：多数数据库的 DDL 隐式提交，事务保护有限。
     * 对于幂等性DDL错误（如Duplicate column、Can't DROP等）跳过而非失败。
     */
    private void executeDdl(String sql, String sourceDb) throws SQLException {
        boolean originalAutoCommit = targetConnection.getAutoCommit();
        try {
            targetConnection.setAutoCommit(false);
            try (Statement stmt = targetConnection.createStatement()) {
                // 处理多语句（按分号分割）
                String[] parts = sql.split(";");
                for (String part : parts) {
                    String trimmed = part.trim();
                    if (!trimmed.isEmpty()) {
                        logger.debug("执行 DDL: {}", trimmed);
                        try {
                            stmt.execute(trimmed);
                        } catch (SQLException e) {
                            // 幂等性DDL错误容错：列已存在、索引已存在、列不存在等
                            String msg = e.getMessage();
                            if (isIdempotentDdlError(msg)) {
                                logger.warn("DDL 幂等性错误，跳过: sql={} | error={}", truncate(trimmed), msg);
                            } else {
                                throw e;
                            }
                        }
                    }
                }
            }
            targetConnection.commit();
        } catch (SQLException e) {
            try {
                targetConnection.rollback();
            } catch (SQLException re) {
                logger.warn("DDL 回滚失败: {}", re.getMessage());
            }
            throw e;
        } finally {
            try {
                targetConnection.setAutoCommit(originalAutoCommit);
            } catch (SQLException e) {
                logger.warn("恢复 autoCommit 状态失败: {}", e.getMessage());
            }
        }
    }

    /**
     * 判断DDL错误是否为幂等性错误（可安全跳过）。
     * 包括：列已存在、索引已存在、列不存在、表已存在等。
     */
    private boolean isIdempotentDdlError(String message) {
        if (message == null) return false;
        String lower = message.toLowerCase();
        return lower.contains("duplicate column name")
                || lower.contains("already exists")
                || lower.contains("can't drop")
                || lower.contains("check that column")
                || lower.contains("key column")
                || lower.contains("duplicate key name")
                || lower.contains("cannot drop index")
                || lower.contains("unknown column");
    }

    /** 记录需人工处理的 DDL */
    private void logManualDdl(String sql, String ddlSubType, String sourceDb, String reason) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = sdf.format(new Date());
        String logLine = String.format("[%s] subtype=%s | db=%s | reason=%s | sql=%s%n",
                timestamp, ddlSubType, sourceDb, reason, sql);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(manualDdlLogPath, true))) {
            writer.write(logLine);
        } catch (IOException e) {
            logger.error("写入人工 DDL 日志失败: {}", e.getMessage());
        }
    }

    /** 获取统计信息 */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalProcessed", totalDdlProcessed);
        stats.put("totalApplied", totalDdlApplied);
        stats.put("totalSkipped", totalDdlSkipped);
        stats.put("totalFailed", totalDdlFailed);
        stats.put("totalManual", totalDdlManual);
        stats.put("subtypeStats", new LinkedHashMap<>(subtypeStats));
        stats.put("applyPolicy", mappingConfig.getDdlApplyPolicy().name());
        stats.put("crossDbTypeConvert", mappingConfig.isCrossDbTypeConvert());
        stats.put("onlineDdlStats", onlineDdlService.getStats());
        return stats;
    }

    public SchemaMappingConfig getMappingConfig() {
        return mappingConfig;
    }

    private String truncate(String sql) {
        if (sql == null) return "null";
        return sql.length() > 120 ? sql.substring(0, 120) + "..." : sql;
    }

    /** DDL 应用结果 */
    public static class ApplyResult {
        public enum Status { APPLIED, SKIPPED, MANUAL, FAILED }

        private final Status status;
        private final String message;
        private final String executedSql;

        private ApplyResult(Status status, String message, String executedSql) {
            this.status = status;
            this.message = message;
            this.executedSql = executedSql;
        }

        static ApplyResult applied(String sql) {
            return new ApplyResult(Status.APPLIED, "DDL 应用成功", sql);
        }

        static ApplyResult skipped(String reason) {
            return new ApplyResult(Status.SKIPPED, reason, null);
        }

        static ApplyResult manual(String reason) {
            return new ApplyResult(Status.MANUAL, reason, null);
        }

        static ApplyResult failed(String reason) {
            return new ApplyResult(Status.FAILED, reason, null);
        }

        static ApplyResult failed(String reason, String executedSql) {
            return new ApplyResult(Status.FAILED, reason, executedSql);
        }

        public Status getStatus() { return status; }
        public String getMessage() { return message; }
        public String getExecutedSql() { return executedSql; }
        public boolean isSuccess() { return status == Status.APPLIED; }
    }
}
