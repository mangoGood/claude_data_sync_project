package com.migration.full.migration;

import com.migration.db.DatabaseConnection;
import com.migration.dialect.SqlDialect;
import com.migration.dialect.TypeTranslator;
import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SchemaMigration {
    private static final Logger logger = LoggerFactory.getLogger(SchemaMigration.class);

    private DatabaseConnection sourceConnection;
    private DatabaseConnection targetConnection;
    private boolean dropTables;
    private boolean isPostgresql;
    private boolean sourceIsPostgresql;
    private boolean targetIsPostgresql;
    private boolean sourceIsOracle;
    private SqlDialect targetDialect;
    // 跨库类型翻译器：按源→目标库对生成建表 DDL（取代散落的 createTableFromXToY 分发）
    private TypeTranslator translator;
    // 列处理（仅表级同步、mysql→mysql）：建表期列名改写 + 附加列（DEFAULT 子句承载时间/来源语义）
    private com.migration.config.ColumnProcessingConfig columnProcessing;

    public SchemaMigration(DatabaseConnection sourceConnection, DatabaseConnection targetConnection, boolean dropTables) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.dropTables = dropTables;
        this.sourceIsPostgresql = "postgresql".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
        this.sourceIsOracle = "oracle".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
        this.targetIsPostgresql = "postgresql".equalsIgnoreCase(targetConnection.getConfig().getDbType());
        this.isPostgresql = targetIsPostgresql;
        this.targetDialect = SqlDialect.forType(targetConnection.getConfig().getDbType());
        this.translator = TypeTranslator.forPair(sourceConnection.getConfig().getDbType(), targetConnection.getConfig().getDbType());
    }

    /** 注入列处理配置（未注入 = 无列处理，行为与既有逻辑完全一致）。 */
    public void setColumnProcessing(com.migration.config.ColumnProcessingConfig columnProcessing) {
        this.columnProcessing = columnProcessing;
    }

    /** 列处理仅在 mysql→mysql 同构链路生效（其余库对建表路径不改动）。 */
    private boolean columnProcessingApplicable() {
        return columnProcessing != null && !columnProcessing.isEmpty()
                && "mysql".equalsIgnoreCase(sourceConnection.getConfig().getDbType())
                && "mysql".equalsIgnoreCase(targetConnection.getConfig().getDbType());
    }

    public void migrateAllTables(List<TableInfo> tables) throws SQLException {
        logger.info("开始迁移表结构，共 {} 个表", tables.size());

        // MySQL 目标关闭本会话外键检查：带 FK 的建表语句若父表尚未创建会直接失败导致表缺失
        // （建表顺序不保证父先子后）。DatabaseConnection 缓存单连接，一次设置覆盖整个建表阶段。
        if ("mysql".equalsIgnoreCase(targetConnection.getConfig().getDbType())) {
            try {
                targetConnection.execute("SET FOREIGN_KEY_CHECKS=0");
            } catch (SQLException e) {
                logger.warn("设置 FOREIGN_KEY_CHECKS=0 失败（继续执行）: {}", e.getMessage());
            }
        }

        int successCount = 0;
        int failCount = 0;

        for (TableInfo table : tables) {
            try {
                migrateTable(table);
                successCount++;
                logger.info("表 {} 结构迁移成功", table.getTableName());
            } catch (SQLException e) {
                failCount++;
                logger.error("表 {} 结构迁移失败，已忽略该错误继续执行", table.getTableName(), e);
            }
        }

        logger.info("表结构迁移完成，成功: {}, 失败: {}", successCount, failCount);
    }

    public void migrateTable(TableInfo table) throws SQLException {
        // 表名映射：目标端建表/删表一律用目标表名（未配置映射时 = 源表名）
        String targetTableName = table.getTargetTableName();

        if (dropTables) {
            dropTableIfExists(targetTableName);
        }

        createTable(table);
    }

    private void dropTableIfExists(String tableName) throws SQLException {
        // Oracle→PG 场景下，目标表已统一转为小写，DROP 时也使用小写
        String targetName = (sourceIsOracle && targetIsPostgresql) ? tableName.toLowerCase() : tableName;
        String sql = "DROP TABLE IF EXISTS " + quoteIdentifier(targetName);
        targetConnection.execute(sql);
        logger.debug("已删除表: {}", targetName);
    }

    private void createTable(TableInfo table) throws SQLException {
        // 异构迁移：按源→目标库对的翻译器生成目标建表 SQL
        if (!translator.isHomogeneous()) {
            String createSql = translator.generateCreateTable(table, targetDialect);
            logger.debug("跨库生成建表SQL: {}", createSql);
            targetConnection.execute(createSql);
            logger.debug("已创建表: {}", table.getTableName());
            return;
        }

        // 同构迁移：沿用源端 CREATE TABLE 语句
        String createSql = table.getCreateSql();
        if (createSql == null || createSql.isEmpty()) {
            // 源端没有提供 CREATE TABLE SQL（如 Oracle），并且未走上述专门路径，则跳过
            logger.warn("表 {} 未提供 CREATE TABLE SQL，跳过结构迁移", table.getTableName());
            return;
        }
        createSql = cleanCreateSql(createSql);
        createSql = renameTableInCreateSql(createSql, table.getTableName(), table.getTargetTableName());
        if (columnProcessingApplicable()) {
            String srcDb = sourceConnection.getConfig().getDatabase();
            createSql = rewriteColumnNamesInCreateSql(createSql, srcDb, table.getTableName());
            createSql = appendExtraColumnsToCreateSql(createSql, srcDb, table.getTableName());
        }
        targetConnection.execute(createSql);
        logger.debug("已创建表: {}", table.getTargetTableName());
    }

    /**
     * 列名映射：把 CREATE TABLE 定义体内的源列名改写为目标列名（不改类型）。
     * SHOW CREATE TABLE 的列名/索引列引用均为反引号包裹，整体替换 `src` → `tgt` 可同时
     * 覆盖列定义与 PRIMARY KEY/KEY 里的列引用；只处理首个 '(' 之后的定义体，
     * 避免误伤语句头的表名（表名与列名同名时）。
     */
    private String rewriteColumnNamesInCreateSql(String createSql, String srcDb, String srcTable) {
        java.util.Map<String, String> mapping = columnProcessing.getColumnMapping(srcDb, srcTable);
        if (mapping.isEmpty()) {
            return createSql;
        }
        int bodyStart = createSql.indexOf('(');
        if (bodyStart < 0) {
            logger.warn("CREATE TABLE 语句无定义体，列名映射未生效: {}.{}", srcDb, srcTable);
            return createSql;
        }
        String head = createSql.substring(0, bodyStart);
        String body = createSql.substring(bodyStart);
        for (java.util.Map.Entry<String, String> e : mapping.entrySet()) {
            body = body.replace("`" + e.getKey() + "`", "`" + e.getValue() + "`");
        }
        return head + body;
    }

    /**
     * 附加列：在 CREATE TABLE 定义体末尾追加列定义。
     * SHOW CREATE TABLE 输出的定义体闭括号固定独占一行（"\n) ENGINE=..."），锚定该位置插入；
     * CREATE_TIME/UPDATE_TIME 由 DATETIME DEFAULT/ON UPDATE CURRENT_TIMESTAMP 承载语义，
     * CUSTOM 为常量 DEFAULT '输入值@源库@源表'，全量与增量 INSERT 均无需注值。
     */
    private String appendExtraColumnsToCreateSql(String createSql, String srcDb, String srcTable) {
        java.util.List<com.migration.config.ColumnProcessingConfig.ExtraColumn> extraColumns =
                columnProcessing.getExtraColumns(srcDb, srcTable);
        if (extraColumns.isEmpty()) {
            return createSql;
        }
        int closeIdx = createSql.lastIndexOf("\n)");
        if (closeIdx < 0) {
            // 兜底：非 SHOW CREATE TABLE 排版（如手写单行 DDL），取最后一个闭括号
            closeIdx = createSql.lastIndexOf(')');
            if (closeIdx < 0) {
                logger.warn("CREATE TABLE 语句未找到定义体闭括号，附加列未生效: {}.{}", srcDb, srcTable);
                return createSql;
            }
        }
        StringBuilder defs = new StringBuilder();
        for (com.migration.config.ColumnProcessingConfig.ExtraColumn extra : extraColumns) {
            defs.append(",\n  ").append(extra.toMysqlColumnDef(srcDb, srcTable));
        }
        logger.info("附加列已加入建表语句: {}.{} 共 {} 列", srcDb, srcTable, extraColumns.size());
        return createSql.substring(0, closeIdx) + defs + createSql.substring(closeIdx);
    }

    /**
     * 表名映射：把 CREATE TABLE 语句头部的表名改写为目标表名。
     * 源端 createSql 表名固定紧跟在 "CREATE TABLE " 之后（SHOW CREATE TABLE 反引号 /
     * PG 元数据生成双引号），锚定语句头替换，不触碰列定义/注释/默认值里的同名文本。
     */
    private String renameTableInCreateSql(String createSql, String sourceName, String targetName) {
        if (targetName == null || targetName.equals(sourceName)) {
            return createSql;
        }
        String[] heads = {
                "CREATE TABLE `" + sourceName + "`",
                "CREATE TABLE \"" + sourceName + "\"",
                "CREATE TABLE " + sourceName
        };
        String[] replacements = {
                "CREATE TABLE `" + targetName + "`",
                "CREATE TABLE \"" + targetName + "\"",
                "CREATE TABLE " + quoteIdentifier(targetName)
        };
        for (int i = 0; i < heads.length; i++) {
            int idx = createSql.indexOf(heads[i]);
            if (idx >= 0) {
                return createSql.substring(0, idx) + replacements[i]
                        + createSql.substring(idx + heads[i].length());
            }
        }
        logger.warn("CREATE TABLE 语句未匹配到表名 {}，表名映射未生效: {}", sourceName, createSql);
        return createSql;
    }

    // 旧的 createTableFromXToY / generate*CreateSql 已迁移到
    // com.migration.dialect.{MysqlToPg,PgToMysql,OracleToPg}Translator.generateCreateTable

    private String cleanCreateSql(String createSql) {
        if (isPostgresql) {
            createSql = createSql.replaceAll("\"[^\"]+\"\\.\"", "\"");
            createSql = createSql.replaceAll("`[^`]+`\\.`", "\"");
            createSql = createSql.replaceAll("`", "\"");
            createSql = createSql.replaceAll("ENGINE\\s*=\\s*\\S+", "");
            createSql = createSql.replaceAll("DEFAULT\\s+CHARSET\\s*=\\s*\\S+", "");
            createSql = createSql.replaceAll("COLLATE\\s*=\\s*\\S+", "");
            createSql = createSql.replaceAll("AUTO_INCREMENT\\s*=\\s*\\d+", "");
            createSql = createSql.replaceAll(",\\s*,", ",");
            createSql = createSql.replaceAll("\\(\\s*,", "(");
            createSql = createSql.replaceAll(",\\s*\\)", ")");
            return createSql;
        }

        createSql = createSql.replaceAll("`[^`]+`\\.`", "`");
        createSql = createSql.replaceAll("AUTO_INCREMENT=\\d+", "AUTO_INCREMENT=1");

        return createSql;
    }

    public boolean tableExists(String tableName) throws SQLException {
        if (isPostgresql) {
            String schema = targetConnection.getConfig().getSchema();
            if (schema == null || schema.isEmpty()) {
                schema = "public";
            }
            String sql = "SELECT COUNT(*) FROM pg_tables WHERE schemaname = '" + schema + "' AND tablename = '" + tableName + "'";
            try (var stmt = targetConnection.getConnection().createStatement();
                 var rs = stmt.executeQuery(sql)) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }

        String sql = "SHOW TABLES LIKE '" + tableName + "'";
        try (var stmt = targetConnection.getConnection().createStatement();
                 var rs = stmt.executeQuery(sql)) {
            return rs.next();
        }
    }

    private String quoteIdentifier(String identifier) {
        // 目标库方言决定引用字符（MySQL 反引号 / PostgreSQL·Oracle 双引号）
        return targetDialect.quoteIdentifier(identifier);
    }
}
