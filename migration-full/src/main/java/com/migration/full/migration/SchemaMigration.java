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

    public void migrateAllTables(List<TableInfo> tables) throws SQLException {
        logger.info("开始迁移表结构，共 {} 个表", tables.size());

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
        String tableName = table.getTableName();

        if (dropTables) {
            dropTableIfExists(tableName);
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
        targetConnection.execute(createSql);
        logger.debug("已创建表: {}", table.getTableName());
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
