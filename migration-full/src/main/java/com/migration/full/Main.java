package com.migration.full;

import com.migration.config.DatabaseConfig;
import com.migration.config.MigrationConfig;
import com.migration.db.DatabaseConnection;
import com.migration.full.checkpoint.CheckpointRecorder;
import com.migration.full.metadata.MetadataReader;
import com.migration.full.migration.DataMigration;
import com.migration.full.migration.SchemaMigration;
import com.migration.model.TableInfo;
import com.migration.full.progress.ProgressManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        com.migration.common.OracleNetCompat.apply();
        logger.info("========================================");
        logger.info("数据库全量迁移工具启动");
        logger.info("========================================");

        ProgressManager progressManager = null;

        try {
            String taskId = getTaskId(args);
            String configFile = getConfigFile(args, taskId);
            logger.info("任务 ID: {}", taskId);
            logger.info("使用配置文件: {}", configFile);

            MigrationConfig config = taskId != null ? 
                new MigrationConfig(configFile, taskId) : new MigrationConfig(configFile);
            logger.info("源数据库: {}", config.getSourceConfig().getDatabase());
            logger.info("目标数据库: {}", config.getTargetConfig().getDatabase());

            String progressDbPath = taskId != null ? 
                "./files/" + taskId + "/migration_progress" : "./migration_progress";
            progressManager = new ProgressManager(progressDbPath, config.isEnableResume());
            
            if (progressManager.isEnabled() && progressManager.hasIncompleteProgress()) {
                logger.info("\n========================================");
                logger.info("检测到未完成的迁移");
                logger.info("========================================");
                progressManager.printProgressSummary();
                logger.info("将从上次中断的位置继续迁移\n");
            }

            String sourceDb = config.getSourceConfig().getDatabase();
            Set<String> includedDatabases = config.getIncludedDatabases();

            boolean isMultiDbMode = (sourceDb == null || sourceDb.isEmpty()) 
                && includedDatabases != null && !includedDatabases.isEmpty();

            if (isMultiDbMode) {
                logger.info("检测到多库迁移模式，共 {} 个数据库: {}", includedDatabases.size(), includedDatabases);
                migrateMultipleDatabases(config, includedDatabases, progressManager);
            } else {
                migrateSingleDatabase(config, progressManager);
            }

            logger.info("\n========================================");
            logger.info("数据库全量迁移成功完成！");
            logger.info("========================================");

        } catch (Exception e) {
            logger.error("数据库全量迁移失败", e);
            System.exit(1);
        } finally {
            if (progressManager != null) {
                progressManager.close();
            }
        }
    }

    private static void migrateMultipleDatabases(MigrationConfig config, Set<String> includedDatabases, 
                                                  ProgressManager progressManager) throws Exception {
        Set<String> includedTables = config.getIncludedTables();
        Map<String, Set<String>> dbTablesMap = groupTablesByDatabase(includedTables, includedDatabases);

        for (Map.Entry<String, Set<String>> entry : dbTablesMap.entrySet()) {
            String dbName = entry.getKey();
            Set<String> dbTables = entry.getValue();

            logger.info("\n========================================");
            logger.info("开始迁移数据库: {} (共 {} 个表)", dbName, dbTables.size());
            logger.info("========================================");

            DatabaseConfig sourceDbConfig = createDbConfig(config.getSourceConfig(), dbName);
            DatabaseConfig targetDbConfig = createDbConfig(config.getTargetConfig(), dbName);

            DatabaseConnection sourceConn = new DatabaseConnection(sourceDbConfig);
            DatabaseConnection targetConn = new DatabaseConnection(targetDbConfig);

            try {
                if (!sourceConn.testConnection()) {
                    throw new SQLException("无法连接到源数据库: " + dbName);
                }

                targetConn.ensureDatabaseExists();

                if (!targetConn.testConnection()) {
                    throw new SQLException("无法连接到目标数据库: " + dbName);
                }

                String checkpointDbPath = config.getCheckpointDbPath();
                if (checkpointDbPath != null && !checkpointDbPath.isEmpty() && config.isRecordCheckpoint()) {
                    CheckpointRecorder checkpointRecorder = new CheckpointRecorder(checkpointDbPath);
                    try {
                        checkpointRecorder.recordSnapshot(sourceConn);
                    } finally {
                        checkpointRecorder.close();
                    }
                } else {
                    logger.info("跳过 checkpoint 记录（已由 agent 在启动前记录）");
                }

                MetadataReader metadataReader = new MetadataReader(sourceConn);
                List<TableInfo> tables = metadataReader.getFilteredTablesInfo(dbTables);

                if (tables.isEmpty()) {
                    logger.warn("数据库 {} 中没有找到任何表，跳过", dbName);
                    continue;
                }

                logger.info("数据库 {} 找到 {} 个表需要迁移", dbName, tables.size());
                for (TableInfo table : tables) {
                    long rowCount = metadataReader.getTableRowCount(table.getTableName());
                    logger.info("  - {}: {} 行", table.getTableName(), rowCount);
                }

                migrateTables(config, sourceConn, targetConn, tables, progressManager);
                logger.info("数据库 {} 迁移完成", dbName);
            } finally {
                sourceConn.close();
                targetConn.close();
            }
        }
    }

    private static void migrateSingleDatabase(MigrationConfig config, ProgressManager progressManager) throws Exception {
        DatabaseConnection sourceConn = new DatabaseConnection(config.getSourceConfig());
        DatabaseConnection targetConn = new DatabaseConnection(config.getTargetConfig());

        try {
            if (!sourceConn.testConnection()) {
                throw new SQLException("无法连接到源数据库");
            }

            targetConn.ensureDatabaseExists();

            if (!targetConn.testConnection()) {
                throw new SQLException("无法连接到目标数据库");
            }

            String checkpointDbPath = config.getCheckpointDbPath();
            if (checkpointDbPath != null && !checkpointDbPath.isEmpty() && config.isRecordCheckpoint()) {
                CheckpointRecorder checkpointRecorder = new CheckpointRecorder(checkpointDbPath);
                try {
                    checkpointRecorder.recordSnapshot(sourceConn);
                } finally {
                    checkpointRecorder.close();
                }
            } else {
                logger.info("跳过 checkpoint 记录（已由 agent 在启动前记录）");
            }

            MetadataReader metadataReader = new MetadataReader(sourceConn);
            
            List<TableInfo> tables;
            Set<String> includedTables = config.getIncludedTables();
            
            if (includedTables != null && !includedTables.isEmpty()) {
                logger.info("使用同步对象过滤，共指定 {} 个表", includedTables.size());
                tables = metadataReader.getFilteredTablesInfo(includedTables);
            } else {
                tables = metadataReader.getAllTablesInfo();
            }
            
            if (tables.isEmpty()) {
                logger.warn("源数据库中没有找到任何表");
                return;
            }

            logger.info("找到 {} 个表需要迁移", tables.size());
            for (TableInfo table : tables) {
                long rowCount = metadataReader.getTableRowCount(table.getTableName());
                logger.info("  - {}: {} 行", table.getTableName(), rowCount);
            }

            migrateTables(config, sourceConn, targetConn, tables, progressManager);
        } finally {
            sourceConn.close();
            targetConn.close();
        }
    }

    private static void migrateTables(MigrationConfig config, DatabaseConnection sourceConn, 
                                       DatabaseConnection targetConn, List<TableInfo> tables,
                                       ProgressManager progressManager) throws Exception {
        if (config.isCreateTables()) {
            logger.info("\n========================================");
            logger.info("开始迁移表结构");
            logger.info("========================================");
            
            SchemaMigration schemaMigration = new SchemaMigration(
                sourceConn, targetConn, config.isDropTables()
            );
            schemaMigration.migrateAllTables(tables);
            logger.info("表结构迁移完成");
        }

        if (config.isMigrateData()) {
            logger.info("\n========================================");
            logger.info("开始迁移数据");
            logger.info("========================================");

            int parallelism = Math.min(config.getFullParallelism(), tables.size());
            if (parallelism > 1) {
                // 表级并行：每个 worker 独立连接对，从共享队列领表（详见 ParallelDataMigration）
                new com.migration.full.migration.ParallelDataMigration(config, progressManager)
                        .migrateAllData(tables, parallelism);
            } else {
                DataMigration dataMigration = new DataMigration(
                    sourceConn, targetConn,
                    config.getBatchSize(),
                    config.isContinueOnError(),
                    progressManager
                );
                dataMigration.migrateAllData(tables);
            }
            logger.info("数据迁移完成");
        }

        if (progressManager.isEnabled()) {
            logger.info("\n========================================");
            logger.info("迁移进度摘要");
            logger.info("========================================");
            progressManager.printProgressSummary();
        }
    }

    private static Map<String, Set<String>> groupTablesByDatabase(Set<String> includedTables, Set<String> includedDatabases) {
        Map<String, Set<String>> result = new LinkedHashMap<>();
        for (String db : includedDatabases) {
            result.put(db, new LinkedHashSet<>());
        }

        if (includedTables != null) {
            for (String tableRef : includedTables) {
                if (tableRef.contains(".")) {
                    String[] parts = tableRef.split("\\.", 2);
                    String db = parts[0];
                    String table = parts[1];
                    if (result.containsKey(db)) {
                        result.get(db).add(table);
                    }
                }
            }
        }

        return result;
    }

    private static DatabaseConfig createDbConfig(DatabaseConfig baseConfig, String dbName) {
        return new DatabaseConfig(
            baseConfig.getHost(),
            baseConfig.getPort(),
            dbName,
            baseConfig.getUsername(),
            baseConfig.getPassword(),
            baseConfig.getDbType()
        );
    }

    /**
     * 获取任务 ID
     */
    private static String getTaskId(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--task-id".equals(args[i]) || "-t".equals(args[i])) {
                return args[i + 1];
            }
        }
        String taskId = System.getProperty("task.id");
        if (taskId != null && !taskId.isEmpty()) {
            return taskId;
        }
        taskId = System.getenv("TASK_ID");
        if (taskId != null && !taskId.isEmpty()) {
            return taskId;
        }
        return null;
    }

    /**
     * 获取配置文件路径
     */
    private static String getConfigFile(String[] args, String taskId) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i]) || "-c".equals(args[i])) {
                return args[i + 1];
            }
        }
        
        String defaultConfig;
        if (taskId != null) {
            defaultConfig = "files/" + taskId + "/config.properties";
        } else {
            defaultConfig = "config.properties";
        }
        
        File configFile = new File(defaultConfig);
        
        if (configFile.exists()) {
            return defaultConfig;
        }
        
        throw new RuntimeException("配置文件不存在: " + defaultConfig + 
                                 "\n请提供配置文件路径作为参数，或确保 config.properties 存在");
    }
}