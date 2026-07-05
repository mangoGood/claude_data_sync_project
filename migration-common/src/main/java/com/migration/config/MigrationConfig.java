package com.migration.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class MigrationConfig {
    private DatabaseConfig sourceConfig;
    private DatabaseConfig targetConfig;
    private int batchSize;
    private boolean dropTables;
    private boolean createTables;
    private boolean migrateData;
    private boolean continueOnError;
    private boolean enableResume;
    private boolean enableIncremental;
    private boolean recordCheckpoint;
    private int fullParallelism;
    private Set<String> includedDatabases;
    private Set<String> includedTables;
    private String checkpointDbPath;
    private String taskId;
    private String sourceDbType;
    private String targetDbType;

    public MigrationConfig(String configFile) throws IOException {
        loadConfig(configFile);
    }
    
    public MigrationConfig(String configFile, String taskId) throws IOException {
        this.taskId = taskId;
        loadConfig(configFile);
    }

    private void loadConfig(String configFile) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(configFile)) {
            props.load(input);
        }

        sourceDbType = props.getProperty("source.db.type", "mysql");
        targetDbType = props.getProperty("target.db.type", "mysql");

        sourceConfig = new DatabaseConfig(
            props.getProperty("source.db.host", "localhost"),
            Integer.parseInt(props.getProperty("source.db.port", "3306")),
            props.getProperty("source.db.database"),
            props.getProperty("source.db.username"),
            props.getProperty("source.db.password"),
            sourceDbType
        );

        String sourceSchema = props.getProperty("source.db.schema");
        if (sourceSchema != null && !sourceSchema.isEmpty()) {
            sourceConfig.setSchema(sourceSchema);
        }

        targetConfig = new DatabaseConfig(
            props.getProperty("target.db.host", "localhost"),
            Integer.parseInt(props.getProperty("target.db.port", "3306")),
            props.getProperty("target.db.database"),
            props.getProperty("target.db.username"),
            props.getProperty("target.db.password"),
            targetDbType
        );

        String targetSchema = props.getProperty("target.db.schema");
        if (targetSchema != null && !targetSchema.isEmpty()) {
            targetConfig.setSchema(targetSchema);
        }

        batchSize = Integer.parseInt(props.getProperty("migration.batch.size", "1000"));
        dropTables = Boolean.parseBoolean(props.getProperty("migration.drop.tables", "false"));
        createTables = Boolean.parseBoolean(props.getProperty("migration.create.tables", "true"));
        migrateData = Boolean.parseBoolean(props.getProperty("migration.migrate.data", "true"));
        continueOnError = Boolean.parseBoolean(props.getProperty("migration.continue.on.error", "false"));
        enableResume = Boolean.parseBoolean(props.getProperty("migration.enable.resume", "true"));
        enableIncremental = Boolean.parseBoolean(props.getProperty("migration.enable.incremental", "false"));
        recordCheckpoint = Boolean.parseBoolean(props.getProperty("migration.record.checkpoint", "true"));

        // 全量数据迁移的表级并行度：>1 时按表并行搬数（每个 worker 独立连接对）；1 = 原串行行为
        fullParallelism = Integer.parseInt(props.getProperty("migration.full.parallelism", "4"));
        if (fullParallelism < 1) {
            fullParallelism = 1;
        }


        includedDatabases = parseStringSet(props.getProperty("migration.included.databases", ""));
        includedTables = parseStringSet(props.getProperty("migration.included.tables", ""));
        
        String defaultCheckpointPath = taskId != null ? 
            "./files/" + taskId + "/checkpoint/checkpoint" : "./checkpoint/checkpoint";
        checkpointDbPath = props.getProperty("migration.checkpoint.db.path", defaultCheckpointPath);
    }
    
    private Set<String> parseStringSet(String value) {
        Set<String> result = new HashSet<>();
        if (value != null && !value.trim().isEmpty()) {
            String[] parts = value.split(",");
            for (String part : parts) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    result.add(trimmed);
                }
            }
        }
        return result;
    }

    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public DatabaseConfig getTargetConfig() {
        return targetConfig;
    }

    /** 全量数据迁移的表级并行度（migration.full.parallelism，默认 4，最小 1）。 */
    public int getFullParallelism() {
        return fullParallelism;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isDropTables() {
        return dropTables;
    }

    public boolean isCreateTables() {
        return createTables;
    }

    public boolean isMigrateData() {
        return migrateData;
    }

    public boolean isContinueOnError() {
        return continueOnError;
    }

    public boolean isEnableResume() {
        return enableResume;
    }
    
    public boolean isEnableIncremental() {
        return enableIncremental;
    }
    
    public boolean isRecordCheckpoint() {
        return recordCheckpoint;
    }
    
    public Set<String> getIncludedDatabases() {
        return includedDatabases;
    }
    
    public Set<String> getIncludedTables() {
        return includedTables;
    }
    
    public String getCheckpointDbPath() {
        return checkpointDbPath;
    }
    
    public String getTaskId() {
        return taskId;
    }

    public String getSourceDbType() {
        return sourceDbType;
    }

    public String getTargetDbType() {
        return targetDbType;
    }
}
