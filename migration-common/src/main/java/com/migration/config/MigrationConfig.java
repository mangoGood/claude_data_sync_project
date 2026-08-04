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
    private boolean shardEnabled;
    private long shardMinRows;
    private int shardCount;
    /** 全量写侧批量装载（migration.full.bulk.enabled，默认 true）。 */
    private boolean bulkLoadEnabled;
    /** 单批提交行数（migration.full.bulk.rows，默认 batchSize×5）。 */
    private int bulkBatchRows;
    /** 批量装载的完整配置（档位/行阈值/字节阈值），与 Mongo/ES/Redis 各链路共用同一组键。 */
    private com.migration.common.bulk.BulkLoadOptions bulkLoadOptions;
    /** 全量一致性快照模式（migration.full.snapshot.mode）：NONE / GTID_ONLY / CONSISTENT。 */
    private String snapshotMode;
    private Set<String> includedDatabases;
    private Set<String> includedTables;
    private Set<String> dbLevelDatabases;
    /** 表名映射："源库.源表" → 目标表名（仅表级同步配置，空 map = 无映射） */
    private Map<String, String> tableNameMapping;
    /** 库名映射（schema.mapping.db.*）：源库 → 目标库；多库全量按此把每个源库路由到目标库 */
    private Map<String, String> databaseMapping;
    /** 列处理配置（列过滤/列名映射/附加列，仅表级同步下发；无配置时为空实例） */
    private ColumnProcessingConfig columnProcessingConfig;
    /** 聚合路由配置（分库分表汇聚/拆分；未下发 route.mode 时为 NONE） */
    private com.migration.common.route.RoutingConfig routingConfig;
    /** 路由器（懒构造，校验失败时构造即抛） */
    private transient com.migration.common.route.TableRouter tableRouter;
    /** 本条管线的来源实例标识（route.node.id）：汇聚的 _src_node 来源标识列取它 */
    private String routeNodeId;
    private String checkpointDbPath;
    private String taskId;
    private String sourceDbType;
    private String targetDbType;
    /** 账号同步（sync.account.enabled，仅 mysql→mysql）：全量阶段把源库存量账号同步到目标库。 */
    private boolean syncAccount;
    /** 是否同步超级账号权限（sync.account.super）：false 时全局 GRANT 剔除超级/管理权限。 */
    private boolean syncAccountSuper;

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
        // 解密 config.properties 中的加密口令（ENC: 前缀）；历史明文配置无前缀，原样通过。
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

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

        // flavor：TiDB 归一成 dbType=mysql，快照手法却完全不同（MVCC 无锁 vs FTWRL），
        // 故单独带一个 flavor 传给一致性快照
        sourceConfig.setFlavor(props.getProperty("source.db.flavor"));

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

        targetConfig.setFlavor(props.getProperty("target.db.flavor"));

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

        // 单表内 PK 范围分片并行：大表（行数 >= shard.min.rows）按数值型主键切分为 shard.count 段并发搬数，
        // 收益独立于表级并行（对单表/表数少于并行度的场景尤其有效）
        shardEnabled = Boolean.parseBoolean(props.getProperty("migration.full.shard.enabled", "true"));
        shardMinRows = Long.parseLong(props.getProperty("migration.full.shard.min.rows", "200000"));
        shardCount = Integer.parseInt(props.getProperty("migration.full.shard.count", "4"));
        if (shardCount < 1) {
            shardCount = 1;
        }

        // 批量装载：AUTO/BATCH 档让驱动把 executeBatch 重写成一条多值 INSERT，往返次数从 N 降到 1；
        // COPY（PG 二进制）/ DIRECT_PATH（Oracle 直接路径）由 migration.full.bulk.mode 显式选中。
        // 只加驱动参数、不改协议（仍是 PreparedStatement 类型绑定），故默认开启。
        bulkLoadOptions = com.migration.common.bulk.BulkLoadOptions.from(props);
        bulkLoadEnabled = bulkLoadOptions.isEnabled();
        // 未显式配置行阈值时按 batchSize 放大：重写后的多值 INSERT 每批越大往返越少，
        // 但单条语句过大会撞 max_allowed_packet，取 5 倍是实测的稳妥档位（另有字节阈值兜底）
        bulkBatchRows = bulkLoadOptions.rows(batchSize * 5);
        if (bulkLoadEnabled) {
            applyBulkJdbcOptions(targetConfig, targetDbType);
        }

        // 全量一致性快照（P2-3）：NONE / GTID_ONLY（默认，只记位点不加锁）/ CONSISTENT
        snapshotMode = props.getProperty("migration.full.snapshot.mode", "GTID_ONLY").trim().toUpperCase();

        includedDatabases = parseStringSet(props.getProperty("migration.included.databases", ""));
        includedTables = parseStringSet(props.getProperty("migration.included.tables", ""));
        dbLevelDatabases = parseStringSet(props.getProperty("sync.db.level.databases", ""));

        // 表名映射（仅表级同步下发）：schema.mapping.table.<源库>.<源表>=<目标库>.<目标表>。
        // 单库全量目标库已由 target.db.database 决定，这里只取目标表名部分；key 保留 "源库.源表"。
        tableNameMapping = new HashMap<>();
        // 库名映射：schema.mapping.db.<源库>=<目标库>——多库全量按每个源库解析目标库
        databaseMapping = new HashMap<>();
        String tableMappingPrefix = "schema.mapping.table.";
        String dbMappingPrefix = "schema.mapping.db.";
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(tableMappingPrefix)) {
                String key = name.substring(tableMappingPrefix.length());
                String value = props.getProperty(name, "");
                String targetTable = value.contains(".") ? value.substring(value.indexOf('.') + 1) : value;
                if (!key.isEmpty() && !targetTable.isEmpty()) {
                    tableNameMapping.put(key, targetTable);
                }
            } else if (name.startsWith(dbMappingPrefix)) {
                String srcDb = name.substring(dbMappingPrefix.length());
                String tgtDb = props.getProperty(name, "");
                if (!srcDb.isEmpty() && !tgtDb.isEmpty()) {
                    databaseMapping.put(srcDb, tgtDb);
                }
            }
        }
        
        // 列处理（仅表级同步下发，mysql→mysql）：column.filter./column.mapping./column.extra.<源库>.<源表>
        columnProcessingConfig = ColumnProcessingConfig.loadFromProperties(props);

        // 聚合路由（分库分表汇聚 / 拆分）：route.mode + route.merge.*/route.split.*/route.node.*。
        // 未下发 route.mode 时为 NONE，全链路走原 1:1 路径。
        routingConfig = com.migration.common.route.RoutingConfig.loadFromProperties(props);
        // 本条管线的来源实例标识：跨实例汇聚时由 agent 按 leg 下发，用于 _src_node 来源标识列。
        // 未下发时退回源实例地址——同名库表来自不同实例时，这是唯一能区分它们的东西。
        routeNodeId = props.getProperty("route.node.id", "").trim();
        if (routeNodeId.isEmpty()) {
            routeNodeId = sourceConfig.getHost() + ":" + sourceConfig.getPort();
        }

        // 账号同步（仅 mysql→mysql）：sync.account.enabled 打开后全量阶段同步存量账号，
        // sync.account.super 决定是否连同超级/管理权限一并同步
        syncAccount = Boolean.parseBoolean(props.getProperty("sync.account.enabled", "false"));
        syncAccountSuper = Boolean.parseBoolean(props.getProperty("sync.account.super", "false"));

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

    /** 单表 PK 范围分片是否启用（migration.full.shard.enabled，默认 true）。 */
    public boolean isShardEnabled() {
        return shardEnabled;
    }

    /** 触发分片的最小行数阈值（migration.full.shard.min.rows，默认 200000）。 */
    public long getShardMinRows() {
        return shardMinRows;
    }

    /**
     * 给目标连接挂上批量语句重写参数。MySQL 与 PostgreSQL 的驱动都能把一批单行 INSERT
     * 合并成一条多值 INSERT；Oracle 的 executeBatch 本身即数组绑定，无需参数。
     *
     * <p>注意：开了重写之后 executeBatch 返回的是 {@code SUCCESS_NO_INFO(-2)} 而非逐行影响数，
     * 计数口径必须同步改（见 {@code JdbcBatchChannel.countBatchResults}），否则全量会把所有行报成失败。
     */
    private static void applyBulkJdbcOptions(DatabaseConfig target, String targetDbType) {
        if ("mysql".equalsIgnoreCase(targetDbType) || "tidb".equalsIgnoreCase(targetDbType)) {
            target.setJdbcOption("rewriteBatchedStatements", "true");
        } else if ("postgresql".equalsIgnoreCase(targetDbType)) {
            target.setJdbcOption("reWriteBatchedInserts", "true");
        }
    }

    /** 全量写侧批量装载是否启用。 */
    public boolean isBulkLoadEnabled() {
        return bulkLoadEnabled;
    }

    /** 单批提交行数。 */
    public int getBulkBatchRows() {
        return bulkBatchRows;
    }

    /** 批量装载的完整配置（档位/行阈值/字节阈值）。 */
    public com.migration.common.bulk.BulkLoadOptions getBulkLoadOptions() {
        return bulkLoadOptions != null
                ? bulkLoadOptions
                : com.migration.common.bulk.BulkLoadOptions.of(true, com.migration.common.bulk.BulkLoadOptions.Mode.AUTO, 0, 0);
    }

    /** 全量一致性快照模式：NONE / GTID_ONLY / CONSISTENT。 */
    public String getSnapshotMode() {
        return snapshotMode;
    }

    /** 单表分片数（migration.full.shard.count，默认 4，最小 1）。 */
    public int getShardCount() {
        return shardCount;
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

    /** 库级同步选中的数据库（sync.db.level.databases）：表数据迁移完成后需同步其存储过程/函数。 */
    public Set<String> getDbLevelDatabases() {
        return dbLevelDatabases;
    }

    /** 表名映射："源库.源表" → 目标表名；未配置返回空 map。 */
    public Map<String, String> getTableNameMapping() {
        return tableNameMapping != null ? tableNameMapping : Collections.emptyMap();
    }

    /** 列处理配置（列过滤/列名映射/附加列）；未配置返回空实例。 */
    public ColumnProcessingConfig getColumnProcessingConfig() {
        return columnProcessingConfig != null ? columnProcessingConfig : new ColumnProcessingConfig();
    }

    /** 本条管线的来源实例标识（跨实例汇聚的 leg 标识；未下发时为源实例 host:port）。 */
    public String getRouteNodeId() {
        return routeNodeId == null ? "" : routeNodeId;
    }

    /** 聚合路由配置；未配置返回 NONE 实例。 */
    public com.migration.common.route.RoutingConfig getRoutingConfig() {
        return routingConfig != null ? routingConfig : com.migration.common.route.RoutingConfig.none();
    }

    /**
     * 表级路由器（汇聚/拆分/1:1 的统一入口）。未写目标库的规则沿用既有库名映射解析，
     * 不在路由层重复实现一遍。
     *
     * @throws IllegalStateException 路由配置有校验错误——路由错了就是数据写错地方，必须 fail-stop
     */
    public com.migration.common.route.TableRouter getTableRouter() {
        if (tableRouter == null) {
            tableRouter = getRoutingConfig().router(this::getTargetDatabaseFor);
        }
        return tableRouter;
    }

    /**
     * 目标库解析（多库全量用）：库名映射命中返回映射值，未命中返回源库名。
     * 精确命中优先，小写回退（适配 MySQL 源 lower_case_table_names 不区分大小写）。
     */
    public String getTargetDatabaseFor(String sourceDb) {
        if (sourceDb == null || databaseMapping == null || databaseMapping.isEmpty()) {
            return sourceDb;
        }
        String mapped = databaseMapping.get(sourceDb);
        if (mapped != null) {
            return mapped;
        }
        for (Map.Entry<String, String> e : databaseMapping.entrySet()) {
            if (e.getKey().equalsIgnoreCase(sourceDb)) {
                return e.getValue();
            }
        }
        return sourceDb;
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

    /** 账号同步是否启用（sync.account.enabled，仅 mysql→mysql 全量阶段生效）。 */
    public boolean isSyncAccount() {
        return syncAccount;
    }

    /** 是否同步超级账号权限（sync.account.super）。 */
    public boolean isSyncAccountSuper() {
        return syncAccountSuper;
    }
}
