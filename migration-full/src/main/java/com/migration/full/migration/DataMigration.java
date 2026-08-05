package com.migration.full.migration;

import com.migration.config.DatabaseConfig;
import com.migration.db.DatabaseConnection;
import com.migration.model.ColumnInfo;
import com.migration.model.TableInfo;
import com.migration.model.TypeMapper;
import com.migration.dialect.SqlDialect;
import com.migration.dialect.TypeTranslator;
import com.migration.full.progress.MigrationProgress;
import com.migration.full.progress.ProgressManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DataMigration {
    private static final Logger logger = LoggerFactory.getLogger(DataMigration.class);
    
    private DatabaseConnection sourceConnection;
    private DatabaseConnection targetConnection;
    private int batchSize;
    private boolean continueOnError;
    private ProgressManager progressManager;
    private boolean isPostgresql;
    private boolean sourceIsPostgresql;
    private boolean targetIsPostgresql;
    // SQL 方言：集中处理标识符引用、分页等各库语法差异（取代散落的 isOracle/isPostgresql 分支）
    private SqlDialect sourceDialect;
    private SqlDialect targetDialect;
    // 跨库类型/值翻译器：按源→目标库对集中处理值转换（取代散落的 convertXToYValue 分发）
    private TypeTranslator translator;
    // 单表 PK 范围分片并行：大表按数值型主键切分为多段，各段独立连接对并发搬数
    private boolean shardEnabled;
    private long shardMinRows;
    private int shardCount;
    /**
     * 分片迁移写入进度时用的 lastMigratedId 哨兵值。分片有多个并发游标，各自搬运不同 id 区间，
     * 无法归一成单一续传位点——若像串行那样记录某个游标的 id，崩溃续传会按该 id 做
     * {@code WHERE id > lastId} 单游标扫描，从而跳过其它游标尚未搬运的低位区间而<b>丢数据</b>。
     * 故分片进度一律记 -1：续传时据此识别“上次是未完成的分片迁移”，清空目标表后从头重搬。
     */
    private static final long SHARDED_LAST_ID_SENTINEL = -1L;
    // 列处理（仅表级同步、mysql→mysql）：SELECT 行过滤 + INSERT 列名映射；附加列由建表 DEFAULT 承载
    private com.migration.config.ColumnProcessingConfig columnProcessing;
    // 全量一致性快照（P2-3）：未注入 = 旧行为（每页新建源连接、无快照语义）
    private com.migration.common.snapshot.ConsistentSnapshot snapshot;
    // 表级路由（拆分按行分发）；未注入 = 不拆分
    private com.migration.common.route.TableRouter router;
    // 路由配置（跨实例拆分按它解析目标实例连接）
    private com.migration.common.route.RoutingConfig routingConfig;
    // 跨实例分片的目标实例连接（nodeId → 连接），表迁移收尾时统一关闭
    private final java.util.Map<String, DatabaseConnection> nodeConnections = new java.util.LinkedHashMap<>();
    // 批量装载档位（BATCH / PG 二进制 COPY / Oracle direct-path）；未注入 = AUTO（即 BATCH）
    private com.migration.common.bulk.BulkLoadOptions bulkLoadOptions =
            com.migration.common.bulk.BulkLoadOptions.of(true, com.migration.common.bulk.BulkLoadOptions.Mode.AUTO, 0, 0);

    public DataMigration(DatabaseConnection sourceConnection, DatabaseConnection targetConnection,
                        int batchSize, boolean continueOnError, ProgressManager progressManager) {
        this(sourceConnection, targetConnection, batchSize, continueOnError, progressManager,
                false, Long.MAX_VALUE, 1);
    }

    public DataMigration(DatabaseConnection sourceConnection, DatabaseConnection targetConnection,
                        int batchSize, boolean continueOnError, ProgressManager progressManager,
                        boolean shardEnabled, long shardMinRows, int shardCount) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
        this.batchSize = batchSize;
        this.continueOnError = continueOnError;
        this.progressManager = progressManager;
        this.sourceIsPostgresql = "postgresql".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
        this.targetIsPostgresql = "postgresql".equalsIgnoreCase(targetConnection.getConfig().getDbType());
        this.isPostgresql = targetIsPostgresql;
        this.sourceDialect = SqlDialect.forType(sourceConnection.getConfig().getDbType());
        this.targetDialect = SqlDialect.forType(targetConnection.getConfig().getDbType());
        this.translator = TypeTranslator.forPair(sourceConnection.getConfig().getDbType(), targetConnection.getConfig().getDbType());
        this.shardEnabled = shardEnabled;
        this.shardMinRows = shardMinRows;
        this.shardCount = Math.max(1, shardCount);
    }

    private boolean sourceIsOracle() {
        return "oracle".equalsIgnoreCase(sourceConnection.getConfig().getDbType());
    }

    /** 注入列处理配置（未注入 = 无列处理，行为与既有逻辑完全一致）。 */
    public void setColumnProcessing(com.migration.config.ColumnProcessingConfig columnProcessing) {
        this.columnProcessing = columnProcessing;
    }

    /** 注入一致性快照（未注入 = 无快照，读取路径与旧行为一致）。 */
    public void setSnapshot(com.migration.common.snapshot.ConsistentSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    /** 注入批量装载档位（未注入 = AUTO，即驱动语句重写，与既有行为一致）。 */
    public void setBulkLoadOptions(com.migration.common.bulk.BulkLoadOptions bulkLoadOptions) {
        if (bulkLoadOptions != null) {
            this.bulkLoadOptions = bulkLoadOptions;
        }
    }

    /** 注入表级路由（拆分按行分发要用；未注入 = 不拆分，行为与既有一致）。 */
    public void setTableRouter(com.migration.common.route.TableRouter router) {
        this.router = router;
    }

    /** 注入路由配置（跨实例拆分要按 route.node.* 建目标实例连接）。 */
    public void setRoutingConfig(com.migration.common.route.RoutingConfig routingConfig) {
        this.routingConfig = routingConfig;
    }

    /**
     * 取跨实例分片的目标实例连接（按 nodeId 缓存）。库名由分片模板决定，
     * 连接本身连到实例默认库即可——写入一律用 {@code 库.表} 限定名。
     */
    private Connection nodeConnection(String nodeId) throws SQLException {
        DatabaseConnection existing = nodeConnections.get(nodeId);
        if (existing != null) {
            return existing.getConnection();
        }
        com.migration.common.route.RouteNode node =
                routingConfig == null ? null : routingConfig.getNode(nodeId);
        if (node == null) {
            throw new SQLException("路由指向未配置的目标实例: " + nodeId);
        }
        DatabaseConfig base = targetConnection.getConfig();
        DatabaseConfig cfg = new DatabaseConfig(node.getHost(), node.getPort(),
                node.getDatabase() != null && !node.getDatabase().isEmpty()
                        ? node.getDatabase() : base.getDatabase(),
                node.getUsername() != null ? node.getUsername() : base.getUsername(),
                node.getPassword() != null ? node.getPassword() : base.getPassword(),
                base.getDbType());
        cfg.copyJdbcOptionsFrom(base);
        DatabaseConnection conn = new DatabaseConnection(cfg);
        nodeConnections.put(nodeId, conn);
        logger.info("跨实例拆分：已连接目标实例 {} ({}:{})", nodeId, node.getHost(), node.getPort());
        return acquireTargetConnection(conn);
    }

    /** 关闭跨实例分片连接（表迁移收尾时调用）。 */
    private void closeNodeConnections() {
        for (DatabaseConnection conn : nodeConnections.values()) {
            try {
                conn.close();
            } catch (RuntimeException e) {
                logger.warn("关闭目标实例连接失败: {}", e.getMessage());
            }
        }
        nodeConnections.clear();
    }

    /** 分片键在行值数组里的下标（行值按 {@link #buildSourceQuotedColumnList} 的列序）。 */
    private int shardKeyIndexOf(TableInfo table) {
        List<ColumnInfo> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getColumnName().equalsIgnoreCase(table.getShardKeyColumn())) {
                return i;
            }
        }
        return -1;
    }

    /** 分片落点的目标端表引用（MySQL 带库名限定；PG 一条连接跨不了库，只用表名）。 */
    private String shardTableRef(com.migration.common.route.RouteTarget target) {
        String table = targetQuoteIdentifier(target.getTable());
        if (targetIsPostgresql || target.getDatabase() == null || target.getDatabase().isEmpty()) {
            return table;
        }
        return quoteIdentifier(target.getDatabase()) + "." + table;
    }

    /**
     * 打开目标端装载通道。
     *
     * @param exclusiveWriter 本通道是否为该表唯一的写入者——单表 PK 分片并行时为 false，
     *                        Oracle direct-path 会据此降级（表级排他锁下并发写同表只会互相阻塞）
     */
    private com.migration.common.bulk.JdbcBulkChannel openBulkChannel(
            Connection targetConn, String insertSql, TableInfo table, String columnList,
            String tableName, boolean exclusiveWriter) throws SQLException {
        // 拆分：每行按分片键路由到对应分片的子通道；对上层仍是一条普通装载通道，
        // 分页/断点/重连/进度那一整套循环不用改
        if (table.isSplitRouted() && router != null) {
            int shardKeyIndex = shardKeyIndexOf(table);
            List<String> targetColumns = getColumnNames(table);
            return new com.migration.common.bulk.ShardedJdbcBulkChannel(
                    router, table.getSourceDatabase(), table.getTableName(), shardKeyIndex,
                    target -> {
                        String ref = shardTableRef(target);
                        String sql = "INSERT INTO " + ref + " (" + String.join(", ", targetColumns)
                                + ") VALUES (" + String.join(", ", createPlaceholders(targetColumns.size())) + ")";
                        // 跨实例拆分：分片落在别的实例上时换连接——限定表名跨得了库、跨不了实例
                        Connection conn = target.getNodeId() == null
                                ? targetConn : nodeConnection(target.getNodeId());
                        return com.migration.common.bulk.JdbcBulkChannels.open(
                                conn, sql, ref, columnList, tableName,
                                targetConnection.getConfig().getDbType(), bulkLoadOptions,
                                batchSize, exclusiveWriter);
                    });
        }
        com.migration.common.bulk.BulkLoadOptions options = bulkLoadOptions;
        if (table.isUpsertLoad()) {
            // 幂等装载（汇聚）：PG 二进制 COPY 与 Oracle direct-path 都没有 upsert 语义，
            // 用它们装载重复行会直接冲突失败，必须退回驱动语句重写的 BATCH 档
            options = options.withMode(com.migration.common.bulk.BulkLoadOptions.Mode.BATCH);
        }
        return com.migration.common.bulk.JdbcBulkChannels.open(
                targetConn, insertSql,
                targetQuoteIdentifier(table.getTargetTableName()), columnList, tableName,
                targetConnection.getConfig().getDbType(), options, batchSize, exclusiveWriter);
    }

    /**
     * 取一个源端分页读连接。
     * 默认每页新建（关闭后强制释放 Oracle 会话 PGA，避免 ORA-04036）；
     * 一致性快照下改为向快照借用——MySQL 的快照必须绑在固定会话上，每页新建就不是同一个快照了。
     */
    private Connection acquirePageConnection(DatabaseConnection src) throws SQLException {
        if (snapshot != null && snapshot.providesReaders()) {
            return snapshot.borrowReader();
        }
        return src.getConnection();
    }

    /** 归还分页读连接：快照连接交回快照管理（不能提交），否则按原逻辑关闭。 */
    private void releasePageConnection(Connection conn) {
        if (snapshot != null && snapshot.providesReaders()) {
            snapshot.releaseReader(conn);
            return;
        }
        try { conn.close(); } catch (SQLException e) { /* ignore */ }
    }

    /** 表引用加快照修饰（Oracle 闪回 {@code AS OF SCN}；其它库原样）。 */
    private String snapshotTable(String quotedTable) {
        return snapshot != null ? snapshot.decorateTable(quotedTable) : quotedTable;
    }

    /**
     * 读一行并按源→目标库对做值转换，返回可直接绑定到 INSERT 的值数组。
     * 汇聚时在末尾补上来源标识列的值（顺序与 {@link #getColumnNames} 一致）。
     */
    private Object[] readRowValues(ResultSet rs, ResultSetMetaData metaData, int columnCount, TableInfo table)
            throws SQLException {
        // 顺序必须与 getColumnNames 严格一致：源列 → 逐行注值的附加列 → 来源标识列
        java.util.Collection<String> extraValues = perRowExtras(table).values();
        java.util.Collection<String> tagValues = table.getMergeTagValues().values();
        Object[] values = new Object[columnCount + extraValues.size() + tagValues.size()];
        for (int i = 1; i <= columnCount; i++) {
            Object value = readColumnValue(rs, i, metaData, table);
            values[i - 1] = translator.convertValue(value, metaData.getColumnTypeName(i), rs, i);
        }
        int idx = columnCount;
        for (String extra : extraValues) {
            values[idx++] = extra;
        }
        for (String tag : tagValues) {
            values[idx++] = tag;
        }
        return values;
    }

    /**
     * 目标端写入语句。汇聚（{@code upsertLoad}）走幂等 upsert——冲突目标是目标表主键
     * （含并入主键的来源标识列）；多个源表写同一张目标表时，"没搬完就清表重搬"会清掉
     * 其它源已搬完的数据，幂等重写是唯一安全的续传方式。
     *
     * <p>目标端不支持 upsert（非 MySQL/PG）或主键缺失时退回普通 INSERT 并告警，
     * 不拼一条跑不通的语句。
     */
    private String buildWriteSql(TableInfo table, List<String> targetColumns) {
        String plainInsert = "INSERT INTO " + targetQuoteIdentifier(table.getTargetTableName())
                + " (" + String.join(", ", targetColumns) + ") VALUES ("
                + String.join(", ", createPlaceholders(targetColumns.size())) + ")";
        if (!table.isUpsertLoad()) {
            return plainInsert;
        }
        List<String> pkColumns = new ArrayList<>();
        boolean applyColumnMapping = columnProcessingApplicable();
        String srcDb = applyColumnMapping ? columnProcessingDbOf(table) : null;
        for (ColumnInfo column : table.getColumns()) {
            if (column.isPrimaryKey()) {
                // 冲突目标是<b>目标表</b>的主键列名：配了列名映射时目标端叫的是映射后的名字，
                // 拿源列名去写 ON CONFLICT 会直接报列不存在
                pkColumns.add(applyColumnMapping
                        ? columnProcessing.mapColumn(srcDb, table.getTableName(), column.getColumnName())
                        : column.getColumnName());
            }
        }
        if (table.isMergeCompositePk()) {
            pkColumns.addAll(table.getMergeTagValues().keySet());
        }
        String upsert = com.migration.common.route.UpsertSqlBuilder.build(
                targetConnection.getConfig().getDbType(),
                targetQuoteIdentifier(table.getTargetTableName()), targetColumns, pkColumns);
        if (upsert == null) {
            logger.warn("表 {} 需要幂等装载，但目标端 {} 或主键条件不满足（主键列: {}），退回普通 INSERT——"
                            + "断点续传下重复行会因主键冲突被跳过", table.getTargetTableName(),
                    targetConnection.getConfig().getDbType(), pkColumns);
            return plainInsert;
        }
        return upsert;
    }

    /**
     * 获取目标连接并确保该会话已关闭外键检查（MySQL 目标）。
     * 此前只有并行 worker 的主连接关了 FK 检查——串行路径、PK 分片 worker、错误重连
     * 产生的新会话都带着 FK 检查跑，带外键的库在这些路径下会因表间顺序插入失败。
     * SET 是会话级的且重连后不继承，故每个获取点统一走这里。
     */
    private Connection acquireTargetConnection(DatabaseConnection tgt) throws SQLException {
        Connection conn = tgt.getConnection();
        if ("mysql".equalsIgnoreCase(targetConnection.getConfig().getDbType())) {
            try (Statement st = conn.createStatement()) {
                st.execute("SET FOREIGN_KEY_CHECKS=0");
            } catch (SQLException e) {
                logger.warn("设置 FOREIGN_KEY_CHECKS=0 失败（继续执行）: {}", e.getMessage());
            }
        }
        return conn;
    }

    /** 列处理仅在同引擎链路（mysql→mysql / pg→pg）生效（引用符已按方言生成）。 */
    private boolean columnProcessingApplicable() {
        if (columnProcessing == null || columnProcessing.isEmpty()) {
            return false;
        }
        String src = sourceConnection.getConfig().getDbType();
        String tgt = targetConnection.getConfig().getDbType();
        return ("mysql".equalsIgnoreCase(src) && "mysql".equalsIgnoreCase(tgt))
                || ("postgresql".equalsIgnoreCase(src) && "postgresql".equalsIgnoreCase(tgt));
    }

    /**
     * 列处理规则的源库 key：<b>按表取</b>，不能用连接上的库名。
     *
     * <p>汇聚一条通道要搬多个源库（shard_1/shard_2/...），连接上的 database 只有一个，
     * 拿它当 key 的话除那一个库外，其余源库的过滤/映射规则一条都命中不了，而且不报错——
     * 增量侧的源库名取自 binlog 事件是对的，于是同一张表全量放行、增量过滤，越跑越不一致。
     */
    private String columnProcessingDbOf(TableInfo table) {
        String srcDb = table.getSourceDatabase();
        return srcDb != null && !srcDb.isEmpty() ? srcDb : sourceConnection.getConfig().getDatabase();
    }

    /**
     * 列过滤的 "保留行" WHERE 片段（源端 SELECT/COUNT 共用）；无过滤配置返回 null。
     * 命中过滤条件（如 col1 &lt; 1）的行不同步；过滤列为 NULL 的行保留。
     */
    private String filterKeepClause(TableInfo table) {
        if (!columnProcessingApplicable()) {
            return null;
        }
        return columnProcessing.buildKeepClause(columnProcessingDbOf(table), table.getTableName(),
                this::sourceQuoteIdentifier);
    }

    /**
     * 汇聚下需要逐行注值的附加列（CUSTOM 类型）。1:1 与拆分下返回空——
     * 那两种情形目标表由该源表独占，建表 DEFAULT 里的来源标识就是对的。
     */
    private java.util.LinkedHashMap<String, String> perRowExtras(TableInfo table) {
        if (!columnProcessingApplicable() || !table.isUpsertLoad()) {
            return new java.util.LinkedHashMap<>();
        }
        return columnProcessing.perRowExtraValues(columnProcessingDbOf(table), table.getTableName());
    }

    public void migrateAllData(List<TableInfo> tables) throws SQLException {
        logger.info("开始迁移数据，共 {} 个表", tables.size());
        
        int totalSuccessCount = 0;
        int totalFailCount = 0;
        
        for (TableInfo table : tables) {
            try {
                int[] result = migrateTableData(table);
                totalSuccessCount += result[0];
                totalFailCount += result[1];
                logger.info("表 {} 数据迁移完成，成功: {}, 失败: {}", 
                           table.getTableName(), result[0], result[1]);
            } catch (SQLException e) {
                logger.error("表 {} 数据迁移失败", table.getTableName(), e);
                if (progressManager != null && progressManager.isEnabled()) {
                    progressManager.failMigration(table.getProgressKey(), e.getMessage());
                }
                if (!continueOnError) {
                    throw e;
                }
            }
        }
        
        closeNodeConnections();
        logger.info("数据迁移完成，总成功: {}, 总失败: {}", totalSuccessCount, totalFailCount);
        // 逐行写入失败此前只记了个数：进程照样退出 0，任务报"完成"，而目标端可能一行都没写进去
        // （实测汇聚下某个来源列名对不上，那一整个来源的行全部失败，任务仍然成功退出）。
        // continueOnError=false 的语义就是"有失败就别装作成功"，这里必须抛。
        if (totalFailCount > 0 && !continueOnError) {
            throw new SQLException("全量迁移有 " + totalFailCount + " 行写入失败（成功 "
                    + totalSuccessCount + " 行）。已按 migration.continue.on.error=false 终止，"
                    + "具体失败原因见上方日志");
        }
    }

    public int[] migrateTableData(TableInfo table) throws SQLException {
        String tableName = table.getTableName();
        
        long totalRows = getTableRowCount(table);
        logger.info("开始迁移表 {} 的数据，总行数: {}", tableName, totalRows);
        
        if (totalRows == 0) {
            logger.info("表 {} 没有数据，跳过", tableName);
            return new int[]{0, 0};
        }
        
        // 崩溃续传前的进度纠偏：上次未完成（分片或非分片）都清空目标表后从头重搬。
        // 全量 INSERT 非幂等，任何"从中断点增量续搬"都不安全：SIGKILL 可能使进度 lastMigratedId
        // 领先于实际已提交行（被杀批次未落库），续搬 WHERE id>lastId 会整段跳过这些行而漏数据
        // （实测 pg 目标续搬后目标缺失一整段 id）。唯一安全做法是清表 + 全新重搬。
        resetIfIncompleteProgress(table);

        List<String> columns = getColumnNames(table);
        String columnList = String.join(", ", columns);

        String primaryKeyColumn = getPrimaryKeyColumn(table);

        if (shardEnabled && shardCount > 1 && primaryKeyColumn != null && totalRows >= shardMinRows
                && !hasUnresumableProgress(table.getProgressKey())) {
            long[] bounds = queryNumericPkBounds(tableName, primaryKeyColumn);
            if (bounds != null) {
                return migrateTableDataSharded(table, columnList, totalRows, primaryKeyColumn, bounds[0], bounds[1]);
            }
        }

        return migrateDataBatch(table, columnList, totalRows, primaryKeyColumn);
    }

    /**
     * 若上次是未完成的全量迁移（进度存在且非 COMPLETED，无论分片与否），清空目标表并删除该表进度，
     * 使本次从头重搬。原因：全量 INSERT 非幂等，增量续搬不安全——
     *  - 分片：各游标部分成果无法归一成单一续搬点，串行续搬按单 id 扫描会漏搬其它游标未覆盖区间；
     *  - 非分片：SIGKILL 可能使进度 lastMigratedId 领先于实际已提交行，续搬 WHERE id>lastId 会整段跳过。
     * 两者都会漏数据。先 TRUNCATE 目标表再全新搬运是唯一安全做法。清表失败仅告警（最坏退回旧行为）。
     */
    private void resetIfIncompleteProgress(TableInfo table) {
        if (progressManager == null || !progressManager.isEnabled()) {
            return;
        }
        String tableName = table.getTableName();
        String progressKey = table.getProgressKey();
        try {
            MigrationProgress existing = progressManager.getProgress(progressKey);
            if (existing == null || "COMPLETED".equals(existing.getStatus())) {
                return;
            }
            // 幂等装载（汇聚）：绝不能清目标表——同一张汇聚表里还有其它源已经搬完的数据。
            // upsert 重写同值是安全的，只需丢掉旧进度让本表从头重搬。
            if (table.isUpsertLoad()) {
                logger.warn("表 {} 上次为未完成的全量迁移；幂等装载下从头重搬（不清目标表，"
                        + "避免清掉同一汇聚表里其它来源的数据）", tableName);
                progressManager.deleteProgress(progressKey);
                return;
            }
            logger.warn("表 {} 上次为未完成的全量迁移，续搬不安全（非幂等 INSERT）：清空目标表后从头重搬", tableName);
            // 拆分：一张源表散在 N 张分片表里，只清其中一张等于留下另外 N-1 张的半截数据
            if (table.isSplitRouted()) {
                for (com.migration.common.route.RouteTarget target : table.getRouteTargets()) {
                    truncateTarget(shardTableRef(target));
                }
            } else {
                truncateTarget(targetQuoteIdentifier(table.getTargetTableName()));
            }
            progressManager.deleteProgress(progressKey);
            logger.info("表 {} 目标已清空、进度已重置，将从头重新迁移", tableName);
        } catch (SQLException e) {
            logger.error("表 {} 分片续传纠偏失败（继续按原逻辑，可能残留不一致）: {}", tableName, e.getMessage());
        }
    }

    /** 清空一张目标表；TRUNCATE 不可用（权限/引擎）时退回 DELETE。 */
    private void truncateTarget(String quotedRef) throws SQLException {
        try {
            targetConnection.execute("TRUNCATE TABLE " + quotedRef);
        } catch (SQLException e) {
            logger.warn("TRUNCATE 失败（{}），改用 DELETE 清空目标表 {}", e.getMessage(), quotedRef);
            targetConnection.execute("DELETE FROM " + quotedRef);
        }
    }

    /**
     * 是否存在尚未完成的断点续传进度。分片并行不支持从单个 lastMigratedId 续传
     * （多个并发游标各有各的位置），存在这种情况时退化为串行迁移以保证续传正确性。
     */
    private boolean hasUnresumableProgress(String tableName) {
        if (progressManager == null || !progressManager.isEnabled()) {
            return false;
        }
        try {
            MigrationProgress existing = progressManager.getProgress(tableName);
            return existing != null && !"COMPLETED".equals(existing.getStatus()) && existing.getLastMigratedId() != 0;
        } catch (SQLException e) {
            logger.warn("读取表 {} 已有进度失败，跳过分片评估: {}", tableName, e.getMessage());
            return true;
        }
    }

    /**
     * 查询数值型主键的 [MIN, MAX] 边界，用于切分分片范围。
     * 主键非数值类型（字符串/UUID 等）时返回 null，调用方回退到无分片的单游标分页。
     */
    private long[] queryNumericPkBounds(String tableName, String pkColumn) throws SQLException {
        String sql = "SELECT MIN(" + sourceQuoteIdentifier(pkColumn) + "), MAX(" + sourceQuoteIdentifier(pkColumn) +
                ") FROM " + sourceQuoteIdentifier(tableName);
        try (Statement stmt = sourceConnection.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                Object minObj = rs.getObject(1);
                Object maxObj = rs.getObject(2);
                if (minObj instanceof Number && maxObj instanceof Number) {
                    long min = ((Number) minObj).longValue();
                    long max = ((Number) maxObj).longValue();
                    if (max > min) {
                        return new long[]{min, max};
                    }
                }
            }
        }
        return null;
    }

    /**
     * 单表 PK 范围分片并行迁移：按数值型主键把 [minId, maxId] 均分为多段，
     * 每段用独立源/目标连接对并发搬数（{@link DatabaseConnection} 非线程安全，不可跨线程共享）。
     * 进度聚合写入同一张 migration_progress 记录（{@link ProgressManager} 落库方法已 synchronized）。
     */
    private int[] migrateTableDataSharded(TableInfo table, String columnList, long totalRows,
                                          String primaryKeyColumn, long minId, long maxId) throws SQLException {
        String tableName = table.getTableName();
        // 进度 key：汇聚下带源库名，避免多个源库的同名分表共用一条进度记录
        final String progressKey = table.getProgressKey();
        long span = maxId - minId + 1;
        int shards = (int) Math.max(1, Math.min(shardCount, span));
        long width = (span + shards - 1) / shards;

        logger.info("表 {} 启用 PK 范围分片并行迁移，总行数: {}，PK 范围: [{}, {}]，分片数: {}",
                tableName, totalRows, minId, maxId, shards);

        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progressManager.startMigration(progressKey, totalRows);
            } catch (SQLException e) {
                logger.error("获取迁移进度失败", e);
            }
        }

        DatabaseConfig sourceCfg = sourceConnection.getConfig();
        DatabaseConfig targetCfg = targetConnection.getConfig();

        AtomicLong aggregateRows = new AtomicLong(0);
        AtomicLong maxSeenId = new AtomicLong(minId - 1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicBoolean abort = new AtomicBoolean(false);
        AtomicReference<SQLException> firstError = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(shards);

        // 进度落库由单独的低频 reporter 线程统一执行（而非每个分片线程各自落库）：
        // 分片并发写 H2（AUTO_SERVER 模式）曾在实测中把 agent 侧的进度轮询连接
        // 阻塞到整次迁移结束才报 "Connection is broken"——本质是把落库频率从
        // "1 次/批" 放大成 "shards 次/批" 后打满了 H2 的 TCP accept 线程。
        // 聚合计数（aggregateRows/maxSeenId）本身仍是分片线程内的纯内存原子操作，
        // 不受此影响；这里只把"写库"这一步收敛到每秒 1 次。
        AtomicBoolean shardingDone = new AtomicBoolean(false);
        Thread progressReporter = new Thread(() -> {
            while (!shardingDone.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                if (progressManager != null && progressManager.isEnabled()) {
                    try {
                        progressManager.updateProgress(progressKey, aggregateRows.get(), SHARDED_LAST_ID_SENTINEL);
                    } catch (SQLException e) {
                        logger.error("更新进度失败", e);
                    }
                }
            }
        }, "shard-progress-reporter-" + tableName);
        progressReporter.setDaemon(true);
        progressReporter.start();

        for (int i = 0; i < shards; i++) {
            final long lowerExclusive = (i == 0) ? (minId - 1) : (minId + (long) i * width - 1);
            final long upperInclusive = (i == shards - 1) ? maxId : Math.min(maxId, minId + (long) (i + 1) * width - 1);
            Thread worker = new Thread(() -> {
                DatabaseConnection shardSrc = new DatabaseConnection(sourceCfg);
                DatabaseConnection shardTgt = new DatabaseConnection(targetCfg);
                try {
                    int[] r = copyShardRange(shardSrc, shardTgt, table, columnList, primaryKeyColumn,
                            lowerExclusive, upperInclusive, tableName, aggregateRows, maxSeenId, abort);
                    successCount.addAndGet(r[0]);
                    failCount.addAndGet(r[1]);
                } catch (SQLException e) {
                    logger.error("表 {} 分片 ({}, {}] 迁移失败", tableName, lowerExclusive, upperInclusive, e);
                    if (!continueOnError) {
                        firstError.compareAndSet(null, e);
                        abort.set(true);
                    }
                } finally {
                    shardSrc.close();
                    shardTgt.close();
                    latch.countDown();
                }
            }, "shard-" + tableName + "-" + i);
            worker.setDaemon(false);
            worker.start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            shardingDone.set(true);
            progressReporter.interrupt();
            throw new SQLException("分片并行迁移被中断", e);
        }
        shardingDone.set(true);
        progressReporter.interrupt();

        SQLException error = firstError.get();
        if (error != null) {
            if (progressManager != null && progressManager.isEnabled()) {
                try { progressManager.failMigration(progressKey, error.getMessage()); } catch (SQLException ignore) { }
            }
            throw error;
        }

        logger.info("表 {} 分片并行迁移完成，成功: {}, 失败: {}", tableName, successCount.get(), failCount.get());
        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progressManager.updateProgress(progressKey, aggregateRows.get(), SHARDED_LAST_ID_SENTINEL);
                progressManager.completeMigration(progressKey);
            } catch (SQLException e) { logger.error("标记迁移完成失败", e); }
        }
        return new int[]{successCount.get(), failCount.get()};
    }

    /** 分片 worker：分页搬运 (lowerExclusive, upperInclusive] 范围内的数据，聚合进度写入共享的 aggregateRows/maxSeenId。 */
    private int[] copyShardRange(DatabaseConnection shardSrc, DatabaseConnection shardTgt, TableInfo table,
                                 String columnList, String primaryKeyColumn,
                                 long lowerExclusive, long upperInclusive, String tableName,
                                 AtomicLong aggregateRows, AtomicLong maxSeenId, AtomicBoolean abort) throws SQLException {
        long successCount = 0;
        long failCount = 0;
        String sourceQuoteColumnList = buildSourceQuotedColumnList(table);
        // 表名映射：目标端 INSERT 用目标表名；源端 SELECT 与进度 key 仍用源表名
        String insertSql = buildWriteSql(table, getColumnNames(table));

        Connection targetConn = acquireTargetConnection(shardTgt);
        // 分片路径：同一张表有多个 worker 并发写，故 exclusiveWriter=false
        com.migration.common.bulk.JdbcBulkChannel writer =
                openBulkChannel(targetConn, insertSql, table, columnList, tableName, false);
        final int pageSize = 1000;
        long currentLastId = lowerExclusive;

        try {
            while (!abort.get() && currentLastId < upperInclusive) {
                // 每页用独立连接：避免 Oracle 源端 PGA 累积（ORA-04036），做法与串行分页路径一致；
                // 一致性快照下改为向快照借用固定的快照会话（见 acquirePageConnection）
                Connection pageConn = acquirePageConnection(shardSrc);
                String shardKeepClause = filterKeepClause(table);
                String pageSql = "SELECT " + sourceQuoteColumnList + " FROM " + snapshotTable(sourceQuoteIdentifier(tableName)) +
                        " WHERE " + sourceQuoteIdentifier(primaryKeyColumn) + " > ? AND " +
                        sourceQuoteIdentifier(primaryKeyColumn) + " <= ? " +
                        (shardKeepClause != null ? "AND " + shardKeepClause + " " : "") +
                        "ORDER BY " +
                        sourceQuoteIdentifier(primaryKeyColumn) + " " + sourceDialect.limitClause(pageSize);
                PreparedStatement selectStmt = pageConn.prepareStatement(pageSql);
                selectStmt.setLong(1, currentLastId);
                selectStmt.setLong(2, upperInclusive);
                ResultSet rs = selectStmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                int pageRows = 0;
                int pageFetched = 0;   // 本页从源取到的行数（含冲突跳过的），用于判末页
                while (rs.next()) {
                    pageFetched++;
                    try {
                        if (targetConn.isClosed()) {
                            targetConn = acquireTargetConnection(shardTgt);
                            writer.rebind(targetConn);
                        }
                        Object[] values = readRowValues(rs, metaData, columnCount, table);
                        for (int i = 1; i <= columnCount; i++) {
                            if (metaData.getColumnName(i).equals(primaryKeyColumn)) {
                                Object idValue = rs.getObject(i);
                                if (idValue instanceof Number) {
                                    currentLastId = ((Number) idValue).longValue();
                                }
                                break;
                            }
                        }
                        writer.add(values);
                        if (writer.isFull()) {
                            long[] r = writer.flush();
                            successCount += r[0];
                            failCount += r[1];
                        }
                        pageRows++;
                    } catch (SQLException e) {
                        failCount++;
                        logger.error("读取/写入数据失败，表: {}", tableName, e);
                        if (!continueOnError) { throw e; }
                        try {
                            if (targetConn.isClosed()) {
                                targetConn = acquireTargetConnection(shardTgt);
                                writer.rebind(targetConn);
                            }
                        } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                    }
                }
                if (!writer.isEmpty()) {
                    long[] r = writer.flush();
                    successCount += r[0];
                    failCount += r[1];
                }
                rs.close();
                selectStmt.close();
                releasePageConnection(pageConn);

                // 仅更新内存中的聚合计数（纯原子操作，无 DB I/O）；落库由外层统一的
                // 低频 progressReporter 线程完成，避免 shards 个线程各自落库造成的写压力
                aggregateRows.addAndGet(pageRows);
                final long pageLastId = currentLastId;
                maxSeenId.updateAndGet(prev -> Math.max(prev, pageLastId));

                // 末页判断按「取到的行数」而非「成功插入数」，避免主键冲突跳过导致 pageRows<pageSize 时漏搬后续页
                if (pageFetched < pageSize) break;
            }
        } finally {
            writer.close();
        }

        return new int[]{(int) successCount, (int) failCount};
    }

    private int[] migrateDataBatch(TableInfo table, String columnList, long totalRows, String primaryKeyColumn) throws SQLException {
        String tableName = table.getTableName();
        // 进度 key：汇聚下带源库名，避免多个源库的同名分表共用一条进度记录
        final String progressKey = table.getProgressKey();
        long successCount = 0;
        long failCount = 0;

        MigrationProgress progress = null;
        Long lastMigratedId = null;
        long startOffset = 0;
        
        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progress = progressManager.startMigration(progressKey, totalRows);
                if (progress != null && progress.getLastMigratedId() != 0) {
                    lastMigratedId = progress.getLastMigratedId();
                    startOffset = progress.getMigratedRows();
                    logger.info("从上次中断位置继续迁移，已迁移: {}, 最后ID: {}", startOffset, lastMigratedId);
                }
            } catch (SQLException e) {
                logger.error("获取迁移进度失败", e);
            }
        }
        
        String sourceQuoteColumnList = buildSourceQuotedColumnList(table);
        // 表名映射：目标端 INSERT 用目标表名；源端 SELECT 与进度 key 仍用源表名
        String insertSql = buildWriteSql(table, getColumnNames(table));

        Connection targetConn = acquireTargetConnection(targetConnection);
        // 串行路径：本通道是该表唯一写入者，Oracle direct-path 可用
        com.migration.common.bulk.JdbcBulkChannel writer =
                openBulkChannel(targetConn, insertSql, table, columnList, tableName, true);

        // 分页大小：有主键时启用分页查询，避免大结果集（尤其含 LOB）占用源端 PGA 导致 ORA-04036
        final int pageSize = 1000;
        boolean usePaging = primaryKeyColumn != null;

        try {
            long processedRows = startOffset;
            Long currentLastId = (lastMigratedId != null) ? lastMigratedId : 0L;
            // 首页是否带下界。旧实现无条件用 `pk > 0` 起扫，于是<b>主键 ≤ 0 的行被整行跳过</b>——
            // 表里只有一行 id=0 时，日志还是"本页 0 行 / 迁移成功完成"，静默丢数据。
            // 只有断点续传（有已落盘的 lastMigratedId）才需要下界；首次搬运不加，扫全表。
            boolean withLowerBound = (lastMigratedId != null);

            if (usePaging) {
                // ===== 分页循环：每页查询 pageSize 行，处理完关闭 ResultSet 释放源端 PGA =====
                while (true) {
                    // 每页用独立连接：上一页查询后 Oracle 会话 PGA 累积不释放（ORA-04036），
                    // 关闭并重建连接强制释放源端会话 PGA；一致性快照下改为借用快照会话
                    Connection pageConn = acquirePageConnection(sourceConnection);
                    // 分页子句按源库方言生成：MySQL → LIMIT，Oracle/PostgreSQL → FETCH FIRST ... ROWS ONLY
                    String pageKeepClause = filterKeepClause(table);
                    String lowerBoundClause = withLowerBound
                            ? sourceQuoteIdentifier(primaryKeyColumn) + " > ? "
                            : null;
                    String whereClause = "";
                    if (lowerBoundClause != null && pageKeepClause != null) {
                        whereClause = " WHERE " + lowerBoundClause + "AND " + pageKeepClause + " ";
                    } else if (lowerBoundClause != null) {
                        whereClause = " WHERE " + lowerBoundClause;
                    } else if (pageKeepClause != null) {
                        whereClause = " WHERE " + pageKeepClause + " ";
                    }
                    String pageSql = "SELECT " + sourceQuoteColumnList + " FROM " + snapshotTable(sourceQuoteIdentifier(tableName)) +
                            whereClause +
                            "ORDER BY " +
                            sourceQuoteIdentifier(primaryKeyColumn) + " " + sourceDialect.limitClause(pageSize);
                    PreparedStatement selectStmt = pageConn.prepareStatement(pageSql);
                    if (withLowerBound) {
                        selectStmt.setLong(1, currentLastId);
                    }
                    ResultSet rs = selectStmt.executeQuery();
                    // 每页重新获取 metaData：上一页 rs.close() 后旧的 metaData 会失效（ORA-17009）
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    int pageRows = 0;
                    int pageFetched = 0;   // 本页从源取到的行数（含冲突跳过的），用于判末页
                    while (rs.next()) {
                        pageFetched++;
                        try {
                            if (targetConn.isClosed()) {
                                logger.warn("目标数据库连接已关闭，重新建立连接");
                                targetConn = acquireTargetConnection(targetConnection);
                                writer.rebind(targetConn);
                            }
                            Object[] values = readRowValues(rs, metaData, columnCount, table);
                            for (int i = 1; i <= columnCount; i++) {
                                if (metaData.getColumnName(i).equals(primaryKeyColumn)) {
                                    Object idValue = rs.getObject(i);
                                    if (idValue instanceof Number) {
                                        currentLastId = ((Number) idValue).longValue();
                                    }
                                    break;
                                }
                            }
                            writer.add(values);
                            if (writer.isFull()) {
                                long[] r = writer.flush();
                                successCount += r[0];
                                failCount += r[1];
                                if (progressManager != null && progressManager.isEnabled()) {
                                    progressManager.updateProgress(progressKey, processedRows, currentLastId);
                                }
                            }
                            processedRows++;
                            pageRows++;
                        } catch (SQLException e) {
                            failCount++;
                            logger.error("读取/写入数据失败，表: {}, 行: {}", tableName, processedRows, e);
                            if (progressManager != null && progressManager.isEnabled()) {
                                try { progressManager.updateProgress(progressKey, processedRows, currentLastId); } catch (SQLException ex) { logger.error("更新进度失败", ex); }
                            }
                            if (!continueOnError) { throw e; }
                            try {
                                if (targetConn.isClosed()) {
                                    targetConn = acquireTargetConnection(targetConnection);
                                    writer.rebind(targetConn);
                                }
                            } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                        }
                    }
                    if (!writer.isEmpty()) {
                        long[] r = writer.flush();
                        successCount += r[0];
                        failCount += r[1];
                    }
                    if (progressManager != null && progressManager.isEnabled()) {
                        try { progressManager.updateProgress(progressKey, processedRows, currentLastId); } catch (SQLException ex) { logger.error("更新进度失败", ex); }
                    }
                    rs.close();
                    selectStmt.close();
                    // 关闭源连接，强制释放 Oracle 会话 PGA，避免 ORA-04036（快照模式下交回快照池）
                    releasePageConnection(pageConn);
                    logger.info("表 {} 分页迁移一页完成，本页 {} 行，累计 {}/{}", tableName, pageRows, processedRows, totalRows);
                    // 首页扫完后 currentLastId 已经是真实的主键值，后续页一律带下界翻页
                    withLowerBound = true;
                    // 是否末页必须按「从源取到的行数」判断，而非「成功插入数」：断点续传从
                    // lastMigratedId 续扫时会与已插入区间重叠、触发主键冲突被跳过，pageRows 因此
                    // 小于 pageSize；若据此判末页会 break 掉后续所有页，任务却标 COMPLETED → 丢数据。
                    if (pageFetched < pageSize) break;
                }
            } else {
                // ===== 无主键 fallback：单次全表查询 =====
                String selectSql = "SELECT " + sourceQuoteColumnList + " FROM " + snapshotTable(sourceQuoteIdentifier(tableName));
                String fullKeepClause = filterKeepClause(table);
                if (fullKeepClause != null) {
                    selectSql += " WHERE " + fullKeepClause;
                }
                Connection scanConn = acquirePageConnection(sourceConnection);
                PreparedStatement selectStmt = scanConn.prepareStatement(selectSql);
                ResultSet rs = selectStmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    try {
                        if (targetConn.isClosed()) {
                            targetConn = acquireTargetConnection(targetConnection);
                            writer.rebind(targetConn);
                        }
                        writer.add(readRowValues(rs, metaData, columnCount, table));
                        if (writer.isFull()) {
                            long[] r = writer.flush();
                            successCount += r[0];
                            failCount += r[1];
                            if (progressManager != null && progressManager.isEnabled()) {
                                progressManager.updateProgress(progressKey, processedRows, currentLastId);
                            }
                        }
                        processedRows++;
                    } catch (SQLException e) {
                        failCount++;
                        logger.error("读取/写入数据失败，表: {}, 行: {}", tableName, processedRows, e);
                        if (!continueOnError) { throw e; }
                        try {
                            if (targetConn.isClosed()) {
                                targetConn = acquireTargetConnection(targetConnection);
                                writer.rebind(targetConn);
                            }
                        } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                    }
                }
                if (!writer.isEmpty()) {
                    long[] r = writer.flush();
                    successCount += r[0];
                    failCount += r[1];
                }
                rs.close();
                selectStmt.close();
                // 无主键路径此前用的是共享的 sourceConnection（不关闭）；快照下必须归还借出的会话
                if (snapshot != null && snapshot.providesReaders()) {
                    releasePageConnection(scanConn);
                }
            }

            logger.info("表 {} 数据迁移完成，成功: {}, 失败: {}", tableName, successCount, failCount);
            if (progressManager != null && progressManager.isEnabled()) {
                try { progressManager.completeMigration(progressKey); } catch (SQLException e) { logger.error("标记迁移完成失败", e); }
            }
        } finally {
            writer.close();
        }

        return new int[]{(int) successCount, (int) failCount};
    }

    private Object readColumnValue(ResultSet rs, int i, ResultSetMetaData metaData, TableInfo table) throws SQLException {
        int columnType = metaData.getColumnType(i);
        String columnTypeName = metaData.getColumnTypeName(i);

        if (sourceIsPostgresql && columnTypeName != null) {
            String lowerType = columnTypeName.toLowerCase().trim();
            if ("json".equals(lowerType) || "jsonb".equals(lowerType)) {
                return rs.getString(i);
            }
            if ("bytea".equals(lowerType)) {
                return rs.getBytes(i);
            }
            if ("uuid".equals(lowerType)) {
                return rs.getString(i);
            }
            if (lowerType.endsWith("[]")) {
                return rs.getString(i);
            }
        }

        if (sourceIsOracle() && columnTypeName != null) {
            String lowerType = columnTypeName.toLowerCase().trim();
            // Oracle CLOB/NCLOB 通过 getClob 读取并转为字符串
            if ("clob".equals(lowerType) || "nclob".equals(lowerType)) {
                java.sql.Clob clob = rs.getClob(i);
                return clob == null ? null : clob.getSubString(1, (int) clob.length());
            }
            // BLOB 转为 byte[]
            if ("blob".equals(lowerType)) {
                java.sql.Blob blob = rs.getBlob(i);
                return blob == null ? null : blob.getBytes(1, (int) blob.length());
            }
            // RAW / LONG RAW 转 byte[]
            if ("raw".equals(lowerType) || "long raw".equals(lowerType)) {
                return rs.getBytes(i);
            }
            // LONG 转 String
            if ("long".equals(lowerType)) {
                return rs.getString(i);
            }
            // ROWID/UROWID 转 String
            if ("rowid".equals(lowerType) || "urowid".equals(lowerType)) {
                return rs.getString(i);
            }
            // TIMESTAMP WITH [LOCAL] TIME ZONE — 转 String，避免 oracle.sql.TIMESTAMPTZ 不可识别
            if (TypeMapper.isOracleTimestampTzType(lowerType)) {
                return rs.getString(i);
            }
            // 普通 TIMESTAMP（不带时区）— 用 getTimestamp 转为 java.sql.Timestamp，避免 oracle.sql.TIMESTAMP 对象无法被 PG JDBC 识别
            if (lowerType.startsWith("timestamp")) {
                return rs.getTimestamp(i);
            }
            // DATE — 用 getTimestamp 获取包含时间分量的值，避免丢失时分秒
            if ("date".equals(lowerType)) {
                return rs.getTimestamp(i);
            }
            // INTERVAL YEAR TO MONTH / INTERVAL DAY TO SECOND — 转 String
            if (lowerType.startsWith("interval")) {
                return rs.getString(i);
            }
            // XMLTYPE / JSON 转 String
            if ("xmltype".equals(lowerType) || "json".equals(lowerType)) {
                return rs.getString(i);
            }
        }

        if (columnType == Types.TIME) {
            return rs.getString(i);
        } else if (columnType == Types.BIGINT && columnTypeName != null
                && columnTypeName.toLowerCase().contains("unsigned")) {
            return rs.getBigDecimal(i);
        } else if (columnType == Types.DATE && "YEAR".equalsIgnoreCase(columnTypeName)) {
            Object value = rs.getObject(i);
            if (value instanceof java.sql.Date) {
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime((java.sql.Date) value);
                return cal.get(java.util.Calendar.YEAR);
            } else if (value == null) {
                return null;
            } else if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return value;
        } else {
            return rs.getObject(i);
        }
    }

    // convertPgToMysqlValue → 已迁移到 com.migration.dialect.PgToMysqlTranslator.convertValue

    private String buildSourceQuotedColumnList(TableInfo table) {
        List<String> columns = new ArrayList<>();
        for (ColumnInfo column : table.getColumns()) {
            String quoted = sourceQuoteIdentifier(column.getColumnName());
            // Oracle XMLTYPE 列直接 SELECT 会返回 oracle.xdb.XMLType 对象，PG JDBC 无法识别，
            // 且缺少 xdb 依赖时会 NoClassDefFoundError。这里在 SQL 层用 GETCLOBVAL 转为 CLOB。
            if (sourceIsOracle() && column.getDataType() != null
                    && column.getDataType().toLowerCase().contains("xmltype")) {
                columns.add("XMLTYPE.GETCLOBVAL(" + quoted + ") AS " + quoted);
            } else {
                columns.add(quoted);
            }
        }
        return String.join(", ", columns);
    }

    private long getTableRowCount(TableInfo table) throws SQLException {
        String tableName = table.getTableName();
        String sql = "SELECT COUNT(*) FROM " + sourceQuoteIdentifier(tableName);
        // 列过滤：总行数按过滤后口径统计，进度/日志与实际搬运行数一致
        String keepClause = filterKeepClause(table);
        if (keepClause != null) {
            sql += " WHERE " + keepClause;
        }

        try (Statement stmt = sourceConnection.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        
        return 0;
    }

    private List<String> getColumnNames(TableInfo table) {
        List<String> columns = new ArrayList<>();
        boolean applyColumnMapping = columnProcessingApplicable();
        String srcDb = applyColumnMapping ? columnProcessingDbOf(table) : null;
        for (var column : table.getColumns()) {
            // Oracle→PG 场景：源端列名通常为大写，目标 PG 表已建为小写，这里转小写以匹配
            String colName = (sourceIsOracle() && targetIsPostgresql) ? column.getColumnName().toLowerCase() : column.getColumnName();
            // 列名映射（mysql→mysql / pg→pg）：目标端 INSERT 列表用目标列名；源端 SELECT 仍用源列名
            if (applyColumnMapping) {
                colName = columnProcessing.mapColumn(srcDb, table.getTableName(), colName);
            }
            columns.add(quoteIdentifier(colName));
        }
        // 汇聚下 CUSTOM 附加列改为逐行注值（建表不带 DEFAULT），排在源列之后、来源标识列之前
        for (String extraColumn : perRowExtras(table).keySet()) {
            columns.add(quoteIdentifier(extraColumn));
        }
        // 汇聚来源标识列排在末尾：与 readRowValues 的补值顺序严格对应
        for (String tagColumn : table.getMergeTagValues().keySet()) {
            columns.add(quoteIdentifier(tagColumn));
        }
        return columns;
    }

    /**
     * 目标端标识符引用。Oracle→PG 场景下将表名转为小写，与 SchemaMigration 中建表时一致。
     */
    private String targetQuoteIdentifier(String identifier) {
        if (sourceIsOracle() && targetIsPostgresql) {
            return quoteIdentifier(identifier.toLowerCase());
        }
        return quoteIdentifier(identifier);
    }

    private String getPrimaryKeyColumn(TableInfo table) {
        for (var column : table.getColumns()) {
            if (column.isPrimaryKey()) {
                return column.getColumnName();
            }
        }
        return null;
    }

    private String[] createPlaceholders(int count) {
        String[] placeholders = new String[count];
        for (int i = 0; i < count; i++) {
            placeholders[i] = "?";
        }
        return placeholders;
    }

    // countSuccess / countFailures / isDuplicateKeyError → 已随批量装载通道收敛到 JdbcBatchChannel：
    // 批结果码的判定（SUCCESS_NO_INFO 必须算成功）与冲突跳过必须和装载方式绑在一起，分开写必错。

    private String quoteIdentifier(String identifier) {
        return targetDialect.quoteIdentifier(identifier);
    }

    // convertMysqlToPgValue → 已迁移到 com.migration.dialect.MysqlToPgTranslator.convertValue

    // convertOracleToPgValue → 已迁移到 com.migration.dialect.OracleToPgTranslator.convertValue

    private String sourceQuoteIdentifier(String identifier) {
        // MySQL 反引号；PostgreSQL/Oracle 双引号（Oracle 保留原始大小写）
        return sourceDialect.quoteIdentifier(identifier);
    }
}
