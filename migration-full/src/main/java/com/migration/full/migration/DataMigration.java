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
    /** 全量装载限速器；未注入 = 不限速（历史行为） */
    private com.migration.common.ratelimit.RowRateLimiter rowRateLimiter;
    /**
     * 大字段旁路流式搬运。关闭 = 完全走原路径（把 LOB 列跟其它列一起 SELECT 进堆），
     * 也就是"1GB 字段必 OOM"的历史行为。仅 mysql→mysql 生效。
     */
    private boolean lobStreamEnabled;
    private com.migration.common.lob.LobWriteOptions lobWriteOptions =
            com.migration.common.lob.LobWriteOptions.defaults();
    /** 目标端流式专用连接：必须带 useServerPrepStmts=true，否则驱动会把整个流读进内存组包。 */
    private Connection lobTargetConn;
    /** 源端分块读专用连接：与分页扫描连接分开，避免翻页游标与 SUBSTRING 查询互相打断。 */
    private Connection lobSourceConn;
    private long lobPacketLimit = -1;

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

    /**
     * 开启大字段旁路流式搬运（仅 mysql→mysql）。
     *
     * <p>不开时行为与改造前逐字节一致：LOB 列照旧跟其它列一起 SELECT，
     * 一行一个 1GB 的字段就会把堆撑爆。开启后这些列改走
     * "瘦扫描（只取长度）+ 分块拉取 + 流式写入"，进程内存与字段大小脱钩。
     */
    public void setLobStreaming(boolean enabled, com.migration.common.lob.LobWriteOptions options) {
        this.lobStreamEnabled = enabled;
        if (options != null) {
            this.lobWriteOptions = options;
        }
    }

    /** 大字段流式是否对本链路可用：只在 mysql→mysql 上做，异构链路的值语义另说。 */
    private boolean lobStreamApplicable() {
        if (!lobStreamEnabled) {
            return false;
        }
        String src = sourceConnection.getConfig().getDbType();
        String tgt = targetConnection.getConfig().getDbType();
        return isMysqlFamily(src) && isMysqlFamily(tgt);
    }

    private static boolean isMysqlFamily(String dbType) {
        return "mysql".equalsIgnoreCase(dbType) || "tidb".equalsIgnoreCase(dbType);
    }

    /** 本表的大字段计划；不适用时返回空计划（调用方据此走原路径）。 */
    private com.migration.common.lob.LobStreamPlan lobPlanFor(TableInfo table) {
        if (!lobStreamApplicable()) {
            return com.migration.common.lob.LobStreamPlan.EMPTY;
        }
        // 拆分/汇聚路由下一行可能落到多张目标表，大字段的目标位置不再唯一，暂不支持
        if (table.isSplitRouted()) {
            return com.migration.common.lob.LobStreamPlan.EMPTY;
        }
        boolean applyColumnMapping = columnProcessingApplicable();
        String srcDb = applyColumnMapping ? columnProcessingDbOf(table) : null;
        return com.migration.common.lob.LobStreamPlan.build(table, applyColumnMapping
                ? name -> columnProcessing.mapColumn(srcDb, table.getTableName(), name)
                : null);
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
    /**
     * 全量装载限速。
     *
     * <p>第 5 批把全量从 291 行/秒提到 38,365 行/秒之后，这个缺口的性质就变了：
     * **提速本身成了新的风险**——一个没人看着的全量任务可以把源库 IO 打满。
     * 而在此之前全量链路上一个限速阀门都没有（{@code RowRateLimiter} 在 migration-full 里引用数为 0），
     * 只有增量有。
     *
     * <p>限在 flush 之后按"这一批实际写了多少行"计费：限在读侧会让页缓冲攒着不写、
     * 内存和事务都拖长；限在 add 上则每行一次判定，热路径开销白花。
     */
    public void setRowRateLimiter(com.migration.common.ratelimit.RowRateLimiter limiter) {
        if (limiter != null) {
            this.rowRateLimiter = limiter;
        }
    }

    /** 按本批实际写入行数限速。中断只还原中断位，绝不把它变成一次搬运失败。 */
    private void throttle(long rows) {
        if (rowRateLimiter == null || rowRateLimiter.isUnlimited() || rows <= 0) {
            return;
        }
        try {
            rowRateLimiter.acquire(rows);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

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

    /**
     * 从列清单里剔掉生成列（STORED/VIRTUAL），仅同引擎 mysql→mysql。
     *
     * <p>目标表的建表语句是照搬源端的，所以源端的生成列在目标端<b>还是</b>生成列，
     * 而 MySQL 拒绝显式写入生成列：{@code ERROR 3105 The value specified for generated column
     * ... is not allowed}——一条这样的 INSERT 就让整表搬运失败。它们的值本来也不该搬，
     * 目标库会按表达式自己算。
     *
     * <p>剔除动作放在<b>这一个地方</b>、且直接改 {@link TableInfo} 的列清单：SELECT 列表、
     * 瘦扫描列表、INSERT 列表、主键下标、分片键下标、大字段计划全都按同一份列清单的
     * <b>下标</b>互相对齐，任何一处单独过滤都会让行值整体错位——那是比报错更糟的静默写坏。
     *
     * <p>异构目标（mysql→pg 等）不动：那边的目标列是普通列，值照常搬过去才是对的。
     */
    private void stripGeneratedColumns(TableInfo table) {
        if (targetIsPostgresql || !"mysql".equalsIgnoreCase(sourceConnection.getConfig().getDbType())
                || !"mysql".equalsIgnoreCase(targetConnection.getConfig().getDbType())) {
            return;
        }
        List<ColumnInfo> kept = new ArrayList<>();
        List<String> dropped = new ArrayList<>();
        for (ColumnInfo column : table.getColumns()) {
            if (column.isGenerated()) {
                dropped.add(column.getColumnName());
            } else {
                kept.add(column);
            }
        }
        if (dropped.isEmpty()) {
            return;
        }
        table.setColumns(kept);
        logger.info("表 {} 的生成列 {} 不参与数据搬运（目标库按表达式自行计算，显式写入会报 3105）",
                table.getTableName(), dropped);
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

        stripGeneratedColumns(table);

        long totalRows = getTableRowCount(table);
        logger.info("开始迁移表 {} 的数据，总行数: {}", tableName, totalRows);
        
        if (totalRows == 0) {
            logger.info("表 {} 没有数据，跳过", tableName);
            return new int[]{0, 0};
        }
        
        com.migration.common.lob.LobStreamPlan lobPlan = lobPlanFor(table);

        // 崩溃续传前的进度纠偏：上次未完成（分片或非分片）都清空目标表后从头重搬。
        // 全量 INSERT 非幂等，任何"从中断点增量续搬"都不安全：SIGKILL 可能使进度 lastMigratedId
        // 领先于实际已提交行（被杀批次未落库），续搬 WHERE id>lastId 会整段跳过这些行而漏数据
        // （实测 pg 目标续搬后目标缺失一整段 id）。唯一安全做法是清表 + 全新重搬。
        //
        // 大字段流式路径是例外：它按行 upsert、按目标端已有长度续写，整条路径幂等，
        // 清表反而会把已经搬好的几个 GB 全部丢掉重来——那才是真正搬不完的原因。
        if (lobPlan.isEmpty()) {
            resetIfIncompleteProgress(table);
        }

        List<String> columns = getColumnNames(table);
        String columnList = String.join(", ", columns);

        String primaryKeyColumn = getPrimaryKeyColumn(table);

        if (!lobPlan.isEmpty()) {
            if (primaryKeyColumn == null) {
                // 无主键就没法按行定位大字段（把 1GB 的值放进 WHERE 里既不可行也无意义）
                throw new SQLException("表 " + tableName + " 含大字段（" + lobPlan
                        + "）但没有主键，无法做大字段旁路搬运。请为该表加主键，或关闭 "
                        + "migration.lob.stream.enabled 后自行确保单值不会超出可用内存");
            }
            return migrateDataWithLobStreaming(table, totalRows, primaryKeyColumn, lobPlan);
        }

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
                            throttle(r[0] + r[1]);
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
                    throttle(r[0] + r[1]);
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

    // ================================================================================
    // 大字段旁路流式搬运
    // ================================================================================

    /**
     * 含大字段表的搬运：<b>瘦扫描 + 旁路流式</b>。
     *
     * <p>与常规路径的根本区别在扫描 SQL：LOB 列<b>不出现在 SELECT 列表里</b>，只取
     * {@code OCTET_LENGTH(col)}。这一点是整条链路能在受限内存下跑起来的前提——
     * JDBC 驱动读一行时会把整行读进堆，只要 1GB 的列还在 SELECT 列表里，
     * 后面做什么优化都没用。
     *
     * <p>每行两步：先 upsert 瘦行（LOB 列写 NULL），再逐个大字段流式补齐。
     * 顺序不能反：{@code UPDATE ... SET col=?} 需要行已经存在。
     *
     * <p>整条路径幂等：瘦行是 upsert，大字段按目标端已有长度续写。所以崩溃续传不清表、
     * 不重搬已完成的行——对 10 行 × 1GB 来说，"从头再来"和"搬不完"是一回事。
     */
    private int[] migrateDataWithLobStreaming(TableInfo table, long totalRows, String primaryKeyColumn,
                                              com.migration.common.lob.LobStreamPlan plan) throws SQLException {
        String tableName = table.getTableName();
        String progressKey = table.getProgressKey();
        long successCount = 0;
        long failCount = 0;

        Long lastMigratedId = null;
        long processedRows = 0;
        if (progressManager != null && progressManager.isEnabled()) {
            try {
                MigrationProgress progress = progressManager.startMigration(progressKey, totalRows);
                if (progress != null && progress.getLastMigratedId() != 0
                        && progress.getLastMigratedId() != SHARDED_LAST_ID_SENTINEL) {
                    lastMigratedId = progress.getLastMigratedId();
                    processedRows = progress.getMigratedRows();
                    logger.info("表 {} 大字段续传：已搬 {} 行，从主键 {} 继续", tableName, processedRows, lastMigratedId);
                }
            } catch (SQLException e) {
                logger.error("获取迁移进度失败", e);
            }
        }

        String thinSelectList = buildThinSelectList(table, plan);
        String insertSql = buildLobThinUpsertSql(table, plan);
        List<String> targetColumns = getColumnNames(table);
        String qualifiedSourceTable = sourceQuoteIdentifier(tableName);
        String qualifiedTargetTable = targetQuoteIdentifier(table.getTargetTableName());

        Connection targetConn = acquireTargetConnection(targetConnection);
        Connection lobTarget = lobTargetConnection();
        Connection lobSource = lobSourceConnection();
        long packetLimit = lobPacketLimit(lobTarget);
        logger.info("表 {} 走大字段旁路搬运：流式列 {}，目标端单语句上限 {} 字节",
                tableName, plan, packetLimit);

        final int pageSize = 100;   // 瘦行很小，页大小只影响翻页次数
        boolean withLowerBound = (lastMigratedId != null);
        Object currentLastId = lastMigratedId;

        try (PreparedStatement insertStmt = targetConn.prepareStatement(insertSql)) {
            while (true) {
                String keepClause = filterKeepClause(table);
                StringBuilder where = new StringBuilder();
                if (withLowerBound) {
                    // 续传用 >= 而不是 >：最后一行可能只搬了一半的大字段，必须重新处理它。
                    // 整条路径幂等，重处理一行的代价远小于漏掉半个 1GB 的字段。
                    where.append(" WHERE ").append(sourceQuoteIdentifier(primaryKeyColumn)).append(" >= ? ");
                    if (keepClause != null) {
                        where.append("AND ").append(keepClause).append(' ');
                    }
                } else if (keepClause != null) {
                    where.append(" WHERE ").append(keepClause).append(' ');
                }
                String pageSql = "SELECT " + thinSelectList + " FROM " + snapshotTable(qualifiedSourceTable)
                        + where + "ORDER BY " + sourceQuoteIdentifier(primaryKeyColumn) + " "
                        + sourceDialect.limitClause(pageSize);

                int pageFetched = 0;
                List<Object[]> pageRows = new ArrayList<>();
                List<long[]> pageLobLengths = new ArrayList<>();
                List<Object> pagePks = new ArrayList<>();

                Connection pageConn = acquirePageConnection(sourceConnection);
                try (PreparedStatement selectStmt = pageConn.prepareStatement(pageSql)) {
                    if (withLowerBound) {
                        selectStmt.setObject(1, currentLastId);
                    }
                    try (ResultSet rs = selectStmt.executeQuery()) {
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        while (rs.next()) {
                            pageFetched++;
                            long[] lobLengths = new long[plan.columns().size()];
                            Object[] values = readThinRowValues(rs, metaData, columnCount, table, plan, lobLengths);
                            pageRows.add(values);
                            pageLobLengths.add(lobLengths);
                            pagePks.add(rs.getObject(primaryKeyIndex(table, primaryKeyColumn) + 1));
                        }
                    }
                } finally {
                    // 整页读完就放掉扫描连接：大字段的流式读走的是另一条连接，
                    // 不能让翻页游标在整行搬运期间一直挂着
                    releasePageConnection(pageConn);
                }

                for (int r = 0; r < pageRows.size(); r++) {
                    Object pk = pagePks.get(r);
                    try {
                        long inserted = upsertThinRow(insertStmt, pageRows.get(r), targetColumns.size());
                        // MySQL: 1 = 新插入的行（目标端大字段必然是 NULL，省掉一次昂贵的长度探测）；
                        //        2 = 命中已存在行（续传场景，必须探测已写到哪）
                        boolean freshRow = (inserted == 1);
                        streamLobColumns(table, plan, pk, pageLobLengths.get(r), qualifiedSourceTable,
                                qualifiedTargetTable, lobSource, lobTarget, packetLimit, freshRow);
                        successCount++;
                        processedRows++;
                        currentLastId = pk;
                        withLowerBound = true;
                        if (progressManager != null && progressManager.isEnabled() && pk instanceof Number) {
                            progressManager.updateProgress(progressKey, processedRows, ((Number) pk).longValue());
                        }
                        throttle(1);
                    } catch (SQLException | java.io.IOException e) {
                        failCount++;
                        logger.error("大字段行搬运失败，表: {}, 主键: {}", tableName, pk, e);
                        if (!continueOnError) {
                            throw (e instanceof SQLException) ? (SQLException) e
                                    : new SQLException("大字段搬运失败: " + tableName + " pk=" + pk, e);
                        }
                    }
                }

                logger.info("表 {} 大字段搬运一页完成，本页 {} 行，累计 {}/{}",
                        tableName, pageRows.size(), processedRows, totalRows);
                if (pageFetched < pageSize) {
                    break;
                }
            }
        }

        logger.info("表 {} 大字段搬运完成，成功: {}, 失败: {}", tableName, successCount, failCount);
        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progressManager.completeMigration(progressKey);
            } catch (SQLException e) {
                logger.error("标记迁移完成失败", e);
            }
        }
        return new int[]{(int) successCount, (int) failCount};
    }

    /** 逐个大字段流式搬运。 */
    private void streamLobColumns(TableInfo table, com.migration.common.lob.LobStreamPlan plan,
                                  Object pk, long[] lobLengths, String qualifiedSourceTable,
                                  String qualifiedTargetTable, Connection lobSource, Connection lobTarget,
                                  long packetLimit, boolean freshRow) throws SQLException, java.io.IOException {
        String pkColumn = getPrimaryKeyColumn(table);
        com.migration.common.lob.MySqlLobDialect dialect = com.migration.common.lob.MySqlLobDialect.INSTANCE;
        List<com.migration.common.lob.LobStreamPlan.LobColumn> cols = plan.columns();
        for (int i = 0; i < cols.size(); i++) {
            com.migration.common.lob.LobStreamPlan.LobColumn col = cols.get(i);
            long length = lobLengths[i];
            if (length < 0) {
                continue;   // 源端为 NULL：瘦行里已经写了 NULL，不用管
            }
            try (com.migration.common.lob.LobChunkSource source = new com.migration.common.lob.JdbcLobChunkSource(
                    lobSource, dialect, qualifiedSourceTable, col.sourceName, pkColumn, pk,
                    col.textColumn, length);
                 com.migration.common.lob.StreamingBlobWriter writer = new com.migration.common.lob.StreamingBlobWriter(
                         lobTarget, dialect, lobWriteOptions, qualifiedTargetTable, col.targetName,
                         pkColumn, pk, packetLimit)) {
                long known = freshRow
                        ? com.migration.common.lob.StreamingBlobWriter.TARGET_NULL
                        : writer.probeTargetLength();
                com.migration.common.lob.StreamingBlobWriter.Result result = writer.write(source, known);
                if (result.bytesWritten > 0 && logger.isDebugEnabled()) {
                    logger.debug("大字段已搬: {}.{} pk={} {}", table.getTableName(), col.sourceName, pk, result);
                }
            }
        }
    }

    /** 瘦扫描列表：大字段列换成 {@code OCTET_LENGTH(col)}，其余列原样。 */
    private String buildThinSelectList(TableInfo table, com.migration.common.lob.LobStreamPlan plan) {
        List<String> parts = new ArrayList<>();
        List<ColumnInfo> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            String quoted = sourceQuoteIdentifier(columns.get(i).getColumnName());
            if (plan.isLobIndex(i)) {
                parts.add("OCTET_LENGTH(" + quoted + ")");
            } else {
                parts.add(quoted);
            }
        }
        return String.join(", ", parts);
    }

    /**
     * 瘦行写入 SQL：{@code INSERT ... ON DUPLICATE KEY UPDATE}。
     *
     * <p>更新列表里<b>必须排除大字段列</b>。否则续传时这条 upsert 会把 LOB 列
     * 重新写成 VALUES(col)（也就是 NULL），把上一轮已经搬好的几百 MB 直接抹掉，
     * 而且不报错——每次续传都从 0 开始，任务永远跑不完。
     */
    private String buildLobThinUpsertSql(TableInfo table, com.migration.common.lob.LobStreamPlan plan) {
        List<String> targetColumns = getColumnNames(table);
        String base = "INSERT INTO " + targetQuoteIdentifier(table.getTargetTableName())
                + " (" + String.join(", ", targetColumns) + ") VALUES ("
                + String.join(", ", createPlaceholders(targetColumns.size())) + ")";

        java.util.Set<String> lobTargets = new java.util.HashSet<>();
        for (com.migration.common.lob.LobStreamPlan.LobColumn c : plan.columns()) {
            lobTargets.add(quoteIdentifier(c.targetName));
        }
        List<String> assignments = new ArrayList<>();
        for (String col : targetColumns) {
            if (!lobTargets.contains(col)) {
                assignments.add(col + " = VALUES(" + col + ")");
            }
        }
        if (assignments.isEmpty()) {
            // 全是大字段列（理论上不会，主键至少不是）：退化成无副作用的自赋值，保住幂等
            assignments.add(targetColumns.get(0) + " = " + targetColumns.get(0));
        }
        return base + " ON DUPLICATE KEY UPDATE " + String.join(", ", assignments);
    }

    /** 绑定并执行瘦行 upsert，返回受影响行数（MySQL: 1=新插入，2=更新已存在行）。 */
    private long upsertThinRow(PreparedStatement stmt, Object[] values, int columnCount) throws SQLException {
        for (int i = 0; i < columnCount; i++) {
            stmt.setObject(i + 1, values[i]);
        }
        return stmt.executeUpdate();
    }

    /**
     * 读瘦行：大字段位置放 null（瘦行先写 NULL），同时把该列的字节长度收进 lobLengths。
     * 源端为 NULL 时长度记 -1，与"长度 0 的空值"区分开。
     */
    private Object[] readThinRowValues(ResultSet rs, ResultSetMetaData metaData, int columnCount,
                                       TableInfo table, com.migration.common.lob.LobStreamPlan plan,
                                       long[] lobLengths) throws SQLException {
        java.util.Collection<String> extraValues = perRowExtras(table).values();
        java.util.Collection<String> tagValues = table.getMergeTagValues().values();
        Object[] values = new Object[columnCount + extraValues.size() + tagValues.size()];
        int lobIdx = 0;
        for (int i = 1; i <= columnCount; i++) {
            if (plan.isLobIndex(i - 1)) {
                long len = rs.getLong(i);
                lobLengths[lobIdx++] = rs.wasNull() ? -1 : len;
                values[i - 1] = null;
                continue;
            }
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

    /** 主键列在源列序里的 0-based 下标。 */
    private int primaryKeyIndex(TableInfo table, String primaryKeyColumn) throws SQLException {
        List<ColumnInfo> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getColumnName().equalsIgnoreCase(primaryKeyColumn)) {
                return i;
            }
        }
        throw new SQLException("主键列 " + primaryKeyColumn + " 不在表 " + table.getTableName() + " 的列清单里");
    }

    /**
     * 目标端流式专用连接。
     *
     * <p>必须带 {@code useServerPrepStmts=true}：客户端预编译下驱动会把整个
     * {@code setBinaryStream} 读进内存再组包，1GB 照样 OOM（实测有对照）。
     * 之所以单开一条连接而不是改全局 URL，是因为主写入连接开着
     * {@code rewriteBatchedStatements}，两者的批量语义会互相影响——
     * 常规表的批量装载路径一个字都不该被这次改造碰到。
     */
    private Connection lobTargetConnection() throws SQLException {
        if (lobTargetConn == null || lobTargetConn.isClosed()) {
            DatabaseConfig cfg = targetConnection.getConfig();
            String url = com.migration.common.lob.LobJdbc.withStreamingParams(cfg.getJdbcUrl());
            lobTargetConn = DriverManager.getConnection(url, cfg.getUsername(), cfg.getPassword());
            try (Statement st = lobTargetConn.createStatement()) {
                st.execute("SET FOREIGN_KEY_CHECKS=0");
            } catch (SQLException e) {
                logger.warn("大字段连接设置 FOREIGN_KEY_CHECKS=0 失败（继续）: {}", e.getMessage());
            }
            logger.info("已建立大字段流式写入连接（useServerPrepStmts=true）");
        }
        return lobTargetConn;
    }

    /** 源端分块读专用连接（与分页扫描分开，避免两个游标互相打断）。 */
    private Connection lobSourceConnection() throws SQLException {
        if (lobSourceConn == null || lobSourceConn.isClosed()) {
            DatabaseConfig cfg = sourceConnection.getConfig();
            lobSourceConn = DriverManager.getConnection(cfg.getJdbcUrl(), cfg.getUsername(), cfg.getPassword());
        }
        return lobSourceConn;
    }

    private long lobPacketLimit(Connection lobTarget) {
        if (lobPacketLimit <= 0) {
            lobPacketLimit = com.migration.common.lob.StreamingBlobWriter.probePacketLimit(
                    lobTarget, com.migration.common.lob.MySqlLobDialect.INSTANCE);
        }
        return lobPacketLimit;
    }

    /** 关闭大字段专用连接（表迁移全部结束时调用）。 */
    public void closeLobConnections() {
        for (Connection c : new Connection[]{lobTargetConn, lobSourceConn}) {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ignored) {
                    // 收尾关连接失败不影响已完成的搬运结果
                }
            }
        }
        lobTargetConn = null;
        lobSourceConn = null;
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
                                throttle(r[0] + r[1]);
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
                        throttle(r[0] + r[1]);
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
                            throttle(r[0] + r[1]);
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
                    throttle(r[0] + r[1]);
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

    /**
     * 读一个列值。<b>全量侧的脱敏挂在这里</b>——它是行值的单一出口，
     * 分页扫描与瘦行扫描两条路径都经过它。
     *
     * <p>脱敏必须全量与增量<b>共用同一份规则</b>：只在增量脱敏的话，
     * 全量已经把原值整表搬过去了，等于没脱。
     */
    private Object readColumnValue(ResultSet rs, int i, ResultSetMetaData metaData, TableInfo table) throws SQLException {
        Object raw = readRawColumnValue(rs, i, metaData, table);
        return maskIfNeeded(metaData, i, table, raw);
    }

    /** 按配置对列值脱敏；未配脱敏时零开销原样返回。 */
    private Object maskIfNeeded(ResultSetMetaData metaData, int i, TableInfo table, Object raw)
            throws SQLException {
        if (columnProcessing == null || raw == null) {
            return raw;
        }
        String srcDb = columnProcessingDbOf(table);
        if (srcDb == null || !columnProcessing.hasMask(srcDb, table.getTableName())) {
            return raw;
        }
        return columnProcessing.maskValue(srcDb, table.getTableName(),
                metaData.getColumnName(i), raw);
    }

    private Object readRawColumnValue(ResultSet rs, int i, ResultSetMetaData metaData, TableInfo table) throws SQLException {
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
