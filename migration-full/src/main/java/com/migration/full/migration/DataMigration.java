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
     * 列过滤的 "保留行" WHERE 片段（源端 SELECT/COUNT 共用）；无过滤配置返回 null。
     * 命中过滤条件（如 col1 &lt; 1）的行不同步；过滤列为 NULL 的行保留。
     */
    private String filterKeepClause(String tableName) {
        if (!columnProcessingApplicable()) {
            return null;
        }
        String srcDb = sourceConnection.getConfig().getDatabase();
        return columnProcessing.buildKeepClause(srcDb, tableName, this::sourceQuoteIdentifier);
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
                    progressManager.failMigration(table.getTableName(), e.getMessage());
                }
                if (!continueOnError) {
                    throw e;
                }
            }
        }
        
        logger.info("数据迁移完成，总成功: {}, 总失败: {}", totalSuccessCount, totalFailCount);
    }

    public int[] migrateTableData(TableInfo table) throws SQLException {
        String tableName = table.getTableName();
        
        long totalRows = getTableRowCount(tableName);
        logger.info("开始迁移表 {} 的数据，总行数: {}", tableName, totalRows);
        
        if (totalRows == 0) {
            logger.info("表 {} 没有数据，跳过", tableName);
            return new int[]{0, 0};
        }
        
        // 崩溃续传前的分片进度纠偏：上次是未完成的分片迁移（lastMigratedId 记为 -1 哨兵）时，
        // 各分片游标的部分成果无法归一成单一续传点，必须清空目标表后从头重搬，否则会漏数据。
        resetIfIncompleteShardedProgress(table);

        List<String> columns = getColumnNames(table);
        String columnList = String.join(", ", columns);

        String primaryKeyColumn = getPrimaryKeyColumn(table);

        if (shardEnabled && shardCount > 1 && primaryKeyColumn != null && totalRows >= shardMinRows
                && !hasUnresumableProgress(tableName)) {
            long[] bounds = queryNumericPkBounds(tableName, primaryKeyColumn);
            if (bounds != null) {
                return migrateTableDataSharded(table, columnList, totalRows, primaryKeyColumn, bounds[0], bounds[1]);
            }
        }

        return migrateDataBatch(table, columnList, totalRows, primaryKeyColumn);
    }

    /**
     * 若上次是未完成的分片迁移（进度存在、非 COMPLETED、lastMigratedId 为分片哨兵 -1），
     * 清空目标表并删除该表进度，使本次从头重搬。分片各游标的部分成果不可信：串行续传按
     * 单一 id 扫描会漏搬其它游标未覆盖的低位区间，且全量 INSERT 非幂等、直接重搬会撞已存在的行，
     * 故先 TRUNCATE 目标表再全新搬运。清表失败仅告警（最坏退回旧行为，不比不清更糟）。
     */
    private void resetIfIncompleteShardedProgress(TableInfo table) {
        if (progressManager == null || !progressManager.isEnabled()) {
            return;
        }
        String tableName = table.getTableName();
        try {
            MigrationProgress existing = progressManager.getProgress(tableName);
            if (existing == null || "COMPLETED".equals(existing.getStatus())
                    || existing.getLastMigratedId() != SHARDED_LAST_ID_SENTINEL) {
                return;
            }
            logger.warn("表 {} 上次为未完成的分片迁移，分片进度不可续传：清空目标表后从头重搬", tableName);
            String truncateSql = "TRUNCATE TABLE " + targetQuoteIdentifier(table.getTargetTableName());
            try {
                targetConnection.execute(truncateSql);
            } catch (SQLException e) {
                // 个别库/权限不支持 TRUNCATE 时退回 DELETE
                logger.warn("TRUNCATE 失败（{}），改用 DELETE 清空目标表 {}", e.getMessage(), table.getTargetTableName());
                targetConnection.execute("DELETE FROM " + targetQuoteIdentifier(table.getTargetTableName()));
            }
            progressManager.deleteProgress(tableName);
            logger.info("表 {} 目标已清空、进度已重置，将从头重新迁移", tableName);
        } catch (SQLException e) {
            logger.error("表 {} 分片续传纠偏失败（继续按原逻辑，可能残留不一致）: {}", tableName, e.getMessage());
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
        long span = maxId - minId + 1;
        int shards = (int) Math.max(1, Math.min(shardCount, span));
        long width = (span + shards - 1) / shards;

        logger.info("表 {} 启用 PK 范围分片并行迁移，总行数: {}，PK 范围: [{}, {}]，分片数: {}",
                tableName, totalRows, minId, maxId, shards);

        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progressManager.startMigration(tableName, totalRows);
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
                        progressManager.updateProgress(tableName, aggregateRows.get(), SHARDED_LAST_ID_SENTINEL);
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
                try { progressManager.failMigration(tableName, error.getMessage()); } catch (SQLException ignore) { }
            }
            throw error;
        }

        logger.info("表 {} 分片并行迁移完成，成功: {}, 失败: {}", tableName, successCount.get(), failCount.get());
        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progressManager.updateProgress(tableName, aggregateRows.get(), SHARDED_LAST_ID_SENTINEL);
                progressManager.completeMigration(tableName);
            } catch (SQLException e) { logger.error("标记迁移完成失败", e); }
        }
        return new int[]{successCount.get(), failCount.get()};
    }

    /** 分片 worker：分页搬运 (lowerExclusive, upperInclusive] 范围内的数据，聚合进度写入共享的 aggregateRows/maxSeenId。 */
    private int[] copyShardRange(DatabaseConnection shardSrc, DatabaseConnection shardTgt, TableInfo table,
                                 String columnList, String primaryKeyColumn,
                                 long lowerExclusive, long upperInclusive, String tableName,
                                 AtomicLong aggregateRows, AtomicLong maxSeenId, AtomicBoolean abort) throws SQLException {
        int successCount = 0;
        int failCount = 0;
        String sourceQuoteColumnList = buildSourceQuotedColumnList(table);
        // 表名映射：目标端 INSERT 用目标表名；源端 SELECT 与进度 key 仍用源表名
        String insertSql = "INSERT INTO " + targetQuoteIdentifier(table.getTargetTableName()) + " (" + columnList + ") VALUES (" +
                String.join(", ", createPlaceholders(table.getColumns().size())) + ")";

        Connection targetConn = acquireTargetConnection(shardTgt);
        PreparedStatement insertStmt = targetConn.prepareStatement(insertSql);
        final int pageSize = 1000;
        long currentLastId = lowerExclusive;

        try {
            while (!abort.get() && currentLastId < upperInclusive) {
                // 每页用独立连接：避免 Oracle 源端 PGA 累积（ORA-04036），做法与串行分页路径一致
                Connection pageConn = shardSrc.getConnection();
                String shardKeepClause = filterKeepClause(tableName);
                String pageSql = "SELECT " + sourceQuoteColumnList + " FROM " + sourceQuoteIdentifier(tableName) +
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
                int batchCount = 0;
                int pageRows = 0;
                while (rs.next()) {
                    try {
                        if (targetConn.isClosed()) {
                            targetConn = acquireTargetConnection(shardTgt);
                            insertStmt = targetConn.prepareStatement(insertSql);
                        }
                        for (int i = 1; i <= columnCount; i++) {
                            Object value = readColumnValue(rs, i, metaData, table);
                            value = translator.convertValue(value, metaData.getColumnTypeName(i), rs, i);
                            insertStmt.setObject(i, value);
                        }
                        for (int i = 1; i <= columnCount; i++) {
                            if (metaData.getColumnName(i).equals(primaryKeyColumn)) {
                                Object idValue = rs.getObject(i);
                                if (idValue instanceof Number) {
                                    currentLastId = ((Number) idValue).longValue();
                                }
                                break;
                            }
                        }
                        insertStmt.addBatch();
                        batchCount++;
                        if (batchCount >= batchSize) {
                            int[] results = insertStmt.executeBatch();
                            successCount += countSuccess(results);
                            failCount += countFailures(results);
                            batchCount = 0;
                        }
                        pageRows++;
                    } catch (SQLException e) {
                        if (isDuplicateKeyError(e)) {
                            logger.warn("主键冲突，跳过该行，表: {}", tableName);
                        } else {
                            failCount++;
                            logger.error("插入数据失败，表: {}", tableName, e);
                            if (!continueOnError) { throw e; }
                            try {
                                if (targetConn.isClosed()) { targetConn = acquireTargetConnection(shardTgt); }
                                insertStmt = targetConn.prepareStatement(insertSql);
                            } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                        }
                    }
                }
                if (batchCount > 0) {
                    try {
                        int[] results = insertStmt.executeBatch();
                        successCount += countSuccess(results);
                        failCount += countFailures(results);
                    } catch (SQLException e) {
                        if (!isDuplicateKeyError(e)) {
                            logger.error("执行批数据失败，表: {}", tableName, e);
                            if (!continueOnError) { throw e; }
                        }
                    }
                }
                rs.close();
                selectStmt.close();
                try { pageConn.close(); } catch (SQLException e) { /* ignore */ }

                // 仅更新内存中的聚合计数（纯原子操作，无 DB I/O）；落库由外层统一的
                // 低频 progressReporter 线程完成，避免 shards 个线程各自落库造成的写压力
                aggregateRows.addAndGet(pageRows);
                final long pageLastId = currentLastId;
                maxSeenId.updateAndGet(prev -> Math.max(prev, pageLastId));

                if (pageRows < pageSize) break;
            }
        } finally {
            try { insertStmt.close(); } catch (SQLException e) { /* ignore */ }
        }

        return new int[]{successCount, failCount};
    }

    private int[] migrateDataBatch(TableInfo table, String columnList, long totalRows, String primaryKeyColumn) throws SQLException {
        String tableName = table.getTableName();
        int successCount = 0;
        int failCount = 0;
        
        MigrationProgress progress = null;
        Long lastMigratedId = null;
        long startOffset = 0;
        
        if (progressManager != null && progressManager.isEnabled()) {
            try {
                progress = progressManager.startMigration(tableName, totalRows);
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
        String insertSql = "INSERT INTO " + targetQuoteIdentifier(table.getTargetTableName()) + " (" + columnList + ") VALUES (" +
                          String.join(", ", createPlaceholders(table.getColumns().size())) + ")";

        Connection sourceConn = sourceConnection.getConnection();
        Connection targetConn = acquireTargetConnection(targetConnection);
        PreparedStatement insertStmt = targetConn.prepareStatement(insertSql);

        // 分页大小：有主键时启用分页查询，避免大结果集（尤其含 LOB）占用源端 PGA 导致 ORA-04036
        final int pageSize = 1000;
        boolean usePaging = primaryKeyColumn != null;

        try {
            long processedRows = startOffset;
            Long currentLastId = (lastMigratedId != null) ? lastMigratedId : 0L;

            if (usePaging) {
                // ===== 分页循环：每页查询 pageSize 行，处理完关闭 ResultSet 释放源端 PGA =====
                while (true) {
                    // 每页用独立连接：上一页查询后 Oracle 会话 PGA 累积不释放（ORA-04036），
                    // 关闭并重建连接强制释放源端会话 PGA
                    Connection pageConn = sourceConnection.getConnection();
                    // 分页子句按源库方言生成：MySQL → LIMIT，Oracle/PostgreSQL → FETCH FIRST ... ROWS ONLY
                    String pageKeepClause = filterKeepClause(tableName);
                    String pageSql = "SELECT " + sourceQuoteColumnList + " FROM " + sourceQuoteIdentifier(tableName) +
                            " WHERE " + sourceQuoteIdentifier(primaryKeyColumn) + " > ? " +
                            (pageKeepClause != null ? "AND " + pageKeepClause + " " : "") +
                            "ORDER BY " +
                            sourceQuoteIdentifier(primaryKeyColumn) + " " + sourceDialect.limitClause(pageSize);
                    PreparedStatement selectStmt = pageConn.prepareStatement(pageSql);
                    selectStmt.setLong(1, currentLastId);
                    ResultSet rs = selectStmt.executeQuery();
                    // 每页重新获取 metaData：上一页 rs.close() 后旧的 metaData 会失效（ORA-17009）
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    int batchCount = 0;
                    int pageRows = 0;
                    while (rs.next()) {
                        try {
                            if (targetConn.isClosed()) {
                                logger.warn("目标数据库连接已关闭，重新建立连接");
                                targetConn = acquireTargetConnection(targetConnection);
                                insertStmt = targetConn.prepareStatement(insertSql);
                            }
                            for (int i = 1; i <= columnCount; i++) {
                                Object value = readColumnValue(rs, i, metaData, table);
                                value = translator.convertValue(value, metaData.getColumnTypeName(i), rs, i);
                                insertStmt.setObject(i, value);
                            }
                            for (int i = 1; i <= columnCount; i++) {
                                if (metaData.getColumnName(i).equals(primaryKeyColumn)) {
                                    Object idValue = rs.getObject(i);
                                    if (idValue instanceof Number) {
                                        currentLastId = ((Number) idValue).longValue();
                                    }
                                    break;
                                }
                            }
                            insertStmt.addBatch();
                            batchCount++;
                            if (batchCount >= batchSize) {
                                int[] results = insertStmt.executeBatch();
                                successCount += countSuccess(results);
                                failCount += countFailures(results);
                                batchCount = 0;
                                if (progressManager != null && progressManager.isEnabled()) {
                                    progressManager.updateProgress(tableName, processedRows, currentLastId);
                                }
                            }
                            processedRows++;
                            pageRows++;
                        } catch (SQLException e) {
                            if (isDuplicateKeyError(e)) {
                                logger.warn("主键冲突，跳过该行，表: {}, 行: {}", tableName, processedRows);
                            } else {
                                failCount++;
                                logger.error("插入数据失败，表: {}, 行: {}", tableName, processedRows, e);
                                if (progressManager != null && progressManager.isEnabled()) {
                                    try { progressManager.updateProgress(tableName, processedRows, currentLastId); } catch (SQLException ex) { logger.error("更新进度失败", ex); }
                                }
                                if (!continueOnError) { throw e; }
                                try {
                                    if (targetConn.isClosed()) { targetConn = acquireTargetConnection(targetConnection); }
                                    insertStmt = targetConn.prepareStatement(insertSql);
                                } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                            }
                        }
                    }
                    if (batchCount > 0) {
                        try {
                            int[] results = insertStmt.executeBatch();
                            successCount += countSuccess(results);
                            failCount += countFailures(results);
                        } catch (SQLException e) {
                            if (!isDuplicateKeyError(e)) {
                                logger.error("执行批数据失败，表: {}", tableName, e);
                                if (!continueOnError) { throw e; }
                            }
                        }
                    }
                    if (progressManager != null && progressManager.isEnabled()) {
                        try { progressManager.updateProgress(tableName, processedRows, currentLastId); } catch (SQLException ex) { logger.error("更新进度失败", ex); }
                    }
                    rs.close();
                    selectStmt.close();
                    // 关闭源连接，强制释放 Oracle 会话 PGA，避免 ORA-04036
                    try { pageConn.close(); } catch (SQLException e) { /* ignore */ }
                    logger.info("表 {} 分页迁移一页完成，本页 {} 行，累计 {}/{}", tableName, pageRows, processedRows, totalRows);
                    if (pageRows < pageSize) break;
                }
            } else {
                // ===== 无主键 fallback：单次全表查询 =====
                String selectSql = "SELECT " + sourceQuoteColumnList + " FROM " + sourceQuoteIdentifier(tableName);
                String fullKeepClause = filterKeepClause(tableName);
                if (fullKeepClause != null) {
                    selectSql += " WHERE " + fullKeepClause;
                }
                PreparedStatement selectStmt = sourceConn.prepareStatement(selectSql);
                ResultSet rs = selectStmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                int batchCount = 0;
                while (rs.next()) {
                    try {
                        if (targetConn.isClosed()) {
                            targetConn = acquireTargetConnection(targetConnection);
                            insertStmt = targetConn.prepareStatement(insertSql);
                        }
                        for (int i = 1; i <= columnCount; i++) {
                            Object value = readColumnValue(rs, i, metaData, table);
                            value = translator.convertValue(value, metaData.getColumnTypeName(i), rs, i);
                            insertStmt.setObject(i, value);
                        }
                        insertStmt.addBatch();
                        batchCount++;
                        if (batchCount >= batchSize) {
                            int[] results = insertStmt.executeBatch();
                            successCount += countSuccess(results);
                            failCount += countFailures(results);
                            batchCount = 0;
                            if (progressManager != null && progressManager.isEnabled()) {
                                progressManager.updateProgress(tableName, processedRows, currentLastId);
                            }
                        }
                        processedRows++;
                    } catch (SQLException e) {
                        if (isDuplicateKeyError(e)) {
                            logger.warn("主键冲突，跳过该行，表: {}, 行: {}", tableName, processedRows);
                        } else {
                            failCount++;
                            logger.error("插入数据失败，表: {}, 行: {}", tableName, processedRows, e);
                            if (!continueOnError) { throw e; }
                            try { insertStmt = targetConn.prepareStatement(insertSql); } catch (SQLException ex2) { logger.error("重建目标连接失败", ex2); }
                        }
                    }
                }
                if (batchCount > 0) {
                    int[] results = insertStmt.executeBatch();
                    successCount += countSuccess(results);
                    failCount += countFailures(results);
                }
                rs.close();
                selectStmt.close();
            }

            logger.info("表 {} 数据迁移完成，成功: {}, 失败: {}", tableName, successCount, failCount);
            if (progressManager != null && progressManager.isEnabled()) {
                try { progressManager.completeMigration(tableName); } catch (SQLException e) { logger.error("标记迁移完成失败", e); }
            }
        } finally {
            try { insertStmt.close(); } catch (SQLException e) { /* ignore */ }
        }

        return new int[]{successCount, failCount};
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

    private long getTableRowCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + sourceQuoteIdentifier(tableName);
        // 列过滤：总行数按过滤后口径统计，进度/日志与实际搬运行数一致
        String keepClause = filterKeepClause(tableName);
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
        String srcDb = applyColumnMapping ? sourceConnection.getConfig().getDatabase() : null;
        for (var column : table.getColumns()) {
            // Oracle→PG 场景：源端列名通常为大写，目标 PG 表已建为小写，这里转小写以匹配
            String colName = (sourceIsOracle() && targetIsPostgresql) ? column.getColumnName().toLowerCase() : column.getColumnName();
            // 列名映射（mysql→mysql / pg→pg）：目标端 INSERT 列表用目标列名；源端 SELECT 仍用源列名
            if (applyColumnMapping) {
                colName = columnProcessing.mapColumn(srcDb, table.getTableName(), colName);
            }
            columns.add(quoteIdentifier(colName));
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

    private int countSuccess(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result >= 0) {
                count++;
            }
        }
        return count;
    }

    private int countFailures(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result < 0) {
                count++;
            }
        }
        return count;
    }
    
    private boolean isDuplicateKeyError(SQLException e) {
        int errorCode = e.getErrorCode();
        String sqlState = e.getSQLState();
        
        if (errorCode == 1062 || "23000".equals(sqlState)) {
            return true;
        }
        
        String message = e.getMessage();
        if (message != null && (message.contains("Duplicate entry") || 
            message.contains("duplicate key value") || 
            message.contains("PRIMARY") || message.contains("UNIQUE"))) {
            return true;
        }
        
        return false;
    }

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
