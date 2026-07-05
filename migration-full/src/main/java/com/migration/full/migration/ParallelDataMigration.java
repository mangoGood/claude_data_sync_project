package com.migration.full.migration;

import com.migration.config.MigrationConfig;
import com.migration.db.DatabaseConnection;
import com.migration.full.progress.ProgressManager;
import com.migration.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 表级并行全量数据迁移。
 *
 * <p>把待迁移表放入共享队列，启动 N 个 worker 线程（{@code migration.full.parallelism}，
 * 自动收敛到表数），每个 worker 持有<b>独立的源/目标连接对与 {@link DataMigration} 实例</b>
 * （{@link DatabaseConnection} 是单连接、非线程安全，绝不跨线程共享），从队列领表迁移，
 * 天然实现大小表混合下的负载均衡（work-stealing）。
 *
 * <p>进度仍写入共享的 {@link ProgressManager}（其落库方法已 synchronized），
 * agent 侧按 COMPLETED 表数/总表数计算总进度的逻辑不受影响。
 *
 * <p>MySQL 目标端在 worker 会话上设置 {@code FOREIGN_KEY_CHECKS=0}：并行搬数打破了
 * 表间的隐式加载顺序，同构迁移（SHOW CREATE TABLE 带外键）时避免子表先于父表加载报错。
 *
 * <p>错误语义与串行 {@link DataMigration#migrateAllData} 一致：
 * {@code continueOnError=false} 时首个表级失败让所有 worker 尽快停止并向上抛出；
 * {@code true} 时记录失败继续其余表。
 */
public class ParallelDataMigration {
    private static final Logger logger = LoggerFactory.getLogger(ParallelDataMigration.class);

    private final MigrationConfig config;
    private final ProgressManager progressManager;

    public ParallelDataMigration(MigrationConfig config, ProgressManager progressManager) {
        this.config = config;
        this.progressManager = progressManager;
    }

    public void migrateAllData(List<TableInfo> tables, int parallelism) throws SQLException {
        int workers = Math.max(1, Math.min(parallelism, tables.size()));
        logger.info("开始并行迁移数据，共 {} 个表，并行度: {}", tables.size(), workers);

        Queue<TableInfo> queue = new ConcurrentLinkedQueue<>(tables);
        AtomicInteger totalSuccess = new AtomicInteger();
        AtomicInteger totalFail = new AtomicInteger();
        AtomicBoolean abort = new AtomicBoolean(false);
        AtomicReference<SQLException> firstError = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(workers);

        for (int i = 0; i < workers; i++) {
            Thread worker = new Thread(() -> {
                DatabaseConnection src = new DatabaseConnection(config.getSourceConfig());
                DatabaseConnection tgt = new DatabaseConnection(config.getTargetConfig());
                try {
                    prepareTargetSession(tgt);
                    DataMigration dataMigration = new DataMigration(
                            src, tgt, config.getBatchSize(), config.isContinueOnError(), progressManager);

                    TableInfo table;
                    while ((table = queue.poll()) != null) {
                        if (abort.get()) {
                            // 已有不可继续的失败：剩余表不再开始
                            break;
                        }
                        String tableName = table.getTableName();
                        try {
                            int[] result = dataMigration.migrateTableData(table);
                            totalSuccess.addAndGet(result[0]);
                            totalFail.addAndGet(result[1]);
                            logger.info("表 {} 数据迁移完成，成功: {}, 失败: {}", tableName, result[0], result[1]);
                        } catch (SQLException e) {
                            logger.error("表 {} 数据迁移失败", tableName, e);
                            try {
                                if (progressManager != null && progressManager.isEnabled()) {
                                    progressManager.failMigration(tableName, e.getMessage());
                                }
                            } catch (SQLException pe) {
                                logger.warn("记录表 {} 失败状态出错: {}", tableName, pe.getMessage());
                            }
                            if (!config.isContinueOnError()) {
                                firstError.compareAndSet(null, e);
                                abort.set(true);
                                break;
                            }
                        }
                    }
                } finally {
                    src.close();
                    tgt.close();
                    done.countDown();
                }
            }, "full-parallel-" + i);
            worker.setDaemon(false);
            worker.start();
        }

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abort.set(true);
            throw new SQLException("并行全量迁移被中断", e);
        }

        SQLException error = firstError.get();
        if (error != null) {
            throw error;
        }
        logger.info("并行数据迁移完成，总成功: {}, 总失败: {}", totalSuccess.get(), totalFail.get());
    }

    /** MySQL 目标端关闭当前会话外键检查，避免并行加载因表间顺序触发外键约束失败。 */
    private void prepareTargetSession(DatabaseConnection tgt) {
        if ("mysql".equalsIgnoreCase(config.getTargetDbType())) {
            try {
                tgt.execute("SET FOREIGN_KEY_CHECKS=0");
            } catch (SQLException e) {
                logger.warn("设置 FOREIGN_KEY_CHECKS=0 失败（继续执行）: {}", e.getMessage());
            }
        }
    }
}
