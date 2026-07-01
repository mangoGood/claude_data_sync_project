package com.migration.full.progress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进度管理类，负责管理迁移进度的业务逻辑
 */
public class ProgressManager {
    private static final Logger logger = LoggerFactory.getLogger(ProgressManager.class);
    
    private ProgressDatabase progressDatabase;
    private Map<String, MigrationProgress> progressCache;
    private boolean enabled;

    public ProgressManager(boolean enabled) {
        this.enabled = enabled;
        this.progressCache = new ConcurrentHashMap<>();
        
        if (enabled) {
            try {
                progressDatabase = new ProgressDatabase();
                progressDatabase.initialize();
                logger.info("进度管理器已启用");
            } catch (SQLException e) {
                logger.error("初始化进度数据库失败", e);
                throw new RuntimeException("初始化进度数据库失败", e);
            }
        } else {
            logger.info("进度管理器已禁用");
        }
    }

    public ProgressManager(String dbPath, boolean enabled) {
        this.enabled = enabled;
        this.progressCache = new ConcurrentHashMap<>();
        
        if (enabled) {
            try {
                progressDatabase = new ProgressDatabase(dbPath);
                progressDatabase.initialize();
                logger.info("进度管理器已启用，数据库路径: {}", dbPath);
            } catch (SQLException e) {
                logger.error("初始化进度数据库失败", e);
                throw new RuntimeException("初始化进度数据库失败", e);
            }
        } else {
            logger.info("进度管理器已禁用");
        }
    }

    /**
     * 开始表的迁移
     */
    public MigrationProgress startMigration(String tableName, long totalRows) throws SQLException {
        if (!enabled) {
            return null;
        }
        
        MigrationProgress progress = progressDatabase.getProgress(tableName);
        
        if (progress == null) {
            progress = new MigrationProgress(tableName, totalRows);
            progress.markInProgress();
            progressDatabase.saveProgress(progress);
            logger.info("开始新迁移: {}, 总行数: {}", tableName, totalRows);
        } else if (progress.getStatus().equals("COMPLETED")) {
            progress.reset();
            progress.setTotalRows(totalRows);
            progress.markInProgress();
            progressDatabase.saveProgress(progress);
            logger.info("重新开始迁移: {}, 总行数: {}", tableName, totalRows);
        } else {
            progress.markInProgress();
            progressDatabase.saveProgress(progress);
            logger.info("继续迁移: {}, 已迁移: {}/{}, 最后ID: {}", 
                       tableName, progress.getMigratedRows(), totalRows, progress.getLastMigratedId());
        }
        
        progressCache.put(tableName, progress);
        return progress;
    }

    /**
     * 更新迁移进度
     */
    public void updateProgress(String tableName, long migratedRows, Long lastMigratedId) throws SQLException {
        if (!enabled) {
            return;
        }
        
        MigrationProgress progress = progressCache.get(tableName);
        if (progress == null) {
            logger.warn("未找到表的迁移进度: {}", tableName);
            return;
        }
        
        progress.setMigratedRows(migratedRows);
        if (lastMigratedId != null) {
            progress.setLastMigratedId(lastMigratedId);
        }
        progress.updateLastUpdateTime();
        
        progressDatabase.saveProgress(progress);
        
        // 每迁移1000行或完成时打印进度
        if (migratedRows % 1000 == 0 || migratedRows == progress.getTotalRows()) {
            double percentage = progress.getProgressPercentage();
            logger.info("迁移进度: {} - {}/{} ({:.2f}%)", 
                       tableName, migratedRows, progress.getTotalRows(), percentage);
        }
    }

    /**
     * 完成表的迁移
     */
    public void completeMigration(String tableName) throws SQLException {
        if (!enabled) {
            return;
        }
        
        MigrationProgress progress = progressCache.get(tableName);
        if (progress == null) {
            logger.warn("未找到表的迁移进度: {}", tableName);
            return;
        }
        
        progress.markCompleted();
        progressDatabase.saveProgress(progress);
        logger.info("迁移完成: {}", tableName);
    }

    /**
     * 标记迁移失败
     */
    public void failMigration(String tableName, String errorMessage) throws SQLException {
        if (!enabled) {
            return;
        }
        
        MigrationProgress progress = progressCache.get(tableName);
        if (progress == null) {
            logger.warn("未找到表的迁移进度: {}", tableName);
            return;
        }
        
        progress.markFailed(errorMessage);
        progressDatabase.saveProgress(progress);
        logger.error("迁移失败: {}, 错误: {}", tableName, errorMessage);
    }

    /**
     * 获取表的迁移进度
     */
    public MigrationProgress getProgress(String tableName) throws SQLException {
        if (!enabled) {
            return null;
        }
        
        return progressDatabase.getProgress(tableName);
    }

    /**
     * 获取所有迁移进度
     */
    public List<MigrationProgress> getAllProgress() throws SQLException {
        if (!enabled) {
            return null;
        }
        
        return progressDatabase.getAllProgress();
    }

    /**
     * 获取未完成的迁移进度
     */
    public List<MigrationProgress> getIncompleteProgress() throws SQLException {
        if (!enabled) {
            return null;
        }
        
        return progressDatabase.getIncompleteProgress();
    }

    /**
     * 检查是否有未完成的迁移
     */
    public boolean hasIncompleteProgress() throws SQLException {
        if (!enabled) {
            return false;
        }
        
        return progressDatabase.hasIncompleteProgress();
    }

    /**
     * 删除表的迁移进度
     */
    public void deleteProgress(String tableName) throws SQLException {
        if (!enabled) {
            return;
        }
        
        progressDatabase.deleteProgress(tableName);
        progressCache.remove(tableName);
        logger.info("删除迁移进度: {}", tableName);
    }

    /**
     * 清空所有迁移进度
     */
    public void clearAllProgress() throws SQLException {
        if (!enabled) {
            return;
        }
        
        progressDatabase.clearAllProgress();
        progressCache.clear();
        logger.info("清空所有迁移进度");
    }

    /**
     * 重置表的迁移进度（重新开始）
     */
    public void resetProgress(String tableName) throws SQLException {
        if (!enabled) {
            return;
        }
        
        MigrationProgress progress = progressDatabase.getProgress(tableName);
        if (progress != null) {
            progress.reset();
            progressDatabase.saveProgress(progress);
            progressCache.put(tableName, progress);
            logger.info("重置迁移进度: {}", tableName);
        }
    }

    /**
     * 打印迁移进度摘要
     */
    public void printProgressSummary() throws SQLException {
        if (!enabled) {
            logger.info("进度管理器已禁用");
            return;
        }
        
        List<MigrationProgress> progressList = getAllProgress();
        
        if (progressList == null || progressList.isEmpty()) {
            logger.info("暂无迁移进度记录");
            return;
        }
        
        logger.info("========== 迁移进度摘要 ==========");
        logger.info("总表数: {}", progressList.size());
        
        int completed = 0;
        int inProgress = 0;
        int failed = 0;
        int pending = 0;
        
        for (MigrationProgress progress : progressList) {
            String status = progress.getStatus();
            switch (status) {
                case "COMPLETED":
                    completed++;
                    break;
                case "IN_PROGRESS":
                    inProgress++;
                    break;
                case "FAILED":
                    failed++;
                    break;
                case "PENDING":
                    pending++;
                    break;
            }
            
            logger.info("表: {}, 状态: {}, 进度: {}/{} ({:.2f}%)", 
                       progress.getTableName(), status, 
                       progress.getMigratedRows(), progress.getTotalRows(),
                       progress.getProgressPercentage());
        }
        
        logger.info("----------------------------------");
        logger.info("已完成: {}, 进行中: {}, 失败: {}, 待处理: {}", 
                   completed, inProgress, failed, pending);
        logger.info("==================================");
    }

    /**
     * 关闭进度管理器
     */
    public void close() {
        if (progressDatabase != null) {
            progressDatabase.close();
            logger.info("进度管理器已关闭");
        }
    }

    /**
     * 检查是否启用
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * 获取缓存的进度对象
     */
    public MigrationProgress getCachedProgress(String tableName) {
        return progressCache.get(tableName);
    }
}
