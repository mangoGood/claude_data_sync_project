package com.migration.agent.checkpoint;

import com.migration.common.position.LocalCheckpointStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作废一个任务的全部位点（统一载体 + 中心库 + 上卷缓存）。
 *
 * <p>倒换与重做全量各有一条清理路径（{@code AgentMain.cleanupFailoverArtifacts} 与
 * {@code FailoverService.cleanFailoverFiles}），两条都必须清到中心库，所以收在这里一处。
 *
 * <p><b>为什么中心位点非清不可</b>：本地位点清干净了，中心位点却留着，接管方一回灌
 * 就把刚清掉的旧源位点原样请回来。倒换后源库已经换成原目标实例，旧实例的 GTID 在新源上
 * 会让服务端从 binlog <b>最开头</b>整段重放，直接冲垮备库——这正是
 * {@code FailoverCleanupInvariantTest} 锁死的那条不变量，中心化之后它的边界也得跟着扩。
 */
public final class CheckpointCleaner {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointCleaner.class);

    private CheckpointCleaner() {
    }

    /**
     * @param reason 写进位点历史的原因（FAILOVER / RESET…），便于事后对账"位点是什么时候、被谁清的"
     */
    public static void clear(String taskId, String reason) {
        LocalCheckpointStore.deleteAll(taskId);

        // 上卷缓存也要清：它按"内容有没有变"决定要不要写库，不清的话倒换后的新位点
        // 会因为指纹碰巧没变而被跳过，中心库里就一直是空的
        CheckpointUploader uploader = CheckpointUploader.getInstance();
        if (uploader != null) {
            uploader.forget(taskId);
        }

        CentralCheckpointStore store = CentralCheckpointStore.getInstance();
        if (store != null) {
            store.deleteTask(taskId, reason);
        }
        logger.info("[{}] 位点已作废（{}）：统一载体 + 中心库 + 上卷缓存", taskId, reason);
    }
}
