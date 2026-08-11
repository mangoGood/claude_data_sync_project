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

        // 全量表级断点同理：倒换/重做全量之后留着它，下次接管会跳过其实需要重搬的表
        FullProgressStore fullProgress = FullProgressStoreHolder.get();
        if (fullProgress != null) {
            fullProgress.clear(taskId);
        }

        // 表结构时序库同理，而且后果更隐蔽：位点全作废意味着任务会从一个<b>更靠后</b>的位点
        // 重启，而时序库最新版本停在很久以前——中间那段的 DDL 它一条都没见过。留着它，
        // 抽取端会拿这份过期的结构去解析新事件，整行的值与列错位，写进目标库的是合法值、
        // 看不出异常。必须一起清掉，让 capture 重新打基线。
        //（注意：PITR 把位点往<b>前</b>调不走这里——那种情况时序库覆盖得到，保留才对。）
        SchemaVersionStore schemaStore = SchemaVersionStore.getInstance();
        if (schemaStore != null) {
            schemaStore.deleteTask(taskId, reason);
        }
        deleteLocalSchemaHistory(taskId);

        logger.info("[{}] 位点已作废（{}）：统一载体 + 中心库 + 上卷缓存 + 全量表级断点 + 表结构时序库",
                taskId, reason);
    }

    /** 本地时序库文件也要删——中心库清了、本地留着，extract 一启动照样把过期版本装载回来。 */
    private static void deleteLocalSchemaHistory(String taskId) {
        try {
            java.nio.file.Files.deleteIfExists(SchemaVersionStore.historyPath(taskId));
        } catch (Exception e) {
            logger.warn("[{}] 删除本地表结构历史失败: {}", taskId, e.getMessage());
        }
    }
}
