package com.migration.agent.service;

import com.migration.agent.model.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Fan-out 管理器
 *
 * <p>根据 TaskMessage 中的多目标库配置创建并管理 FanoutDispatcherService。
 * 当任务启用 fan-out 时，将增量同步的 SQL 分发到多个目标库。
 */
public class FanoutManager {
    private static final Logger logger = LoggerFactory.getLogger(FanoutManager.class);

    private final AgentHttpServer httpServer;

    public FanoutManager(AgentHttpServer httpServer) {
        this.httpServer = httpServer;
    }

    /**
     * 根据任务消息初始化 fan-out 分发服务
     *
     * @return FanoutDispatcherService 实例（如果未启用 fan-out 则返回 null）
     */
    public FanoutDispatcherService initializeFanout(TaskMessage taskMessage) {
        if (taskMessage == null) return null;
        Boolean fanoutEnabled = taskMessage.getFanoutEnabled();
        if (fanoutEnabled == null || !fanoutEnabled) {
            logger.info("任务 {} 未启用 fan-out，使用单目标库模式", taskMessage.getTaskId());
            return null;
        }

        List<TaskMessage.DatabaseConfig> targetConfigs = taskMessage.getTargetConnections();
        if (targetConfigs == null || targetConfigs.isEmpty()) {
            logger.warn("任务 {} 启用了 fan-out 但未配置目标库列表，回退到单目标库模式", taskMessage.getTaskId());
            return null;
        }

        List<FanoutDispatcherService.TargetConfig> targets = new ArrayList<>();
        for (int i = 0; i < targetConfigs.size(); i++) {
            TaskMessage.DatabaseConfig config = targetConfigs.get(i);
            String name = String.format("target-%d-%s", i + 1, config.getDatabase());
            targets.add(new FanoutDispatcherService.TargetConfig(
                    name, config.getHost(), config.getPort(),
                    config.getDatabase(), config.getUsername(), config.getPassword()));
        }

        try {
            FanoutDispatcherService dispatcher = new FanoutDispatcherService(targets);
            dispatcher.initialize();
            httpServer.registerFanoutService(taskMessage.getTaskId(), dispatcher);
            logger.info("任务 {} fan-out 分发服务已初始化，目标库数量: {}",
                    taskMessage.getTaskId(), targets.size());
            return dispatcher;
        } catch (Exception e) {
            logger.error("任务 {} 初始化 fan-out 分发服务失败", taskMessage.getTaskId(), e);
            return null;
        }
    }

    /** 关闭任务的 fan-out 分发服务 */
    public void shutdownFanout(String taskId) {
        if (taskId == null) return;
        httpServer.unregisterFanoutService(taskId);
        logger.info("任务 {} fan-out 分发服务已关闭", taskId);
    }
}
