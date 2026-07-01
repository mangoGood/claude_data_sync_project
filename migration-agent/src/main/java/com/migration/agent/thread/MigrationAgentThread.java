package com.migration.agent.thread;

import com.migration.agent.model.TaskMessage;
import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.TaskStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务执行入口，根据任务类型委托给具体的执行器。
 *
 * <p>本类保留作为对外 API 的兼容入口（被 AgentMain、TaskProcessService、
 * TaskMessageHandler、TaskRecoveryService、FailoverService 等使用）。
 * 实际执行逻辑已拆分至：
 * <ul>
 *   <li>{@link FullMigrationTask} - 全量迁移（含可选增量同步）</li>
 *   <li>{@link IncrementSyncTask} - 仅增量同步恢复</li>
 *   <li>{@link SubscribeTask} - 数据订阅任务</li>
 * </ul>
 *
 * <p>本类实现 Runnable 并将所有方法委托给内部 executor，保持原有 API 兼容。
 */
public class MigrationAgentThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MigrationAgentThread.class);

    private final AbstractTaskExecutor executor;

    public MigrationAgentThread(TaskMessage taskMessage, KafkaProducerService kafkaProducer) {
        this(taskMessage, kafkaProducer, null, false, new AgentConfig());
    }

    public MigrationAgentThread(TaskMessage taskMessage, KafkaProducerService kafkaProducer, boolean skipFullMigration) {
        this(taskMessage, kafkaProducer, null, skipFullMigration, new AgentConfig());
    }

    public MigrationAgentThread(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                                TaskStateService taskStateService, boolean skipFullMigration) {
        this(taskMessage, kafkaProducer, taskStateService, skipFullMigration, new AgentConfig());
    }

    public MigrationAgentThread(TaskMessage taskMessage, KafkaProducerService kafkaProducer,
                                TaskStateService taskStateService, boolean skipFullMigration, AgentConfig config) {
        this.executor = createExecutor(taskMessage, kafkaProducer, taskStateService, skipFullMigration, config);
    }

    /**
     * 根据任务类型创建对应的执行器。
     *
     * <p>选择策略：
     * <ul>
     *   <li>taskType=SUBSCRIBE → {@link SubscribeTask}</li>
     *   <li>skipFullMigration=true → {@link IncrementSyncTask}（从增量阶段恢复）</li>
     *   <li>其它 → {@link FullMigrationTask}（全量迁移，可能含增量）</li>
     * </ul>
     */
    private static AbstractTaskExecutor createExecutor(TaskMessage taskMessage,
                                                       KafkaProducerService kafkaProducer,
                                                       TaskStateService taskStateService,
                                                       boolean skipFullMigration,
                                                       AgentConfig config) {
        String taskType = taskMessage.getTaskType();
        logger.info("[{}] 创建任务执行器, taskType={}, skipFullMigration={}",
                taskMessage.getTaskId(), taskType, skipFullMigration);

        if ("SUBSCRIBE".equals(taskType)) {
            return new SubscribeTask(taskMessage, kafkaProducer, taskStateService, config);
        }
        if (skipFullMigration) {
            return new IncrementSyncTask(taskMessage, kafkaProducer, taskStateService, config);
        }
        return new FullMigrationTask(taskMessage, kafkaProducer, taskStateService, config);
    }

    @Override
    public void run() {
        executor.run();
    }

    public void stop() {
        executor.stop();
    }

    public void stopAndInterrupt(Thread thread) {
        executor.stopAndInterrupt(thread);
    }

    public boolean isRunning() {
        return executor.isRunning();
    }

    public String getTaskId() {
        return executor.getTaskId();
    }
}
