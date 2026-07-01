package com.synctask.service;

import com.synctask.entity.TaskDependency;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.TaskDependencyRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 任务依赖编排服务
 * 任务A完成后自动启动任务B
 */
@Service
public class TaskDependencyService {
    private static final Logger logger = LoggerFactory.getLogger(TaskDependencyService.class);

    @Autowired
    private TaskDependencyRepository dependencyRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private WorkflowService workflowService;

    @Transactional
    public TaskDependency createDependency(String upstreamWorkflowId, String downstreamWorkflowId,
                                           Long userId, String triggerCondition) {
        // 验证两个任务都属于该用户
        Workflow upstream = workflowRepository.findById(upstreamWorkflowId)
                .orElseThrow(() -> new RuntimeException("上游任务不存在"));
        Workflow downstream = workflowRepository.findById(downstreamWorkflowId)
                .orElseThrow(() -> new RuntimeException("下游任务不存在"));

        if (!upstream.getUserId().equals(userId) || !downstream.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作这些任务");
        }

        if (upstreamWorkflowId.equals(downstreamWorkflowId)) {
            throw new RuntimeException("不能创建自依赖");
        }

        TaskDependency dependency = new TaskDependency();
        dependency.setUpstreamWorkflowId(upstreamWorkflowId);
        dependency.setDownstreamWorkflowId(downstreamWorkflowId);
        dependency.setUserId(userId);
        dependency.setTriggerCondition(triggerCondition != null ? triggerCondition : "ON_SUCCESS");
        dependency.setEnabled(true);
        dependency.setTriggerCount(0);

        return dependencyRepository.save(dependency);
    }

    public List<TaskDependency> getDependenciesByUser(Long userId) {
        return dependencyRepository.findByUserId(userId);
    }

    @Transactional
    public void deleteDependency(Long dependencyId, Long userId) {
        TaskDependency dep = dependencyRepository.findById(dependencyId)
                .orElseThrow(() -> new RuntimeException("依赖关系不存在"));
        if (!dep.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此依赖");
        }
        dependencyRepository.delete(dep);
    }

    /**
     * 检查并触发下游任务
     * @param upstreamWorkflowId 上游任务ID
     * @param upstreamResult 上游结果: ON_SUCCESS, ON_FAILURE, ON_COMPLETION
     */
    @Transactional
    public void checkAndTriggerDependencies(String upstreamWorkflowId, String upstreamResult) {
        List<TaskDependency> deps = dependencyRepository
                .findByUpstreamWorkflowIdAndEnabledTrue(upstreamWorkflowId);

        for (TaskDependency dep : deps) {
            boolean shouldTrigger = false;
            if ("ON_SUCCESS".equals(dep.getTriggerCondition()) && "ON_SUCCESS".equals(upstreamResult)) {
                shouldTrigger = true;
            } else if ("ON_FAILURE".equals(dep.getTriggerCondition()) && "ON_FAILURE".equals(upstreamResult)) {
                shouldTrigger = true;
            } else if ("ON_COMPLETION".equals(dep.getTriggerCondition())) {
                shouldTrigger = true;
            }

            if (shouldTrigger) {
                try {
                    triggerDownstreamTask(dep);
                } catch (Exception e) {
                    logger.error("触发下游任务失败: upstream={}, downstream={}",
                            upstreamWorkflowId, dep.getDownstreamWorkflowId(), e);
                }
            }
        }
    }

    private void triggerDownstreamTask(TaskDependency dep) {
        Workflow downstream = workflowRepository.findById(dep.getDownstreamWorkflowId())
                .orElse(null);
        if (downstream == null) {
            logger.warn("下游任务不存在: {}", dep.getDownstreamWorkflowId());
            return;
        }

        // 只有配置中或失败的任务才能被触发
        WorkflowStatus status = downstream.getStatus();
        if (status != WorkflowStatus.CONFIGURING && status != WorkflowStatus.FAILED
                && status != WorkflowStatus.COMPLETED) {
            logger.info("下游任务状态为 {}，跳过触发: {}", status, downstream.getId());
            return;
        }

        logger.info("任务依赖触发: {} -> {}", dep.getUpstreamWorkflowId(), dep.getDownstreamWorkflowId());

        // 重置状态并启动
        if (status == WorkflowStatus.FAILED || status == WorkflowStatus.COMPLETED) {
            downstream.setStatus(WorkflowStatus.CONFIGURING);
            downstream.setErrorMessage(null);
            downstream.setCompletedAt(null);
            workflowRepository.save(downstream);
        }

        workflowService.launchWorkflow(dep.getDownstreamWorkflowId(), dep.getUserId());

        dep.setLastTriggeredAt(LocalDateTime.now());
        dep.setTriggerCount(dep.getTriggerCount() + 1);
        dependencyRepository.save(dep);
    }
}
