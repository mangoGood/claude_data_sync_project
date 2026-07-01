package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * 任务克隆服务
 * 一键复制现有任务配置创建新任务
 */
@Service
public class TaskCloneService {
    private static final Logger logger = LoggerFactory.getLogger(TaskCloneService.class);

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private ConfigVersionService configVersionService;

    @Transactional
    public Workflow cloneTask(String sourceWorkflowId, Long userId, String newName) {
        Workflow source = workflowRepository.findById(sourceWorkflowId)
                .orElseThrow(() -> new RuntimeException("源任务不存在"));

        if (!source.getUserId().equals(userId)) {
            throw new RuntimeException("无权克隆此任务");
        }

        // 创建新任务
        Workflow cloned = workflowService.createWorkflow(
                newName != null ? newName : source.getName() + "_副本",
                source.getSourceType(),
                source.getTargetType(),
                userId,
                source.getTaskType()
        );

        // 复制配置
        cloned.setSourceConnection(source.getSourceConnection());
        cloned.setTargetConnection(source.getTargetConnection());
        cloned.setMigrationMode(source.getMigrationMode());
        cloned.setSyncObjects(source.getSyncObjects());
        cloned.setSourceDbName(source.getSourceDbName());
        cloned.setTargetDbName(source.getTargetDbName());
        cloned.setKafkaBootstrapServers(source.getKafkaBootstrapServers());
        cloned.setKafkaTopicPrefix(source.getKafkaTopicPrefix());
        cloned.setKafkaTopicStrategy(source.getKafkaTopicStrategy());
        cloned.setSubscribeFormat(source.getSubscribeFormat());
        cloned.setFanoutEnabled(source.getFanoutEnabled());
        cloned.setTargetConnections(source.getTargetConnections());
        cloned.setFanoutTargetCount(source.getFanoutTargetCount());

        workflowRepository.save(cloned);

        // 保存初始配置版本
        configVersionService.saveVersion(cloned.getId(), userId, "克隆自任务: " + source.getName(), "clone");

        logger.info("任务克隆成功: {} -> {} ({})", sourceWorkflowId, cloned.getId(), cloned.getName());
        return cloned;
    }
}
