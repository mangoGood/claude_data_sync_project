package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 批量任务操作服务
 * 支持批量启动/停止/删除任务，批量导出任务配置
 */
@Service
public class BatchOperationService {
    private static final Logger logger = LoggerFactory.getLogger(BatchOperationService.class);

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private WorkflowService workflowService;

    /**
     * 批量启动任务
     */
    @Transactional
    public Map<String, Object> batchLaunch(List<String> workflowIds, Long userId) {
        return executeBatch(workflowIds, userId, "launch", id -> {
            workflowService.launchWorkflow(id, userId);
        });
    }

    /**
     * 批量停止任务
     */
    @Transactional
    public Map<String, Object> batchStop(List<String> workflowIds, Long userId) {
        return executeBatch(workflowIds, userId, "stop", id -> {
            workflowService.stopWorkflow(id, userId);
        });
    }

    /**
     * 批量删除任务
     */
    @Transactional
    public Map<String, Object> batchDelete(List<String> workflowIds, Long userId) {
        return executeBatch(workflowIds, userId, "delete", id -> {
            workflowService.deleteWorkflow(id, userId);
        });
    }

    /**
     * 批量导出任务配置
     */
    public Map<String, Object> batchExport(List<String> workflowIds, Long userId) {
        List<Map<String, Object>> exported = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        for (String id : workflowIds) {
            try {
                Workflow wf = workflowRepository.findById(id).orElse(null);
                if (wf == null || !wf.getUserId().equals(userId)) {
                    failed.add(id + " (不存在或无权)");
                    continue;
                }
                Map<String, Object> config = new HashMap<>();
                config.put("id", wf.getId());
                config.put("name", wf.getName());
                config.put("sourceConnection", wf.getSourceConnection());
                config.put("targetConnection", wf.getTargetConnection());
                config.put("migrationMode", wf.getMigrationMode());
                config.put("syncObjects", wf.getSyncObjects());
                config.put("sourceDbName", wf.getSourceDbName());
                config.put("targetDbName", wf.getTargetDbName());
                config.put("sourceType", wf.getSourceType());
                config.put("targetType", wf.getTargetType());
                config.put("taskType", wf.getTaskType());
                config.put("kafkaBootstrapServers", wf.getKafkaBootstrapServers());
                config.put("kafkaTopicPrefix", wf.getKafkaTopicPrefix());
                config.put("kafkaTopicStrategy", wf.getKafkaTopicStrategy());
                config.put("subscribeFormat", wf.getSubscribeFormat());
                config.put("fanoutEnabled", wf.getFanoutEnabled());
                config.put("targetConnections", wf.getTargetConnections());
                exported.add(config);
            } catch (Exception e) {
                failed.add(id + " (" + e.getMessage() + ")");
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("exported", exported);
        result.put("exportedCount", exported.size());
        result.put("failed", failed);
        result.put("failedCount", failed.size());
        return result;
    }

    @FunctionalInterface
    private interface WorkflowAction {
        void execute(String workflowId) throws Exception;
    }

    private Map<String, Object> executeBatch(List<String> workflowIds, Long userId,
                                             String action, WorkflowAction operation) {
        List<String> success = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        for (String id : workflowIds) {
            try {
                operation.execute(id);
                success.add(id);
            } catch (Exception e) {
                failed.add(id + ": " + e.getMessage());
                logger.error("批量{}失败: workflowId={}", action, id, e);
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("action", action);
        result.put("successCount", success.size());
        result.put("failedCount", failed.size());
        result.put("success", success);
        result.put("failed", failed);
        return result;
    }
}
