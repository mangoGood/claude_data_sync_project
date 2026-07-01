package com.synctask.service;

import com.google.gson.Gson;
import com.synctask.entity.ConfigVersion;
import com.synctask.entity.Workflow;
import com.synctask.repository.ConfigVersionRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置版本管理服务
 * 记录任务配置变更历史，支持回滚
 */
@Service
public class ConfigVersionService {
    private static final Logger logger = LoggerFactory.getLogger(ConfigVersionService.class);
    private static final Gson gson = new Gson();

    @Autowired
    private ConfigVersionRepository versionRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    /**
     * 保存配置快照（在配置变更时调用）
     */
    @Transactional
    public ConfigVersion saveVersion(String workflowId, Long userId, String changeDescription, String createdBy) {
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));

        // 获取当前最大版本号
        Integer maxVersion = versionRepository.findTopByWorkflowIdOrderByVersionNumberDesc(workflowId)
                .map(ConfigVersion::getVersionNumber).orElse(0);

        ConfigVersion version = new ConfigVersion();
        version.setWorkflowId(workflowId);
        version.setUserId(userId);
        version.setVersionNumber(maxVersion + 1);
        version.setConfigSnapshot(createSnapshot(workflow));
        version.setChangeDescription(changeDescription);
        version.setCreatedBy(createdBy);

        return versionRepository.save(version);
    }

    public List<ConfigVersion> getVersions(String workflowId) {
        return versionRepository.findByWorkflowIdOrderByVersionNumberDesc(workflowId);
    }

    /**
     * 回滚到指定版本
     */
    @Transactional
    public Workflow rollbackToVersion(String workflowId, Long userId, Integer versionNumber) {
        ConfigVersion version = versionRepository.findByWorkflowIdAndVersionNumber(workflowId, versionNumber)
                .orElseThrow(() -> new RuntimeException("版本不存在: v" + versionNumber));

        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));

        if (!workflow.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此任务");
        }

        // 只允许配置中的任务回滚
        if (workflow.getStatus() != com.synctask.entity.WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能回滚配置中的任务");
        }

        // 从快照恢复配置
        restoreFromSnapshot(workflow, version.getConfigSnapshot());
        workflowRepository.save(workflow);

        // 保存回滚操作为新版本
        saveVersion(workflowId, userId, "回滚到版本 v" + versionNumber, "system");

        logger.info("配置回滚: workflowId={}, 从v{}回滚", workflowId, versionNumber);
        return workflow;
    }

    private String createSnapshot(Workflow workflow) {
        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("sourceConnection", workflow.getSourceConnection());
        snapshot.put("targetConnection", workflow.getTargetConnection());
        snapshot.put("migrationMode", workflow.getMigrationMode());
        snapshot.put("syncObjects", workflow.getSyncObjects());
        snapshot.put("sourceDbName", workflow.getSourceDbName());
        snapshot.put("targetDbName", workflow.getTargetDbName());
        snapshot.put("sourceType", workflow.getSourceType());
        snapshot.put("targetType", workflow.getTargetType());
        snapshot.put("kafkaBootstrapServers", workflow.getKafkaBootstrapServers());
        snapshot.put("kafkaTopicPrefix", workflow.getKafkaTopicPrefix());
        snapshot.put("kafkaTopicStrategy", workflow.getKafkaTopicStrategy());
        snapshot.put("subscribeFormat", workflow.getSubscribeFormat());
        snapshot.put("fanoutEnabled", workflow.getFanoutEnabled());
        snapshot.put("targetConnections", workflow.getTargetConnections());
        return gson.toJson(snapshot);
    }

    @SuppressWarnings("unchecked")
    private void restoreFromSnapshot(Workflow workflow, String snapshotJson) {
        Map<String, Object> snapshot = gson.fromJson(snapshotJson, Map.class);
        if (snapshot.get("sourceConnection") != null) workflow.setSourceConnection((String) snapshot.get("sourceConnection"));
        if (snapshot.get("targetConnection") != null) workflow.setTargetConnection((String) snapshot.get("targetConnection"));
        if (snapshot.get("migrationMode") != null) workflow.setMigrationMode((String) snapshot.get("migrationMode"));
        if (snapshot.get("syncObjects") != null) workflow.setSyncObjects((String) snapshot.get("syncObjects"));
        if (snapshot.get("sourceDbName") != null) workflow.setSourceDbName((String) snapshot.get("sourceDbName"));
        if (snapshot.get("targetDbName") != null) workflow.setTargetDbName((String) snapshot.get("targetDbName"));
        if (snapshot.get("sourceType") != null) workflow.setSourceType((String) snapshot.get("sourceType"));
        if (snapshot.get("targetType") != null) workflow.setTargetType((String) snapshot.get("targetType"));
        if (snapshot.get("kafkaBootstrapServers") != null) workflow.setKafkaBootstrapServers((String) snapshot.get("kafkaBootstrapServers"));
        if (snapshot.get("kafkaTopicPrefix") != null) workflow.setKafkaTopicPrefix((String) snapshot.get("kafkaTopicPrefix"));
        if (snapshot.get("kafkaTopicStrategy") != null) workflow.setKafkaTopicStrategy((String) snapshot.get("kafkaTopicStrategy"));
        if (snapshot.get("subscribeFormat") != null) workflow.setSubscribeFormat((String) snapshot.get("subscribeFormat"));
        if (snapshot.get("fanoutEnabled") != null) workflow.setFanoutEnabled((Boolean) snapshot.get("fanoutEnabled"));
        if (snapshot.get("targetConnections") != null) workflow.setTargetConnections((String) snapshot.get("targetConnections"));
    }
}
