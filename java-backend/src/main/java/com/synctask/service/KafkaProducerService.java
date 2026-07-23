package com.synctask.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.synctask.dto.TaskCreatedMessage;
import com.synctask.entity.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private static final Gson gson = new Gson();

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.topics.task-created}")
    private String taskCreatedTopic;

    public void sendTaskCreatedMessage(Workflow workflow) {
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setSourceDbName(workflow.getSourceDbName());
        message.setTargetDbName(workflow.getTargetDbName());
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setTaskType(workflow.getTaskType());
        message.setDrMode(workflow.getDrMode());
        message.setKafkaBootstrapServers(workflow.getKafkaBootstrapServers());
        message.setKafkaTopicPrefix(workflow.getKafkaTopicPrefix());
        message.setKafkaTopicStrategy(workflow.getKafkaTopicStrategy());
        message.setSubscribeFormat(workflow.getSubscribeFormat());
        message.setSyncAccount(workflow.getSyncAccount());
        message.setSyncAccountSuperPrivilege(workflow.getSyncAccountSuperPrivilege());

        if (workflow.getSyncObjects() != null && !workflow.getSyncObjects().isEmpty()) {
            try {
                Type type = new TypeToken<Map<String, Object>>(){}.getType();
                Map<String, Object> syncObjects = gson.fromJson(workflow.getSyncObjects(), type);
                message.setSyncObjects(syncObjects);
            } catch (Exception e) {
                logger.warn("解析 syncObjects 失败: {}", e.getMessage());
            }
        }

        logger.info("发送任务创建消息到 Kafka: taskId={}, topic={}", workflow.getId(), taskCreatedTopic);

        CompletableFuture<SendResult<String, Object>> future = 
            kafkaTemplate.send(taskCreatedTopic, workflow.getId(), message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("任务创建消息发送成功: taskId={}, partition={}, offset={}", 
                    workflow.getId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                logger.error("任务创建消息发送失败: taskId={}", workflow.getId(), ex);
            }
        });
    }

    public void sendTaskCreatedMessageSync(Workflow workflow) throws Exception {
        TaskCreatedMessage message = new TaskCreatedMessage();
        message.setTaskId(workflow.getId());
        message.setTaskName(workflow.getName());
        message.setUserId(workflow.getUserId());
        message.setSourceConnection(workflow.getSourceConnection());
        message.setTargetConnection(workflow.getTargetConnection());
        message.setMigrationMode(workflow.getMigrationMode());
        message.setCreatedAt(workflow.getCreatedAt());
        message.setSourceDbName(workflow.getSourceDbName());
        message.setTargetDbName(workflow.getTargetDbName());
        message.setSourceType(workflow.getSourceType());
        message.setTargetType(workflow.getTargetType());
        message.setTaskType(workflow.getTaskType());
        message.setDrMode(workflow.getDrMode());
        message.setKafkaBootstrapServers(workflow.getKafkaBootstrapServers());
        message.setKafkaTopicPrefix(workflow.getKafkaTopicPrefix());
        message.setKafkaTopicStrategy(workflow.getKafkaTopicStrategy());
        message.setSubscribeFormat(workflow.getSubscribeFormat());
        message.setSyncAccount(workflow.getSyncAccount());
        message.setSyncAccountSuperPrivilege(workflow.getSyncAccountSuperPrivilege());

        if (workflow.getSyncObjects() != null && !workflow.getSyncObjects().isEmpty()) {
            try {
                Type type = new TypeToken<Map<String, Object>>(){}.getType();
                Map<String, Object> syncObjects = gson.fromJson(workflow.getSyncObjects(), type);
                message.setSyncObjects(syncObjects);
            } catch (Exception e) {
                logger.warn("解析 syncObjects 失败: {}", e.getMessage());
            }
        }

        logger.info("同步发送任务创建消息到 Kafka: taskId={}, topic={}", workflow.getId(), taskCreatedTopic);

        try {
            SendResult<String, Object> result = kafkaTemplate.send(taskCreatedTopic, workflow.getId(), message).get();
            logger.info("任务创建消息发送成功: taskId={}, partition={}, offset={}", 
                workflow.getId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (Exception e) {
            logger.error("任务创建消息发送失败: taskId={}", workflow.getId(), e);
            throw e;
        }
    }

    public void sendControlMessage(TaskCreatedMessage message) {
        String messageType = message.getMessageType();
        logger.info("发送控制消息到 Kafka: taskId={}, messageType={}, topic={}", 
            message.getTaskId(), messageType, taskCreatedTopic);

        try {
            CompletableFuture<SendResult<String, Object>> future = 
                kafkaTemplate.send(taskCreatedTopic, message.getTaskId(), message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("控制消息发送成功: taskId={}, messageType={}, partition={}, offset={}", 
                        message.getTaskId(), messageType, result.getRecordMetadata().partition(), 
                        result.getRecordMetadata().offset());
                } else {
                    logger.error("控制消息发送失败: taskId={}, messageType={}", message.getTaskId(), messageType, ex);
                }
            });
        } catch (Exception e) {
            logger.error("发送控制消息时发生异常: taskId={}, messageType={}, error={}", 
                message.getTaskId(), messageType, e.getMessage());
            throw e;
        }
    }
}
