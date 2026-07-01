package com.synctask.executor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synctask.executor.dto.TaskCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Service
public class TaskExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutorService.class);

    @Value("${app.task.file-base-path:./files}")
    private String fileBasePath;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "${spring.kafka.topics.task-created}", groupId = "${spring.kafka.consumer.task-created-group-id:task-created-group}")
    public void consumeTaskCreatedMessage(Map<String, Object> messageMap) {
        logger.info("收到任务创建消息: {}", messageMap);

        try {
            TaskCreatedEvent event = convertToTaskCreatedEvent(messageMap);
            
            if (!"TASK_CREATED".equals(event.getMessageType())) {
                logger.warn("忽略非 TASK_CREATED 类型的消息: messageType={}", event.getMessageType());
                return;
            }

            Path taskDirPath = createTaskDirectory(event.getTaskId());
            
            writeTaskInfoFile(taskDirPath, event);
            
            writeTaskConfigFile(taskDirPath, event);
            
            logger.info("任务目录创建成功: taskId={}, path={}", event.getTaskId(), taskDirPath.toAbsolutePath());
            
        } catch (Exception e) {
            logger.error("处理任务创建消息失败: {}", messageMap, e);
        }
    }

    private TaskCreatedEvent convertToTaskCreatedEvent(Map<String, Object> map) {
        TaskCreatedEvent event = new TaskCreatedEvent();
        
        if (map.get("taskId") != null) {
            event.setTaskId(String.valueOf(map.get("taskId")));
        }
        if (map.get("taskName") != null) {
            event.setTaskName(String.valueOf(map.get("taskName")));
        }
        if (map.get("userId") != null) {
            event.setUserId(Long.valueOf(String.valueOf(map.get("userId"))));
        }
        if (map.get("sourceConnection") != null) {
            event.setSourceConnection(String.valueOf(map.get("sourceConnection")));
        }
        if (map.get("targetConnection") != null) {
            event.setTargetConnection(String.valueOf(map.get("targetConnection")));
        }
        if (map.get("messageType") != null) {
            event.setMessageType(String.valueOf(map.get("messageType")));
        }
        if (map.get("sourceDbType") != null) {
            event.setSourceDbType(String.valueOf(map.get("sourceDbType")));
        } else if (map.get("sourceType") != null) {
            event.setSourceDbType(String.valueOf(map.get("sourceType")));
        }
        if (map.get("targetDbType") != null) {
            event.setTargetDbType(String.valueOf(map.get("targetDbType")));
        } else if (map.get("targetType") != null) {
            event.setTargetDbType(String.valueOf(map.get("targetType")));
        }
        
        return event;
    }

    private Path createTaskDirectory(String taskId) throws IOException {
        Path basePath = Paths.get(fileBasePath).toAbsolutePath();
        Files.createDirectories(basePath);
        logger.info("基础文件目录: {}", basePath);

        Path taskDirPath = basePath.resolve(taskId);
        Files.createDirectories(taskDirPath);
        logger.info("任务目录: {}", taskDirPath);

        Path tempDirPath = taskDirPath.resolve("temp");
        Files.createDirectories(tempDirPath);
        logger.info("临时文件目录: {}", tempDirPath);

        Path logsDirPath = taskDirPath.resolve("logs");
        Files.createDirectories(logsDirPath);
        logger.info("日志目录: {}", logsDirPath);

        return taskDirPath;
    }

    private void writeTaskInfoFile(Path taskDirPath, TaskCreatedEvent event) throws IOException {
        Path infoFilePath = taskDirPath.resolve("task-info.json");
        
        Files.createDirectories(infoFilePath.getParent());
        
        String jsonContent = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(event);
        
        try (FileWriter writer = new FileWriter(infoFilePath.toFile())) {
            writer.write(jsonContent);
        }
        
        logger.info("写入任务信息文件: {}", infoFilePath.toAbsolutePath());
    }

    private void writeTaskConfigFile(Path taskDirPath, TaskCreatedEvent event) throws IOException {
        Path configFilePath = taskDirPath.resolve("task-config.properties");
        
        Files.createDirectories(configFilePath.getParent());
        
        StringBuilder configBuilder = new StringBuilder();
        configBuilder.append("# 任务配置文件\n");
        configBuilder.append("# 自动生成于: ").append(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).append("\n\n");
        
        configBuilder.append("# 任务基本信息\n");
        configBuilder.append("task.id=").append(event.getTaskId()).append("\n");
        configBuilder.append("task.name=").append(event.getTaskName() != null ? event.getTaskName() : "").append("\n");
        configBuilder.append("task.user.id=").append(event.getUserId() != null ? event.getUserId() : "").append("\n");
        configBuilder.append("task.created.at=").append(event.getCreatedAt() != null ? event.getCreatedAt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : "").append("\n");
        configBuilder.append("task.message.type=").append(event.getMessageType() != null ? event.getMessageType() : "").append("\n\n");
        
        configBuilder.append("# 数据库连接信息\n");
        configBuilder.append("source.connection=").append(event.getSourceConnection() != null ? event.getSourceConnection() : "").append("\n");
        configBuilder.append("target.connection=").append(event.getTargetConnection() != null ? event.getTargetConnection() : "").append("\n");
        configBuilder.append("source.db.type=").append(event.getSourceDbType() != null ? event.getSourceDbType() : "mysql").append("\n");
        configBuilder.append("target.db.type=").append(event.getTargetDbType() != null ? event.getTargetDbType() : "mysql").append("\n");
        
        try (FileWriter writer = new FileWriter(configFilePath.toFile())) {
            writer.write(configBuilder.toString());
        }
        
        logger.info("写入任务配置文件: {}", configFilePath.toAbsolutePath());
    }
}
    