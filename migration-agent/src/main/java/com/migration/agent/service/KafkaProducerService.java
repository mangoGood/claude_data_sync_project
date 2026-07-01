package com.migration.agent.service;

import com.google.gson.Gson;
import com.migration.agent.model.TaskStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private static final String TASK_STATUS_TOPIC = "sync-task-status";
    
    private final String bootstrapServers;
    private final Gson gson = new Gson();
    private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    
    public KafkaProducerService(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        initProducer();
    }
    
    private void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("retries", 3);
        
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        logger.info("Kafka producer initialized");
    }
    
    public void sendStatus(TaskStatusMessage statusMessage) {
        try {
            String json = gson.toJson(statusMessage);
            logger.info("Sending status update: {}", json);
            
            Future<org.apache.kafka.clients.producer.RecordMetadata> future = 
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                    TASK_STATUS_TOPIC, 
                    statusMessage.getTaskId(), 
                    json
                ));
            
            future.get(5, TimeUnit.SECONDS);
            logger.info("Status update sent successfully for task: {}", statusMessage.getTaskId());
        } catch (Exception e) {
            logger.error("Error sending status update for task: {}", statusMessage.getTaskId(), e);
        }
    }
    
    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("Kafka producer closed");
        }
    }
}