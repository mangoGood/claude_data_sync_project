package com.migration.agent.service;

import com.google.gson.*;
import com.migration.agent.model.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final String TASK_CREATED_TOPIC = "sync-task-created";
    
    private final String bootstrapServers;
    private final String groupId;
    private final TaskHandler taskHandler;
    private final Gson gson;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;
    
    public interface TaskHandler {
        void handleTask(TaskMessage taskMessage);
    }
    
    public KafkaConsumerService(String bootstrapServers, String groupId, TaskHandler taskHandler) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.taskHandler = taskHandler;
        
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(LocalDateTime.class, new JsonDeserializer<LocalDateTime>() {
            private final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            
            @Override
            public LocalDateTime deserialize(JsonElement json, Type typeOfT, 
                    JsonDeserializationContext context) throws JsonParseException {
                if (json.isJsonArray()) {
                    JsonArray arr = json.getAsJsonArray();
                    int year = arr.get(0).getAsInt();
                    int month = arr.get(1).getAsInt();
                    int day = arr.get(2).getAsInt();
                    int hour = arr.get(3).getAsInt();
                    int minute = arr.get(4).getAsInt();
                    int second = arr.size() > 5 ? arr.get(5).getAsInt() : 0;
                    return LocalDateTime.of(year, month, day, hour, minute, second);
                } else if (json.isJsonPrimitive()) {
                    return LocalDateTime.parse(json.getAsString(), formatter);
                }
                return null;
            }
        });
        
        gson = builder.create();
    }
    
    public void start() {
        running.set(true);
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this::consumeMessages);
        logger.info("Kafka consumer started for topic: {}", TASK_CREATED_TOPIC);
    }
    
    public void stop() {
        running.set(false);
        if (executorService != null) {
            executorService.shutdown();
        }
        logger.info("Kafka consumer stopped");
    }
    
    private void consumeMessages() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = 
            new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        consumer.subscribe(java.util.Collections.singletonList(TASK_CREATED_TOPIC));
        
        try {
            while (running.get()) {
                try {
                    org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records = 
                        consumer.poll(java.time.Duration.ofMillis(1000));
                    
                    for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record : records) {
                        try {
                            logger.info("Received message: {}", record.value());
                            TaskMessage taskMessage = gson.fromJson(record.value(), TaskMessage.class);
                            taskHandler.handleTask(taskMessage);
                        } catch (Exception e) {
                            logger.error("Error processing message: {}", record.value(), e);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        logger.error("Error consuming messages", e);
                        Thread.sleep(5000);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Fatal error in consumer thread", e);
        } finally {
            consumer.close();
        }
    }
}
