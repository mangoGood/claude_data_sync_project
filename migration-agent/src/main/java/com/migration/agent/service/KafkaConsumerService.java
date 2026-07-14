package com.migration.agent.service;

import com.google.gson.*;
import com.migration.agent.model.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final String TASK_CREATED_TOPIC = "sync-task-created";
    private static final String DLQ_TOPIC = "sync-task-created-dlq";

    /** 同步处理失败的最大重试次数（超过后进 DLQ 并跳过，避免毒消息永久阻塞分区）。 */
    private static final int MAX_PROCESS_ATTEMPTS = 3;
    private static final long RETRY_INITIAL_DELAY_MS = 1000;
    private static final long RETRY_MAX_DELAY_MS = 10000;

    private final String bootstrapServers;
    private final String groupId;
    private final TaskHandler taskHandler;
    private final Gson gson;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;
    private org.apache.kafka.clients.producer.KafkaProducer<String, String> dlqProducer;
    
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
        // 手动提交：只有消息处理成功（或明确判定为毒消息进 DLQ）后才推进 offset，
        // 避免"处理失败但 offset 已自动提交 → 消息永久丢失、任务静默不启动"。
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        initDlqProducer();

        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer =
            new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        consumer.subscribe(java.util.Collections.singletonList(TASK_CREATED_TOPIC));

        try {
            while (running.get()) {
                try {
                    org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records =
                        consumer.poll(java.time.Duration.ofMillis(1000));

                    for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record : records) {
                        processRecordWithRetry(record);
                    }

                    // 整批处理完（成功或已 DLQ 跳过）后同步提交 offset
                    if (!records.isEmpty()) {
                        try {
                            consumer.commitSync();
                        } catch (Exception ce) {
                            logger.error("提交 offset 失败，下轮将重投这批消息（业务侧需保证幂等）", ce);
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
            closeDlqProducer();
        }
    }

    /**
     * 单条消息处理：带指数退避的有限次重试；耗尽后送 DLQ 并跳过（不阻塞分区）。
     * 注意：handleTask 对大多数消息类型是"提交异步任务后快速返回"，这里的失败面主要是
     * 反序列化错误或同步派发失败——毒消息（如 JSON 损坏）每次都失败，必须 DLQ 跳过。
     */
    private void processRecordWithRetry(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
        long delay = RETRY_INITIAL_DELAY_MS;
        for (int attempt = 1; attempt <= MAX_PROCESS_ATTEMPTS; attempt++) {
            try {
                logger.info("Received message (attempt {}/{}): {}", attempt, MAX_PROCESS_ATTEMPTS, record.value());
                TaskMessage taskMessage = gson.fromJson(record.value(), TaskMessage.class);
                taskHandler.handleTask(taskMessage);
                return; // 成功
            } catch (Exception e) {
                logger.error("处理消息失败 (attempt {}/{}): {}", attempt, MAX_PROCESS_ATTEMPTS, record.value(), e);
                if (attempt < MAX_PROCESS_ATTEMPTS) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    delay = Math.min(delay * 2, RETRY_MAX_DELAY_MS);
                } else {
                    sendToDlq(record, e);
                }
            }
        }
    }

    private void initDlqProducer() {
        try {
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrapServers);
            p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            p.put("acks", "1");
            p.put("retries", 3);
            dlqProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(p);
        } catch (Exception e) {
            logger.warn("DLQ producer 初始化失败，毒消息将只记录日志不入 DLQ: {}", e.getMessage());
        }
    }

    private void sendToDlq(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record, Exception cause) {
        logger.error("消息重试 {} 次仍失败，送入 DLQ 并跳过: key={}, value={}",
                MAX_PROCESS_ATTEMPTS, record.key(), record.value());
        if (dlqProducer == null) {
            logger.error("DLQ producer 不可用，毒消息仅记录日志（offset 仍会前进以免阻塞分区）");
            return;
        }
        try {
            org.apache.kafka.clients.producer.ProducerRecord<String, String> dlqRecord =
                new org.apache.kafka.clients.producer.ProducerRecord<>(DLQ_TOPIC, record.key(), record.value());
            dlqRecord.headers().add("dlq-reason",
                (cause.getClass().getSimpleName() + ": " + cause.getMessage()).getBytes(StandardCharsets.UTF_8));
            dlqRecord.headers().add("dlq-origin-topic", TASK_CREATED_TOPIC.getBytes(StandardCharsets.UTF_8));
            dlqProducer.send(dlqRecord).get(5, java.util.concurrent.TimeUnit.SECONDS);
            logger.info("毒消息已送入 DLQ topic: {}", DLQ_TOPIC);
        } catch (Exception e) {
            logger.error("送 DLQ 失败，毒消息仅记录日志: {}", e.getMessage());
        }
    }

    private void closeDlqProducer() {
        if (dlqProducer != null) {
            try {
                dlqProducer.close();
            } catch (Exception ignored) {
            }
        }
    }
}
