package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class AgentConfig {
    private static final Logger logger = LoggerFactory.getLogger(AgentConfig.class);

    private static final String CONFIG_FILE = "agent.properties";
    private static final String ENV_PREFIX = "MIGRATION_AGENT_";

    private final Properties props;

    public AgentConfig() {
        props = new Properties();
        loadDefaults();
        loadFromFile();
        loadFromEnv();
    }

    private void loadDefaults() {
        props.setProperty("kafka.bootstrap.servers", "192.168.117.2:19092");
        props.setProperty("kafka.consumer.group.id", "migration-agent-group");
        props.setProperty("kafka.topic.task-created", "sync-task-created");
        props.setProperty("kafka.topic.task-status", "sync-task-status");

        props.setProperty("mysql.db.url", "jdbc:mysql://192.168.107.2:3306/sync_task_db?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8&allowPublicKeyRetrieval=true");
        props.setProperty("mysql.db.user", "root");
        props.setProperty("mysql.db.password", "rootpassword");

        props.setProperty("h2.metadata.user", "sa");
        props.setProperty("h2.metadata.password", "");

        props.setProperty("jar.capture.path", "migration-capture/target/migration-capture-1.0.0.jar");
        props.setProperty("jar.migration-full.path", "migration-full/target/migration-full-1.0.0.jar");
        props.setProperty("jar.extract.path", "migration-extract/target/migration-extract-1.0.0.jar");
        props.setProperty("jar.increment.path", "migration-increment/target/migration-increment-1.0.0.jar");
        props.setProperty("jar.subscribe.path", "migration-subscribe/target/migration-subscribe-1.0.0.jar");

        props.setProperty("monitor.capture.interval.ms", "30000");
        props.setProperty("monitor.extract.interval.ms", "30000");
        props.setProperty("monitor.increment.interval.ms", "10000");
        props.setProperty("monitor.progress.interval.ms", "3000");

        props.setProperty("http.server.port", "8083");

        props.setProperty("retry.max.attempts", "5");
        props.setProperty("retry.initial.delay.ms", "5000");
        props.setProperty("retry.multiplier", "2.0");
        props.setProperty("retry.max.delay.ms", "300000");

        props.setProperty("circuit.breaker.failure.threshold", "5");
    }

    private void loadFromFile() {
        File externalConfig = new File(CONFIG_FILE);
        if (externalConfig.exists()) {
            try (InputStream is = new FileInputStream(externalConfig)) {
                props.load(is);
                logger.info("Loaded external config from: {}", externalConfig.getAbsolutePath());
            } catch (Exception e) {
                logger.warn("Failed to load external config from: {}", externalConfig.getAbsolutePath(), e);
            }
            return;
        }

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (is != null) {
                props.load(is);
                logger.info("Loaded classpath config from: {}", CONFIG_FILE);
            }
        } catch (Exception e) {
            logger.warn("Failed to load classpath config from: {}", CONFIG_FILE, e);
        }
    }

    private void loadFromEnv() {
        for (String key : props.stringPropertyNames()) {
            String envKey = ENV_PREFIX + key.toUpperCase().replace('.', '_').replace('-', '_');
            String envValue = System.getenv(envKey);
            if (envValue != null && !envValue.isEmpty()) {
                props.setProperty(key, envValue);
                logger.info("Override config from env: {} = {}", key, maskSensitive(key, envValue));
            }
        }
    }

    public String getKafkaBootstrapServers() {
        return props.getProperty("kafka.bootstrap.servers");
    }

    public String getKafkaConsumerGroupId() {
        return props.getProperty("kafka.consumer.group.id");
    }

    public String getKafkaTopicTaskCreated() {
        return props.getProperty("kafka.topic.task-created");
    }

    public String getKafkaTopicTaskStatus() {
        return props.getProperty("kafka.topic.task-status");
    }

    public String getMysqlDbUrl() {
        return props.getProperty("mysql.db.url");
    }

    public String getMysqlDbUser() {
        return props.getProperty("mysql.db.user");
    }

    public String getMysqlDbPassword() {
        return props.getProperty("mysql.db.password");
    }

    public String getH2MetadataUser() {
        return props.getProperty("h2.metadata.user");
    }

    public String getH2MetadataPassword() {
        return props.getProperty("h2.metadata.password");
    }

    public String getCaptureJarPath() {
        return props.getProperty("jar.capture.path");
    }

    public String getMigrationFullJarPath() {
        return props.getProperty("jar.migration-full.path");
    }

    public String getExtractJarPath() {
        return props.getProperty("jar.extract.path");
    }

    public String getIncrementJarPath() {
        return props.getProperty("jar.increment.path");
    }

    public String getSubscribeJarPath() {
        return props.getProperty("jar.subscribe.path");
    }

    public long getCaptureMonitorIntervalMs() {
        return Long.parseLong(props.getProperty("monitor.capture.interval.ms"));
    }

    public long getExtractMonitorIntervalMs() {
        return Long.parseLong(props.getProperty("monitor.extract.interval.ms"));
    }

    public long getIncrementMonitorIntervalMs() {
        return Long.parseLong(props.getProperty("monitor.increment.interval.ms"));
    }

    public long getProgressMonitorIntervalMs() {
        return Long.parseLong(props.getProperty("monitor.progress.interval.ms"));
    }

    public int getHttpServerPort() {
        return Integer.parseInt(props.getProperty("http.server.port"));
    }

    public int getRetryMaxAttempts() {
        return Integer.parseInt(props.getProperty("retry.max.attempts"));
    }

    public long getRetryInitialDelayMs() {
        return Long.parseLong(props.getProperty("retry.initial.delay.ms"));
    }

    public double getRetryMultiplier() {
        return Double.parseDouble(props.getProperty("retry.multiplier"));
    }

    public long getRetryMaxDelayMs() {
        return Long.parseLong(props.getProperty("retry.max.delay.ms"));
    }

    public int getCircuitBreakerFailureThreshold() {
        return Integer.parseInt(props.getProperty("circuit.breaker.failure.threshold"));
    }

    public long getMetricsFlushIntervalMs() {
        return Long.parseLong(props.getProperty("metrics.persistence.flush.interval.ms", "30000"));
    }

    public int getMetricsBatchSize() {
        return Integer.parseInt(props.getProperty("metrics.persistence.batch.size", "100"));
    }

    public int getMetricsRetentionDays() {
        return Integer.parseInt(props.getProperty("metrics.persistence.retention.days", "30"));
    }

    private String maskSensitive(String key, String value) {
        if (key.contains("password") || key.contains("secret")) {
            return "****";
        }
        return value;
    }
}
