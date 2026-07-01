package com.migration.agent.spring;

import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.ConfigService;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.TaskStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring 配置类，集中定义 Agent 核心组件 Bean。
 *
 * <p>通过 Spring DI 管理以下组件：
 * <ul>
 *   <li>{@link AgentConfig} - 配置读取（agent.properties + 环境变量）</li>
 *   <li>{@link KafkaProducerService} - Kafka 状态上报生产者</li>
 *   <li>{@link TaskStateService} - H2 任务状态持久化</li>
 *   <li>{@link ConfigService} - 任务配置文件管理</li>
 * </ul>
 *
 * <p>使用 {@code @Bean} 方式注册，保持原有类的构造逻辑不变，
 * 便于在不修改原类源码的前提下引入 DI。
 */
@Configuration
public class AgentSpringConfig {
    private static final Logger logger = LoggerFactory.getLogger(AgentSpringConfig.class);

    @Bean
    public AgentConfig agentConfig() {
        logger.info("Initializing AgentConfig bean");
        return new AgentConfig();
    }

    @Bean
    public KafkaProducerService kafkaProducerService(AgentConfig agentConfig) {
        logger.info("Initializing KafkaProducerService bean with bootstrap servers: {}",
                agentConfig.getKafkaBootstrapServers());
        return new KafkaProducerService(agentConfig.getKafkaBootstrapServers());
    }

    @Bean
    public TaskStateService taskStateService() {
        logger.info("Initializing TaskStateService bean");
        return new TaskStateService();
    }

    @Bean
    public ConfigService configService() {
        logger.info("Initializing ConfigService bean");
        return new ConfigService();
    }
}
