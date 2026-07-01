package com.migration.agent.spring;

import com.migration.agent.service.AgentConfig;
import com.migration.agent.service.ConfigService;
import com.migration.agent.service.KafkaProducerService;
import com.migration.agent.service.TaskStateService;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证 Spring DI 容器能正确装配 Agent 核心组件。
 */
class AgentSpringConfigTest {

    @Test
    void shouldLoadAllCoreBeans() {
        try (AnnotationConfigApplicationContext ctx =
                     new AnnotationConfigApplicationContext(AgentSpringConfig.class)) {

            AgentConfig agentConfig = ctx.getBean(AgentConfig.class);
            assertNotNull(agentConfig, "AgentConfig bean should be present");
            assertNotNull(agentConfig.getKafkaBootstrapServers(), "Kafka bootstrap servers should be configured");

            ConfigService configService = ctx.getBean(ConfigService.class);
            assertNotNull(configService, "ConfigService bean should be present");

            TaskStateService taskStateService = ctx.getBean(TaskStateService.class);
            assertNotNull(taskStateService, "TaskStateService bean should be present");

            KafkaProducerService kafkaProducer = ctx.getBean(KafkaProducerService.class);
            assertNotNull(kafkaProducer, "KafkaProducerService bean should be present");
        }
    }

    @Test
    void beansShouldBeSingletonByDefault() {
        try (AnnotationConfigApplicationContext ctx =
                     new AnnotationConfigApplicationContext(AgentSpringConfig.class)) {

            AgentConfig first = ctx.getBean(AgentConfig.class);
            AgentConfig second = ctx.getBean(AgentConfig.class);
            assertSame(first, second, "AgentConfig should be singleton");
        }
    }
}
