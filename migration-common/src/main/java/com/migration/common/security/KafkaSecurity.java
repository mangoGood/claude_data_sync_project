package com.migration.common.security;

import java.util.Map;
import java.util.Properties;

/**
 * Kafka 连接的传输层加密 / 认证参数。
 *
 * <p>平台里有<b>四个</b>地方创建 Kafka 客户端：后端的生产者与消费者（控制面：任务下发、状态回传）、
 * agent 的消费者（控制面）、订阅引擎的生产者（数据面：业务变更本身）。
 * 它们此前一律裸 PLAINTEXT——控制面走的是任务配置（含库名表名），
 * 数据面走的就是<b>业务数据本身</b>，而这两条都在同一套网络里跑。
 *
 * <p>参数一律从环境变量取而不是每个任务配置：一个部署环境要么整体接的是加密的 Kafka、
 * 要么不是，按任务配会出现"同一个集群有的连接加密有的不加密"这种没人能推理的状态。
 *
 * <p>默认全空 = 完全不写任何安全参数 = 与之前行为逐字节相同。
 */
public final class KafkaSecurity {

    private KafkaSecurity() {
    }

    /**
     * 把安全参数灌进 Kafka 客户端配置。
     *
     * @param sink 生产者/消费者的配置 map（{@code Properties} 也是 {@code Map}）
     */
    @SuppressWarnings("unchecked")
    public static void apply(Map<String, Object> sink) {
        put(sink, "security.protocol", env("KAFKA_SECURITY_PROTOCOL"));
        put(sink, "ssl.truststore.location", env("KAFKA_SSL_TRUSTSTORE_LOCATION"));
        put(sink, "ssl.truststore.password", env("KAFKA_SSL_TRUSTSTORE_PASSWORD"));
        put(sink, "ssl.keystore.location", env("KAFKA_SSL_KEYSTORE_LOCATION"));
        put(sink, "ssl.keystore.password", env("KAFKA_SSL_KEYSTORE_PASSWORD"));
        put(sink, "ssl.key.password", env("KAFKA_SSL_KEY_PASSWORD"));
        put(sink, "sasl.mechanism", env("KAFKA_SASL_MECHANISM"));
        put(sink, "sasl.jaas.config", env("KAFKA_SASL_JAAS_CONFIG"));
        // 关闭主机名校验是**显式**动作：把它做成一个单独的开关，
        // 而不是让人为了跑通去把 security.protocol 降级回 PLAINTEXT
        if ("false".equalsIgnoreCase(env("KAFKA_SSL_VERIFY_HOSTNAME"))) {
            sink.put("ssl.endpoint.identification.algorithm", "");
        }
    }

    /** {@link Properties} 版（agent 与订阅引擎用的是 Properties）。 */
    public static void apply(Properties props) {
        Properties tmp = new Properties();
        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<String, Object> asMap = (Map) tmp;
        apply(asMap);
        props.putAll(tmp);
    }

    /** 当前是否配了加密（供预检/日志判断用）。 */
    public static boolean enabled() {
        String p = env("KAFKA_SECURITY_PROTOCOL");
        return p != null && !p.isEmpty() && !"PLAINTEXT".equalsIgnoreCase(p);
    }

    public static String protocol() {
        String p = env("KAFKA_SECURITY_PROTOCOL");
        return (p == null || p.isEmpty()) ? "PLAINTEXT" : p;
    }

    private static String env(String key) {
        String v = System.getenv(key);
        return v == null ? null : v.trim();
    }

    private static void put(Map<String, Object> sink, String key, String value) {
        if (value != null && !value.isEmpty()) {
            sink.put(key, value);
        }
    }
}
