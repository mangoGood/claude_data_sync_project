package com.synctask.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * "URL 只放标识符、参数走 Properties" 的判据。
 *
 * <p>两件事：URL 里<b>不能再出现 {@code ?}</b>（结构上断掉参数注入），
 * 以及<b>参数一项都不能丢</b>（改造的硬要求是行为不变——少一个
 * {@code characterEncoding} 就是一次静默的乱码事故）。
 */
class JdbcConnectionsTest {

    // ---------------- URL 不带查询串 ----------------

    @Test
    @DisplayName("MySQL / PostgreSQL 的 URL 都不含 ?、&、#")
    void urlsCarryNoQueryString() {
        for (String url : new String[]{
                JdbcConnections.mysqlUrl("127.0.0.1", 3306, "sync_task_db"),
                JdbcConnections.mysqlUrl("db.internal", 3306, null),
                JdbcConnections.postgresUrl("127.0.0.1", 5432, "appdb"),
                JdbcConnections.postgresUrl("127.0.0.1", 5432, null)}) {
            assertFalse(url.contains("?"), "URL 不应含 ?: " + url);
            assertFalse(url.contains("&"), "URL 不应含 &: " + url);
            assertFalse(url.contains("#"), "URL 不应含 #: " + url);
        }
    }

    @Test
    @DisplayName("URL 形状与驱动期望一致")
    void urlShape() {
        assertEquals("jdbc:mysql://127.0.0.1:3306/sync_task_db",
                JdbcConnections.mysqlUrl("127.0.0.1", 3306, "sync_task_db"));
        assertEquals("jdbc:mysql://127.0.0.1:3306/",
                JdbcConnections.mysqlUrl("127.0.0.1", 3306, null));
        assertEquals("jdbc:postgresql://h:5432/appdb",
                JdbcConnections.postgresUrl("h", 5432, "appdb"));
    }

    @Test
    @DisplayName("注入 payload 在 URL 构造阶段就被拒（不是等到连接才发现）")
    void urlBuilderRejectsInjection() {
        assertThrows(IllegalArgumentException.class, () ->
                JdbcConnections.mysqlUrl("h", 3306, "db?allowLoadLocalInfile=true"));
        assertThrows(IllegalArgumentException.class, () ->
                JdbcConnections.postgresUrl("h", 5432, "db?socketFactory=x"));
        assertThrows(IllegalArgumentException.class, () ->
                JdbcConnections.mysqlUrl("h/x?y=1", 3306, "db"));
    }

    // ---------------- 参数不丢 ----------------

    @Test
    @DisplayName("MySQL 参数一项不少（与改造前拼在 URL 里的那串对齐）")
    void mysqlPropsComplete() {
        Properties p = JdbcConnections.mysqlProps("root", "pw", null);
        assertEquals("root", p.getProperty("user"));
        assertEquals("pw", p.getProperty("password"));
        assertEquals("UTC", p.getProperty("serverTimezone"));
        assertEquals("utf8", p.getProperty("characterEncoding"));
        assertEquals("true", p.getProperty("allowPublicKeyRetrieval"));
        // 加密档位来自 JdbcSslOptions：默认 DISABLED → useSSL=false
        assertEquals("false", p.getProperty("useSSL"),
                "默认档位 DISABLED 必须落成 useSSL=false，与改造前逐字一致");
    }

    @Test
    @DisplayName("PostgreSQL 参数一项不少")
    void postgresPropsComplete() {
        Properties p = JdbcConnections.postgresProps("pg", "pw", null);
        assertEquals("pg", p.getProperty("user"));
        assertEquals("unspecified", p.getProperty("stringtype"));
        // 默认档位 DISABLED → sslmode=disable（不是 ssl=false，两者不等价）
        assertEquals("disable", p.getProperty("sslmode"));
    }

    @Test
    @DisplayName("SSL 参数由 JdbcSslOptions 解析而来，不是这里重新推导的")
    void sslParamsComeFromSingleSource() {
        // 直接比对：解析出来的每一项都必须能在 JdbcSslOptions 的输出里找到
        Properties parsed = JdbcConnections.parseParams(JdbcSslOptions.mysql());
        Properties actual = JdbcConnections.mysqlProps("u", "p", null);
        for (String name : parsed.stringPropertyNames()) {
            assertEquals(parsed.getProperty(name), actual.getProperty(name),
                    "SSL 参数 " + name + " 与 JdbcSslOptions 不一致");
        }
    }

    @Test
    @DisplayName("覆盖项生效，且 null 值表示删除")
    void overridesApply() {
        Properties p = JdbcConnections.mysqlProps("u", "p",
                JdbcConnections.mysqlTimeouts(15000, 15000));
        assertEquals("15000", p.getProperty("connectTimeout"));
        assertEquals("15000", p.getProperty("socketTimeout"));

        Properties q = JdbcConnections.mysqlProps("u", "p",
                java.util.Collections.singletonMap("characterEncoding", null));
        assertNull(q.getProperty("characterEncoding"), "null 覆盖应删除该项");
    }

    @Test
    @DisplayName("覆盖项同样过危险参数黑名单——换成 Properties 不等于攻击面消失")
    void overridesGoThroughBlacklist() {
        for (String bad : new String[]{
                "socketFactory", "allowLoadLocalInfile", "autoDeserialize", "queryInterceptors"}) {
            assertThrows(IllegalArgumentException.class, () ->
                            JdbcConnections.mysqlProps("u", "p", Map.of(bad, "x")),
                    "危险参数 " + bad + " 从 Properties 进来照样能生效，必须拒绝");
        }
    }

    // ---------------- 参数解析 ----------------

    @Test
    @DisplayName("参数段解析")
    void parseParams() {
        Properties p = JdbcConnections.parseParams("a=1&b=2&c=");
        assertEquals("1", p.getProperty("a"));
        assertEquals("2", p.getProperty("b"));
        assertEquals("", p.getProperty("c"));
        assertTrue(JdbcConnections.parseParams(null).isEmpty());
        assertTrue(JdbcConnections.parseParams("").isEmpty());
        // 文件路径里带冒号斜杠的值不能被切坏
        Properties q = JdbcConnections.parseParams("trustCertificateKeyStoreUrl=file:/c/ca.p12");
        assertEquals("file:/c/ca.p12", q.getProperty("trustCertificateKeyStoreUrl"));
    }
}
