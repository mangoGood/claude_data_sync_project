package com.migration.traffic;

import com.migration.traffic.capture.SessionSchemaTracker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("会话默认库跟踪")
class SessionSchemaTrackerTest {

    @Test
    @DisplayName("无默认库的 Connect 行不能被解析成 using Socket")
    void connectWithoutSchema() {
        // 实测格式：无默认库时 " on " 与 " using " 之间是空的（两个空格挨着）
        assertNull(SessionSchemaTracker.parseConnectSchema("root@localhost on  using Socket"));
        assertNull(SessionSchemaTracker.parseConnectSchema("app@10.0.0.5 on  using SSL/TLS"));
    }

    @Test
    @DisplayName("带默认库的 Connect 行")
    void connectWithSchema() {
        assertEquals("testdb",
                SessionSchemaTracker.parseConnectSchema("root@localhost on testdb using TCP/IP"));
        assertEquals("sync_task_db",
                SessionSchemaTracker.parseConnectSchema("root@127.0.0.1 on sync_task_db using SSL/TLS"));
    }

    @Test
    @DisplayName("Init DB 与 USE 都要认 —— CLI 的 USE 只产生 Init DB 行，不产生 Query 行")
    void initDbAndUseBothTracked() {
        SessionSchemaTracker t = new SessionSchemaTracker(100);
        t.onConnect(1L, "root@localhost on  using Socket");
        assertNull(t.schemaOf(1L));

        t.onInitDb(1L, "db_a");
        assertEquals("db_a", t.schemaOf(1L));

        t.onQuery(1L, "USE `db_b`");
        assertEquals("db_b", t.schemaOf(1L));

        t.onQuery(1L, "SELECT 1");
        assertEquals("db_b", t.schemaOf(1L));

        t.onQuit(1L);
        assertNull(t.schemaOf(1L));
    }

    @Test
    @DisplayName("PROCESSLIST 播种：连接池里的连接在捕获开始前就建好了")
    void seedFromSnapshot() {
        SessionSchemaTracker t = new SessionSchemaTracker(100);
        t.seed(Map.of(42L, "pooled_db"));
        assertEquals("pooled_db", t.schemaOf(42L));
    }

    @Test
    @DisplayName("会话数上限：短连接风暴不能把映射表撑爆")
    void evictsBeyondCap() {
        SessionSchemaTracker t = new SessionSchemaTracker(64);
        for (long i = 0; i < 500; i++) {
            t.onInitDb(i, "db" + i);
        }
        assertEquals(64, t.activeSessions());
        assertEquals("db499", t.schemaOf(499L));
    }

    @Test
    @DisplayName("user_host 归一：已认证会话与 Connect 行是两种写法")
    void normalizeUserHost() {
        assertEquals("root@localhost",
                SessionSchemaTracker.normalizeUserHost("root[root] @ localhost []"));
        // Connect 行用户名前缀是空的，早先会记成 [root]@...，同一会话两种账号写法
        assertEquals("root@127.0.0.1",
                SessionSchemaTracker.normalizeUserHost("[root] @  [127.0.0.1]"));
        assertEquals("app@10.0.0.5",
                SessionSchemaTracker.normalizeUserHost("app[app] @  [10.0.0.5]"));
    }
}
