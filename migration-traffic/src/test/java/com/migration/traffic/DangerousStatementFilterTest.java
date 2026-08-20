package com.migration.traffic;

import com.migration.traffic.replay.DangerousStatementFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("危险语句拦截")
class DangerousStatementFilterTest {

    private final DangerousStatementFilter blocking = new DangerousStatementFilter(false);
    private final DangerousStatementFilter permissive = new DangerousStatementFilter(true);

    @Test
    @DisplayName("会毁掉目标库/实例的语句默认拦下")
    void blocksDestructive() {
        assertNotNull(blocking.blockReason("DROP DATABASE prod"));
        assertNotNull(blocking.blockReason("drop schema prod"));
        assertNotNull(blocking.blockReason("DROP USER 'a'@'%'"));
        assertNotNull(blocking.blockReason("RENAME USER a TO b"));
        assertNotNull(blocking.blockReason("SET GLOBAL max_connections=10"));
        assertNotNull(blocking.blockReason("SET PERSIST read_only=ON"));
        assertNotNull(blocking.blockReason("SHUTDOWN"));
        assertNotNull(blocking.blockReason("RESET MASTER"));
        assertNotNull(blocking.blockReason("PURGE BINARY LOGS TO 'x'"));
        assertNotNull(blocking.blockReason("FLUSH TABLES WITH READ LOCK"));
        assertNotNull(blocking.blockReason("INSTALL PLUGIN x SONAME 'y.so'"));
        assertNotNull(blocking.blockReason("ALTER INSTANCE ROTATE INNODB MASTER KEY"));
        assertNotNull(blocking.blockReason("GRANT ALL PRIVILEGES ON *.* TO 'a'@'%'"));
    }

    @Test
    @DisplayName("普通业务语句不能被误伤 —— 误伤的代价是回放悄悄缺了一大块")
    void doesNotBlockOrdinaryStatements() {
        assertNull(blocking.blockReason("SELECT * FROM t"));
        assertNull(blocking.blockReason("INSERT INTO t VALUES (1)"));
        assertNull(blocking.blockReason("DROP TABLE t"), "删表是正常 DDL，不在黑名单");
        assertNull(blocking.blockReason("DROP INDEX i ON t"));
        assertNull(blocking.blockReason("SET autocommit=0"));
        assertNull(blocking.blockReason("SET SESSION sql_mode='STRICT_TRANS_TABLES'"));
        assertNull(blocking.blockReason("FLUSH TABLES"), "不带 WITH READ LOCK 的 FLUSH TABLES 不锁库");
        assertNull(blocking.blockReason("GRANT SELECT ON app.* TO 'a'@'%'"), "普通授权不算提权");
    }

    @Test
    @DisplayName("前导注释不能成为绕过黑名单的口子")
    void commentPrefixCannotBypass() {
        assertNotNull(blocking.blockReason("/* hint */ DROP DATABASE prod"));
        assertNotNull(blocking.blockReason("-- note\nSHUTDOWN"));
    }

    @Test
    @DisplayName("显式放行开关打开后不再拦，但判定本身仍然成立")
    void allowDangerousOptsOut() {
        assertNull(permissive.blockReason("DROP DATABASE prod"));
        assertNotNull(DangerousStatementFilter.match("DROP DATABASE prod"),
                "开关只影响是否拦截，不影响\"这条语句危险\"这个事实");
    }

    @Test
    @DisplayName("空值不炸")
    void nullSafe() {
        assertNull(blocking.blockReason(null));
        assertNull(blocking.blockReason(""));
        assertNull(blocking.blockReason("   "));
    }
}
