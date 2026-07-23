package com.migration.account;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AccountSyncSupport} 单元测试：账号语句识别、系统账号判定、超级权限判定，
 * 以及"是否同步超级权限"的 GRANT 过滤（全局降权 / 库表级保留 / ALL PRIVILEGES 跳过）。
 */
@DisplayName("AccountSyncSupport 账号同步共享逻辑")
class AccountSyncSupportTest {

    @Test
    @DisplayName("识别各类账号管理语句")
    void detectAccountStatements() {
        assertTrue(AccountSyncSupport.isAccountStatement("CREATE USER 'a'@'%' IDENTIFIED BY 'x'"));
        assertTrue(AccountSyncSupport.isAccountStatement("  create user 'a'@'%'"));
        assertTrue(AccountSyncSupport.isAccountStatement("ALTER USER 'a'@'%' IDENTIFIED BY 'y'"));
        assertTrue(AccountSyncSupport.isAccountStatement("DROP USER 'a'@'%'"));
        assertTrue(AccountSyncSupport.isAccountStatement("RENAME USER 'a'@'%' TO 'b'@'%'"));
        assertTrue(AccountSyncSupport.isAccountStatement("GRANT SELECT ON db.* TO 'a'@'%'"));
        assertTrue(AccountSyncSupport.isAccountStatement("REVOKE SELECT ON db.* FROM 'a'@'%'"));
        assertTrue(AccountSyncSupport.isAccountStatement("SET PASSWORD FOR 'a'@'%' = 'x'"));
        assertTrue(AccountSyncSupport.isAccountStatement("FLUSH PRIVILEGES"));
        assertTrue(AccountSyncSupport.isAccountStatement("/*!80000 CREATE USER */ CREATE USER 'a'@'%'"));

        // 非账号语句
        assertFalse(AccountSyncSupport.isAccountStatement("CREATE TABLE t (id INT)"));
        assertFalse(AccountSyncSupport.isAccountStatement("INSERT INTO t VALUES (1)"));
        assertFalse(AccountSyncSupport.isAccountStatement("SET NAMES utf8"));
        assertFalse(AccountSyncSupport.isAccountStatement("SET TIMESTAMP=123"));
        assertFalse(AccountSyncSupport.isAccountStatement("SELECT * FROM users"));
    }

    @Test
    @DisplayName("系统账号识别")
    void systemUsers() {
        assertTrue(AccountSyncSupport.isSystemUser("root"));
        assertTrue(AccountSyncSupport.isSystemUser("mysql.sys"));
        assertTrue(AccountSyncSupport.isSystemUser("mysql.session"));
        assertTrue(AccountSyncSupport.isSystemUser("MYSQL.INFOSCHEMA"));
        assertFalse(AccountSyncSupport.isSystemUser("appuser"));
    }

    @Test
    @DisplayName("超级权限判定：静态清单 + 动态 *_ADMIN")
    void superPrivileges() {
        assertTrue(AccountSyncSupport.isSuperPrivilege("SUPER"));
        assertTrue(AccountSyncSupport.isSuperPrivilege("file"));
        assertTrue(AccountSyncSupport.isSuperPrivilege("CREATE USER"));
        assertTrue(AccountSyncSupport.isSuperPrivilege("REPLICATION SLAVE"));
        assertTrue(AccountSyncSupport.isSuperPrivilege("GRANT OPTION"));
        assertTrue(AccountSyncSupport.isSuperPrivilege("SYSTEM_VARIABLES_ADMIN"));
        assertTrue(AccountSyncSupport.isSuperPrivilege("BINLOG_ADMIN"));
        assertTrue(AccountSyncSupport.isSuperPrivilege("SYSTEM_USER"));

        assertFalse(AccountSyncSupport.isSuperPrivilege("SELECT"));
        assertFalse(AccountSyncSupport.isSuperPrivilege("INSERT"));
        assertFalse(AccountSyncSupport.isSuperPrivilege("USAGE"));
    }

    @Test
    @DisplayName("提取账号语句涉及的用户名")
    void extractUsernames() {
        assertEquals(Arrays.asList("a"),
                AccountSyncSupport.extractTargetUsernames("GRANT SELECT ON db.* TO 'a'@'%'"));
        assertEquals(Arrays.asList("a", "b"),
                AccountSyncSupport.extractTargetUsernames("RENAME USER 'a'@'%' TO 'b'@'localhost'"));
        assertEquals(Arrays.asList("appuser"),
                AccountSyncSupport.extractTargetUsernames("ALTER USER `appuser`@`10.0.0.1` IDENTIFIED BY 'x'"));
        assertTrue(AccountSyncSupport.extractTargetUsernames("FLUSH PRIVILEGES").isEmpty());
    }

    @Test
    @DisplayName("受保护账号触及判定")
    void protectedUsers() {
        assertTrue(AccountSyncSupport.touchesProtectedUser("ALTER USER 'root'@'localhost' IDENTIFIED BY 'x'", null));
        assertTrue(AccountSyncSupport.touchesProtectedUser("DROP USER 'syncuser'@'%'",
                new HashSet<>(Arrays.asList("syncuser"))));
        assertFalse(AccountSyncSupport.touchesProtectedUser("CREATE USER 'appuser'@'%'",
                new HashSet<>(Arrays.asList("syncuser"))));
    }

    @Test
    @DisplayName("同步超级权限=true 时 GRANT 原样返回")
    void grantPassthroughWhenSuperEnabled() {
        String g = "GRANT ALL PRIVILEGES ON *.* TO 'a'@'%' WITH GRANT OPTION";
        assertEquals(g, AccountSyncSupport.filterGrantSuper(g, true));
    }

    @Test
    @DisplayName("库/表级 GRANT 不受超级权限过滤影响")
    void dbScopedGrantsKept() {
        String g1 = "GRANT SELECT, INSERT ON db1.* TO 'a'@'%'";
        assertEquals(g1, AccountSyncSupport.filterGrantSuper(g1, false));
        String g2 = "GRANT ALL PRIVILEGES ON db1.t1 TO 'a'@'%'";
        assertEquals(g2, AccountSyncSupport.filterGrantSuper(g2, false));
    }

    @Test
    @DisplayName("全局 GRANT ALL PRIVILEGES 在不同步超级权限时整条跳过")
    void globalAllPrivilegesSkipped() {
        assertNull(AccountSyncSupport.filterGrantSuper(
                "GRANT ALL PRIVILEGES ON *.* TO 'a'@'%' WITH GRANT OPTION", false));
        assertNull(AccountSyncSupport.filterGrantSuper(
                "GRANT ALL ON *.* TO 'a'@'%'", false));
    }

    @Test
    @DisplayName("全局 GRANT 剔除超级权限、去掉 WITH GRANT OPTION，保留普通权限")
    void globalGrantDowngraded() {
        String result = AccountSyncSupport.filterGrantSuper(
                "GRANT SELECT, INSERT, SUPER, RELOAD ON *.* TO 'a'@'%' WITH GRANT OPTION", false);
        assertEquals("GRANT SELECT, INSERT ON *.* TO 'a'@'%'", result);
    }

    @Test
    @DisplayName("全局 GRANT 仅含超级权限时整条跳过")
    void globalGrantOnlySuperSkipped() {
        assertNull(AccountSyncSupport.filterGrantSuper(
                "GRANT SUPER, RELOAD ON *.* TO 'a'@'%'", false));
    }

    @Test
    @DisplayName("全局 GRANT USAGE 保留（无权限占位）")
    void globalUsageKept() {
        String result = AccountSyncSupport.filterGrantSuper("GRANT USAGE ON *.* TO 'a'@'%'", false);
        assertEquals("GRANT USAGE ON *.* TO 'a'@'%'", result);
    }

    @Test
    @DisplayName("角色授予（无 ON 子句）原样返回")
    void roleGrantKept() {
        String g = "GRANT 'role1'@'%' TO 'a'@'%'";
        assertEquals(g, AccountSyncSupport.filterGrantSuper(g, false));
    }
}
