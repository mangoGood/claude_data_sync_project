package com.migration.account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 全量账号同步：把源库现存账号（含口令哈希与授权）复制到目标库。
 *
 * <p>用于 mysql→mysql 全量阶段"同步表结构时把存量账号一并同步"。对每个非系统账号：
 * <ol>
 *   <li>{@code SHOW CREATE USER} 拿到含口令哈希的建账号语句（无需明文口令）；</li>
 *   <li>目标库 {@code DROP USER IF EXISTS} 后重建，保证幂等与口令一致；</li>
 *   <li>{@code SHOW GRANTS} 逐条授权，按"是否同步超级权限"用
 *       {@link AccountSyncSupport#filterGrantSuper} 过滤后应用。</li>
 * </ol>
 * 系统账号（root、mysql.*）与目标连接账号跳过，避免破坏目标实例自身的管理账号/连接账号。
 */
public final class MySqlAccountReplicator {
    private static final Logger logger = LoggerFactory.getLogger(MySqlAccountReplicator.class);

    private MySqlAccountReplicator() {}

    /**
     * 复制源库账号到目标库。
     *
     * @param source    源库连接
     * @param target    目标库连接
     * @param syncSuper 是否同步超级账号权限
     * @param extraSkipUsers 额外跳过的用户名（如目标连接账号），可为 null；内部按小写比较
     * @return 成功同步的账号数
     */
    public static int replicate(Connection source, Connection target, boolean syncSuper,
                                Set<String> extraSkipUsers) throws SQLException {
        Set<String> skip = new HashSet<>();
        if (extraSkipUsers != null) {
            for (String u : extraSkipUsers) {
                if (u != null && !u.isEmpty()) skip.add(u.toLowerCase());
            }
        }

        List<String[]> accounts = listAccounts(source);
        logger.info("全量账号同步：源库共 {} 个账号，开始同步（含超级权限={}）", accounts.size(), syncSuper);

        int synced = 0;
        for (String[] account : accounts) {
            String user = account[0];
            String host = account[1];
            if (AccountSyncSupport.isSystemUser(user) || skip.contains(user.toLowerCase())) {
                logger.info("全量账号同步：跳过受保护账号 {}@{}", user, host);
                continue;
            }
            try {
                syncOneAccount(source, target, user, host, syncSuper);
                synced++;
            } catch (SQLException e) {
                logger.warn("全量账号同步：账号 {}@{} 同步失败（跳过，不影响数据迁移）: {}", user, host, e.getMessage());
            }
        }
        logger.info("全量账号同步完成：成功同步 {} 个账号", synced);
        return synced;
    }

    /** 读取源库全部账号（user, host）。 */
    private static List<String[]> listAccounts(Connection source) throws SQLException {
        List<String[]> accounts = new ArrayList<>();
        try (Statement stmt = source.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT user, host FROM mysql.user")) {
            while (rs.next()) {
                String user = rs.getString(1);
                String host = rs.getString(2);
                if (user != null) {
                    accounts.add(new String[]{user, host == null ? "%" : host});
                }
            }
        }
        return accounts;
    }

    private static void syncOneAccount(Connection source, Connection target,
                                       String user, String host, boolean syncSuper) throws SQLException {
        String account = quoteAccount(user, host);

        // 1) 建账号语句（含口令哈希）
        String createUserSql = showCreateUser(source, account);
        if (createUserSql == null) {
            logger.warn("全量账号同步：无法获取 {} 的建账号语句，跳过", account);
            return;
        }

        try (Statement stmt = target.createStatement()) {
            // 幂等：先删后建，保证目标账号的口令/认证方式与源端一致
            execTolerant(stmt, "DROP USER IF EXISTS " + account);
            stmt.execute(createUserSql);
            logger.info("全量账号同步：已创建账号 {}", account);
        }

        // 2) 授权（按是否同步超级权限过滤）
        List<String> grants = showGrants(source, account);
        try (Statement stmt = target.createStatement()) {
            for (String grant : grants) {
                String filtered = AccountSyncSupport.filterGrantSuper(grant, syncSuper);
                if (filtered == null) {
                    continue;   // 超级权限被过滤，整条跳过
                }
                execTolerant(stmt, filtered);
            }
        }
    }

    private static String showCreateUser(Connection source, String account) throws SQLException {
        try (Statement stmt = source.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW CREATE USER " + account)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

    private static List<String> showGrants(Connection source, String account) throws SQLException {
        List<String> grants = new ArrayList<>();
        try (Statement stmt = source.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW GRANTS FOR " + account)) {
            while (rs.next()) {
                String g = rs.getString(1);
                if (g != null && !g.trim().isEmpty()) {
                    grants.add(g.trim());
                }
            }
        }
        return grants;
    }

    /** 执行 DDL，幂等错误（账号已存在/授权重复等）降级为告警不抛出。 */
    private static void execTolerant(Statement stmt, String sql) {
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            logger.warn("全量账号同步：语句执行失败（跳过）: {} | error={}",
                    sql.length() > 160 ? sql.substring(0, 160) + "..." : sql, e.getMessage());
        }
    }

    /** 拼 'user'@'host'，单引号转义（防注入并容纳含特殊字符的账号）。 */
    private static String quoteAccount(String user, String host) {
        return "'" + user.replace("'", "''") + "'@'" + host.replace("'", "''") + "'";
    }
}
