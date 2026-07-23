package com.migration.increment;

import com.migration.account.AccountSyncSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * 增量账号同步（仅 mysql→mysql）：把源库 binlog 里的账号管理语句（QUERY 事件）
 * 直接应用到目标库，覆盖增量期新建账号、改口令、改权限、删账号等。
 *
 * <p>账号语句在 binlog 中恒为 statement 形式（CREATE USER 带口令哈希），且与具体库无关，
 * 因此不能走通用 DDL 链路（会被库/表范围过滤误判，且会被 DdlIdentifierRewriter 误改库名）。
 * 由 {@link THLToSqlConverter} 在通用 DDL 分派之前拦截交给本服务处理：
 * <ul>
 *   <li>账号同步未开启：吞掉账号语句（不同步账号，与开启前行为一致）。</li>
 *   <li>受保护账号（系统账号 root/mysql.*、目标连接账号）：跳过，避免破坏目标实例自身账号。</li>
 *   <li>GRANT：按"是否同步超级权限"用 {@link AccountSyncSupport#filterGrantSuper} 过滤后应用。</li>
 *   <li>其它账号语句（CREATE/ALTER/DROP/RENAME USER、SET PASSWORD、REVOKE 等）：原样应用。</li>
 * </ul>
 * 应用在转换主线程串行完成（账号语句为 QUERY 事件，走 barrier），与 DDL 应用同一目标连接。
 */
public class AccountSyncService {
    private static final Logger logger = LoggerFactory.getLogger(AccountSyncService.class);

    private final boolean activePair;
    private final boolean enabled;
    private final boolean syncSuper;
    private final Connection targetConnection;
    private final Set<String> skipUsers = new HashSet<>();

    public AccountSyncService(Properties props, Connection targetConnection) {
        this.targetConnection = targetConnection;
        boolean sourceMysql = "mysql".equalsIgnoreCase(props.getProperty("source.db.type", "mysql"));
        boolean targetMysql = "mysql".equalsIgnoreCase(props.getProperty("target.db.type", "mysql"));
        this.activePair = sourceMysql && targetMysql;
        this.enabled = Boolean.parseBoolean(props.getProperty("sync.account.enabled", "false"));
        this.syncSuper = Boolean.parseBoolean(props.getProperty("sync.account.super", "false"));
        // 目标连接账号跳过，避免增量期 DROP/ALTER 改坏同步进程自身连接
        String targetUser = props.getProperty("target.db.username", props.getProperty("target.user", ""));
        if (targetUser != null && !targetUser.isEmpty()) {
            skipUsers.add(targetUser.toLowerCase());
        }
        if (activePair && enabled) {
            logger.info("增量账号同步已启用（含超级账号权限={}）", syncSuper);
        }
    }

    /**
     * 尝试把一条 SQL 作为账号语句处理。
     *
     * @return true 表示这是账号语句且已由本服务处理（已应用或有意跳过），调用方不应再走通用 DDL 路径；
     *         false 表示不是账号语句、或非 mysql→mysql（交回原有链路处理）。
     */
    public boolean handle(String sql) {
        if (!AccountSyncSupport.isAccountStatement(sql)) {
            return false;
        }
        if (!activePair) {
            // 非 mysql→mysql：账号同步不适用，交回原有链路（保持既有行为）
            return false;
        }
        if (!enabled) {
            // 未开启账号同步：不把账号语句同步到目标（吞掉，避免误落库）
            logger.debug("账号同步未开启，跳过账号语句: {}", truncate(sql));
            return true;
        }
        if (AccountSyncSupport.touchesProtectedUser(sql, skipUsers)) {
            logger.info("增量账号同步：语句涉及受保护账号（系统账号/目标连接账号），跳过: {}", truncate(sql));
            return true;
        }

        String toExec;
        if (AccountSyncSupport.isGrantStatement(sql)) {
            toExec = AccountSyncSupport.filterGrantSuper(sql, syncSuper);
            if (toExec == null) {
                logger.info("增量账号同步：GRANT 含超级权限且未开启超级权限同步，整条跳过: {}", truncate(sql));
                return true;
            }
        } else {
            toExec = stripTrailingSemicolon(sql);
        }

        applyToTarget(toExec);
        return true;
    }

    private void applyToTarget(String sql) {
        if (targetConnection == null) {
            logger.error("增量账号同步：目标连接为空，无法应用: {}", truncate(sql));
            return;
        }
        try (Statement stmt = targetConnection.createStatement()) {
            stmt.execute(sql);
            // 账号 DDL 在 MySQL 隐式提交；自动提交关闭时补一次提交，确保落库
            if (!targetConnection.getAutoCommit()) {
                targetConnection.commit();
            }
            logger.info("增量账号同步：已应用账号语句: {}", truncate(sql));
        } catch (SQLException e) {
            if (isIdempotentAccountError(e.getMessage())) {
                logger.warn("增量账号同步：幂等错误，跳过: {} | error={}", truncate(sql), e.getMessage());
            } else {
                logger.error("增量账号同步：账号语句应用失败: {} | error={}", truncate(sql), e.getMessage());
            }
        }
    }

    /** 账号语句的幂等错误（重放/续传时常见）：账号已存在、账号不存在、授权不存在等，可安全跳过。 */
    private boolean isIdempotentAccountError(String message) {
        if (message == null) return false;
        String lower = message.toLowerCase();
        return lower.contains("operation create user failed")
                || lower.contains("operation drop user failed")
                || lower.contains("operation alter user failed")
                || lower.contains("already exists")
                || lower.contains("there is no such grant")
                || lower.contains("can't drop")
                || lower.contains("unknown user")
                || lower.contains("does not exist");
    }

    private static String stripTrailingSemicolon(String sql) {
        String s = sql.trim();
        while (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1).trim();
        }
        return s;
    }

    private static String truncate(String sql) {
        if (sql == null) return "null";
        return sql.length() > 200 ? sql.substring(0, 200) + "..." : sql;
    }
}
