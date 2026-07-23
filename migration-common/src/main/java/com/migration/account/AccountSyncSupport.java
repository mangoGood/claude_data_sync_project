package com.migration.account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 账号同步（MySQL → MySQL）共享逻辑：账号管理语句识别、系统账号名单、超级权限判定与 GRANT 过滤。
 *
 * <p>全量（{@link MySqlAccountReplicator}）与增量（migration-increment 的 AccountSyncService）
 * 共用这一份纯逻辑，保证两条链路对"哪些是账号语句、哪些算超级权限、如何降权 GRANT"的口径一致。
 *
 * <p>设计要点：
 * <ul>
 *   <li>账号管理语句（CREATE/ALTER/DROP/RENAME USER、GRANT、REVOKE、SET PASSWORD、
 *       CREATE/DROP ROLE、FLUSH PRIVILEGES）在 MySQL 里恒以 statement 形式写 binlog，
 *       CREATE USER 会带口令哈希（IDENTIFIED WITH ... AS '...'），因此账号可无明文迁移。</li>
 *   <li>超级/管理权限只在全局作用域 {@code *.*} 才有意义（SUPER/FILE/PROCESS…只能授在 *.*），
 *       所以"是否同步超级权限"只需在全局 GRANT 上做过滤，库/表级 GRANT 一律保留。</li>
 *   <li>系统账号（root、mysql.*）与目标连接账号永不同步，避免误改目标实例自身的管理账号
 *       或同步进程赖以连接的账号。</li>
 * </ul>
 */
public final class AccountSyncSupport {
    private static final Logger logger = LoggerFactory.getLogger(AccountSyncSupport.class);

    private AccountSyncSupport() {}

    /** 永不同步的 MySQL 内置系统账号（用户名小写）。 */
    public static final Set<String> SYSTEM_USERS = new LinkedHashSet<>(Arrays.asList(
            "root", "mysql.sys", "mysql.session", "mysql.infoschema"));

    /** 静态超级/管理权限（仅在全局 *.* 作用域出现）。动态 *_ADMIN 与 SYSTEM_USER 另按规则判定。 */
    private static final Set<String> STATIC_SUPER_PRIVILEGES = new LinkedHashSet<>(Arrays.asList(
            "SUPER", "FILE", "PROCESS", "RELOAD", "SHUTDOWN", "CREATE USER",
            "REPLICATION SLAVE", "REPLICATION CLIENT", "GRANT OPTION",
            "CREATE TABLESPACE", "SHOW DATABASES"));

    /** 账号管理语句前缀（去掉前导注释/空白后大写匹配）。 */
    private static final Pattern ACCOUNT_STMT_PATTERN = Pattern.compile(
            "^(?:CREATE\\s+USER|ALTER\\s+USER|DROP\\s+USER|RENAME\\s+USER|GRANT\\b|REVOKE\\b|"
            + "SET\\s+PASSWORD|CREATE\\s+ROLE|DROP\\s+ROLE|SET\\s+DEFAULT\\s+ROLE|FLUSH\\s+PRIVILEGES)",
            Pattern.CASE_INSENSITIVE);

    /** 前导版本注释 /*!50701 ... *&#47; 或普通块注释，识别语句类型前先剥掉。 */
    private static final Pattern LEADING_COMMENT = Pattern.compile("^\\s*/\\*.*?\\*/\\s*", Pattern.DOTALL);

    /** GRANT 拆分：privs ON priv_level TO rest（非贪婪，命中第一个 ON / TO）。 */
    private static final Pattern GRANT_PATTERN = Pattern.compile(
            "^\\s*GRANT\\s+(.+?)\\s+ON\\s+(.+?)\\s+TO\\s+(.+)$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** 账号名（'user'@'host' / `user`@`host` / user@host）里 @ 之前的用户名部分。 */
    private static final Pattern ACCOUNT_NAME_PATTERN = Pattern.compile(
            "(?:'((?:[^']|'')*)'|`((?:[^`]|``)*)`|\"((?:[^\"]|\"\")*)\"|([A-Za-z0-9_.$\\-]+))\\s*@");

    /** 去掉语句尾部的分号（若有），供重新拼接。 */
    private static String stripTrailingSemicolon(String sql) {
        String s = sql.trim();
        while (s.endsWith(";")) {
            s = s.substring(0, s.length() - 1).trim();
        }
        return s;
    }

    private static String stripLeadingComment(String sql) {
        Matcher m = LEADING_COMMENT.matcher(sql);
        if (m.find()) {
            return sql.substring(m.end());
        }
        return sql.trim();
    }

    /** 是否为账号管理语句（CREATE/ALTER/DROP/RENAME USER、GRANT、REVOKE、SET PASSWORD、角色、FLUSH PRIVILEGES）。 */
    public static boolean isAccountStatement(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        return ACCOUNT_STMT_PATTERN.matcher(stripLeadingComment(sql)).find();
    }

    /** 是否为 GRANT 语句（用于超级权限过滤路由）。 */
    public static boolean isGrantStatement(String sql) {
        if (sql == null) return false;
        return stripLeadingComment(sql).regionMatches(true, 0, "GRANT", 0, 5);
    }

    /** 是否为内置系统账号（root、mysql.*）——大小写不敏感。 */
    public static boolean isSystemUser(String username) {
        return username != null && SYSTEM_USERS.contains(username.trim().toLowerCase());
    }

    /**
     * 单条权限是否为超级/管理权限。静态清单 + 动态 *_ADMIN + SYSTEM_USER，
     * 覆盖 MySQL 8 所有动态管理权限而无需逐一枚举。
     */
    public static boolean isSuperPrivilege(String priv) {
        if (priv == null) return false;
        String p = priv.trim().toUpperCase();
        if (p.isEmpty()) return false;
        if (STATIC_SUPER_PRIVILEGES.contains(p)) return true;
        if (p.endsWith("_ADMIN")) return true;      // 动态管理权限：*_ADMIN
        return p.equals("SYSTEM_USER") || p.equals("ROLE_ADMIN");
    }

    /**
     * 提取账号语句里涉及的所有用户名（不含主机名），用于系统账号/目标账号跳过判定。
     * 解析不出返回空列表（FLUSH PRIVILEGES 等无账号名的语句）。
     */
    public static List<String> extractTargetUsernames(String sql) {
        List<String> names = new ArrayList<>();
        if (sql == null) return names;
        Matcher m = ACCOUNT_NAME_PATTERN.matcher(sql);
        while (m.find()) {
            String name = m.group(1) != null ? m.group(1).replace("''", "'")
                    : m.group(2) != null ? m.group(2).replace("``", "`")
                    : m.group(3) != null ? m.group(3).replace("\"\"", "\"")
                    : m.group(4);
            if (name != null && !name.isEmpty()) {
                names.add(name);
            }
        }
        return names;
    }

    /**
     * 账号语句是否触及受保护账号（系统账号或额外跳过集，如目标连接账号）。命中则整条语句跳过。
     * @param extraSkip 额外跳过的用户名（小写），可为 null
     */
    public static boolean touchesProtectedUser(String sql, Set<String> extraSkip) {
        for (String name : extractTargetUsernames(sql)) {
            String lower = name.toLowerCase();
            if (isSystemUser(name)) {
                return true;
            }
            if (extraSkip != null && extraSkip.contains(lower)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 按"是否同步超级权限"过滤 GRANT 语句。
     *
     * <ul>
     *   <li>{@code syncSuper=true} 或非 GRANT 语句：原样返回。</li>
     *   <li>库/表级 GRANT（priv_level 非 {@code *.*}）：原样返回（超级权限只在全局作用域）。</li>
     *   <li>全局 GRANT ALL [PRIVILEGES] ON *.*：含超级权限且无法干净减除，整条跳过（返回 null）。</li>
     *   <li>其它全局 GRANT：剔除超级权限项、去掉 WITH GRANT OPTION；剩余为空则跳过（返回 null），
     *       否则返回降权后的 GRANT。</li>
     * </ul>
     *
     * @return 过滤后的 GRANT（不含结尾分号），或 null 表示应整条跳过
     */
    public static String filterGrantSuper(String sql, boolean syncSuper) {
        if (sql == null || syncSuper || !isGrantStatement(sql)) {
            return sql == null ? null : stripTrailingSemicolon(sql);
        }
        String body = stripTrailingSemicolon(stripLeadingComment(sql));
        Matcher m = GRANT_PATTERN.matcher(body);
        if (!m.find()) {
            // 无 ON 子句：角色授予（GRANT role TO user）等，超级权限过滤不适用，原样保留
            return body;
        }
        String privs = m.group(1).trim();
        String privLevel = m.group(2).trim();
        String rest = m.group(3).trim();

        String normLevel = privLevel.replace("`", "").replace("\"", "").replace(" ", "");
        boolean global = normLevel.equals("*.*");
        if (!global) {
            return body;   // 库/表级 GRANT 一律保留
        }

        // 全局 ALL PRIVILEGES：含超级权限，整条跳过
        String privsUpper = privs.toUpperCase().trim();
        if (privsUpper.equals("ALL") || privsUpper.equals("ALL PRIVILEGES")) {
            logger.warn("账号同步（不含超级权限）：跳过全局 GRANT ALL PRIVILEGES ON *.*，账号本身与库/表级授权仍会同步。原语句: {}",
                    truncate(body));
            return null;
        }

        List<String> kept = new ArrayList<>();
        List<String> dropped = new ArrayList<>();
        for (String priv : splitPrivileges(privs)) {
            String p = priv.trim();
            if (p.isEmpty()) continue;
            if (isSuperPrivilege(p)) {
                dropped.add(p);
            } else {
                kept.add(p);
            }
        }
        if (kept.isEmpty()) {
            logger.warn("账号同步（不含超级权限）：全局 GRANT 仅含超级权限，整条跳过: {}", truncate(body));
            return null;
        }

        // 去掉 WITH GRANT OPTION（GRANT OPTION 属超级权限）
        String toClause = rest.replaceAll("(?i)\\s+WITH\\s+GRANT\\s+OPTION", "");
        toClause = stripTrailingSemicolon(toClause);
        String result = "GRANT " + String.join(", ", kept) + " ON *.* TO " + toClause;
        if (!dropped.isEmpty()) {
            logger.info("账号同步（不含超级权限）：全局 GRANT 剔除超级权限 {}，保留 {}", dropped, kept);
        }
        return result;
    }

    /** 逗号拆分权限清单（顶层逗号，跳过括号内——列级权限 SELECT(col1,col2) 的逗号不拆）。 */
    private static List<String> splitPrivileges(String privs) {
        List<String> result = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < privs.length(); i++) {
            char c = privs.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                result.add(privs.substring(start, i));
                start = i + 1;
            }
        }
        result.add(privs.substring(start));
        return result;
    }

    private static String truncate(String s) {
        if (s == null) return "null";
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }
}
