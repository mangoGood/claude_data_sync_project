package com.synctask.entity;

import java.util.Set;

/**
 * 角色常量与层级。
 *
 * <p>本平台此前只有 {@code anyRequest().authenticated()} 一条授权规则——任何账号
 * 都等价于超管：可读写全部同步任务、全部数据库连接凭证、全部 TLS 证书、全部审计日志。
 * 这里补上三级模型，并**沿用库里已有的取值**（V1 的 {@code role} 列注释就是
 * "USER/ADMIN"），因此存量数据不需要迁移：
 *
 * <ul>
 *   <li>{@link #ADMIN}——全权。用户管理、证书私钥、连接凭证、排障包只有它能碰。</li>
 *   <li>{@link #USER}——操作员。建任务、跑任务、看指标；碰不到凭证库与用户表。</li>
 *   <li>{@link #VIEWER}——只读。本次新增，给审计/值班这类"要看不要动"的角色。</li>
 * </ul>
 *
 * <p>Spring Security 侧的权限串是 {@code ROLE_<name>}（见 {@code UserPrincipal#create}），
 * 这里只存裸名，拼接由框架完成。
 */
public final class Role {

    public static final String ADMIN = "ADMIN";
    public static final String USER = "USER";
    public static final String VIEWER = "VIEWER";

    private static final Set<String> ALL = Set.of(ADMIN, USER, VIEWER);

    private Role() {
    }

    /** 是否是本系统认识的角色。注册/改角色时用它挡住拼写错误与越权自定义值。 */
    public static boolean isValid(String role) {
        return role != null && ALL.contains(role.trim().toUpperCase(java.util.Locale.ROOT));
    }

    /** 归一化：去空白 + 大写。非法值抛，不做"宽容降级"——宽容的结果是权限比预期大。 */
    public static String normalize(String role) {
        if (!isValid(role)) {
            throw new IllegalArgumentException(
                    "非法角色: '" + role + "'。可选值: ADMIN / USER / VIEWER");
        }
        return role.trim().toUpperCase(java.util.Locale.ROOT);
    }
}
