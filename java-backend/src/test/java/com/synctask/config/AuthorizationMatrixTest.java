package com.synctask.config;

import com.synctask.entity.Role;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 授权矩阵判据。
 *
 * <p>改造前全系统只有一条 {@code anyRequest().authenticated()}——任何账号都等价于超管，
 * 可读写全部同步任务、全部数据库连接凭证、全部 TLS 证书、全部审计日志。
 * 这里守住三件事，都是"改回去就会静默失去防护"的那种：
 *
 * <ol>
 *   <li><b>注册不能是匿名的</b>——它是提权动作；</li>
 *   <li><b>碰私钥/审计/排障包的路径必须是 ADMIN</b>；</li>
 *   <li><b>不能有人把 permitAll 加回敏感路径</b>。</li>
 * </ol>
 *
 * <p>读配置源文件而不是起 Spring 上下文：授权规则是声明式的，
 * 起完整上下文要连元数据库，而单测不连外部库（本仓库既有约定）。
 * 这样守不住运行期行为，但守得住"规则有没有被人删掉"——
 * 而实际发生过的退化恰恰是后者。
 */
class AuthorizationMatrixTest {

    private static final Path CONFIG =
            Path.of("src/main/java/com/synctask/config/SecurityConfig.java");

    private static String source() throws IOException {
        assertTrue(Files.exists(CONFIG), "找不到 SecurityConfig: " + CONFIG.toAbsolutePath());
        return Files.readString(CONFIG);
    }

    /** 抽出某个路径模式所在那条规则的授权动作（hasRole/hasAnyRole/authenticated/permitAll）。 */
    private static String ruleFor(String source, String pathLiteral) {
        // 匹配 requestMatchers(... "<path>" ...) 之后到下一个 . 方法调用
        Pattern p = Pattern.compile(
                Pattern.quote("\"" + pathLiteral + "\"") + "[^;]{0,200}?\\.(\\w+)\\(",
                Pattern.DOTALL);
        Matcher m = p.matcher(source);
        assertTrue(m.find(), "配置里找不到路径规则: " + pathLiteral);
        return m.group(1);
    }

    @Test
    @DisplayName("注册接口必须是 ADMIN——匿名注册是整条未授权攻击链的入口")
    void registerRequiresAdmin() throws IOException {
        String s = source();
        assertEquals("hasRole", ruleFor(s, "/api/auth/register"),
                "/api/auth/register 必须 hasRole(ADMIN)");
        assertTrue(s.contains("\"/api/auth/register\").hasRole(Role.ADMIN)"),
                "/api/auth/register 的角色必须是 ADMIN");
        // 登录仍然要能匿名，否则谁也进不来
        assertEquals("permitAll", ruleFor(s, "/api/auth/login"));
    }

    @Test
    @DisplayName("整段 /api/auth/** 不能再是 permitAll")
    void authNamespaceNotWideOpen() throws IOException {
        String s = source();
        assertFalse(s.contains("\"/api/auth/**\").permitAll()"),
                "/api/auth/** 整段 permitAll 会把 register 一起放开——这正是原来的洞");
    }

    @Test
    @DisplayName("碰私钥 / 审计 / 排障包 / API 契约的路径必须是 ADMIN")
    void sensitivePathsRequireAdmin() throws IOException {
        String s = source();
        for (String path : new String[]{
                "/api/certificates/**",
                "/api/audit-logs/**",
                "/api/workflows/*/diagnostics"}) {
            assertEquals("hasRole", ruleFor(s, path), path + " 必须 hasRole");
            assertTrue(s.contains("\"" + path + "\").hasRole(Role.ADMIN)"),
                    path + " 的角色必须是 ADMIN");
        }
        assertTrue(s.contains("/v3/api-docs/**"), "API 契约路径必须显式授权");
    }

    @Test
    @DisplayName("WebSocket 与 advanced/metrics 不能是匿名的")
    void noAnonymousOnRuntimePaths() throws IOException {
        String s = source();
        assertFalse(s.contains("\"/ws/**\").permitAll()"),
                "/ws/** permitAll 等于任务状态推送对匿名开放");
        assertFalse(s.contains("\"/api/advanced/metrics\").permitAll()"),
                "/api/advanced/metrics permitAll 会泄露运行指标");
    }

    @Test
    @DisplayName("VIEWER 只出现在 GET 规则上——只读角色不能出现在写规则里")
    void viewerIsReadOnly() throws IOException {
        String s = source();
        assertTrue(s.contains("Role.VIEWER"), "VIEWER 角色必须被用上，否则三级模型名存实亡");

        // 检查**每一处** VIEWER：往前找最近的 requestMatchers，都必须带 HttpMethod.GET。
        //
        // 原来断言的是"VIEWER 全文只出现一次"——那是写这条判据时恰好只有一条 GET 规则，
        // 把当时的现状误当成了不变量。新增一条只读授权（数据治理的查询）就会误报。
        // 真正的不变量是"VIEWER 不能出现在写规则上"，逐处检查才守得住。
        int occurrences = 0;
        int from = 0;
        while (true) {
            int idx = s.indexOf("Role.VIEWER", from);
            if (idx < 0) {
                break;
            }
            occurrences++;
            int lastMatcher = s.lastIndexOf("requestMatchers(", idx);
            assertTrue(lastMatcher > 0, "第 " + occurrences + " 处 VIEWER 前找不到 requestMatchers");
            String rule = s.substring(lastMatcher, idx);
            assertTrue(rule.contains("HttpMethod.GET"),
                    "第 " + occurrences + " 处 VIEWER 不在 GET 规则上，所在规则: "
                            + rule.replaceAll("\\s+", " "));
            from = idx + 1;
        }
        assertTrue(occurrences >= 1);
    }

    @Test
    @DisplayName("401 与 403 分开返回")
    void authnAndAuthzErrorsAreDistinct() throws IOException {
        String s = source();
        assertTrue(s.contains("authenticationEntryPoint"), "缺少 401 处理");
        assertTrue(s.contains("accessDeniedHandler"), "缺少 403 处理");
        assertTrue(s.contains("response.setStatus(401)"));
        assertTrue(s.contains("response.setStatus(403)"));
    }

    @Test
    @DisplayName("角色常量：三级齐全且校验非法值")
    void roleConstants() {
        assertTrue(Role.isValid(Role.ADMIN));
        assertTrue(Role.isValid(Role.USER));
        assertTrue(Role.isValid(Role.VIEWER));
        assertTrue(Role.isValid("admin"), "大小写不敏感");
        assertFalse(Role.isValid("SUPERUSER"));
        assertFalse(Role.isValid(null));
        assertEquals("ADMIN", Role.normalize(" admin "));
        // 非法值必须抛，不能静默降级——静默降级的结果是权限比预期大
        assertThrows(IllegalArgumentException.class, () -> Role.normalize("ROOT"));
    }
}
