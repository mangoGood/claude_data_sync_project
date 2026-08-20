package com.synctask.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * 安全响应头。
 *
 * <p><b>为什么现在才加</b>：前端有 209 处 {@code innerHTML}，其中只有 5 处走了
 * {@code escapeHtml}；库名、表名、后端错误消息回显都是用户可控且直插模板串的。
 * 而 JWT 存在 {@code localStorage}——一次存储型 XSS 就是完整的账号接管。
 * 逐个改 209 处注入点回归面太大（那是 B1 的后半段，按页推进），
 * CSP 是**一行配置就能把整类攻击的收益压到最低**的那一半，先上。
 *
 * <p>CSP 的取舍：现有页面大量使用内联 {@code onclick} 与内联 {@code <style>}，
 * 直接上严格 CSP 会白屏。这里给的是**当前代码能承受的最严档位**——
 * {@code script-src 'self' 'unsafe-inline'} 挡不住内联注入，但
 * {@code default-src 'self'} + {@code connect-src} 白名单能挡住**外传**：
 * 注入进来的脚本拿到 token 也发不出去。等前端去掉内联事件后，把
 * {@code app.csp.script-src} 收紧成 {@code 'self'} 即可，无需改代码。
 */
@Component
@Order(1)
public class SecurityHeadersFilter extends OncePerRequestFilter {

    /**
     * 脚本来源。
     *
     * <p>默认带上 {@code cdn.jsdelivr.net}：页面从那里加载 SockJS / STOMP（实时任务状态推送）
     * 与 Chart.js（全部图表）。第一版 CSP 写成 {@code 'self'} 就把这三个全拦了——
     * 实测控制台三条 CSP 拦截 + {@code SockJS is not defined}，实时推送与图表全挂。
     * <b>教训：CSP 必须对着真实页面验证，光看代码看不出来。</b>
     *
     * <p><b>三个库已落到本地</b>（{@code static/vendor/}，见 {@code build-frontend.mjs}），
     * 因此默认收回了 {@code 'self'}——外部脚本源一个都不放。这同时去掉了供应链风险
     * （CDN 被投毒即等于在控制面执行任意脚本）与对公网的依赖（可离线/内网部署）。
     *
     * <p>{@code 'unsafe-inline'} 仍然留着：页面大量使用内联 {@code onclick}，
     * 去掉它会白屏。那是前端重构的事，去掉之后这里改成 {@code 'self'} 即可。
     */
    @Value("${app.csp.script-src:'self' 'unsafe-inline'}")
    private String scriptSrc;

    /** 允许页面发起请求的目标。默认只有同源；agent 直连场景由部署方按需追加。 */
    @Value("${app.csp.connect-src:'self'}")
    private String connectSrc;

    /** 样式来源：Chart.js 等库不引外部样式，但保留可配置以防将来引入。 */
    @Value("${app.csp.style-src:'self' 'unsafe-inline'}")
    private String styleSrc;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        response.setHeader("Content-Security-Policy",
                "default-src 'self'; "
                        + "script-src " + scriptSrc + "; "
                        + "style-src " + styleSrc + "; "
                        + "img-src 'self' data:; "
                        + "font-src 'self' data:; "
                        + "connect-src " + connectSrc + "; "
                        // 注入的脚本即使拿到了 token，也无法把它 POST 到外部收集点
                        + "form-action 'self'; "
                        + "frame-ancestors 'none'; "
                        + "base-uri 'self'; "
                        + "object-src 'none'");

        // 关掉 MIME 嗅探：上传的证书/排障包若被浏览器猜成 text/html 就是一个存储型 XSS
        response.setHeader("X-Content-Type-Options", "nosniff");
        // 与 frame-ancestors 双保险，覆盖不认 CSP3 的老浏览器
        response.setHeader("X-Frame-Options", "DENY");
        // 跳转到外部站点时不要把带 taskId 的完整 URL 带出去
        response.setHeader("Referrer-Policy", "no-referrer");
        response.setHeader("Permissions-Policy", "geolocation=(), microphone=(), camera=()");

        chain.doFilter(request, response);
    }
}
