package com.synctask.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.IOException;

public class JwtAuthenticationFilter extends OncePerRequestFilter {
    @Autowired
    private JwtTokenProvider tokenProvider;

    @Autowired
    private CustomUserDetailsService customUserDetailsService;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        try {
            String jwt = getJwtFromRequest(request);

            if (StringUtils.hasText(jwt) && tokenProvider.validateToken(jwt)) {
                Long userId = tokenProvider.getUserIdFromToken(jwt);

                UserDetails userDetails = customUserDetailsService.loadUserById(userId);

                // 令牌版本校验：token 里的 tv 必须与用户当前 token_version 一致，
                // 否则视为改密后失效的旧 token，拒绝认证（改密会递增该版本）。
                int tokenTv = tokenProvider.getTokenVersionFromToken(jwt);
                int currentTv = (userDetails instanceof UserPrincipal)
                        ? ((UserPrincipal) userDetails).getTokenVersion() : 0;
                if (tokenTv != currentTv) {
                    logger.warn("Token 版本失效（tv=" + tokenTv + ", current=" + currentTv + "），拒绝认证: userId=" + userId);
                } else {
                    UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(
                            userDetails, null, userDetails.getAuthorities());
                    authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

                    SecurityContextHolder.getContext().setAuthentication(authentication);
                }
            }
        } catch (Exception ex) {
            logger.error("无法设置用户认证", ex);
        }

        filterChain.doFilter(request, response);
    }

    /**
     * 取 token。优先 {@code Authorization: Bearer}；WebSocket 握手额外接受
     * {@code ?token=} 查询参数。
     *
     * <p><b>为什么 WebSocket 要走查询参数</b>：浏览器的 WebSocket / SockJS 握手
     * 没有设置自定义请求头的 API，前端只能把 token 放进 URL。
     * {@code /ws/**} 此前是 permitAll，所以这个问题一直没暴露；收成 authenticated
     * 之后握手立刻 401，实时任务状态推送整个断掉（实测控制台
     * "WebSocket 连接错误: Lost connection"）。
     *
     * <p>只对 {@code /ws} 路径放开这条通道：token 出现在 URL 里会被 access log、
     * 浏览器历史、以及各级反向代理记下来，不该扩散到普通 API 上。
     */
    private String getJwtFromRequest(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7);
        }
        String path = request.getRequestURI();
        if (path != null && (path.equals("/ws") || path.startsWith("/ws/"))) {
            String q = request.getParameter("token");
            if (StringUtils.hasText(q)) {
                return q;
            }
        }
        return null;
    }
}
