package com.synctask.security;

import com.synctask.service.ResourceQuotaService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * API限流过滤器
 * 按用户限流，防止单用户耗尽系统资源
 */
@Component
public class ApiRateLimitFilter extends OncePerRequestFilter {
    private static final Logger logger = LoggerFactory.getLogger(ApiRateLimitFilter.class);

    @Autowired
    private ResourceQuotaService quotaService;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        // 只对API请求限流
        String path = request.getRequestURI();
        if (!path.startsWith("/api/")) {
            filterChain.doFilter(request, response);
            return;
        }

        // 获取当前用户
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof UserPrincipal) {
            UserPrincipal userPrincipal = (UserPrincipal) auth.getPrincipal();
            Long userId = userPrincipal.getId();

            // 检查API限流
            if (!quotaService.checkApiRateLimit(userId)) {
                response.setStatus(429);
                response.setContentType("application/json;charset=UTF-8");
                response.getWriter().write("{\"success\":false,\"message\":\"API调用频率超限，请稍后重试\"}");
                logger.warn("API限流触发: userId={}, path={}", userId, path);
                return;
            }
        }

        filterChain.doFilter(request, response);
    }
}
