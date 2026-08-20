package com.synctask.config;

import com.synctask.entity.Role;
import com.synctask.security.ApiRateLimitFilter;
import com.synctask.security.CustomUserDetailsService;
import com.synctask.security.JwtAuthenticationFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {
    @Autowired
    private CustomUserDetailsService customUserDetailsService;

    @Autowired
    private ApiRateLimitFilter apiRateLimitFilter;

    @Value("${app.cors.allowed-origins:http://localhost:8082,http://localhost:3000}")
    private String allowedOrigins;

    @Bean
    public JwtAuthenticationFilter jwtAuthenticationFilter() {
        return new JwtAuthenticationFilter();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public DaoAuthenticationProvider authenticationProvider() {
        DaoAuthenticationProvider authProvider = new DaoAuthenticationProvider();
        authProvider.setUserDetailsService(customUserDetailsService);
        authProvider.setPasswordEncoder(passwordEncoder());
        return authProvider;
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration authConfig) throws Exception {
        return authConfig.getAuthenticationManager();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        List<String> origins = Arrays.asList(allowedOrigins.split(","));
        configuration.setAllowedOrigins(origins);
        configuration.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(List.of("*"));
        configuration.setExposedHeaders(List.of("Authorization", "Content-Type"));
        configuration.setAllowCredentials(true);
        configuration.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.cors(cors -> cors.configurationSource(corsConfigurationSource()))
        .csrf(csrf -> csrf.disable())
        .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(auth ->
            // ---------- 匿名可达（登录 + 静态资源 + 探活）----------
            auth.requestMatchers("/api/auth/login").permitAll()
                .requestMatchers("/api/health").permitAll()
                .requestMatchers("/error").permitAll()
                .requestMatchers("/", "/login.html", "/admin-dashboard.html").permitAll()
                .requestMatchers("/*.css", "/*.js", "/*.png", "/*.jpg", "/*.ico").permitAll()
                // 构建产物与第三方库（static/js、static/vendor）。上面那条 /*.js 只匹配
                // 根级单段路径，多段的 /static/vendor/sockjs.min.js 匹配不上——
                // 实测就是 401，页面上 SockJS/Chart.js 全部加载失败。
                .requestMatchers("/static/**").permitAll()

                // ---------- 仅 ADMIN：碰凭证、私钥、审计与排障包的那些 ----------
                // 建号本身就是提权动作，绝不能匿名（首个管理员的引导见 AuthService#register）。
                .requestMatchers("/api/auth/register").hasRole(Role.ADMIN)
                .requestMatchers("/api/certificates/**").hasRole(Role.ADMIN)
                .requestMatchers("/api/audit-logs/**").hasRole(Role.ADMIN)
                .requestMatchers("/api/workflows/*/diagnostics").hasRole(Role.ADMIN)
                // API 契约与 Swagger UI：端点清单 + DTO 结构 = 一份现成的攻击面地图
                .requestMatchers("/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html")
                    .hasRole(Role.ADMIN)

                // ---------- 登录后即可（个人信息 / 改密）----------
                .requestMatchers("/api/auth/**").authenticated()

                // ---------- 元数据探查会用到连接凭证，VIEWER 不给 ----------
                .requestMatchers("/api/metadata/**").hasAnyRole(Role.ADMIN, Role.USER)

                // ---------- 数据治理 ----------
                // 批 Schema 变更 = 直接改目标库结构，与"改配置"是两个量级的动作，只给 ADMIN。
                .requestMatchers("/api/governance/schema-changes/*/approve",
                        "/api/governance/schema-changes/*/reject").hasRole(Role.ADMIN)
                // 分级规则是全局的：一条正则会影响所有库表的打标，也只给 ADMIN。
                .requestMatchers("/api/governance/classification/rules/**").hasRole(Role.ADMIN)
                // 血缘与分级的查询放到 VIEWER——合规审计要看"敏感数据流到哪儿"，
                // 而那恰恰是只读角色该有的能力；写操作留给 USER 及以上。
                .requestMatchers(HttpMethod.GET, "/api/governance/**")
                    .hasAnyRole(Role.ADMIN, Role.USER, Role.VIEWER)
                .requestMatchers("/api/governance/**").hasAnyRole(Role.ADMIN, Role.USER)

                // ---------- 读写分离：GET 放到 VIEWER，改动留给 USER/ADMIN ----------
                .requestMatchers(HttpMethod.GET, "/api/workflows/**",
                        "/api/validation-tasks/**", "/api/advanced/**")
                    .hasAnyRole(Role.ADMIN, Role.USER, Role.VIEWER)
                .requestMatchers("/api/workflows/**", "/api/validation-tasks/**",
                        "/api/advanced/**")
                    .hasAnyRole(Role.ADMIN, Role.USER)

                // WebSocket 此前是 permitAll，等于任务状态推送对匿名开放。
                .requestMatchers("/ws/**").authenticated()
                .anyRequest().authenticated()
        );

        // 401 与 403 必须分开：Spring Security 默认对匿名请求也返回 403，前端无法与
        // "权限不足"区分，token 失效（如后端重启换 JWT 密钥）后页面会静默空白而不是
        // 引导重新登录。现在有了角色模型，两者语义都用得上——
        //   401 = 没登录 / token 失效  → 前端跳登录页
        //   403 = 登录了但角色不够      → 前端提示无权限，不要跳登录页
        http.exceptionHandling(ex -> ex
            .authenticationEntryPoint((request, response, authException) -> {
                response.setStatus(401);
                response.setContentType("application/json;charset=UTF-8");
                response.getWriter().write("{\"success\":false,\"message\":\"未认证或登录已失效，请重新登录\"}");
            })
            .accessDeniedHandler((request, response, denied) -> {
                response.setStatus(403);
                response.setContentType("application/json;charset=UTF-8");
                response.getWriter().write("{\"success\":false,\"message\":\"当前角色无权执行该操作\"}");
            }));

        http.authenticationProvider(authenticationProvider());
        http.addFilterBefore(jwtAuthenticationFilter(), UsernamePasswordAuthenticationFilter.class);
        http.addFilterAfter(apiRateLimitFilter, JwtAuthenticationFilter.class);

        return http.build();
    }
}
