package com.synctask.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * 启动时安全配置校验：严格模式下密钥缺失/过弱即中止启动，而非默默退化为开发默认值。
 *
 * <p>严格模式触发条件（任一）：环境变量/属性 {@code SYNCTASK_STRICT_SECRETS=true}，
 * 或激活了 {@code prod} profile。默认（开发）保持宽松——仅告警、退化到内置默认，不阻断本地起停。
 *
 * <p>校验项：
 * <ul>
 *   <li>JWT_SECRET：必须注入且原始长度 ≥ 32（缺失会导致每次重启签名密钥随机、旧 token 全失效）</li>
 *   <li>SYNCTASK_MASTER_KEY：必须注入（缺失会退化为内置默认密钥，与生产环境用自定义密钥加密的
 *       连接串/口令密文不兼容，且默认密钥人尽皆知）</li>
 * </ul>
 * 校验失败抛异常，Spring 上下文初始化中断，进程以非零码退出——把"跑起来才发现认证/解密全错"
 * 的问题挡在启动前。
 */
@Component
public class StartupSecurityValidator {
    private static final Logger logger = LoggerFactory.getLogger(StartupSecurityValidator.class);

    private static final int MIN_JWT_SECRET_LEN = 32;

    @Value("${jwt.secret:}")
    private String jwtSecret;

    private final Environment environment;

    public StartupSecurityValidator(Environment environment) {
        this.environment = environment;
    }

    @PostConstruct
    void validate() {
        boolean strict = isStrictMode();
        java.util.List<String> problems = new java.util.ArrayList<>();

        // JWT_SECRET
        if (jwtSecret == null || jwtSecret.isBlank()) {
            problems.add("JWT_SECRET 未配置——签名密钥将每次重启随机生成，所有已签发 token 立即失效");
        } else if (jwtSecret.trim().length() < MIN_JWT_SECRET_LEN) {
            problems.add("JWT_SECRET 过短（当前 " + jwtSecret.trim().length()
                    + " 字符，要求 ≥ " + MIN_JWT_SECRET_LEN + "）");
        }

        // SYNCTASK_MASTER_KEY
        String masterKey = firstNonBlank(System.getenv("SYNCTASK_MASTER_KEY"),
                System.getProperty("synctask.master.key"));
        if (masterKey == null) {
            problems.add("SYNCTASK_MASTER_KEY 未配置——将退化为内置开发默认主密钥（人尽皆知），"
                    + "且与生产用自定义密钥加密的密文不兼容");
        }

        if (problems.isEmpty()) {
            logger.info("启动安全校验通过（strict={}）：JWT_SECRET 与 SYNCTASK_MASTER_KEY 均已注入", strict);
            return;
        }

        String detail = String.join("\n  - ", problems);
        if (strict) {
            String msg = "启动安全校验失败（严格模式）：\n  - " + detail
                    + "\n请注入上述密钥后重启；本地开发可不设 SYNCTASK_STRICT_SECRETS 以宽松模式运行。";
            logger.error(msg);
            throw new IllegalStateException(msg);
        } else {
            logger.warn("⚠ 启动安全校验发现问题（宽松模式，仅告警；生产请设 SYNCTASK_STRICT_SECRETS=true 使其 fail-fast）：\n  - {}", detail);
        }
    }

    private boolean isStrictMode() {
        String flag = firstNonBlank(System.getenv("SYNCTASK_STRICT_SECRETS"),
                System.getProperty("synctask.strict.secrets"));
        if (flag != null && ("true".equalsIgnoreCase(flag.trim()) || "1".equals(flag.trim()))) {
            return true;
        }
        for (String p : environment.getActiveProfiles()) {
            if ("prod".equalsIgnoreCase(p) || "production".equalsIgnoreCase(p)) return true;
        }
        return false;
    }

    private static String firstNonBlank(String... vals) {
        for (String v : vals) {
            if (v != null && !v.isBlank()) return v.trim();
        }
        return null;
    }
}
