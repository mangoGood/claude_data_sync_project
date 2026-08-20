package com.synctask.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 登录失败节流。
 *
 * <p><b>为什么单独做一个</b>：{@code ApiRateLimitFilter} 是按 {@code UserPrincipal} 限流的，
 * 而登录请求恰恰发生在认证之前——过滤器里拿不到 principal，直接放行。结果就是全站唯一
 * 一个**不需要凭证**的写接口，同时也是唯一**没有任何限流**的接口，可以无限撞库。
 *
 * <p>双维度计数：
 * <ul>
 *   <li>按<b>用户名</b>——挡住针对某个账号的密码穷举；</li>
 *   <li>按<b>来源 IP</b>——挡住拿一份弱口令表横扫所有账号（"密码喷洒"），
 *       这种打法每个用户名只试一两次，单看用户名维度永远触发不了。</li>
 * </ul>
 *
 * <p>锁定是<b>软锁</b>：到时间自动解除，不写库、不需要管理员介入。硬锁会把
 * "拒绝服务"这个能力送给攻击者——知道用户名就能锁死任何人的账号。
 */
@Component
public class LoginAttemptGuard {

    private static final Logger logger = LoggerFactory.getLogger(LoginAttemptGuard.class);

    @Value("${app.login.max-failures:10}")
    private int maxFailures;

    @Value("${app.login.lock-minutes:15}")
    private int lockMinutes;

    /** 计数窗口：超过这个时间没有新的失败就重新计数，避免长期累积把正常用户锁死。 */
    @Value("${app.login.window-minutes:15}")
    private int windowMinutes;

    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    private static final class Counter {
        final AtomicInteger failures = new AtomicInteger();
        volatile Instant lastFailure = Instant.now();
        volatile Instant lockedUntil = Instant.EPOCH;
    }

    /**
     * 登录前调用。被锁时抛，由 {@code AuthController} 转成用户可读的提示。
     */
    public void assertNotLocked(String username, String clientIp) {
        Instant now = Instant.now();
        for (String key : keys(username, clientIp)) {
            Counter c = counters.get(key);
            if (c != null && c.lockedUntil.isAfter(now)) {
                long left = Duration.between(now, c.lockedUntil).toMinutes() + 1;
                throw new LockedException("登录失败次数过多，请 " + left + " 分钟后再试");
            }
        }
    }

    /** 登录失败后调用。 */
    public void recordFailure(String username, String clientIp) {
        Instant now = Instant.now();
        for (String key : keys(username, clientIp)) {
            Counter c = counters.computeIfAbsent(key, k -> new Counter());
            // 超出计数窗口：从头算，不要让几天前的零星失败攒成锁定
            if (Duration.between(c.lastFailure, now).toMinutes() >= windowMinutes) {
                c.failures.set(0);
            }
            c.lastFailure = now;
            if (c.failures.incrementAndGet() >= maxFailures) {
                c.lockedUntil = now.plus(Duration.ofMinutes(lockMinutes));
                c.failures.set(0);
                logger.warn("登录失败次数超限，已临时锁定 {} 分钟: key={}", lockMinutes, key);
            }
        }
        evictStale(now);
    }

    /** 登录成功后调用，清掉该用户名与该 IP 的计数。 */
    public void recordSuccess(String username, String clientIp) {
        for (String key : keys(username, clientIp)) {
            counters.remove(key);
        }
    }

    private String[] keys(String username, String clientIp) {
        return new String[]{
                "u:" + (username == null ? "" : username.toLowerCase()),
                "i:" + (clientIp == null ? "" : clientIp)
        };
    }

    /**
     * 清理过期条目。不清的话这个 map 就是一条无界增长——撞库本身会造出海量不同的用户名，
     * 正好把它撑爆（本仓库在 §2.6 已经吃过三处无界增长的亏）。
     */
    private void evictStale(Instant now) {
        if (counters.size() < 10_000) {
            return;
        }
        counters.entrySet().removeIf(e ->
                e.getValue().lockedUntil.isBefore(now)
                        && Duration.between(e.getValue().lastFailure, now).toMinutes() >= windowMinutes);
    }

    /** 账号被临时锁定。独立异常类型，便于 controller 区分于"密码错误"。 */
    public static class LockedException extends RuntimeException {
        public LockedException(String message) {
            super(message);
        }
    }
}
