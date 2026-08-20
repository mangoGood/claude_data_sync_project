package com.synctask.audit;

import com.synctask.entity.AuditLog;
import com.synctask.security.UserPrincipal;
import com.synctask.service.AuditLogService;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link Audited} 的执行者：成功走 logSuccess，抛异常走 logFailure。
 *
 * <p>两条原则：
 * <ul>
 *   <li><b>审计失败不能影响业务</b>——写审计本身抛异常时只记 warn，不往外扩散。
 *       否则审计表满了会把整个控制面拖停。</li>
 *   <li><b>默认不记入参</b>——见 {@link Audited#recordArgs()}。</li>
 * </ul>
 */
@Aspect
@Component
public class AuditAspect {

    private static final Logger logger = LoggerFactory.getLogger(AuditAspect.class);

    @Autowired
    private AuditLogService auditLogService;

    @Around("@annotation(audited)")
    public Object around(ProceedingJoinPoint pjp, Audited audited) throws Throwable {
        Long userId = currentUserId();
        String targetId = resolveTargetId(pjp, audited);

        Object result;
        try {
            result = pjp.proceed();
        } catch (Throwable t) {
            safeLog(() -> auditLogService.logFailure(userId, audited.value(), targetId,
                    details(pjp, audited), String.valueOf(t.getMessage())));
            throw t;
        }
        safeLog(() -> auditLogService.logSuccess(userId, audited.value(), targetId,
                details(pjp, audited)));
        return result;
    }

    private Map<String, Object> details(ProceedingJoinPoint pjp, Audited audited) {
        Map<String, Object> d = new HashMap<>();
        d.put("method", pjp.getSignature().getName());
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof UserPrincipal p) {
            d.put("username", p.getUsername());
            d.put("role", p.getRole());
        }
        if (audited.recordArgs()) {
            // 只在注解显式打开时才记；参数里可能有连接串/口令/证书 PEM
            d.put("args", java.util.Arrays.toString(pjp.getArgs()));
        }
        return d;
    }

    private String resolveTargetId(ProceedingJoinPoint pjp, Audited audited) {
        int idx = audited.idArg();
        if (idx < 0) {
            return null;
        }
        Object[] args = pjp.getArgs();
        if (idx >= args.length || args[idx] == null) {
            return null;
        }
        return String.valueOf(args[idx]);
    }

    private static Long currentUserId() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof UserPrincipal p) {
            return p.getId();
        }
        return null;
    }

    private void safeLog(Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            // 审计写失败绝不能把业务请求也带失败
            logger.warn("写审计日志失败（不影响业务）: {}", e.toString());
        }
    }
}
