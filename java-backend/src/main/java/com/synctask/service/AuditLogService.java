package com.synctask.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synctask.entity.AuditLog;
import com.synctask.entity.User;
import com.synctask.entity.Workflow;
import com.synctask.repository.AuditLogRepository;
import com.synctask.repository.UserRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 审计日志服务
 *
 * <p>记录用户对系统执行的关键操作，支持审计追溯。
 * 通过 {@link Async} 异步写入，避免阻塞业务流程。
 */
@Service
public class AuditLogService {
    private static final Logger logger = LoggerFactory.getLogger(AuditLogService.class);

    @Autowired
    private AuditLogRepository auditLogRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 记录审计日志（异步）
     *
     * @param userId       用户ID
     * @param action       操作类型
     * @param workflowId   工作流ID（可空）
     * @param details      操作详情
     * @param result       操作结果
     * @param errorMessage 错误信息（失败时）
     */
    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logAsync(Long userId, AuditLog.Action action, String workflowId,
                         Object details, AuditLog.Result result, String errorMessage) {
        try {
            AuditLog auditLog = new AuditLog();
            auditLog.setUserId(userId);
            auditLog.setAction(action);
            auditLog.setWorkflowId(workflowId);
            auditLog.setResult(result);
            auditLog.setErrorMessage(errorMessage);

            // 填充用户名
            if (userId != null) {
                Optional<User> userOpt = userRepository.findById(userId);
                userOpt.ifPresent(user -> auditLog.setUsername(user.getUsername()));
            }

            // 填充工作流名称
            if (workflowId != null) {
                Optional<Workflow> wfOpt = workflowRepository.findById(workflowId);
                wfOpt.ifPresent(wf -> auditLog.setWorkflowName(wf.getName()));
            }

            // 序列化详情
            if (details != null) {
                if (details instanceof String) {
                    auditLog.setDetails((String) details);
                } else {
                    try {
                        auditLog.setDetails(objectMapper.writeValueAsString(details));
                    } catch (Exception e) {
                        auditLog.setDetails(details.toString());
                    }
                }
            }

            // 填充请求信息
            fillRequestInfo(auditLog);

            auditLogRepository.save(auditLog);
            logger.debug("审计日志已记录: action={}, userId={}, workflowId={}, result={}",
                    action, userId, workflowId, result);
        } catch (Exception e) {
            logger.error("记录审计日志失败: action={}, userId={}", action, userId, e);
        }
    }

    /** 记录成功操作 */
    public void logSuccess(Long userId, AuditLog.Action action, String workflowId, Object details) {
        logAsync(userId, action, workflowId, details, AuditLog.Result.SUCCESS, null);
    }

    /** 记录失败操作 */
    public void logFailure(Long userId, AuditLog.Action action, String workflowId,
                           Object details, String errorMessage) {
        logAsync(userId, action, workflowId, details, AuditLog.Result.FAILURE, errorMessage);
    }

    /** 填充请求信息（IP、User-Agent） */
    private void fillRequestInfo(AuditLog auditLog) {
        try {
            ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs != null) {
                HttpServletRequest request = attrs.getRequest();
                auditLog.setClientIp(extractClientIp(request));
                String userAgent = request.getHeader("User-Agent");
                if (userAgent != null && userAgent.length() > 500) {
                    userAgent = userAgent.substring(0, 500);
                }
                auditLog.setUserAgent(userAgent);
            }
        } catch (Exception e) {
            // 非 Web 上下文中调用，忽略
        }
    }

    private String extractClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }

    /** 查询所有审计日志（分页） */
    public Page<AuditLog> findAll(Pageable pageable) {
        return auditLogRepository.findAll(pageable);
    }

    /** 按用户查询 */
    public Page<AuditLog> findByUserId(Long userId, Pageable pageable) {
        return auditLogRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);
    }

    /** 按工作流查询 */
    public List<AuditLog> findByWorkflowId(String workflowId) {
        return auditLogRepository.findByWorkflowIdOrderByCreatedAtDesc(workflowId);
    }

    /** 按操作类型查询 */
    public Page<AuditLog> findByAction(AuditLog.Action action, Pageable pageable) {
        return auditLogRepository.findByActionOrderByCreatedAtDesc(action, pageable);
    }

    /** 按时间范围查询 */
    public List<AuditLog> findByTimeRange(LocalDateTime start, LocalDateTime end) {
        return auditLogRepository.findByCreatedAtBetweenOrderByCreatedAtDesc(start, end);
    }

    /** 构建操作详情 */
    public static Map<String, Object> buildDetails(String workflowName, String sourceDb, String targetDb,
                                                    String migrationMode, String taskType) {
        Map<String, Object> details = new HashMap<>();
        if (workflowName != null) details.put("workflowName", workflowName);
        if (sourceDb != null) details.put("sourceDb", sourceDb);
        if (targetDb != null) details.put("targetDb", targetDb);
        if (migrationMode != null) details.put("migrationMode", migrationMode);
        if (taskType != null) details.put("taskType", taskType);
        return details;
    }
}
