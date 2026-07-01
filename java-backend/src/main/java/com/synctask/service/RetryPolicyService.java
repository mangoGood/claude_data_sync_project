package com.synctask.service;

import com.synctask.entity.RetryPolicy;
import com.synctask.repository.RetryPolicyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * 重试策略服务
 * 按错误类型配置重试次数和间隔
 */
@Service
public class RetryPolicyService {
    private static final Logger logger = LoggerFactory.getLogger(RetryPolicyService.class);

    @Autowired
    private RetryPolicyRepository retryPolicyRepository;

    @Transactional
    public RetryPolicy createPolicy(String workflowId, Long userId, String policyName,
                                    String errorType, Integer maxRetries, Long retryIntervalMs,
                                    String backoffStrategy, Long maxIntervalMs) {
        RetryPolicy policy = new RetryPolicy();
        policy.setWorkflowId(workflowId);
        policy.setUserId(userId);
        policy.setPolicyName(policyName);
        policy.setErrorType(errorType != null ? errorType : "ALL");
        policy.setMaxRetries(maxRetries != null ? maxRetries : 3);
        policy.setRetryIntervalMs(retryIntervalMs != null ? retryIntervalMs : 5000L);
        policy.setBackoffStrategy(backoffStrategy != null ? backoffStrategy : "EXPONENTIAL");
        policy.setMaxIntervalMs(maxIntervalMs != null ? maxIntervalMs : 60000L);
        policy.setEnabled(true);
        policy.setCurrentRetryCount(0);

        return retryPolicyRepository.save(policy);
    }

    public List<RetryPolicy> getPoliciesByUser(Long userId) {
        return retryPolicyRepository.findByUserId(userId);
    }

    public List<RetryPolicy> getPoliciesByWorkflow(String workflowId, Long userId) {
        return retryPolicyRepository.findByWorkflowIdAndUserId(workflowId, userId);
    }

    @Transactional
    public RetryPolicy updatePolicy(Long policyId, Long userId, Integer maxRetries,
                                    Long retryIntervalMs, String backoffStrategy, Boolean enabled) {
        RetryPolicy policy = retryPolicyRepository.findById(policyId)
                .orElseThrow(() -> new RuntimeException("重试策略不存在"));
        if (!policy.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此重试策略");
        }
        if (maxRetries != null) policy.setMaxRetries(maxRetries);
        if (retryIntervalMs != null) policy.setRetryIntervalMs(retryIntervalMs);
        if (backoffStrategy != null) policy.setBackoffStrategy(backoffStrategy);
        if (enabled != null) policy.setEnabled(enabled);
        return retryPolicyRepository.save(policy);
    }

    @Transactional
    public void deletePolicy(Long policyId, Long userId) {
        RetryPolicy policy = retryPolicyRepository.findById(policyId)
                .orElseThrow(() -> new RuntimeException("重试策略不存在"));
        if (!policy.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此重试策略");
        }
        retryPolicyRepository.delete(policy);
    }

    /**
     * 根据错误类型获取重试策略
     */
    public Optional<RetryPolicy> getPolicyForError(String workflowId, String errorType) {
        // 优先匹配特定错误类型，其次匹配ALL
        Optional<RetryPolicy> specific = retryPolicyRepository
                .findByWorkflowIdAndErrorTypeAndEnabledTrue(workflowId, errorType);
        if (specific.isPresent()) return specific;
        return retryPolicyRepository.findByWorkflowIdAndErrorTypeAndEnabledTrue(workflowId, "ALL");
    }

    /**
     * 判断是否应该重试
     */
    public boolean shouldRetry(String workflowId, String errorType) {
        Optional<RetryPolicy> optPolicy = getPolicyForError(workflowId, errorType);
        if (!optPolicy.isPresent()) return false;
        RetryPolicy policy = optPolicy.get();
        return policy.getCurrentRetryCount() < policy.getMaxRetries();
    }

    /**
     * 计算下次重试的等待时间(毫秒)
     */
    public long calculateRetryDelay(String workflowId, String errorType) {
        Optional<RetryPolicy> optPolicy = getPolicyForError(workflowId, errorType);
        if (!optPolicy.isPresent()) return 5000L;

        RetryPolicy policy = optPolicy.get();
        int attempt = policy.getCurrentRetryCount();
        long baseInterval = policy.getRetryIntervalMs();

        if ("EXPONENTIAL".equals(policy.getBackoffStrategy())) {
            long delay = baseInterval * (1L << attempt);
            long maxInterval = policy.getMaxIntervalMs() != null ? policy.getMaxIntervalMs() : 60000L;
            return Math.min(delay, maxInterval);
        }
        return baseInterval;
    }

    /**
     * 记录一次重试
     */
    @Transactional
    public void recordRetry(String workflowId, String errorType) {
        Optional<RetryPolicy> optPolicy = getPolicyForError(workflowId, errorType);
        if (optPolicy.isPresent()) {
            RetryPolicy policy = optPolicy.get();
            policy.setCurrentRetryCount(policy.getCurrentRetryCount() + 1);
            retryPolicyRepository.save(policy);
            logger.info("任务 {} 重试次数: {}/{}", workflowId, policy.getCurrentRetryCount(), policy.getMaxRetries());
        }
    }

    /**
     * 重置重试计数
     */
    @Transactional
    public void resetRetryCount(String workflowId) {
        List<RetryPolicy> policies = retryPolicyRepository.findByWorkflowIdAndUserId(workflowId, null);
        for (RetryPolicy p : policies) {
            p.setCurrentRetryCount(0);
            retryPolicyRepository.save(p);
        }
    }
}
