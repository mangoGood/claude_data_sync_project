package com.synctask.service;

import com.synctask.entity.ResourceQuota;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.ResourceQuotaRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 资源配额与API限流服务
 * 多租户隔离：按用户限制最大任务数、并发数、存储空间、API调用频率
 */
@Service
public class ResourceQuotaService {
    private static final Logger logger = LoggerFactory.getLogger(ResourceQuotaService.class);

    @Autowired
    private ResourceQuotaRepository quotaRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    /** API限流计数器：userId -> (分钟时间戳 -> 计数) */
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, AtomicInteger>> apiRateCounters
            = new ConcurrentHashMap<>();

    /**
     * 获取或创建用户配额（默认值）
     */
    @Transactional
    public ResourceQuota getOrCreateQuota(Long userId) {
        return quotaRepository.findByUserId(userId).orElseGet(() -> {
            ResourceQuota quota = new ResourceQuota();
            quota.setUserId(userId);
            return quotaRepository.save(quota);
        });
    }

    @Transactional
    public ResourceQuota updateQuota(Long userId, Integer maxTasks, Integer maxConcurrentTasks,
                                     Long maxStorageMb, Integer apiRateLimitPerMin) {
        return updateQuota(userId, maxTasks, maxConcurrentTasks, maxStorageMb, apiRateLimitPerMin, null, null);
    }

    @Transactional
    public ResourceQuota updateQuota(Long userId, Integer maxTasks, Integer maxConcurrentTasks,
                                     Long maxStorageMb, Integer apiRateLimitPerMin,
                                     Integer maxIncrementRowsPerSec, Integer maxFullSyncConcurrentTables) {
        ResourceQuota quota = getOrCreateQuota(userId);
        if (maxTasks != null) quota.setMaxTasks(maxTasks);
        if (maxConcurrentTasks != null) quota.setMaxConcurrentTasks(maxConcurrentTasks);
        if (maxStorageMb != null) quota.setMaxStorageMb(maxStorageMb);
        if (apiRateLimitPerMin != null) quota.setApiRateLimitPerMin(apiRateLimitPerMin);
        if (maxIncrementRowsPerSec != null) quota.setMaxIncrementRowsPerSec(maxIncrementRowsPerSec);
        if (maxFullSyncConcurrentTables != null) quota.setMaxFullSyncConcurrentTables(maxFullSyncConcurrentTables);
        return quotaRepository.save(quota);
    }

    /**
     * 检查是否可以创建新任务
     */
    public void checkTaskQuota(Long userId) {
        ResourceQuota quota = getOrCreateQuota(userId);
        List<Workflow> userTasks = workflowRepository.findByUserIdAndIsDeletedFalse(userId);
        if (userTasks.size() >= quota.getMaxTasks()) {
            throw new RuntimeException(String.format("已达到最大任务数限制: %d/%d",
                    userTasks.size(), quota.getMaxTasks()));
        }
    }

    /**
     * 检查是否可以启动任务（并发数限制）
     */
    public void checkConcurrentTaskQuota(Long userId) {
        ResourceQuota quota = getOrCreateQuota(userId);
        List<Workflow> runningTasks = workflowRepository
                .findByUserIdAndStatusAndIsDeletedFalse(userId, WorkflowStatus.INCREMENT_RUNNING);
        runningTasks.addAll(workflowRepository
                .findByUserIdAndStatusAndIsDeletedFalse(userId, WorkflowStatus.FULL_MIGRATING));
        runningTasks.addAll(workflowRepository
                .findByUserIdAndStatusAndIsDeletedFalse(userId, WorkflowStatus.SUBSCRIBE_RUNNING));

        if (runningTasks.size() >= quota.getMaxConcurrentTasks()) {
            throw new RuntimeException(String.format("已达到最大并发任务数限制: %d/%d",
                    runningTasks.size(), quota.getMaxConcurrentTasks()));
        }
    }

    /**
     * API限流检查
     * @return true 允许访问, false 超出限制
     */
    public boolean checkApiRateLimit(Long userId) {
        ResourceQuota quota = getOrCreateQuota(userId);
        int limit = quota.getApiRateLimitPerMin();

        long currentMinute = System.currentTimeMillis() / 60000;
        ConcurrentHashMap<Long, AtomicInteger> userCounters = apiRateCounters
                .computeIfAbsent(userId, k -> new ConcurrentHashMap<>());
        AtomicInteger counter = userCounters.computeIfAbsent(currentMinute, k -> new AtomicInteger(0));

        int current = counter.incrementAndGet();
        if (current > limit) {
            logger.warn("API限流: userId={}, 当前={}/{}/min", userId, current, limit);
            return false;
        }

        // 清理旧计数器
        if (userCounters.size() > 5) {
            userCounters.keySet().removeIf(min -> min < currentMinute - 2);
        }
        return true;
    }

    /**
     * 获取用户资源使用情况
     */
    public Map<String, Object> getUsageStats(Long userId) {
        ResourceQuota quota = getOrCreateQuota(userId);
        List<Workflow> allTasks = workflowRepository.findByUserIdAndIsDeletedFalse(userId);

        long runningCount = allTasks.stream()
                .filter(w -> w.getStatus() == WorkflowStatus.INCREMENT_RUNNING
                        || w.getStatus() == WorkflowStatus.FULL_MIGRATING
                        || w.getStatus() == WorkflowStatus.SUBSCRIBE_RUNNING)
                .count();

        Map<String, Object> stats = new HashMap<>();
        stats.put("quota", Map.of(
                "maxTasks", quota.getMaxTasks(),
                "maxConcurrentTasks", quota.getMaxConcurrentTasks(),
                "maxStorageMb", quota.getMaxStorageMb(),
                "apiRateLimitPerMin", quota.getApiRateLimitPerMin()
        ));
        stats.put("usage", Map.of(
                "totalTasks", allTasks.size(),
                "runningTasks", runningCount,
                "taskUsagePercent", allTasks.size() * 100 / Math.max(1, quota.getMaxTasks()),
                "concurrentUsagePercent", runningCount * 100 / Math.max(1, quota.getMaxConcurrentTasks())
        ));
        return stats;
    }
}
