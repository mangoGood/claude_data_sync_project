package com.synctask.service;

import com.synctask.entity.TaskSchedule;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.TaskScheduleRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 任务调度服务
 * 支持Cron表达式调度全量同步任务
 */
@Service
public class TaskScheduleService {
    private static final Logger logger = LoggerFactory.getLogger(TaskScheduleService.class);

    @Autowired
    private TaskScheduleRepository scheduleRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private TaskDependencyService taskDependencyService;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ConcurrentHashMap<Long, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    @Transactional
    public TaskSchedule createSchedule(String workflowId, Long userId, String cronExpression,
                                       String scheduleName, String scheduleType) {
        // 验证Cron表达式
        validateCronExpression(cronExpression);

        TaskSchedule schedule = new TaskSchedule();
        schedule.setWorkflowId(workflowId);
        schedule.setUserId(userId);
        schedule.setCronExpression(cronExpression);
        schedule.setScheduleName(scheduleName);
        schedule.setScheduleType(scheduleType != null ? scheduleType : "FULL_SYNC");
        schedule.setEnabled(true);
        schedule.setTriggerCount(0);

        // 计算下次触发时间
        schedule.setNextTriggerAt(calculateNextTrigger(cronExpression));

        schedule = scheduleRepository.save(schedule);
        scheduleTask(schedule);

        logger.info("创建调度任务: workflowId={}, cron={}", workflowId, cronExpression);
        return schedule;
    }

    public List<TaskSchedule> getSchedulesByUser(Long userId) {
        return scheduleRepository.findByUserIdAndEnabledTrue(userId);
    }

    public List<TaskSchedule> getSchedulesByWorkflow(String workflowId, Long userId) {
        return scheduleRepository.findByWorkflowIdAndUserId(workflowId, userId);
    }

    @Transactional
    public TaskSchedule updateSchedule(Long scheduleId, Long userId, String cronExpression,
                                       Boolean enabled, String scheduleName) {
        TaskSchedule schedule = scheduleRepository.findById(scheduleId)
                .orElseThrow(() -> new RuntimeException("调度任务不存在"));

        if (!schedule.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此调度");
        }

        if (cronExpression != null) {
            validateCronExpression(cronExpression);
            schedule.setCronExpression(cronExpression);
            schedule.setNextTriggerAt(calculateNextTrigger(cronExpression));
        }
        if (enabled != null) schedule.setEnabled(enabled);
        if (scheduleName != null) schedule.setScheduleName(scheduleName);

        // 取消旧的调度，重新调度
        cancelScheduledTask(scheduleId);
        schedule = scheduleRepository.save(schedule);
        if (Boolean.TRUE.equals(schedule.getEnabled())) {
            scheduleTask(schedule);
        }

        return schedule;
    }

    @Transactional
    public void deleteSchedule(Long scheduleId, Long userId) {
        TaskSchedule schedule = scheduleRepository.findById(scheduleId)
                .orElseThrow(() -> new RuntimeException("调度任务不存在"));
        if (!schedule.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此调度");
        }
        cancelScheduledTask(scheduleId);
        scheduleRepository.delete(schedule);
    }

    /**
     * 简化的Cron解析：支持 "秒 分 时 日 月 周" 6段格式
     * 这里使用固定延迟近似实现，每分钟检查一次
     */
    private void scheduleTask(TaskSchedule schedule) {
        if (!Boolean.TRUE.equals(schedule.getEnabled())) return;

        // 简化实现：每分钟检查是否到达触发时间
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndTrigger(schedule.getId());
            } catch (Exception e) {
                logger.error("调度检查失败: scheduleId={}", schedule.getId(), e);
            }
        }, 60, 60, TimeUnit.SECONDS);

        scheduledTasks.put(schedule.getId(), future);
        logger.info("调度任务已注册: scheduleId={}, cron={}", schedule.getId(), schedule.getCronExpression());
    }

    private void cancelScheduledTask(Long scheduleId) {
        ScheduledFuture<?> future = scheduledTasks.remove(scheduleId);
        if (future != null) {
            future.cancel(false);
        }
    }

    @Transactional
    public void checkAndTrigger(Long scheduleId) {
        TaskSchedule schedule = scheduleRepository.findById(scheduleId).orElse(null);
        if (schedule == null || !Boolean.TRUE.equals(schedule.getEnabled())) return;

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextTrigger = schedule.getNextTriggerAt();

        if (nextTrigger == null || !now.isBefore(nextTrigger)) {
            // 触发任务
            triggerSchedule(schedule);
            // 计算下次触发时间
            schedule.setNextTriggerAt(calculateNextTrigger(schedule.getCronExpression()));
            scheduleRepository.save(schedule);
        }
    }

    private void triggerSchedule(TaskSchedule schedule) {
        try {
            String workflowId = schedule.getWorkflowId();
            Workflow workflow = workflowRepository.findById(workflowId).orElse(null);
            if (workflow == null) {
                logger.warn("调度触发失败：任务不存在 workflowId={}", workflowId);
                return;
            }

            logger.info("调度触发: workflowId={}, type={}", workflowId, schedule.getScheduleType());

            if ("FULL_SYNC".equals(schedule.getScheduleType())) {
                // 重置任务状态为PENDING，重新启动全量同步
                workflow.setStatus(WorkflowStatus.PENDING);
                workflow.setProgress(0);
                workflow.setIsBilling(true);
                workflow.setErrorMessage(null);
                workflow.setCompletedAt(null);
                workflowRepository.save(workflow);

                workflowService.retryWorkflow(workflowId, schedule.getUserId());
            } else if ("VALIDATION".equals(schedule.getScheduleType())) {
                // 触发数据校验（由DataValidationService处理）
                logger.info("触发定时数据校验: workflowId={}", workflowId);
            }

            schedule.setLastTriggeredAt(LocalDateTime.now());
            schedule.setTriggerCount(schedule.getTriggerCount() + 1);
            scheduleRepository.save(schedule);

            // 检查任务依赖
            taskDependencyService.checkAndTriggerDependencies(workflowId, "ON_SUCCESS");

        } catch (Exception e) {
            logger.error("调度触发异常: scheduleId={}", schedule.getId(), e);
        }
    }

    private void validateCronExpression(String cron) {
        if (cron == null || cron.trim().isEmpty()) {
            throw new RuntimeException("Cron表达式不能为空");
        }
        String[] parts = cron.trim().split("\\s+");
        if (parts.length < 5 || parts.length > 7) {
            throw new RuntimeException("Cron表达式格式错误，应为5-7段：分 时 日 月 周 [年]");
        }
        // 简单验证每段
        for (int i = 0; i < 5; i++) {
            if (!parts[i].matches("[\\d*/,-]+")) {
                throw new RuntimeException("Cron表达式第" + (i + 1) + "段格式错误: " + parts[i]);
            }
        }
    }

    /**
     * 简化计算下次触发时间（基于5段Cron：分 时 日 月 周）
     */
    private LocalDateTime calculateNextTrigger(String cronExpression) {
        try {
            String[] parts = cronExpression.trim().split("\\s+");
            int minute = parseCronField(parts[0], 0, 59);
            int hour = parseCronField(parts[1], 0, 23);
            // 简化：忽略日、月、周，按分时计算下次触发
            LocalDateTime now = LocalDateTime.now().plusMinutes(1);
            LocalDateTime next = now.withMinute(minute).withHour(hour).withSecond(0).withNano(0);
            if (!next.isAfter(now)) {
                next = next.plusDays(1);
            }
            return next;
        } catch (Exception e) {
            logger.warn("计算下次触发时间失败: {}", cronExpression, e);
            return LocalDateTime.now().plusHours(1);
        }
    }

    private int parseCronField(String field, int min, int max) {
        if ("*".equals(field)) {
            return min;
        }
        try {
            return Integer.parseInt(field);
        } catch (NumberFormatException e) {
            return min;
        }
    }

    /**
     * 应用启动时加载所有启用的调度任务
     */
    @Scheduled(fixedDelay = 300000) // 每5分钟重新加载
    public void reloadSchedules() {
        List<TaskSchedule> schedules = scheduleRepository.findByEnabledTrue();
        for (TaskSchedule schedule : schedules) {
            if (!scheduledTasks.containsKey(schedule.getId())) {
                scheduleTask(schedule);
            }
        }
    }
}
