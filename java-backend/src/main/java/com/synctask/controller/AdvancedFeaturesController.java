package com.synctask.controller;

import com.synctask.entity.*;
import com.synctask.security.UserPrincipal;
import com.synctask.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 高级功能控制器
 * 统一管理任务调度、监控告警、运维效率、数据一致性、多租户配额的API
 */
@RestController
@RequestMapping("/api/advanced")
public class AdvancedFeaturesController {

    @Autowired private TaskScheduleService scheduleService;
    @Autowired private RetryPolicyService retryPolicyService;
    @Autowired private TaskDependencyService dependencyService;
    @Autowired private AlertRuleService alertRuleService;
    @Autowired private SlowSqlService slowSqlService;
    @Autowired private DiagnosticService diagnosticService;
    @Autowired private ConfigVersionService configVersionService;
    @Autowired private BatchOperationService batchOperationService;
    @Autowired private TaskCloneService taskCloneService;
    @Autowired private DataValidationService dataValidationService;
    @Autowired private ResourceQuotaService quotaService;
    @Autowired private MetricsExportService metricsExportService;

    private Long getUserId(Authentication auth) {
        return ((UserPrincipal) auth.getPrincipal()).getId();
    }

    // ============ 1. 任务调度与自动化 ============

    // 1.1 定时全量同步
    @PostMapping("/schedules")
    public ResponseEntity<?> createSchedule(@RequestBody Map<String, Object> req, Authentication auth) {
        try {
            Long userId = getUserId(auth);
            TaskSchedule schedule = scheduleService.createSchedule(
                    (String) req.get("workflowId"), userId,
                    (String) req.get("cronExpression"), (String) req.get("scheduleName"),
                    (String) req.get("scheduleType"));
            return ResponseEntity.ok(Map.of("success", true, "data", schedule));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @GetMapping("/schedules")
    public ResponseEntity<?> getSchedules(Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", scheduleService.getSchedulesByUser(getUserId(auth))));
    }

    @GetMapping("/schedules/workflow/{workflowId}")
    public ResponseEntity<?> getSchedulesByWorkflow(@PathVariable String workflowId, Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", scheduleService.getSchedulesByWorkflow(workflowId, getUserId(auth))));
    }

    @PutMapping("/schedules/{id}")
    public ResponseEntity<?> updateSchedule(@PathVariable Long id, @RequestBody Map<String, Object> req, Authentication auth) {
        try {
            TaskSchedule schedule = scheduleService.updateSchedule(id, getUserId(auth),
                    (String) req.get("cronExpression"), (Boolean) req.get("enabled"), (String) req.get("scheduleName"));
            return ResponseEntity.ok(Map.of("success", true, "data", schedule));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @DeleteMapping("/schedules/{id}")
    public ResponseEntity<?> deleteSchedule(@PathVariable Long id, Authentication auth) {
        try {
            scheduleService.deleteSchedule(id, getUserId(auth));
            return ResponseEntity.ok(Map.of("success", true, "message", "调度已删除"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // 1.2 自动重试策略
    @PostMapping("/retry-policies")
    public ResponseEntity<?> createRetryPolicy(@RequestBody Map<String, Object> req, Authentication auth) {
        try {
            Long userId = getUserId(auth);
            RetryPolicy policy = retryPolicyService.createPolicy(
                    (String) req.get("workflowId"), userId, (String) req.get("policyName"),
                    (String) req.get("errorType"), (Integer) req.get("maxRetries"),
                    req.get("retryIntervalMs") != null ? ((Number) req.get("retryIntervalMs")).longValue() : null,
                    (String) req.get("backoffStrategy"),
                    req.get("maxIntervalMs") != null ? ((Number) req.get("maxIntervalMs")).longValue() : null);
            return ResponseEntity.ok(Map.of("success", true, "data", policy));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @GetMapping("/retry-policies")
    public ResponseEntity<?> getRetryPolicies(Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", retryPolicyService.getPoliciesByUser(getUserId(auth))));
    }

    @DeleteMapping("/retry-policies/{id}")
    public ResponseEntity<?> deleteRetryPolicy(@PathVariable Long id, Authentication auth) {
        try {
            retryPolicyService.deletePolicy(id, getUserId(auth));
            return ResponseEntity.ok(Map.of("success", true, "message", "策略已删除"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // 1.3 任务依赖编排
    @PostMapping("/dependencies")
    public ResponseEntity<?> createDependency(@RequestBody Map<String, Object> req, Authentication auth) {
        try {
            TaskDependency dep = dependencyService.createDependency(
                    (String) req.get("upstreamWorkflowId"), (String) req.get("downstreamWorkflowId"),
                    getUserId(auth), (String) req.get("triggerCondition"));
            return ResponseEntity.ok(Map.of("success", true, "data", dep));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @GetMapping("/dependencies")
    public ResponseEntity<?> getDependencies(Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", dependencyService.getDependenciesByUser(getUserId(auth))));
    }

    @DeleteMapping("/dependencies/{id}")
    public ResponseEntity<?> deleteDependency(@PathVariable Long id, Authentication auth) {
        try {
            dependencyService.deleteDependency(id, getUserId(auth));
            return ResponseEntity.ok(Map.of("success", true, "message", "依赖已删除"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // ============ 2. 监控与告警 ============

    // 2.1 告警规则
    @PostMapping("/alert-rules")
    public ResponseEntity<?> createAlertRule(@RequestBody Map<String, Object> req, Authentication auth) {
        try {
            Long userId = getUserId(auth);
            AlertRule rule = alertRuleService.createRule(userId,
                    (String) req.get("workflowId"), (String) req.get("ruleName"),
                    (String) req.get("metricType"), (String) req.get("operator"),
                    req.get("threshold") != null ? ((Number) req.get("threshold")).doubleValue() : null,
                    (Integer) req.get("durationSeconds"), (String) req.get("notifyChannels"),
                    (String) req.get("webhookUrl"), (String) req.get("emailRecipients"));
            return ResponseEntity.ok(Map.of("success", true, "data", rule));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @GetMapping("/alert-rules")
    public ResponseEntity<?> getAlertRules(Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", alertRuleService.getRulesByUser(getUserId(auth))));
    }

    @DeleteMapping("/alert-rules/{id}")
    public ResponseEntity<?> deleteAlertRule(@PathVariable Long id, Authentication auth) {
        try {
            alertRuleService.deleteRule(id, getUserId(auth));
            return ResponseEntity.ok(Map.of("success", true, "message", "规则已删除"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @GetMapping("/alert-events")
    public ResponseEntity<?> getAlertEvents(@RequestParam(defaultValue = "1") int page,
                                            @RequestParam(defaultValue = "20") int pageSize,
                                            Authentication auth) {
        Page<AlertEvent> events = alertRuleService.getAlertEvents(getUserId(auth), page, pageSize);
        Map<String, Object> resp = new HashMap<>();
        resp.put("list", events.getContent());
        resp.put("total", events.getTotalElements());
        resp.put("page", page);
        resp.put("totalPages", events.getTotalPages());
        return ResponseEntity.ok(Map.of("success", true, "data", resp));
    }

    // 2.2 Prometheus指标
    @GetMapping(value = "/metrics", produces = "text/plain")
    public ResponseEntity<String> getMetrics() {
        return ResponseEntity.ok(metricsExportService.exportPrometheusMetrics());
    }

    // 2.3 慢SQL检测
    @GetMapping("/slow-sql")
    public ResponseEntity<?> getSlowSqlRecords(@RequestParam(defaultValue = "1") int page,
                                               @RequestParam(defaultValue = "20") int pageSize,
                                               Authentication auth) {
        Page<SlowSqlRecord> records = slowSqlService.getSlowSqlRecords(getUserId(auth), page, pageSize);
        Map<String, Object> resp = new HashMap<>();
        resp.put("list", records.getContent());
        resp.put("total", records.getTotalElements());
        resp.put("stats", slowSqlService.getSlowSqlStats(getUserId(auth)));
        return ResponseEntity.ok(Map.of("success", true, "data", resp));
    }

    @GetMapping("/slow-sql/workflow/{workflowId}")
    public ResponseEntity<?> getSlowSqlByWorkflow(@PathVariable String workflowId) {
        return ResponseEntity.ok(Map.of("success", true, "data", slowSqlService.getSlowSqlByWorkflow(workflowId)));
    }

    // ============ 4. 运维效率 ============

    // 4.1 一键诊断
    @PostMapping("/diagnose/{workflowId}")
    public ResponseEntity<?> diagnose(@PathVariable String workflowId, Authentication auth) {
        try {
            Map<String, Object> result = diagnosticService.diagnose(workflowId, getUserId(auth));
            return ResponseEntity.ok(Map.of("success", true, "data", result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // 4.2 配置版本管理
    @GetMapping("/config-versions/{workflowId}")
    public ResponseEntity<?> getConfigVersions(@PathVariable String workflowId) {
        return ResponseEntity.ok(Map.of("success", true, "data", configVersionService.getVersions(workflowId)));
    }

    @PostMapping("/config-versions/{workflowId}/rollback/{versionNumber}")
    public ResponseEntity<?> rollbackConfig(@PathVariable String workflowId,
                                            @PathVariable Integer versionNumber,
                                            Authentication auth) {
        try {
            Workflow wf = configVersionService.rollbackToVersion(workflowId, getUserId(auth), versionNumber);
            return ResponseEntity.ok(Map.of("success", true, "message", "已回滚到版本v" + versionNumber, "data", Map.of(
                    "id", wf.getId(), "name", wf.getName(), "status", wf.getStatus().name())));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // 4.3 批量任务操作
    @PostMapping("/batch/launch")
    public ResponseEntity<?> batchLaunch(@RequestBody Map<String, List<String>> req, Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", batchOperationService.batchLaunch(req.get("workflowIds"), getUserId(auth))));
    }

    @PostMapping("/batch/stop")
    public ResponseEntity<?> batchStop(@RequestBody Map<String, List<String>> req, Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", batchOperationService.batchStop(req.get("workflowIds"), getUserId(auth))));
    }

    @PostMapping("/batch/delete")
    public ResponseEntity<?> batchDelete(@RequestBody Map<String, List<String>> req, Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", batchOperationService.batchDelete(req.get("workflowIds"), getUserId(auth))));
    }

    @PostMapping("/batch/export")
    public ResponseEntity<?> batchExport(@RequestBody Map<String, List<String>> req, Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", batchOperationService.batchExport(req.get("workflowIds"), getUserId(auth))));
    }

    // 4.4 任务克隆
    @PostMapping("/clone/{workflowId}")
    public ResponseEntity<?> cloneTask(@PathVariable String workflowId,
                                       @RequestBody Map<String, String> req,
                                       Authentication auth) {
        try {
            Workflow cloned = taskCloneService.cloneTask(workflowId, getUserId(auth), req.get("newName"));
            return ResponseEntity.ok(Map.of("success", true, "message", "任务克隆成功", "data", Map.of(
                    "id", cloned.getId(), "name", cloned.getName())));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // ============ 5. 数据一致性保障 ============

    // 5.1 自动数据校验
    @PostMapping("/validate/{workflowId}")
    public ResponseEntity<?> validateData(@PathVariable String workflowId,
                                          @RequestParam(defaultValue = "ROW_COUNT") String validationType,
                                          Authentication auth) {
        try {
            dataValidationService.validateWorkflow(workflowId, getUserId(auth), validationType);
            return ResponseEntity.ok(Map.of("success", true, "message", "数据校验已启动"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @GetMapping("/validations")
    public ResponseEntity<?> getValidations(@RequestParam(defaultValue = "1") int page,
                                            @RequestParam(defaultValue = "20") int pageSize,
                                            Authentication auth) {
        Page<DataValidation> validations = dataValidationService.getValidations(getUserId(auth), page, pageSize);
        Map<String, Object> resp = new HashMap<>();
        resp.put("list", validations.getContent());
        resp.put("total", validations.getTotalElements());
        resp.put("page", page);
        resp.put("totalPages", validations.getTotalPages());
        return ResponseEntity.ok(Map.of("success", true, "data", resp));
    }

    @GetMapping("/validations/workflow/{workflowId}")
    public ResponseEntity<?> getValidationsByWorkflow(@PathVariable String workflowId) {
        return ResponseEntity.ok(Map.of("success", true, "data", dataValidationService.getValidationsByWorkflow(workflowId)));
    }

    // 5.2 差异修复
    @PostMapping("/validations/{id}/repair")
    public ResponseEntity<?> executeRepair(@PathVariable Long id, Authentication auth) {
        try {
            DataValidation dv = dataValidationService.executeRepair(id, getUserId(auth));
            return ResponseEntity.ok(Map.of("success", true, "message", "修复已执行", "data", dv));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // 5.3 双向同步冲突检测
    @PostMapping("/conflict-detect/{workflowId}")
    public ResponseEntity<?> detectConflicts(@PathVariable String workflowId, Authentication auth) {
        try {
            Map<String, Object> result = dataValidationService.detectBidirectionalConflicts(workflowId, getUserId(auth));
            return ResponseEntity.ok(Map.of("success", true, "data", result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    // ============ 6. 多租户与配额 ============

    // 6.1 资源配额
    @GetMapping("/quota")
    public ResponseEntity<?> getQuota(Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", quotaService.getOrCreateQuota(getUserId(auth))));
    }

    @PutMapping("/quota")
    public ResponseEntity<?> updateQuota(@RequestBody Map<String, Object> req, Authentication auth) {
        try {
            ResourceQuota quota = quotaService.updateQuota(getUserId(auth),
                    (Integer) req.get("maxTasks"), (Integer) req.get("maxConcurrentTasks"),
                    req.get("maxStorageMb") != null ? ((Number) req.get("maxStorageMb")).longValue() : null,
                    (Integer) req.get("apiRateLimitPerMin"),
                    req.get("maxIncrementRowsPerSec") != null ? ((Number) req.get("maxIncrementRowsPerSec")).intValue() : null,
                    req.get("maxFullSyncConcurrentTables") != null ? ((Number) req.get("maxFullSyncConcurrentTables")).intValue() : null);
            return ResponseEntity.ok(Map.of("success", true, "data", quota));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @GetMapping("/quota/usage")
    public ResponseEntity<?> getQuotaUsage(Authentication auth) {
        return ResponseEntity.ok(Map.of("success", true, "data", quotaService.getUsageStats(getUserId(auth))));
    }
}
