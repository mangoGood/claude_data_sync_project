package com.synctask.controller;

import com.synctask.entity.AuditLog;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowLog;
import com.synctask.security.UserPrincipal;
import com.synctask.service.AuditLogService;
import com.synctask.service.ConfigVersionService;
import com.synctask.service.ResourceQuotaService;
import com.synctask.service.WorkflowService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {
    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private AuditLogService auditLogService;

    @Autowired
    private ResourceQuotaService quotaService;

    @Autowired
    private ConfigVersionService configVersionService;

    @PostMapping
    public ResponseEntity<?> createWorkflow(
            @RequestBody CreateWorkflowRequest request,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            // 资源配额检查
            quotaService.checkTaskQuota(userPrincipal.getId());

            String taskType = request.getTaskType() != null ? request.getTaskType() : "SYNC";
            Workflow workflow = workflowService.createWorkflow(
                    request.getName(),
                    request.getSourceType(),
                    request.getTargetType(),
                    userPrincipal.getId(),
                    taskType,
                    request.getDrMode()
            );
            // 审计日志
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.CREATE_TASK,
                    workflow.getId(),
                    AuditLogService.buildDetails(workflow.getName(), null, null, null, taskType));
            return ResponseEntity.ok(new ApiResponse(true, "任务创建成功", convertToMap(workflow)));
        } catch (Exception e) {
            // 审计日志（失败）
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.CREATE_TASK,
                    null, request.getName(), e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PutMapping("/{id}/config")
    public ResponseEntity<?> updateConfig(
            @PathVariable String id,
            @RequestBody UpdateConfigRequest request,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            Workflow workflow = workflowService.updateConfig(
                    id,
                    userPrincipal.getId(),
                    request.getSourceConnection(),
                    request.getTargetConnection(),
                    request.getMigrationMode(),
                    request.getSyncObjects(),
                    request.getSourceDbName(),
                    request.getTargetDbName(),
                    request.getSourceType(),
                    request.getTargetType(),
                    request.getKafkaBootstrapServers(),
                    request.getKafkaTopicPrefix(),
                    request.getKafkaTopicStrategy(),
                    request.getSubscribeFormat(),
                    request.getFanoutEnabled(),
                    request.getTargetConnections(),
                    request.getSyncAccount(),
                    request.getSyncAccountSuperPrivilege()
            );
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.UPDATE_CONFIG,
                    id, AuditLogService.buildDetails(workflow.getName(),
                            request.getSourceDbName(), request.getTargetDbName(),
                            request.getMigrationMode(), null));
            // 保存配置版本
            configVersionService.saveVersion(id, userPrincipal.getId(), "配置更新", userPrincipal.getUsername());
            return ResponseEntity.ok(new ApiResponse(true, "配置保存成功", convertToMap(workflow)));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.UPDATE_CONFIG,
                    id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/{id}/launch")
    public ResponseEntity<?> launchWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            // 并发任务配额检查
            quotaService.checkConcurrentTaskQuota(userPrincipal.getId());

            Workflow workflow = workflowService.launchWorkflow(id, userPrincipal.getId());
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.LAUNCH_TASK,
                    id, AuditLogService.buildDetails(workflow.getName(), null, null, null, null));
            return ResponseEntity.ok(new ApiResponse(true, "任务启动成功", convertToMap(workflow)));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.LAUNCH_TASK,
                    id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<?> getWorkflows(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int pageSize,
            @RequestParam(defaultValue = "created_at") String sortBy,
            @RequestParam(defaultValue = "DESC") String sortDirection,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String taskType,
            @RequestParam(required = false) String sourceType,
            @RequestParam(required = false) String targetType,
            Authentication authentication) {
        try {
            UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
            Page<Workflow> workflowPage = workflowService.getWorkflowsByUserIdAndFilters(
                    userPrincipal.getId(), keyword, status, taskType, sourceType, targetType, page, pageSize, sortBy, sortDirection
            );

            List<Map<String, Object>> list = new ArrayList<>();
            for (Workflow workflow : workflowPage.getContent()) {
                list.add(convertToMap(workflow));
            }

            Map<String, Object> response = new HashMap<>();
            response.put("list", list);
            response.put("total", workflowPage.getTotalElements());
            response.put("page", page);
            response.put("pageSize", pageSize);

            return ResponseEntity.ok(new ApiResponse(true, "获取成功", response));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }
    
    @GetMapping("/failed")
    public ResponseEntity<?> getFailedWorkflows(Authentication authentication) {
        try {
            UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
            List<Workflow> failedWorkflows = workflowService.getFailedWorkflowsByUserId(userPrincipal.getId());

            List<Map<String, Object>> list = new ArrayList<>();
            for (Workflow workflow : failedWorkflows) {
                list.add(convertToMap(workflow));
            }

            Map<String, Object> response = new HashMap<>();
            response.put("list", list);
            response.put("total", failedWorkflows.size());

            return ResponseEntity.ok(new ApiResponse(true, "获取成功", response));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        try {
            UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
            Workflow workflow = workflowService.getWorkflowById(id, userPrincipal.getId());
            List<WorkflowLog> logs = workflowService.getWorkflowLogs(id, userPrincipal.getId());

            Map<String, Object> response = convertToMap(workflow);
            
            List<Map<String, Object>> logList = new ArrayList<>();
            for (WorkflowLog log : logs) {
                Map<String, Object> logMap = new HashMap<>();
                logMap.put("id", log.getId());
                logMap.put("workflow_id", log.getWorkflowId());
                logMap.put("level", log.getLevel().name());
                logMap.put("message", log.getMessage());
                logMap.put("created_at", log.getCreatedAt());
                logList.add(logMap);
            }
            response.put("logs", logList);

            return ResponseEntity.ok(new ApiResponse(true, "获取成功", response));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/{id}/pause")
    public ResponseEntity<?> pauseWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            workflowService.pauseWorkflow(id, userPrincipal.getId());
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.PAUSE_TASK, id, null);
            return ResponseEntity.ok(new ApiResponse(true, "任务已暂停"));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.PAUSE_TASK, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/{id}/resume")
    public ResponseEntity<?> resumeWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            workflowService.resumeWorkflow(id, userPrincipal.getId());
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.RESUME_TASK, id, null);
            return ResponseEntity.ok(new ApiResponse(true, "任务已恢复"));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.RESUME_TASK, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/{id}/stop")
    public ResponseEntity<?> stopWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            workflowService.stopWorkflow(id, userPrincipal.getId());
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.STOP_TASK, id, null);
            return ResponseEntity.ok(new ApiResponse(true, "任务已结束"));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.STOP_TASK, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            workflowService.deleteWorkflow(id, userPrincipal.getId());
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.DELETE_TASK, id, null);
            return ResponseEntity.ok(new ApiResponse(true, "任务删除成功"));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.DELETE_TASK, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<?> retryWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            workflowService.retryWorkflow(id, userPrincipal.getId());
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.RETRY_TASK, id, null);
            return ResponseEntity.ok(new ApiResponse(true, "任务重试已启动"));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.RETRY_TASK, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /**
     * 人工裁决：跳过失败的增量事件并恢复任务（事件记入死信）。
     * body 可选 {"seqno": N}；不传时从任务错误信息里解析 seqno=N。
     */
    @PostMapping("/{id}/skip-event")
    public ResponseEntity<?> skipEventAndRetry(
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> body,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            Long seqno = null;
            if (body != null && body.get("seqno") != null) {
                seqno = ((Number) body.get("seqno")).longValue();
            }
            long skipped = workflowService.skipEventAndRetry(id, userPrincipal.getId(), seqno);
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.RETRY_TASK, id,
                    "跳过失败事件 seqno=" + skipped + " 并恢复（死信裁决）");
            return ResponseEntity.ok(new ApiResponse(true, "已跳过事件 seqno=" + skipped + " 并恢复任务，该事件记入死信记录"));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.RETRY_TASK, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 同步位点可视化：capture binlog/GTID → THL seqno → 已应用 checkpoint 的全链路位点与差距（代理 agent）。 */
    @GetMapping("/{id}/checkpoint")
    public ResponseEntity<?> getCheckpointVisualization(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getCheckpointVisualization(id, userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 单任务实时监控指标（代理 agent，服务端持 AGENT_API_TOKEN）。 */
    @GetMapping("/{id}/metrics")
    public ResponseEntity<?> getTaskMetrics(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getTaskMetrics(id, userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 单任务历史监控指标（代理 agent）。查询串 last/start/interval 原样透传。 */
    @GetMapping("/{id}/metrics/history")
    public ResponseEntity<?> getTaskMetricsHistory(
            @PathVariable String id,
            @RequestParam(required = false) String last,
            @RequestParam(required = false) String start,
            @RequestParam(required = false) String interval,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            StringBuilder q = new StringBuilder();
            if (last != null && !last.isEmpty()) q.append("last=").append(last);
            if (start != null && !start.isEmpty()) {
                if (q.length() > 0) q.append('&');
                q.append("start=").append(start);
            }
            if (interval != null && !interval.isEmpty()) {
                if (q.length() > 0) q.append('&');
                q.append("interval=").append(interval);
            }
            return ResponseEntity.ok(workflowService.getTaskMetricsHistory(id, userPrincipal.getId(), q.toString()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 表级延迟热力图（代理 agent）。 */
    @GetMapping("/{id}/table-latency")
    public ResponseEntity<?> getTableLatency(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getTableLatency(id, userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 一对多分发状态（代理 agent）。 */
    @GetMapping("/{id}/fanout")
    public ResponseEntity<?> getFanoutStatus(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getFanoutStatus(id, userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** agent 上所有（本用户的）任务的实时指标 —— 监控页用来判断哪些任务有实时数据。 */
    @GetMapping("/metrics/all")
    public ResponseEntity<?> getAllTaskMetrics(Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getAllTaskMetrics(userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** agent 运行态（本用户的活跃任务列表）。 */
    @GetMapping("/agent-status")
    public ResponseEntity<?> getAgentStatus(Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getAgentStatus(userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 排障压缩包下载（代理 agent）：日志尾部 + 脱敏 config + checkpoint + THL 尾部。 */
    @GetMapping("/{id}/diagnostics")
    public ResponseEntity<?> getDiagnosticsBundle(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            byte[] zip = workflowService.getDiagnosticsBundle(id, userPrincipal.getId());
            return ResponseEntity.ok()
                    .header("Content-Type", "application/zip")
                    .header("Content-Disposition", "attachment; filename=\"diagnostics-" + id + ".zip\"")
                    .body(zip);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 死信记录：人工裁决跳过的增量事件清单（代理 agent）。 */
    @GetMapping("/{id}/deadletter")
    public ResponseEntity<?> getDeadletterRecords(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getDeadletterRecords(id, userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    /** 双向写写冲突记录（代理 agent）。 */
    @GetMapping("/{id}/conflicts")
    public ResponseEntity<?> getConflictRecords(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            return ResponseEntity.ok(workflowService.getConflictRecords(id, userPrincipal.getId()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/{id}/failover")
    public ResponseEntity<?> failoverWorkflow(
            @PathVariable String id,
            Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        try {
            Workflow workflow = workflowService.failoverWorkflow(id, userPrincipal.getId());
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.FAILOVER_TASK, id,
                    AuditLogService.buildDetails(workflow.getName(), null, null, null, null));
            return ResponseEntity.ok(new ApiResponse(true, "主备倒换已启动", convertToMap(workflow)));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.FAILOVER_TASK, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    private Map<String, Object> convertToMap(Workflow workflow) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", workflow.getId());
        map.put("name", workflow.getName());
        map.put("source_connection", workflow.getSourceConnection());
        map.put("target_connection", workflow.getTargetConnection());
        map.put("status", workflow.getStatus().name());
        map.put("progress", workflow.getProgress());
        map.put("is_billing", workflow.getIsBilling());
        map.put("migration_mode", workflow.getMigrationMode());
        map.put("sync_objects", workflow.getSyncObjects());
        map.put("sync_account", workflow.getSyncAccount());
        map.put("sync_account_super_privilege", workflow.getSyncAccountSuperPrivilege());
        map.put("is_deleted", workflow.getIsDeleted());
        map.put("created_at", workflow.getCreatedAt());
        map.put("updated_at", workflow.getUpdatedAt());
        map.put("completed_at", workflow.getCompletedAt());
        map.put("error_message", workflow.getErrorMessage());
        map.put("error_code", workflow.getErrorCode());
        map.put("user_id", workflow.getUserId());
        map.put("total_tables", workflow.getTotalTables());
        map.put("completed_tables", workflow.getCompletedTables());
        map.put("current_table", workflow.getCurrentTable());
        map.put("current_table_progress", workflow.getCurrentTableProgress());
        map.put("current_table_rows", workflow.getCurrentTableRows());
        map.put("current_table_total_rows", workflow.getCurrentTableTotalRows());
        map.put("source_type", workflow.getSourceType());
        map.put("target_type", workflow.getTargetType());
        map.put("rpo_ms", workflow.getRpoMs());
        map.put("rto_ms", workflow.getRtoMs());
        // SLA 闭环指标（P2-4）：老 agent 不上报时为 null，前端据此显示 "--" 而不是 0
        map.put("replication_lag_ms", workflow.getReplicationLagMs());
        map.put("capture_replay_bytes", workflow.getCaptureReplayBytes());
        map.put("restart_count_10m", workflow.getRestartCount10m());
        map.put("conflict_count", workflow.getConflictCount());
        map.put("deadletter_count", workflow.getDeadletterCount());
        map.put("disk_usage_bytes", workflow.getDiskUsageBytes());
        map.put("task_type", workflow.getTaskType());
        map.put("dr_status", workflow.getDrStatus());
        map.put("dr_mode", workflow.getDrMode());
        map.put("dr_peer_workflow_id", workflow.getDrPeerWorkflowId());
        map.put("dr_switch_count", workflow.getDrSwitchCount());
        map.put("kafka_bootstrap_servers", workflow.getKafkaBootstrapServers());
        map.put("kafka_topic_prefix", workflow.getKafkaTopicPrefix());
        map.put("kafka_topic_strategy", workflow.getKafkaTopicStrategy());
        map.put("subscribe_format", workflow.getSubscribeFormat());
        return map;
    }

    public static class CreateWorkflowRequest {
        private String name;
        private String sourceType;
        private String targetType;
        private String taskType;
        private String drMode;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getSourceType() { return sourceType; }
        public void setSourceType(String sourceType) { this.sourceType = sourceType; }
        public String getTargetType() { return targetType; }
        public void setTargetType(String targetType) { this.targetType = targetType; }
        public String getTaskType() { return taskType; }
        public void setTaskType(String taskType) { this.taskType = taskType; }
        public String getDrMode() { return drMode; }
        public void setDrMode(String drMode) { this.drMode = drMode; }
    }

    public static class UpdateConfigRequest {
        private String sourceConnection;
        private String targetConnection;
        private String migrationMode;
        private String syncObjects;
        private String sourceDbName;
        private String targetDbName;
        private String sourceType;
        private String targetType;
        private String kafkaBootstrapServers;
        private String kafkaTopicPrefix;
        private String kafkaTopicStrategy;
        private String subscribeFormat;
        private Boolean fanoutEnabled;
        private String targetConnections;
        private Boolean syncAccount;
        private Boolean syncAccountSuperPrivilege;

        public String getSourceConnection() { return sourceConnection; }
        public void setSourceConnection(String sourceConnection) { this.sourceConnection = sourceConnection; }
        public String getTargetConnection() { return targetConnection; }
        public void setTargetConnection(String targetConnection) { this.targetConnection = targetConnection; }
        public String getMigrationMode() { return migrationMode; }
        public void setMigrationMode(String migrationMode) { this.migrationMode = migrationMode; }
        public String getSyncObjects() { return syncObjects; }
        public void setSyncObjects(String syncObjects) { this.syncObjects = syncObjects; }
        public String getSourceDbName() { return sourceDbName; }
        public void setSourceDbName(String sourceDbName) { this.sourceDbName = sourceDbName; }
        public String getTargetDbName() { return targetDbName; }
        public void setTargetDbName(String targetDbName) { this.targetDbName = targetDbName; }
        public String getSourceType() { return sourceType; }
        public void setSourceType(String sourceType) { this.sourceType = sourceType; }
        public String getTargetType() { return targetType; }
        public void setTargetType(String targetType) { this.targetType = targetType; }
        public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
        public void setKafkaBootstrapServers(String kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }
        public String getKafkaTopicPrefix() { return kafkaTopicPrefix; }
        public void setKafkaTopicPrefix(String kafkaTopicPrefix) { this.kafkaTopicPrefix = kafkaTopicPrefix; }
        public String getKafkaTopicStrategy() { return kafkaTopicStrategy; }
        public void setKafkaTopicStrategy(String kafkaTopicStrategy) { this.kafkaTopicStrategy = kafkaTopicStrategy; }
        public String getSubscribeFormat() { return subscribeFormat; }
        public void setSubscribeFormat(String subscribeFormat) { this.subscribeFormat = subscribeFormat; }
        public Boolean getFanoutEnabled() { return fanoutEnabled; }
        public void setFanoutEnabled(Boolean fanoutEnabled) { this.fanoutEnabled = fanoutEnabled; }
        public String getTargetConnections() { return targetConnections; }
        public void setTargetConnections(String targetConnections) { this.targetConnections = targetConnections; }
        public Boolean getSyncAccount() { return syncAccount; }
        public void setSyncAccount(Boolean syncAccount) { this.syncAccount = syncAccount; }
        public Boolean getSyncAccountSuperPrivilege() { return syncAccountSuperPrivilege; }
        public void setSyncAccountSuperPrivilege(Boolean syncAccountSuperPrivilege) { this.syncAccountSuperPrivilege = syncAccountSuperPrivilege; }
    }

    public static class ApiResponse {
        private boolean success;
        private String message;
        private Object data;

        public ApiResponse(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public ApiResponse(boolean success, String message, Object data) {
            this.success = success;
            this.message = message;
            this.data = data;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }
    }
}
