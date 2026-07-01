package com.synctask.controller;

import com.synctask.entity.AuditLog;
import com.synctask.security.UserPrincipal;
import com.synctask.service.AuditLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 审计日志查询接口
 */
@RestController
@RequestMapping("/api/audit-logs")
public class AuditLogController {

    @Autowired
    private AuditLogService auditLogService;

    /** 分页查询所有审计日志 */
    @GetMapping
    public ResponseEntity<?> list(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String action,
            @RequestParam(required = false) Long userId,
            Authentication authentication) {
        try {
            PageRequest pageRequest = PageRequest.of(page, size,
                    Sort.by(Sort.Direction.DESC, "createdAt"));

            Page<AuditLog> result;
            if (action != null && !action.isEmpty()) {
                AuditLog.Action actionEnum = AuditLog.Action.valueOf(action);
                result = auditLogService.findByAction(actionEnum, pageRequest);
            } else if (userId != null) {
                result = auditLogService.findByUserId(userId, pageRequest);
            } else {
                result = auditLogService.findAll(pageRequest);
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("data", result.getContent());
            response.put("total", result.getTotalElements());
            response.put("page", result.getNumber());
            response.put("size", result.getSize());
            response.put("totalPages", result.getTotalPages());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /** 按工作流ID查询审计日志 */
    @GetMapping("/workflow/{workflowId}")
    public ResponseEntity<?> findByWorkflow(@PathVariable String workflowId) {
        try {
            List<AuditLog> logs = auditLogService.findByWorkflowId(workflowId);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("data", logs);
            response.put("total", logs.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /** 按时间范围查询审计日志 */
    @GetMapping("/time-range")
    public ResponseEntity<?> findByTimeRange(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end) {
        try {
            List<AuditLog> logs = auditLogService.findByTimeRange(start, end);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("data", logs);
            response.put("total", logs.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /** 查询当前用户的审计日志 */
    @GetMapping("/me")
    public ResponseEntity<?> findMyLogs(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            Authentication authentication) {
        try {
            UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
            PageRequest pageRequest = PageRequest.of(page, size,
                    Sort.by(Sort.Direction.DESC, "createdAt"));
            Page<AuditLog> result = auditLogService.findByUserId(userPrincipal.getId(), pageRequest);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("data", result.getContent());
            response.put("total", result.getTotalElements());
            response.put("page", result.getNumber());
            response.put("size", result.getSize());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }
}
