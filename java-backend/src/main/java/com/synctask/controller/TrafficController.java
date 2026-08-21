package com.synctask.controller;

import com.synctask.entity.AuditLog;
import com.synctask.entity.TrafficTaskConfig;
import com.synctask.security.UserPrincipal;
import com.synctask.service.AuditLogService;
import com.synctask.service.TrafficTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * 流量复制/回放的配置与录制文件接口。
 *
 * <p>任务的创建/启动/暂停/停止/删除<b>不在这里</b>——那些完全复用 {@code /api/workflows/*}，
 * 只是 {@code taskType} 传 {@code TRAFFIC_CAPTURE}/{@code TRAFFIC_REPLAY}。
 */
@RestController
@RequestMapping("/api/traffic")
public class TrafficController {

    @Autowired
    private TrafficTaskService trafficService;

    @Autowired
    private AuditLogService auditLogService;

    @GetMapping("/config/{taskId}")
    public ResponseEntity<?> getConfig(@PathVariable String taskId, Authentication auth) {
        try {
            TrafficTaskConfig c = trafficService.getOrCreateConfig(taskId);
            return ResponseEntity.ok(ok("获取成功", toMap(c)));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    @PutMapping("/config/{taskId}")
    public ResponseEntity<?> updateConfig(@PathVariable String taskId,
                                          @RequestBody Map<String, Object> body,
                                          Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            TrafficTaskConfig c = trafficService.updateConfig(taskId, user.getId(), body);
            auditLogService.logSuccess(user.getId(), AuditLog.Action.UPDATE_CONFIG, taskId,
                    AuditLogService.buildDetails(null, null, null, null, "TRAFFIC"));
            return ResponseEntity.ok(ok("配置保存成功", toMap(c)));
        } catch (Exception e) {
            auditLogService.logFailure(user.getId(), AuditLog.Action.UPDATE_CONFIG, taskId, null, e.getMessage());
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    /**
     * 启动前预检。
     *
     * <p>与 {@code launch} 里的门禁跑的是同一套（{@link TrafficPrecheckService}）——
     * 页面先看一遍、后端启动时再拦一次，两边结论必然一致。
     */
    @PostMapping("/precheck/{taskId}")
    public ResponseEntity<?> precheck(@PathVariable String taskId, Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            return ResponseEntity.ok(ok("预检完成",
                    trafficService.precheck(taskId, user.getId())));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    // ==================== 录制文件 ====================

    @GetMapping("/recordings")
    public ResponseEntity<?> listRecordings(
            @RequestParam(required = false, defaultValue = "false") boolean sealedOnly,
            Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            return ResponseEntity.ok(ok("获取成功",
                    Map.of("list", trafficService.listRecordings(user.getId(), sealedOnly))));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    @GetMapping("/recordings/{id}")
    public ResponseEntity<?> getRecording(@PathVariable String id, Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            return ResponseEntity.ok(ok("获取成功",
                    trafficService.getRecording(id, user.getId())));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    /** 从 agent 拉回并登记该捕获任务的录制元数据。停止捕获任务后由前端调用。 */
    @PostMapping("/recordings/sync/{captureTaskId}")
    public ResponseEntity<?> syncRecording(@PathVariable String captureTaskId, Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            return ResponseEntity.ok(ok("录制信息已同步",
                    trafficService.syncRecordingFromAgent(captureTaskId, user.getId())));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    /**
     * 下载录制文件（.trfz 打包）。
     *
     * <p>录制里含明文 SQL，也就必然含业务数据，所以走属主校验 + 审计。
     */
    @GetMapping("/recordings/{id}/download")
    public ResponseEntity<?> downloadRecording(@PathVariable String id, Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            byte[] data = trafficService.downloadRecording(id, user.getId());
            String fileName = trafficService.recordingFileName(id, user.getId());
            auditLogService.logSuccess(user.getId(), AuditLog.Action.DOWNLOAD_TRAFFIC_RECORDING, id,
                    AuditLogService.buildDetails(fileName, null, null, null, "TRAFFIC_RECORDING"));
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(data);
        } catch (Exception e) {
            auditLogService.logFailure(user.getId(), AuditLog.Action.DOWNLOAD_TRAFFIC_RECORDING, id, null, e.getMessage());
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    @DeleteMapping("/recordings/{id}")
    public ResponseEntity<?> deleteRecording(@PathVariable String id, Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            trafficService.deleteRecording(id, user.getId());
            return ResponseEntity.ok(ok("录制文件已删除", null));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    // ==================== 回放结果 ====================

    @GetMapping("/replay-errors/{taskId}")
    public ResponseEntity<?> replayErrors(@PathVariable String taskId,
                                          @RequestParam(required = false, defaultValue = "1") int page,
                                          @RequestParam(required = false, defaultValue = "50") int pageSize,
                                          Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            String q = "page=" + page + "&pageSize=" + pageSize;
            return ResponseEntity.ok(ok("获取成功",
                    trafficService.getReplayErrors(taskId, user.getId(), q)));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    @GetMapping("/replay-report/{taskId}")
    public ResponseEntity<?> replayReport(@PathVariable String taskId, Authentication auth) {
        UserPrincipal user = (UserPrincipal) auth.getPrincipal();
        try {
            return ResponseEntity.ok(ok("获取成功",
                    trafficService.getReplayReport(taskId, user.getId())));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    /**
     * 源库语句日志仍未还原的任务清单。
     *
     * <p>这是本功能最大的运维风险的出口：源库 {@code general_log} 一直开着会把它的磁盘写满。
     */
    @GetMapping("/pending-source-restores")
    public ResponseEntity<?> pendingSourceRestores(Authentication auth) {
        try {
            return ResponseEntity.ok(ok("获取成功",
                    Map.of("list", trafficService.pendingSourceRestores())));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    private static Map<String, Object> toMap(TrafficTaskConfig c) {
        Map<String, Object> m = new HashMap<>();
        m.put("taskId", c.getTaskId());
        m.put("captureBackend", c.getCaptureBackend());
        m.put("captureDatabases", c.getCaptureDatabases());
        m.put("captureClasses", c.getCaptureClasses());
        m.put("captureUsers", c.getCaptureUsers());
        m.put("captureSampleRate", scale(c.getCaptureSampleRate()));
        m.put("captureEnrich", c.getCaptureEnrich());
        m.put("captureMaxDurationMs", c.getCaptureMaxDurationMs());
        m.put("captureMaxBytes", c.getCaptureMaxBytes());
        m.put("captureMaxRecords", c.getCaptureMaxRecords());
        m.put("srcGeneralLogBefore", c.getSrcGeneralLogBefore());
        m.put("srcLogOutputBefore", c.getSrcLogOutputBefore());
        m.put("srcRestorePending", c.getSrcRestorePending());
        m.put("replayRecordingId", c.getReplayRecordingId());
        m.put("replaySpeed", scale(c.getReplaySpeed()));
        m.put("replayClasses", c.getReplayClasses());
        m.put("replayLagPolicy", c.getReplayLagPolicy());
        m.put("replayLagSkipMs", c.getReplayLagSkipMs());
        m.put("replayGapPolicy", c.getReplayGapPolicy());
        m.put("replayMaxSessions", c.getReplayMaxSessions());
        m.put("replayCompare", c.getReplayCompare());
        m.put("replayAllowDcl", c.getReplayAllowDcl());
        m.put("replayAllowDangerous", c.getReplayAllowDangerous());
        m.put("replayAllowSameInstance", c.getReplayAllowSameInstance());
        m.put("replayAbortErrorRate", scale(c.getReplayAbortErrorRate()));
        return m;
    }

    private static Double scale(BigDecimal v) {
        return v == null ? null : v.doubleValue();
    }

    private static Map<String, Object> ok(String message, Object data) {
        Map<String, Object> m = new HashMap<>();
        m.put("success", true);
        if (message != null) {
            m.put("message", message);
        }
        m.put("data", data);
        return m;
    }

    private static Map<String, Object> fail(String message) {
        Map<String, Object> m = new HashMap<>();
        m.put("success", false);
        m.put("message", message);
        return m;
    }
}
