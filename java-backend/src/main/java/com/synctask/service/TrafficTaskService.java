package com.synctask.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synctask.entity.TrafficRecording;
import com.synctask.entity.TrafficTaskConfig;
import com.synctask.entity.Workflow;
import com.synctask.repository.TrafficRecordingRepository;
import com.synctask.repository.TrafficTaskConfigRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 流量复制/回放任务的配置与录制文件管理。
 *
 * <p>任务生命周期（创建/启动/暂停/停止/删除）完全复用 {@code /api/workflows/*}，
 * 这里只管两件平台侧特有的事：任务专属配置，以及录制文件的目录与取用。
 */
@Service
public class TrafficTaskService {

    private static final Logger logger = LoggerFactory.getLogger(TrafficTaskService.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String TYPE_CAPTURE = "TRAFFIC_CAPTURE";
    public static final String TYPE_REPLAY = "TRAFFIC_REPLAY";

    @Autowired
    private TrafficTaskConfigRepository configRepository;

    @Autowired
    private TrafficRecordingRepository recordingRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private AgentClusterService agentClusterService;

    @Autowired
    private TrafficPrecheckService precheckService;

    public static boolean isTrafficTask(String taskType) {
        return TYPE_CAPTURE.equals(taskType) || TYPE_REPLAY.equals(taskType);
    }

    // ==================== 任务配置 ====================

    /** 取配置；没有就按默认值建一条（任务刚创建时不必先写配置也能读）。 */
    @Transactional
    public TrafficTaskConfig getOrCreateConfig(String taskId) {
        return configRepository.findById(taskId).orElseGet(() -> {
            TrafficTaskConfig c = new TrafficTaskConfig();
            c.setTaskId(taskId);
            return configRepository.save(c);
        });
    }

    public TrafficTaskConfig findConfig(String taskId) {
        return configRepository.findById(taskId).orElse(null);
    }

    /**
     * 更新配置。
     *
     * <p>只在 {@code CONFIGURING} 放行——与既有 {@code updateConfig} 同规矩：
     * 任务跑起来之后改档位（比如把回放倍速从 1 改成 8）不会作用于已在跑的子进程，
     * 却会让页面显示的配置与实际行为不符。
     */
    @Transactional
    public TrafficTaskConfig updateConfig(String taskId, Long userId, Map<String, Object> body) {
        Workflow w = requireOwnedTask(taskId, userId);
        if (w.getStatus() != com.synctask.entity.WorkflowStatus.CONFIGURING) {
            throw new RuntimeException("只能修改配置中的任务，当前状态: " + w.getStatus().name());
        }
        TrafficTaskConfig c = getOrCreateConfig(taskId);

        applyString(body, "captureBackend", c::setCaptureBackend);
        applyJsonArray(body, "captureDatabases", c::setCaptureDatabases);
        applyString(body, "captureClasses", c::setCaptureClasses);
        applyJsonArray(body, "captureUsers", c::setCaptureUsers);
        applyDecimal(body, "captureSampleRate", c::setCaptureSampleRate);
        applyBool(body, "captureEnrich", c::setCaptureEnrich);
        applyLong(body, "captureMaxDurationMs", c::setCaptureMaxDurationMs);
        applyLong(body, "captureMaxBytes", c::setCaptureMaxBytes);
        applyLong(body, "captureMaxRecords", c::setCaptureMaxRecords);

        applyString(body, "replayRecordingId", c::setReplayRecordingId);
        applyDecimal(body, "replaySpeed", c::setReplaySpeed);
        applyString(body, "replayClasses", c::setReplayClasses);
        applyString(body, "replayLagPolicy", c::setReplayLagPolicy);
        applyLong(body, "replayLagSkipMs", c::setReplayLagSkipMs);
        applyString(body, "replayGapPolicy", c::setReplayGapPolicy);
        applyInt(body, "replayMaxSessions", c::setReplayMaxSessions);
        applyString(body, "replayCompare", c::setReplayCompare);
        applyBool(body, "replayAllowDcl", c::setReplayAllowDcl);
        applyBool(body, "replayAllowDangerous", c::setReplayAllowDangerous);
        applyBool(body, "replayAllowSameInstance", c::setReplayAllowSameInstance);
        applyDecimal(body, "replayAbortErrorRate", c::setReplayAbortErrorRate);

        validate(w, c);
        return configRepository.save(c);
    }

    private void validate(Workflow w, TrafficTaskConfig c) {
        if (c.getCaptureSampleRate() != null
                && (c.getCaptureSampleRate().signum() <= 0
                    || c.getCaptureSampleRate().compareTo(BigDecimal.ONE) > 0)) {
            throw new RuntimeException("采样率必须在 (0, 1] 之间");
        }
        if (c.getReplaySpeed() != null && c.getReplaySpeed().signum() <= 0) {
            throw new RuntimeException("回放倍速必须大于 0");
        }
        if (c.getReplayMaxSessions() != null && c.getReplayMaxSessions() < 1) {
            throw new RuntimeException("回放并发会话上限必须至少为 1");
        }
        if (TYPE_REPLAY.equals(w.getTaskType()) && c.getReplayRecordingId() != null) {
            TrafficRecording rec = recordingRepository
                    .findByIdAndIsDeletedFalse(c.getReplayRecordingId()).orElse(null);
            if (rec == null) {
                throw new RuntimeException("录制文件不存在或已删除: " + c.getReplayRecordingId());
            }
            if (!Boolean.TRUE.equals(rec.getSealed())) {
                throw new RuntimeException("该录制尚未封口（对应的流量复制任务还在跑或异常终止），不能回放");
            }
        }
    }

    /** 启动前的必填校验，由 {@code WorkflowService.launchWorkflow} 调用。 */
    public void assertLaunchable(Workflow w) {
        TrafficTaskConfig c = findConfig(w.getId());
        if (TYPE_REPLAY.equals(w.getTaskType())) {
            if (c == null || c.getReplayRecordingId() == null || c.getReplayRecordingId().isEmpty()) {
                throw new RuntimeException("请先选择要回放的录制文件");
            }
            TrafficRecording rec = recordingRepository
                    .findByIdAndIsDeletedFalse(c.getReplayRecordingId()).orElse(null);
            if (rec == null) {
                throw new RuntimeException("录制文件不存在或已删除");
            }
            if (!Boolean.TRUE.equals(rec.getSealed())) {
                throw new RuntimeException("该录制尚未封口，不能回放");
            }
        }
    }

    /**
     * 把任务的流量配置序列化成派发消息里的 JSON。
     *
     * <p>回放任务额外带上<b>录制文件的位置</b>（哪台 agent、哪个捕获任务的目录）——
     * agent 拿到才知道去哪里取录制；集群里录制文件躺在产出它的那台机器上。
     */
    public String toDispatchJson(String taskId) {
        TrafficTaskConfig c = getOrCreateConfig(taskId);
        Map<String, Object> m = new HashMap<>();
        m.put("captureBackend", c.getCaptureBackend());
        m.put("captureDatabases", jsonArrayToCsv(c.getCaptureDatabases()));
        m.put("captureClasses", c.getCaptureClasses());
        m.put("captureUsers", jsonArrayToCsv(c.getCaptureUsers()));
        m.put("captureSampleRate", c.getCaptureSampleRate());
        m.put("captureEnrich", c.getCaptureEnrich());
        m.put("captureMaxDurationMs", c.getCaptureMaxDurationMs());
        m.put("captureMaxBytes", c.getCaptureMaxBytes());
        m.put("captureMaxRecords", c.getCaptureMaxRecords());
        m.put("replaySpeed", c.getReplaySpeed());
        m.put("replayClasses", c.getReplayClasses());
        m.put("replayLagPolicy", c.getReplayLagPolicy());
        m.put("replayLagSkipMs", c.getReplayLagSkipMs());
        m.put("replayGapPolicy", c.getReplayGapPolicy());
        m.put("replayMaxSessions", c.getReplayMaxSessions());
        m.put("replayCompare", c.getReplayCompare());
        m.put("replayAllowDcl", c.getReplayAllowDcl());
        m.put("replayAllowDangerous", c.getReplayAllowDangerous());
        m.put("replayAllowSameInstance", c.getReplayAllowSameInstance());
        m.put("replayAbortErrorRate", c.getReplayAbortErrorRate());
        if (c.getReplayRecordingId() != null) {
            TrafficRecording rec = recordingRepository
                    .findByIdAndIsDeletedFalse(c.getReplayRecordingId()).orElse(null);
            if (rec != null) {
                m.put("replayRecordingId", rec.getId());
                m.put("replayRecordingTaskId", rec.getCaptureTaskId());
                m.put("replayRecordingAgentId", rec.getAgentId());
                m.put("replayRecordingSha256", rec.getSha256());
            }
        }
        return writeJson(m);
    }

    /** 启动前预检（属主校验后交给 {@link TrafficPrecheckService}）。 */
    public Map<String, Object> precheck(String taskId, Long userId) {
        Workflow w = requireOwnedTask(taskId, userId);
        if (!isTrafficTask(w.getTaskType())) {
            throw new RuntimeException("不是流量任务: " + w.getTaskType());
        }
        return precheckService.precheck(w, userId);
    }

    // ==================== 录制文件 ====================

    public List<Map<String, Object>> listRecordings(Long userId, boolean sealedOnly) {
        List<TrafficRecording> list = sealedOnly
                ? recordingRepository.findByUserIdAndIsDeletedFalseAndSealedTrueOrderByCreatedAtDesc(userId)
                : recordingRepository.findByUserIdAndIsDeletedFalseOrderByCreatedAtDesc(
                        userId, org.springframework.data.domain.PageRequest.of(0, 200)).getContent();
        List<Map<String, Object>> out = new ArrayList<>();
        for (TrafficRecording r : list) {
            out.add(toMap(r));
        }
        return out;
    }

    public Map<String, Object> getRecording(String id, Long userId) {
        TrafficRecording r = requireOwnedRecording(id, userId);
        return toMap(r);
    }

    @Transactional
    public void deleteRecording(String id, Long userId) {
        TrafficRecording r = requireOwnedRecording(id, userId);
        r.setIsDeleted(true);
        recordingRepository.save(r);
        logger.info("录制文件已逻辑删除: {} ({})", r.getName(), id);
    }

    /**
     * 从 agent 拉回该捕获任务的录制元数据并登记。
     *
     * <p>幂等：同一个捕获任务只登记一条，重复调用只更新（捕获任务续录之后统计会变）。
     */
    @Transactional
    public Map<String, Object> syncRecordingFromAgent(String captureTaskId, Long userId) {
        Workflow w = requireOwnedTask(captureTaskId, userId);
        if (!TYPE_CAPTURE.equals(w.getTaskType())) {
            throw new RuntimeException("只有流量复制任务才有录制文件");
        }
        Map<String, Object> manifest = callAgentJson(w.getAgentId(),
                "/api/traffic/recordings/" + captureTaskId,
                "读取录制信息失败（agent 不可达，或该任务尚未产生录制）");

        TrafficRecording rec = recordingRepository
                .findFirstByCaptureTaskIdAndIsDeletedFalse(captureTaskId)
                .orElseGet(() -> {
                    TrafficRecording r = new TrafficRecording();
                    r.setId(UUID.randomUUID().toString());
                    r.setCaptureTaskId(captureTaskId);
                    r.setUserId(w.getUserId());
                    return r;
                });
        rec.setName(w.getName());
        rec.setAgentId(w.getAgentId());
        rec.setT0Wall(parseWall(str(manifest.get("t0Wall"))));
        rec.setEndWall(parseWall(str(manifest.get("endWall"))));
        rec.setDurationMs(lng(manifest.get("durationMs")));
        rec.setRecordCount(lng(manifest.get("records")));
        rec.setByteSize(lng(manifest.get("bytes")));
        rec.setSessionCount((int) lng(manifest.get("sessions")));
        rec.setGapCount((int) lng(manifest.get("gaps")));
        rec.setSha256(str(manifest.get("sha256")));
        rec.setSealed(Boolean.TRUE.equals(manifest.get("sealed")));
        rec.setStatsJson(writeJson(manifest.get("stats")));
        rec.setSourceFingerprint(writeJson(manifest.get("source")));
        recordingRepository.save(rec);
        logger.info("录制元数据已登记: task={}, records={}, sealed={}",
                captureTaskId, rec.getRecordCount(), rec.getSealed());
        return toMap(rec);
    }

    /** 录制文件打包下载（代理到<b>录制所在的那台</b> agent，不是当前任务的 agent）。 */
    public byte[] downloadRecording(String id, Long userId) {
        TrafficRecording r = requireOwnedRecording(id, userId);
        if (r.getCaptureTaskId() == null) {
            throw new RuntimeException("该录制没有关联的捕获任务，无法从 agent 取回文件");
        }
        return callAgentBytes(r.getAgentId(),
                "/api/traffic/bundle/" + r.getCaptureTaskId(),
                "下载录制文件失败（agent 不可达或文件已被清理）");
    }

    public String recordingFileName(String id, Long userId) {
        TrafficRecording r = requireOwnedRecording(id, userId);
        String safe = r.getName() == null ? "recording" : r.getName().replaceAll("[^\\w\\-.]", "_");
        return safe + "-" + r.getId().substring(0, 8) + ".trfz";
    }

    // ==================== 回放结果 ====================

    public Map<String, Object> getReplayErrors(String taskId, Long userId, String query) {
        Workflow w = requireOwnedTask(taskId, userId);
        String path = "/api/traffic/replay-errors/" + taskId + (query == null || query.isEmpty() ? "" : "?" + query);
        return callAgentJson(w.getAgentId(), path, "读取回放错误明细失败（agent 不可达或任务未运行）");
    }

    public Map<String, Object> getReplayReport(String taskId, Long userId) {
        Workflow w = requireOwnedTask(taskId, userId);
        return callAgentJson(w.getAgentId(), "/api/traffic/replay-report/" + taskId,
                "读取回放报告失败（agent 不可达或回放尚未结束）");
    }

    // ==================== 源库开关兜底 ====================

    /**
     * 记录源库开关原值（由 agent 随状态上报带上来）。
     *
     * <p>之所以要在<b>后端</b>也留一份：agent 主机可能整台没了。
     * 那时这份记录就是唯一能告诉运维"哪台源库的 general_log 还开着、原值是什么"的东西。
     */
    @Transactional
    public void recordSourceState(String taskId, String generalLog, String logOutput, boolean pending) {
        TrafficTaskConfig c = getOrCreateConfig(taskId);
        if (generalLog != null) c.setSrcGeneralLogBefore(generalLog);
        if (logOutput != null) c.setSrcLogOutputBefore(logOutput);
        c.setSrcRestorePending(pending);
        configRepository.save(c);
    }

    /** 源库开关仍未还原的任务清单（给告警和运维看）。 */
    public List<Map<String, Object>> pendingSourceRestores() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (TrafficTaskConfig c : configRepository.findBySrcRestorePendingTrue()) {
            Workflow w = workflowRepository.findById(c.getTaskId()).orElse(null);
            Map<String, Object> m = new HashMap<>();
            m.put("taskId", c.getTaskId());
            m.put("taskName", w == null ? null : w.getName());
            m.put("agentId", w == null ? null : w.getAgentId());
            m.put("originalGeneralLog", c.getSrcGeneralLogBefore());
            m.put("originalLogOutput", c.getSrcLogOutputBefore());
            out.add(m);
        }
        return out;
    }

    // ==================== 内部 ====================

    private Workflow requireOwnedTask(String taskId, Long userId) {
        Workflow w = workflowRepository.findById(taskId).orElse(null);
        if (w == null || Boolean.TRUE.equals(w.getIsDeleted())) {
            throw new RuntimeException("任务不存在: " + taskId);
        }
        if (!w.getUserId().equals(userId)) {
            throw new RuntimeException("无权访问该任务");
        }
        return w;
    }

    private TrafficRecording requireOwnedRecording(String id, Long userId) {
        TrafficRecording r = recordingRepository.findByIdAndIsDeletedFalse(id).orElse(null);
        if (r == null) {
            throw new RuntimeException("录制文件不存在: " + id);
        }
        if (!r.getUserId().equals(userId)) {
            throw new RuntimeException("无权访问该录制文件");
        }
        return r;
    }

    private Map<String, Object> toMap(TrafficRecording r) {
        Map<String, Object> m = new HashMap<>();
        m.put("id", r.getId());
        m.put("captureTaskId", r.getCaptureTaskId());
        m.put("agentId", r.getAgentId());
        m.put("name", r.getName());
        m.put("t0Wall", r.getT0Wall());
        m.put("endWall", r.getEndWall());
        m.put("durationMs", r.getDurationMs());
        m.put("recordCount", r.getRecordCount());
        m.put("byteSize", r.getByteSize());
        m.put("sessionCount", r.getSessionCount());
        m.put("gapCount", r.getGapCount());
        m.put("sha256", r.getSha256());
        m.put("sealed", r.getSealed());
        m.put("createdAt", r.getCreatedAt());
        m.put("stats", readJson(r.getStatsJson()));
        m.put("source", readJson(r.getSourceFingerprint()));
        return m;
    }

    private String agentBaseUrl(String agentId) {
        String fallback = System.getenv().getOrDefault("AGENT_BASE_URL",
                com.synctask.util.AgentHttpSupport.scheme() + "://localhost:8083");
        if (agentId == null) return fallback;
        try {
            return agentClusterService.agentBaseUrl(agentId).orElse(fallback);
        } catch (Exception e) {
            return fallback;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> callAgentJson(String agentId, String path, String errPrefix) {
        try {
            byte[] body = callAgentBytes(agentId, path, errPrefix);
            return MAPPER.readValue(body, Map.class);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(errPrefix + ": " + e.getMessage());
        }
    }

    private byte[] callAgentBytes(String agentId, String path, String errPrefix) {
        String agentToken = System.getenv("AGENT_API_TOKEN");
        try {
            java.net.HttpURLConnection conn =
                    com.synctask.util.AgentHttpSupport.open(agentBaseUrl(agentId) + path);
            conn.setRequestMethod("GET");
            if (agentToken != null && !agentToken.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + agentToken);
            }
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(120000);
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException(errPrefix + "（agent 返回 " + conn.getResponseCode() + "）");
            }
            try (java.io.InputStream is = conn.getInputStream()) {
                return is.readAllBytes();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(errPrefix + ": " + e.getMessage());
        }
    }

    private static String writeJson(Object o) {
        if (o == null) return null;
        try {
            return MAPPER.writeValueAsString(o);
        } catch (Exception e) {
            return null;
        }
    }

    private static Object readJson(String s) {
        if (s == null || s.isEmpty()) return null;
        try {
            return MAPPER.readValue(s, Map.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static LocalDateTime parseWall(String s) {
        if (s == null || s.isEmpty()) return null;
        try {
            return OffsetDateTime.parse(s).toLocalDateTime();
        } catch (Exception e) {
            return null;
        }
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static long lng(Object o) {
        if (o instanceof Number n) return n.longValue();
        try {
            return o == null ? 0L : Long.parseLong(String.valueOf(o));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static void applyString(Map<String, Object> body, String key, java.util.function.Consumer<String> set) {
        if (body.containsKey(key)) set.accept(str(body.get(key)));
    }

    private static void applyBool(Map<String, Object> body, String key, java.util.function.Consumer<Boolean> set) {
        if (body.containsKey(key)) set.accept(Boolean.parseBoolean(String.valueOf(body.get(key))));
    }

    private static void applyLong(Map<String, Object> body, String key, java.util.function.Consumer<Long> set) {
        if (body.containsKey(key)) set.accept(lng(body.get(key)));
    }

    private static void applyInt(Map<String, Object> body, String key, java.util.function.Consumer<Integer> set) {
        if (body.containsKey(key)) set.accept((int) lng(body.get(key)));
    }

    private static void applyDecimal(Map<String, Object> body, String key,
                                     java.util.function.Consumer<BigDecimal> set) {
        if (!body.containsKey(key)) return;
        try {
            set.accept(new BigDecimal(String.valueOf(body.get(key))));
        } catch (NumberFormatException e) {
            throw new RuntimeException(key + " 必须是数字: " + body.get(key));
        }
    }

    /** 前端可能传数组也可能传逗号串，统一成 JSON 数组落库。 */
    private static void applyJsonArray(Map<String, Object> body, String key,
                                       java.util.function.Consumer<String> set) {
        if (!body.containsKey(key)) return;
        Object v = body.get(key);
        if (v == null) {
            set.accept(null);
            return;
        }
        if (v instanceof List<?> list) {
            set.accept(writeJson(list));
            return;
        }
        List<String> parts = new ArrayList<>();
        for (String s : String.valueOf(v).split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) parts.add(t);
        }
        set.accept(parts.isEmpty() ? null : writeJson(parts));
    }

    /** 把落库的 JSON 数组还原成逗号串，供下发给 agent 的 config 使用。 */
    public static String jsonArrayToCsv(String json) {
        if (json == null || json.isEmpty()) return "";
        try {
            List<?> list = MAPPER.readValue(json, List.class);
            StringBuilder sb = new StringBuilder();
            for (Object o : list) {
                if (sb.length() > 0) sb.append(',');
                sb.append(o);
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
