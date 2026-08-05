package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 中心位点的读取、历史与重置（B3）。
 *
 * <p>三件事：
 * <ol>
 *   <li><b>降级可读</b>：agent 挂了的时候，位点视图不该跟着一起瞎——那恰恰是最需要知道
 *       "还能不能续、续到哪"的时刻。中心库里的位点即使旧几秒，也远好过一个 500。</li>
 *   <li><b>历史</b>：{@code task_checkpoint_history} 是位点回溯的唯一数据来源。</li>
 *   <li><b>重置</b>：唯一允许位点倒退的路径，且必须留审计（谁、什么时候、从哪退到哪）。</li>
 * </ol>
 */
@Service
public class CheckpointCentralService {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointCentralService.class);

    /** 只有停下来的任务才允许重置位点：跑着的任务本地位点还在推进，改中心库纯属自欺欺人。 */
    private static final Set<WorkflowStatus> RESETTABLE = EnumSet.of(
            WorkflowStatus.PAUSED, WorkflowStatus.FAILED, WorkflowStatus.CONFIGURING);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${checkpoint.history.retention.hours:72}")
    private int historyRetentionHours;

    /** 当前位点（每条链路段一行）。 */
    public List<Map<String, Object>> currentPositions(String taskId) {
        return jdbcTemplate.query(
                "SELECT stage, stream_key, engine, kind, payload, monotonic_key, source_ts, " +
                "       agent_id, lease_epoch, updated_at, reset_at " +
                "  FROM task_checkpoints WHERE task_id=? ORDER BY stage, stream_key",
                (rs, i) -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("stage", rs.getString("stage"));
                    row.put("streamKey", rs.getString("stream_key"));
                    row.put("engine", rs.getString("engine"));
                    row.put("kind", rs.getString("kind"));
                    row.put("payload", rs.getString("payload"));
                    row.put("monotonicKey", rs.getLong("monotonic_key"));
                    row.put("sourceTs", millis(rs.getTimestamp("source_ts")));
                    row.put("agentId", rs.getString("agent_id"));
                    row.put("leaseEpoch", rs.getInt("lease_epoch"));
                    row.put("updatedAt", millis(rs.getTimestamp("updated_at")));
                    row.put("resetAt", millis(rs.getTimestamp("reset_at")));
                    return row;
                }, taskId);
    }

    /**
     * agent 不可达时的降级位点视图。
     *
     * <p>字段刻意与 agent 的 {@code /api/checkpoint/{taskId}} 对齐（binlog / checkpoint 两段），
     * 前端不必为降级路径写第二套渲染；用 {@code source=central} 与 {@code degraded=true}
     * 标明这是"中心库里几秒前的快照"而不是实时值——把降级说清楚比装作正常重要得多。
     */
    public Map<String, Object> degradedVisualization(String taskId, String agentError) {
        List<Map<String, Object>> positions = currentPositions(taskId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", taskId);
        result.put("timestamp", System.currentTimeMillis());
        result.put("source", "central");
        result.put("degraded", true);
        result.put("degradedReason", agentError);
        result.put("positions", positions);

        Map<String, Object> binlog = new LinkedHashMap<>();
        binlog.put("available", false);
        Map<String, Object> checkpoint = new LinkedHashMap<>();
        checkpoint.put("available", false);

        for (Map<String, Object> p : positions) {
            Map<String, String> payload = parsePayload((String) p.get("payload"));
            if ("CAPTURE".equals(p.get("stage"))) {
                String file = payload.get("binlog.file");
                String pos = payload.get("binlog.position");
                if (file != null && pos != null) {
                    binlog.put("file", file);
                    binlog.put("position", parseLong(pos));
                    binlog.put("raw", file + ":" + pos);
                    binlog.put("available", true);
                }
                if (payload.get("gtid.set") != null) {
                    binlog.put("gtid", payload.get("gtid.set"));
                }
                if (payload.get("wal.lsn") != null) {
                    binlog.put("lsn", payload.get("wal.lsn"));
                    binlog.put("available", true);
                }
                if (payload.get("redo.scn") != null) {
                    binlog.put("scn", payload.get("redo.scn"));
                    binlog.put("available", true);
                }
                binlog.put("updatedAt", p.get("updatedAt"));
            } else if ("APPLY".equals(p.get("stage"))) {
                checkpoint.put("seqno", parseLong(payload.get("seqno")));
                checkpoint.put("binlog_file", payload.getOrDefault("binlog.file", ""));
                checkpoint.put("binlog_position", parseLong(payload.get("binlog.position")));
                checkpoint.put("event_id", payload.getOrDefault("event.id", ""));
                checkpoint.put("updated_at", String.valueOf(p.get("updatedAt")));
                checkpoint.put("available", true);
            }
        }
        result.put("binlog", binlog);
        result.put("checkpoint", checkpoint);
        Map<String, Object> thl = new LinkedHashMap<>();
        thl.put("available", false);
        result.put("thl", thl);
        // 用 LinkedHashMap 而不是 Map.of：这里的值必须能是 null（"未知"与"0"是两回事，
        // 前端据此显示 "--" 而不是"无积压"），而 Map.of 不接受 null 值
        Map<String, Object> gaps = new LinkedHashMap<>();
        gaps.put("pending_events", null);
        gaps.put("pending_apply", null);
        gaps.put("pendingApplyStatus", "UNKNOWN");
        result.put("gaps", gaps);
        result.put("linkStatus", positions.isEmpty() ? "UNKNOWN" : "AGENT_UNREACHABLE");
        return result;
    }

    /** 位点历史（最新在前）。 */
    public List<Map<String, Object>> history(String taskId, String stage, int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT id, stage, stream_key, engine, kind, payload, monotonic_key, source_ts, " +
                "       recorded_at, reason, operator FROM task_checkpoint_history WHERE task_id=?");
        List<Object> args = new ArrayList<>();
        args.add(taskId);
        if (stage != null && !stage.isEmpty()) {
            sql.append(" AND stage=?");
            args.add(stage);
        }
        sql.append(" ORDER BY recorded_at DESC, id DESC LIMIT ").append(Math.max(1, Math.min(limit, 500)));
        return jdbcTemplate.query(sql.toString(), (rs, i) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", rs.getLong("id"));
            row.put("stage", rs.getString("stage"));
            row.put("streamKey", rs.getString("stream_key"));
            row.put("engine", rs.getString("engine"));
            row.put("kind", rs.getString("kind"));
            row.put("payload", rs.getString("payload"));
            row.put("monotonicKey", rs.getLong("monotonic_key"));
            row.put("sourceTs", millis(rs.getTimestamp("source_ts")));
            row.put("recordedAt", millis(rs.getTimestamp("recorded_at")));
            row.put("reason", rs.getString("reason"));
            row.put("operator", rs.getString("operator"));
            return row;
        }, args.toArray());
    }

    /**
     * 把某一段的位点重置到历史上的某个点。
     *
     * <p><b>这是全平台唯一允许位点倒退的入口</b>，因此三件事一件都不能少：
     * ① 任务必须已经停下来（跑着的任务本地位点还在推进，改中心库没有意义）；
     * ② 落一条 {@code reason=RESET} 的审计，记下操作人；
     * ③ 打上 {@code reset_at}——否则 agent 看到"本地有位点"就走同机重启分支，重置永远不生效。
     *
     * @param target {@code {type: HISTORY_ID|TIMESTAMP|RAW, value: ...}}
     * @return 重置后的位点描述
     */
    public Map<String, Object> reset(Workflow workflow, String stage, String streamKey,
                                     Map<String, Object> target, String operator) {
        if (!RESETTABLE.contains(workflow.getStatus())) {
            throw new IllegalStateException("只有已暂停/已失败/配置中的任务才能重置位点，当前状态: "
                    + workflow.getStatus());
        }
        String taskId = workflow.getId();
        String key = (streamKey == null || streamKey.isEmpty()) ? "-" : streamKey;
        String type = String.valueOf(target.getOrDefault("type", "HISTORY_ID")).toUpperCase();
        Object value = target.get("value");

        Map<String, Object> picked;
        switch (type) {
            case "HISTORY_ID":
                picked = pickHistoryById(taskId, parseLong(String.valueOf(value)));
                break;
            case "TIMESTAMP":
                picked = pickHistoryAtOrBefore(taskId, stage, key, parseLong(String.valueOf(value)));
                break;
            case "RAW":
                picked = rawTarget(taskId, stage, key, value);
                break;
            default:
                throw new IllegalArgumentException("不支持的重置目标类型: " + type);
        }
        if (picked == null) {
            throw new IllegalStateException("没有找到可用于重置的历史位点（stage=" + stage + "）");
        }

        Timestamp now = new Timestamp(System.currentTimeMillis());
        // 先留审计再改现值：顺序反了的话，改到一半失败就没人知道原来是什么
        jdbcTemplate.update(
                "INSERT INTO task_checkpoint_history " +
                "(task_id, stage, stream_key, engine, kind, payload, monotonic_key, source_ts, recorded_at, reason, operator) " +
                "VALUES (?,?,?,?,?,?,?,?,?,'RESET',?)",
                taskId, stage, key, picked.get("engine"), picked.get("kind"), picked.get("payload"),
                picked.get("monotonicKey"), null, now, operator);

        int updated = jdbcTemplate.update(
                "UPDATE task_checkpoints SET engine=?, kind=?, payload=?, monotonic_key=?, " +
                "       updated_at=?, reset_at=? WHERE task_id=? AND stage=? AND stream_key=?",
                picked.get("engine"), picked.get("kind"), picked.get("payload"),
                picked.get("monotonicKey"), now, now, taskId, stage, key);
        if (updated == 0) {
            jdbcTemplate.update(
                    "INSERT INTO task_checkpoints " +
                    "(task_id, stage, stream_key, engine, kind, payload, monotonic_key, agent_id, lease_epoch, updated_at, reset_at) " +
                    "VALUES (?,?,?,?,?,?,?,'-',0,?,?)",
                    taskId, stage, key, picked.get("engine"), picked.get("kind"), picked.get("payload"),
                    picked.get("monotonicKey"), now, now);
        }
        logger.warn("[{}] 位点已被 {} 重置: stage={} kind={} key={}", taskId, operator, stage,
                picked.get("kind"), picked.get("monotonicKey"));

        Map<String, Object> result = new LinkedHashMap<>(picked);
        result.put("stage", stage);
        result.put("streamKey", key);
        result.put("resetAt", now.getTime());
        result.put("operator", operator);
        return result;
    }

    private Map<String, Object> pickHistoryById(String taskId, long historyId) {
        List<Map<String, Object>> rows = jdbcTemplate.query(
                "SELECT engine, kind, payload, monotonic_key FROM task_checkpoint_history " +
                " WHERE id=? AND task_id=?",
                (rs, i) -> row(rs.getString("engine"), rs.getString("kind"),
                        rs.getString("payload"), rs.getLong("monotonic_key")),
                historyId, taskId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Map<String, Object> pickHistoryAtOrBefore(String taskId, String stage, String streamKey, long ts) {
        List<Map<String, Object>> rows = jdbcTemplate.query(
                "SELECT engine, kind, payload, monotonic_key FROM task_checkpoint_history " +
                " WHERE task_id=? AND stage=? AND stream_key=? AND recorded_at <= ? " +
                " ORDER BY recorded_at DESC, id DESC LIMIT 1",
                (rs, i) -> row(rs.getString("engine"), rs.getString("kind"),
                        rs.getString("payload"), rs.getLong("monotonic_key")),
                taskId, stage, streamKey, new Timestamp(ts));
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * 直接给一段 payload 文本。逃生通道：历史采样没覆盖到想要的那个点时用，
     * 由操作人自己对位点负责，因此 {@code monotonic_key} 一律置 0（不参与单调守卫）。
     */
    private Map<String, Object> rawTarget(String taskId, String stage, String streamKey, Object value) {
        if (value == null || String.valueOf(value).trim().isEmpty()) {
            throw new IllegalArgumentException("RAW 重置必须给出 payload 文本");
        }
        List<Map<String, Object>> current = jdbcTemplate.query(
                "SELECT engine, kind FROM task_checkpoints WHERE task_id=? AND stage=? AND stream_key=?",
                (rs, i) -> row(rs.getString("engine"), rs.getString("kind"), null, 0L),
                taskId, stage, streamKey);
        String engine = current.isEmpty() ? "" : String.valueOf(current.get(0).get("engine"));
        String kind = current.isEmpty() ? "BINLOG_FILE_POS" : String.valueOf(current.get(0).get("kind"));
        return row(engine, kind, String.valueOf(value), 0L);
    }

    private static Map<String, Object> row(String engine, String kind, String payload, long monotonicKey) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("engine", engine);
        m.put("kind", kind);
        m.put("payload", payload);
        m.put("monotonicKey", monotonicKey);
        return m;
    }

    /**
     * 清理过期的位点历史。
     *
     * <p>只清 {@code SAMPLE}：RESET 与 FAILOVER 是审计记录，"位点是什么时候被谁动过的"
     * 这类问题往往在很久以后才被问起，按采样数据的保留期一起删掉就再也查不到了。
     */
    @Scheduled(fixedDelay = 3600_000L, initialDelay = 300_000L)
    public void cleanupHistory() {
        try {
            Timestamp before = new Timestamp(
                    System.currentTimeMillis() - historyRetentionHours * 3600_000L);
            int deleted = jdbcTemplate.update(
                    "DELETE FROM task_checkpoint_history WHERE reason='SAMPLE' AND recorded_at < ?", before);
            if (deleted > 0) {
                logger.info("清理过期位点采样 {} 条（保留 {} 小时）", deleted, historyRetentionHours);
            }
        } catch (Exception e) {
            // 巡检类代码一律吞异常：清理失败最多是表大一点，绝不能把调度线程打死
            logger.warn("清理位点历史失败: {}", e.getMessage());
        }
    }

    private static Map<String, String> parsePayload(String text) {
        Map<String, String> out = new LinkedHashMap<>();
        if (text == null) {
            return out;
        }
        java.util.Properties p = new java.util.Properties();
        try {
            p.load(new java.io.StringReader(text));
        } catch (Exception ignored) {
            return out;
        }
        for (String name : p.stringPropertyNames()) {
            out.put(name, p.getProperty(name));
        }
        return out;
    }

    private static Long millis(Timestamp ts) {
        return ts == null ? null : ts.getTime();
    }

    private static long parseLong(String v) {
        try {
            return Long.parseLong(v.trim());
        } catch (Exception e) {
            return 0L;
        }
    }
}
