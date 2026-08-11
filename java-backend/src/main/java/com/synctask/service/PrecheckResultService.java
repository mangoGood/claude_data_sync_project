package com.synctask.service;

import com.synctask.entity.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 启动前预检结果的留档。
 *
 * <p>为什么值得单独有一张表：预检此前只在前端弹窗里跑，点"确定"就过去了，
 * 既不阻断也不留痕。事后没有任何办法回答"这个任务当初是在什么前提下启动的"、
 * "是谁在什么时候忽略了哪条 FAIL"。留档之后这两个问题才有答案。
 *
 * <p>用 {@link JdbcTemplate} 而不是 JPA 实体：后端开着 {@code ddl-auto: validate}，
 * 加实体就得同步维护映射与类型仲裁，而这张表只写不改、只按 workflow 查——不值。
 */
@Service
public class PrecheckResultService {

    private static final Logger logger = LoggerFactory.getLogger(PrecheckResultService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final com.google.gson.Gson gson = new com.google.gson.Gson();

    /**
     * 记一条。<b>失败只 warn</b>：留档本身绝不该把启动带崩——
     * 它是审计手段，不是业务前置条件。
     */
    public void record(Workflow workflow, String overall, boolean forced, String summary, Object checks) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO task_precheck_results "
                            + "(workflow_id, user_id, overall, forced, summary, checks_json, created_at) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    workflow.getId(), workflow.getUserId(), overall, forced ? 1 : 0,
                    truncate(summary, 1000), checks == null ? null : gson.toJson(checks),
                    // 时间戳一律 JVM 侧绑定：元数据库跑在容器里（UTC）而后端按 JVM 时区读，
                    // 用 SQL NOW() 会差 8 小时（V8 的故障转移就栽在这上面）
                    new Timestamp(System.currentTimeMillis()));
        } catch (Exception e) {
            logger.warn("预检结果留档失败（不阻断启动）: {}", e.getMessage());
        }
    }

    /** 某任务的预检历史，最新在前。 */
    public List<Map<String, Object>> history(String workflowId, int limit) {
        return jdbcTemplate.query(
                "SELECT id, overall, forced, summary, checks_json, created_at "
                        + "FROM task_precheck_results WHERE workflow_id = ? ORDER BY id DESC LIMIT ?",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", rs.getLong("id"));
                    m.put("overall", rs.getString("overall"));
                    m.put("forced", rs.getBoolean("forced"));
                    m.put("summary", rs.getString("summary"));
                    m.put("checks", rs.getString("checks_json"));
                    Timestamp ts = rs.getTimestamp("created_at");
                    m.put("createdAt", ts == null ? null : ts.toString());
                    return m;
                }, workflowId, limit);
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
