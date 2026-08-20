package com.synctask.service;

import com.google.gson.Gson;
import com.synctask.entity.DataClassification;
import com.synctask.entity.SchemaChangeRequest;
import com.synctask.entity.Workflow;
import com.synctask.repository.SchemaChangeRequestRepository;
import com.synctask.repository.WorkflowRepository;
import com.synctask.util.JdbcConnections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Schema 演进审批。
 *
 * <h3>它把什么补齐了</h3>
 * <p>DDL 策略的 {@code MANUAL} 档此前只是"停下来记一条日志"——运维得自己去目标库
 * 手工执行，而平台<b>不知道执行了没有</b>，也没人能事后说清那条 DDL 是谁批的。
 * 这里把它变成：停下来 → 建审批单（带风险标注）→ 通过后由平台应用并留痕。
 *
 * <h3>风险标注来自另外两块能力</h3>
 * <ul>
 *   <li><b>受影响的列</b>来自血缘——审批人真正要看的是"这条 DDL 会波及下游哪些字段"，
 *       而不是一句 SQL；</li>
 *   <li><b>敏感级别</b>来自分级——涉及 RESTRICTED 列的变更应当被更谨慎地看待。</li>
 * </ul>
 * 这也是设计文档把审批排在血缘与分级之后的原因：单独做审批只能给出一句 SQL。
 */
@Service
public class SchemaApprovalService {

    private static final Logger logger = LoggerFactory.getLogger(SchemaApprovalService.class);
    private static final Gson GSON = new Gson();

    @Autowired
    private SchemaChangeRequestRepository requestRepo;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private LineageService lineageService;

    @Autowired
    private ClassificationService classificationService;

    @Autowired
    private MetadataService metadataService;

    /**
     * 任务目录的根。
     *
     * <p>后端跑在 {@code java-backend/} 下（{@code mvn spring-boot:run} 的工作目录），
     * 而任务目录在<b>仓库根</b>的 {@code files/}——直接写 {@code "files/..."} 会解析到
     * {@code java-backend/files/}，找不到任何东西且不报错，表现为"同步了 0 条"。
     * 容器里工作目录又是 {@code /app}、任务目录是 {@code /app/files}。
     * 因此做成可配 + 两种布局都试。
     */
    @org.springframework.beans.factory.annotation.Value("${app.task.files.dir:}")
    private String taskFilesDir;

    /** 定位某任务的待审批 DDL 文件；两种布局都找不到时返回不存在的那个，调用方按"无文件"处理。 */
    private File pendingFileOf(String workflowId) {
        if (taskFilesDir != null && !taskFilesDir.isBlank()) {
            return new File(taskFilesDir, workflowId + "/schema_pending.jsonl");
        }
        File up = new File("../files/" + workflowId + "/schema_pending.jsonl");
        if (up.isFile()) {
            return up;
        }
        return new File("files/" + workflowId + "/schema_pending.jsonl");
    }

    // ================================================================ 采集

    /**
     * 把引擎写下的待审批记录同步成审批单。
     *
     * <p>幂等：按 {@code (workflowId, seqno)} 去重。引擎重启会重放这段 THL、
     * 重复写同一条待审批记录，不去重就会把同一条 DDL 反复建成新单。
     *
     * @return 新建的单数
     */
    @Transactional
    public int syncPending(String workflowId) {
        File f = pendingFileOf(workflowId);
        if (!f.isFile()) {
            return 0;
        }
        Workflow wf = workflowRepository.findById(workflowId).orElse(null);
        int created = 0;
        try (BufferedReader r = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) continue;
                Map<?, ?> m;
                try {
                    m = GSON.fromJson(line, Map.class);
                } catch (Exception e) {
                    // 半行（引擎写到一半崩了）：跳过，重启重放会重新写完整的一条
                    continue;
                }
                if (m == null) continue;
                Long seqno = m.get("seqno") instanceof Number ? ((Number) m.get("seqno")).longValue() : null;
                if (seqno != null && requestRepo.findByWorkflowIdAndSeqno(workflowId, seqno).isPresent()) {
                    continue;
                }
                SchemaChangeRequest req = new SchemaChangeRequest();
                req.setWorkflowId(workflowId);
                req.setSeqno(seqno);
                req.setEventId(str(m.get("eventId")));
                req.setDdlType(str(m.get("ddlType")));
                req.setDbName(str(m.get("db")));
                req.setTableName(str(m.get("table")));
                req.setDdlSql(str(m.get("sql")));
                req.setStatus(SchemaChangeRequest.Status.PENDING);
                annotateRisk(req, wf);
                try {
                    requestRepo.save(req);
                    created++;
                } catch (Exception dup) {
                    // 唯一键冲突 = 并发同步，忽略
                }
            }
        } catch (Exception e) {
            logger.warn("同步待审批 DDL 失败 taskId={}: {}", workflowId, e.getMessage());
        }
        return created;
    }

    /**
     * 给审批单标注风险：受影响的下游列（血缘）＋ 最高敏感级别（分级）。
     *
     * <p>标注失败不阻断建单——没有风险标注的单仍然比没有单好。
     */
    private void annotateRisk(SchemaChangeRequest req, Workflow wf) {
        try {
            if (req.getDbName() == null || req.getTableName() == null) {
                return;
            }
            // 这条 DDL 动到的列。粗解析即可——目的是给审批人一个范围，不是精确语义分析
            Set<String> cols = columnsInDdl(req.getDdlSql());
            if (cols.isEmpty() && wf != null) {
                return;
            }
            List<String> affected = new ArrayList<>();
            for (String c : cols) {
                LineageService.ImpactResult impact =
                        lineageService.impact(req.getDbName(), req.getTableName(), c, 3);
                affected.addAll(impact.downstream);
            }
            if (!affected.isEmpty()) {
                String joined = String.join(", ", affected);
                req.setAffectedColumns(joined.length() > 1000 ? joined.substring(0, 1000) + "…" : joined);
            }
            DataClassification.Level max =
                    classificationService.maxLevelOf(req.getDbName(), req.getTableName(), cols);
            req.setMaxLevel(max);
        } catch (Exception e) {
            logger.debug("标注审批单风险失败（不阻断）: {}", e.getMessage());
        }
    }

    /** 从 DDL 里粗取涉及的列名。 */
    static Set<String> columnsInDdl(String sql) {
        Set<String> out = new LinkedHashSet<>();
        if (sql == null) return out;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "(?i)\\b(?:ADD|DROP|MODIFY|CHANGE|ALTER)\\s+(?:COLUMN\\s+)?`?([A-Za-z_][A-Za-z0-9_$]*)`?")
                .matcher(sql);
        while (m.find()) {
            String c = m.group(1);
            // ALTER TABLE 里的 TABLE 关键字会被上面的正则捞到，剔掉
            if (!"TABLE".equalsIgnoreCase(c) && !"COLUMN".equalsIgnoreCase(c)) {
                out.add(c);
            }
        }
        return out;
    }

    // ================================================================ 审批

    public List<SchemaChangeRequest> listPending() {
        return requestRepo.findByStatusOrderByCreatedAtDesc(SchemaChangeRequest.Status.PENDING);
    }

    public List<SchemaChangeRequest> listByWorkflow(String workflowId) {
        return requestRepo.findByWorkflowIdOrderByCreatedAtDesc(workflowId);
    }

    /**
     * 批准并<b>立即应用</b>到目标库。
     *
     * <p>为什么由平台应用而不是让人去手工执行：MANUAL 档原本的做法就是人工执行，
     * 而那正是问题——平台不知道执行了没有、执行的是不是这一条、谁执行的。
     * 由平台应用之后，"批准"与"生效"是同一个动作，审计链是完整的。
     *
     * <p>应用失败不回退成 PENDING：失败的事实要留在单子上（FAILED + 错误信息），
     * 否则重试的人看不到上一次为什么失败。
     */
    @Transactional
    public SchemaChangeRequest approve(Long id, Long reviewerId, String comment) {
        SchemaChangeRequest req = requestRepo.findById(id)
                .orElseThrow(() -> new RuntimeException("审批单不存在: " + id));
        if (req.getStatus() != SchemaChangeRequest.Status.PENDING) {
            throw new RuntimeException("只能审批 PENDING 状态的单，当前: " + req.getStatus());
        }
        req.setReviewerId(reviewerId);
        req.setReviewComment(comment);
        req.setReviewedAt(LocalDateTime.now());
        req.setStatus(SchemaChangeRequest.Status.APPROVED);

        Workflow wf = workflowRepository.findById(req.getWorkflowId()).orElse(null);
        if (wf == null) {
            req.setStatus(SchemaChangeRequest.Status.FAILED);
            req.setErrorMessage("任务不存在，无法应用");
            return requestRepo.save(req);
        }
        try {
            applyToTarget(wf, req);
            req.setStatus(SchemaChangeRequest.Status.APPLIED);
            req.setAppliedAt(LocalDateTime.now());
            logger.info("Schema 变更已应用: task={} seqno={} sql={}",
                    req.getWorkflowId(), req.getSeqno(), abbreviate(req.getDdlSql()));
        } catch (Exception e) {
            req.setStatus(SchemaChangeRequest.Status.FAILED);
            req.setErrorMessage(abbreviate(e.getMessage()));
            logger.warn("Schema 变更应用失败: task={} err={}", req.getWorkflowId(), e.getMessage());
        }
        return requestRepo.save(req);
    }

    @Transactional
    public SchemaChangeRequest reject(Long id, Long reviewerId, String comment) {
        SchemaChangeRequest req = requestRepo.findById(id)
                .orElseThrow(() -> new RuntimeException("审批单不存在: " + id));
        if (req.getStatus() != SchemaChangeRequest.Status.PENDING) {
            throw new RuntimeException("只能审批 PENDING 状态的单，当前: " + req.getStatus());
        }
        req.setStatus(SchemaChangeRequest.Status.REJECTED);
        req.setReviewerId(reviewerId);
        req.setReviewComment(comment);
        req.setReviewedAt(LocalDateTime.now());
        return requestRepo.save(req);
    }

    /**
     * 把 DDL 应用到目标库。
     *
     * <p>用<b>目标库的连接</b>而不是源库——审批放行的是"目标端要不要跟着变"。
     * 库名按任务的映射解析：源库叫什么与目标库叫什么可以不同。
     */
    private void applyToTarget(Workflow wf, SchemaChangeRequest req) throws Exception {
        MetadataService.ParsedConnection conn =
                metadataService.parseConnection(wf.getTargetConnection());
        String targetDb = wf.getTargetDbName() != null && !wf.getTargetDbName().isEmpty()
                ? wf.getTargetDbName() : req.getDbName();

        boolean isPg = conn.isPostgresql();
        String url = isPg
                ? JdbcConnections.postgresUrl(conn.host, conn.port, targetDb)
                : JdbcConnections.mysqlUrl(conn.host, conn.port, targetDb);
        java.util.Properties props = isPg
                ? JdbcConnections.postgresProps(conn.username, conn.password, null)
                : JdbcConnections.mysqlProps(conn.username, conn.password, null);

        try (Connection c = java.sql.DriverManager.getConnection(url, props);
             Statement st = c.createStatement()) {
            st.execute(req.getDdlSql());
        }
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static String abbreviate(String s) {
        if (s == null) return null;
        return s.length() > 1000 ? s.substring(0, 1000) + "…" : s;
    }
}
