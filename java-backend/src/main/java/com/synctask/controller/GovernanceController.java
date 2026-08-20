package com.synctask.controller;

import com.synctask.audit.Audited;
import com.synctask.entity.AuditLog;
import com.synctask.entity.ClassificationRule;
import com.synctask.entity.DataClassification;
import com.synctask.entity.SchemaChangeRequest;
import com.synctask.entity.Workflow;
import com.synctask.repository.WorkflowRepository;
import com.synctask.security.UserPrincipal;
import com.synctask.service.ClassificationService;
import com.synctask.service.LineageService;
import com.synctask.service.SchemaApprovalService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * 数据治理：字段级血缘、数据分级与标签、Schema 演进审批。
 *
 * <p>三块放在一个 controller 里是因为它们是<b>互相依赖的一套</b>，
 * 而不是三个独立功能：分级的策略校验与审批的风险标注都要读血缘图。
 */
@RestController
@RequestMapping("/api/governance")
public class GovernanceController {

    private static final Logger logger = LoggerFactory.getLogger(GovernanceController.class);

    @Autowired
    private LineageService lineageService;

    @Autowired
    private ClassificationService classificationService;

    @Autowired
    private SchemaApprovalService schemaApprovalService;

    @Autowired
    private WorkflowRepository workflowRepository;

    private static Map<String, Object> ok(Object data) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("success", true);
        m.put("data", data);
        return m;
    }

    private static Map<String, Object> fail(String msg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("success", false);
        m.put("message", msg);
        return m;
    }

    private static Long userIdOf(Authentication auth) {
        return (auth != null && auth.getPrincipal() instanceof UserPrincipal p) ? p.getId() : null;
    }

    // ================================================================ 血缘

    /** 按任务配置重建血缘。 */
    @PostMapping("/lineage/rebuild/{workflowId}")
    @Audited(value = AuditLog.Action.REBUILD_LINEAGE, idArg = 0)
    public ResponseEntity<?> rebuildLineage(@PathVariable String workflowId) {
        Workflow wf = workflowRepository.findById(workflowId).orElse(null);
        if (wf == null) {
            return ResponseEntity.badRequest().body(fail("任务不存在: " + workflowId));
        }
        int edges = lineageService.rebuild(wf);
        return ResponseEntity.ok(ok(Map.of("workflowId", workflowId, "edges", edges)));
    }

    /**
     * 影响面分析：改一个源列，下游哪些列会受影响。
     *
     * <p>这是血缘最主要的用途，也是运维最常问的那个问题。
     */
    @GetMapping("/lineage/impact")
    public ResponseEntity<?> impact(@RequestParam String db,
                                    @RequestParam String table,
                                    @RequestParam String column,
                                    @RequestParam(defaultValue = "5") int depth) {
        LineageService.ImpactResult r = lineageService.impact(db, table, column, depth);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("origin", r.origin);
        m.put("downstream", r.downstream);
        m.put("downstreamByHop", r.downstreamByHop);
        m.put("operators", r.operators);
        m.put("hasMask", r.hasMask);
        if (r.hasMask) {
            m.put("note", "路径上存在脱敏：下游那些列不参与内容对比");
        }
        return ResponseEntity.ok(ok(m));
    }

    /** 上游追溯：这一列的数据从哪来。 */
    @GetMapping("/lineage/upstream")
    public ResponseEntity<?> upstream(@RequestParam String db,
                                      @RequestParam String table,
                                      @RequestParam String column,
                                      @RequestParam(defaultValue = "5") int depth) {
        LineageService.ImpactResult r = lineageService.upstream(db, table, column, depth);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("origin", r.origin);
        m.put("upstream", r.downstream);
        m.put("upstreamByHop", r.downstreamByHop);
        m.put("operators", r.operators);
        return ResponseEntity.ok(ok(m));
    }

    /** 某任务的血缘图（供前端画有向图）。 */
    @GetMapping("/lineage/graph/{workflowId}")
    public ResponseEntity<?> graph(@PathVariable String workflowId) {
        return ResponseEntity.ok(ok(lineageService.graphOf(workflowId)));
    }

    // ================================================================ 分级

    @GetMapping("/classification")
    public ResponseEntity<?> listClassification(@RequestParam String db, @RequestParam String table) {
        return ResponseEntity.ok(ok(classificationService.listByTable(db, table)));
    }

    /** 人工打标。人工值优先级高于规则，重跑规则不会覆盖它。 */
    @PostMapping("/classification")
    @Audited(AuditLog.Action.UPDATE_CLASSIFICATION)
    public ResponseEntity<?> classify(@RequestBody Map<String, Object> body, Authentication auth) {
        try {
            DataClassification.Level level = DataClassification.Level.valueOf(
                    String.valueOf(body.get("level")).toUpperCase());
            DataClassification c = classificationService.classify(
                    String.valueOf(body.get("db")),
                    String.valueOf(body.get("table")),
                    String.valueOf(body.get("column")),
                    level,
                    body.get("tags") == null ? null : String.valueOf(body.get("tags")),
                    userIdOf(auth));
            return ResponseEntity.ok(ok(c));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(fail("级别非法，可选 PUBLIC/INTERNAL/SENSITIVE/RESTRICTED"));
        }
    }

    /** 按规则自动打标。默认不覆盖人工值。 */
    @PostMapping("/classification/apply-rules")
    @Audited(AuditLog.Action.UPDATE_CLASSIFICATION)
    public ResponseEntity<?> applyRules(@RequestBody Map<String, Object> body) {
        Workflow wf = workflowRepository.findById(String.valueOf(body.get("workflowId"))).orElse(null);
        if (wf == null) {
            return ResponseEntity.badRequest().body(fail("任务不存在"));
        }
        boolean overwrite = Boolean.TRUE.equals(body.get("overwriteManual"));
        int n = classificationService.applyRules(wf.getSourceConnection(),
                String.valueOf(body.get("db")), String.valueOf(body.get("table")), overwrite);
        return ResponseEntity.ok(ok(Map.of("classified", n)));
    }

    @GetMapping("/classification/rules")
    public ResponseEntity<?> listRules() {
        return ResponseEntity.ok(ok(classificationService.listRules()));
    }

    @PostMapping("/classification/rules")
    @Audited(AuditLog.Action.UPDATE_CLASSIFICATION)
    public ResponseEntity<?> saveRule(@RequestBody ClassificationRule rule) {
        try {
            return ResponseEntity.ok(ok(classificationService.saveRule(rule)));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    @DeleteMapping("/classification/rules/{id}")
    @Audited(value = AuditLog.Action.UPDATE_CLASSIFICATION, idArg = 0)
    public ResponseEntity<?> deleteRule(@PathVariable Long id) {
        classificationService.deleteRule(id);
        return ResponseEntity.ok(ok(null));
    }

    /**
     * 策略校验：目标端级别不得低于源端、RESTRICTED 列必须脱敏。
     * 这是分级与血缘、分级与脱敏接起来之后才能给出的结论。
     */
    @GetMapping("/classification/policy/{workflowId}")
    public ResponseEntity<?> checkPolicy(@PathVariable String workflowId) {
        Workflow wf = workflowRepository.findById(workflowId).orElse(null);
        if (wf == null) {
            return ResponseEntity.badRequest().body(fail("任务不存在"));
        }
        ClassificationService.PolicyResult r = classificationService.checkPolicy(wf);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("hasViolation", r.hasViolation());
        m.put("downgrades", r.downgrades);
        m.put("unmaskedSensitive", r.unmaskedSensitive);
        m.put("sensitiveFlows", r.sensitiveFlows);
        return ResponseEntity.ok(ok(m));
    }

    /** 某级别的列都流到了哪里——合规审计最常问的那个。 */
    @GetMapping("/classification/flows")
    public ResponseEntity<?> flows(@RequestParam(defaultValue = "RESTRICTED") String level) {
        try {
            return ResponseEntity.ok(ok(classificationService.flowsOfLevel(
                    DataClassification.Level.valueOf(level.toUpperCase()))));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(fail("级别非法"));
        }
    }

    // ================================================================ Schema 审批

    /** 把引擎写下的待审批 DDL 同步成审批单（幂等）。 */
    @PostMapping("/schema-changes/sync/{workflowId}")
    public ResponseEntity<?> syncPending(@PathVariable String workflowId) {
        return ResponseEntity.ok(ok(Map.of("created", schemaApprovalService.syncPending(workflowId))));
    }

    @GetMapping("/schema-changes/pending")
    public ResponseEntity<?> listPending() {
        return ResponseEntity.ok(ok(schemaApprovalService.listPending()));
    }

    @GetMapping("/schema-changes/{workflowId}")
    public ResponseEntity<?> listByWorkflow(@PathVariable String workflowId) {
        return ResponseEntity.ok(ok(schemaApprovalService.listByWorkflow(workflowId)));
    }

    @PostMapping("/schema-changes/{id}/approve")
    @Audited(value = AuditLog.Action.APPROVE_SCHEMA_CHANGE, idArg = 0)
    public ResponseEntity<?> approve(@PathVariable Long id,
                                     @RequestBody(required = false) Map<String, Object> body,
                                     Authentication auth) {
        try {
            String comment = body == null ? null : String.valueOf(body.getOrDefault("comment", ""));
            SchemaChangeRequest r = schemaApprovalService.approve(id, userIdOf(auth), comment);
            return ResponseEntity.ok(ok(r));
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    @PostMapping("/schema-changes/{id}/reject")
    @Audited(value = AuditLog.Action.REJECT_SCHEMA_CHANGE, idArg = 0)
    public ResponseEntity<?> reject(@PathVariable Long id,
                                    @RequestBody(required = false) Map<String, Object> body,
                                    Authentication auth) {
        try {
            String comment = body == null ? null : String.valueOf(body.getOrDefault("comment", ""));
            SchemaChangeRequest r = schemaApprovalService.reject(id, userIdOf(auth), comment);
            return ResponseEntity.ok(ok(r));
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }
}
