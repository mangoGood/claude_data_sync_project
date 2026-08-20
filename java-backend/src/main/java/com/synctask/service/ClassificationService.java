package com.synctask.service;

import com.synctask.entity.ClassificationRule;
import com.synctask.entity.DataClassification;
import com.synctask.entity.LineageEdge;
import com.synctask.entity.LineageNode;
import com.synctask.entity.Workflow;
import com.synctask.repository.ClassificationRuleRepository;
import com.synctask.repository.DataClassificationRepository;
import com.synctask.repository.LineageEdgeRepository;
import com.synctask.repository.LineageNodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 数据分级与标签。
 *
 * <p>单独打标没多大用——价值在<b>与血缘联动</b>：
 * <ul>
 *   <li>"标了 RESTRICTED 的列流到了哪些目标"——合规问的就是这个</li>
 *   <li>"目标端的级别不能低于源端"——数据不该在流动中降级</li>
 * </ul>
 * 所以本服务依赖 {@link LineageService} 的图。
 */
@Service
public class ClassificationService {

    private static final Logger logger = LoggerFactory.getLogger(ClassificationService.class);

    @Autowired
    private DataClassificationRepository classRepo;

    @Autowired
    private ClassificationRuleRepository ruleRepo;

    @Autowired
    private LineageNodeRepository nodeRepo;

    @Autowired
    private LineageEdgeRepository edgeRepo;

    @Autowired
    private MetadataService metadataService;

    // ================================================================ 打标

    /** 人工打标。人工值优先级高于规则，重跑规则不会覆盖它。 */
    @Transactional
    public DataClassification classify(String db, String table, String column,
                                       DataClassification.Level level, String tags, Long userId) {
        DataClassification c = classRepo.findByDbNameAndTableNameAndColumnName(db, table, column)
                .orElseGet(DataClassification::new);
        c.setDbName(db);
        c.setTableName(table);
        c.setColumnName(column);
        c.setLevel(level);
        c.setTags(tags);
        c.setSource(DataClassification.Source.MANUAL);
        c.setMatchedRule(null);
        c.setUpdatedBy(userId);
        return classRepo.save(c);
    }

    /**
     * 按规则自动打标。
     *
     * @param overwriteManual 是否覆盖人工值。默认<b>不覆盖</b>——人工打标代表有人看过并作了判断，
     *                        被一条正则悄悄改掉是最糟的：既丢了判断，又没人知道它变了。
     * @return 新打/更新的列数
     */
    @Transactional
    public int applyRules(String connectionStr, String db, String table, boolean overwriteManual) {
        List<ClassificationRule> rules = ruleRepo.findByEnabledTrueOrderByPriorityAsc();
        if (rules.isEmpty()) {
            return 0;
        }
        List<Map<String, Object>> columns;
        try {
            columns = metadataService.listColumns(connectionStr, db, table);
        } catch (Exception e) {
            logger.warn("探查 {}.{} 的列失败，规则打标跳过: {}", db, table, e.getMessage());
            return 0;
        }

        int n = 0;
        for (Map<String, Object> col : columns) {
            Object nameObj = col.get("name");
            if (nameObj == null) continue;
            String name = String.valueOf(nameObj);

            Optional<DataClassification> existing =
                    classRepo.findByDbNameAndTableNameAndColumnName(db, table, name);
            if (existing.isPresent()
                    && existing.get().getSource() == DataClassification.Source.MANUAL
                    && !overwriteManual) {
                continue;
            }

            ClassificationRule hit = firstMatch(rules, name);
            if (hit == null) {
                continue;
            }
            DataClassification c = existing.orElseGet(DataClassification::new);
            c.setDbName(db);
            c.setTableName(table);
            c.setColumnName(name);
            c.setLevel(hit.getLevel());
            c.setTags(hit.getTags());
            c.setSource(DataClassification.Source.RULE);
            c.setMatchedRule(hit.getName());
            classRepo.save(c);
            n++;
        }
        return n;
    }

    private ClassificationRule firstMatch(List<ClassificationRule> rules, String columnName) {
        for (ClassificationRule r : rules) {
            try {
                if (Pattern.compile(r.getColumnPattern(), Pattern.CASE_INSENSITIVE)
                        .matcher(columnName).find()) {
                    return r;
                }
            } catch (PatternSyntaxException e) {
                // 一条规则的正则写坏了不该让整轮打标失败——跳过它，但要留痕
                logger.warn("分级规则 '{}' 的正则非法，已跳过: {}", r.getName(), e.getMessage());
            }
        }
        return null;
    }

    public DataClassification.Level levelOf(String db, String table, String column) {
        return classRepo.findByDbNameAndTableNameAndColumnName(db, table, column)
                .map(DataClassification::getLevel)
                .orElse(null);
    }

    // ================================================================ 与血缘联动的策略校验

    /** 一次策略校验的结果。 */
    public static class PolicyResult {
        /** 违规项：源端级别高于目标端 */
        public final List<String> downgrades = new ArrayList<>();
        /** 高敏列的流向：RESTRICTED/SENSITIVE 的列流到了哪里 */
        public final List<String> sensitiveFlows = new ArrayList<>();
        /** 高敏但未脱敏的列——最该被看见的那一类 */
        public final List<String> unmaskedSensitive = new ArrayList<>();

        public boolean hasViolation() {
            return !downgrades.isEmpty() || !unmaskedSensitive.isEmpty();
        }
    }

    /**
     * 按血缘图做策略校验。
     *
     * <p>三条策略：
     * <ol>
     *   <li><b>不得降级</b>：目标端列的级别不能低于源端。数据流动不该稀释它的敏感度——
     *       源端标了 RESTRICTED、目标端标成 PUBLIC，等于换个地方就不敏感了。</li>
     *   <li><b>高敏流向</b>：列出 SENSITIVE 及以上的列流到了哪些目标（不是违规，是给人看的）。</li>
     *   <li><b>高敏未脱敏</b>：RESTRICTED 的列流到目标端却没有 MASK 边——
     *       这是最值得拦的一类，也是分级与脱敏两块能力接起来之后才能发现的。</li>
     * </ol>
     */
    public PolicyResult checkPolicy(Workflow workflow) {
        PolicyResult r = new PolicyResult();
        for (LineageEdge e : edgeRepo.findByWorkflowId(workflow.getId())) {
            Optional<LineageNode> so = nodeRepo.findById(e.getSrcNodeId());
            Optional<LineageNode> to = nodeRepo.findById(e.getDstNodeId());
            if (so.isEmpty() || to.isEmpty()) continue;
            LineageNode s = so.get(), t = to.get();

            DataClassification.Level srcLevel =
                    levelOf(s.getDbName(), s.getTableName(), s.getColumnName());
            if (srcLevel == null) {
                continue;   // 源端没打标，无从判断
            }

            DataClassification.Level tgtLevel =
                    levelOf(t.getDbName(), t.getTableName(), t.getColumnName());
            if (tgtLevel != null && !tgtLevel.atLeast(srcLevel)) {
                r.downgrades.add(String.format("%s(%s) → %s(%s)",
                        s.qualifiedName(), srcLevel, t.qualifiedName(), tgtLevel));
            }

            if (srcLevel.atLeast(DataClassification.Level.SENSITIVE)) {
                r.sensitiveFlows.add(String.format("%s(%s) → %s",
                        s.qualifiedName(), srcLevel, t.qualifiedName()));
                if (srcLevel == DataClassification.Level.RESTRICTED
                        && e.getOperator() != LineageEdge.Operator.MASK
                        && e.getOperator() != LineageEdge.Operator.DROP) {
                    r.unmaskedSensitive.add(String.format("%s(RESTRICTED) → %s 未脱敏",
                            s.qualifiedName(), t.qualifiedName()));
                }
            }
        }
        return r;
    }

    /**
     * 某个级别的列都流到了哪里。合规审计最常问的那个问题。
     */
    public List<Map<String, Object>> flowsOfLevel(DataClassification.Level level) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (DataClassification c : classRepo.findByLevel(level)) {
            List<LineageNode> nodes = nodeRepo.findByDbNameAndTableNameAndColumnNameAndValidToSeqnoIsNull(
                    c.getDbName(), c.getTableName(), c.getColumnName());
            for (LineageNode n : nodes) {
                for (LineageEdge e : edgeRepo.findBySrcNodeId(n.getId())) {
                    nodeRepo.findById(e.getDstNodeId()).ifPresent(dst -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("column", c.qualifiedName());
                        m.put("level", c.getLevel().name());
                        m.put("tags", c.getTags());
                        m.put("target", dst.qualifiedName());
                        m.put("operator", e.getOperator().name());
                        m.put("masked", e.getOperator() == LineageEdge.Operator.MASK);
                        m.put("workflowId", e.getWorkflowId());
                        out.add(m);
                    });
                }
            }
        }
        return out;
    }

    /**
     * 仅按<b>列名</b>推断级别（不查已存表）。
     *
     * <p>给"库里还没有这一列"的场景用——最典型的就是一条 {@code ADD COLUMN} 的 DDL：
     * 新列按定义没有分级记录，但它的名字往往已经说明了一切。
     */
    public DataClassification.Level levelByName(String columnName) {
        ClassificationRule hit = firstMatch(ruleRepo.findByEnabledTrueOrderByPriorityAsc(), columnName);
        return hit == null ? null : hit.getLevel();
    }

    /**
     * 一次 DDL 涉及的列里最高的敏感级别。Schema 审批用它标注风险。
     *
     * <p>两级取值：先看<b>已存的分级记录</b>，没有再<b>按列名套规则</b>。
     * 回退不是将就——{@code ADD COLUMN id_card_ext} 这种新增列在库里必然没有分级记录，
     * 而"新增了一个名字像身份证的列"正是审批人最该看到的那条信息。
     * 只查已存记录的话，最需要提醒的那类变更反而一片空白。
     */
    public DataClassification.Level maxLevelOf(String db, String table, Collection<String> columns) {
        DataClassification.Level max = null;
        for (String col : columns) {
            DataClassification.Level l = levelOf(db, table, col);
            if (l == null) {
                l = levelByName(col);
            }
            if (l != null && (max == null || l.atLeast(max))) {
                max = l;
            }
        }
        return max;
    }

    // ================================================================ 规则维护

    public List<ClassificationRule> listRules() {
        return ruleRepo.findAll();
    }

    @Transactional
    public ClassificationRule saveRule(ClassificationRule rule) {
        // 先验一次正则：让规则在保存时就报错，而不是在打标时静默跳过
        try {
            Pattern.compile(rule.getColumnPattern());
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("列名正则非法: " + e.getDescription());
        }
        return ruleRepo.save(rule);
    }

    @Transactional
    public void deleteRule(Long id) {
        ruleRepo.deleteById(id);
    }

    public List<DataClassification> listByTable(String db, String table) {
        return classRepo.findByDbNameAndTableName(db, table);
    }
}
