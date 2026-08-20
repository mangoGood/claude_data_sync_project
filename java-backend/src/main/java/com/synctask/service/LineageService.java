package com.synctask.service;

import com.google.gson.Gson;
import com.synctask.entity.LineageEdge;
import com.synctask.entity.LineageNode;
import com.synctask.entity.Workflow;
import com.synctask.repository.LineageEdgeRepository;
import com.synctask.repository.LineageNodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * 字段级血缘。
 *
 * <h3>核心判断：采集不需要新探针</h3>
 * <p>血缘要的信息平台<b>已经全部掌握</b>，只是散在各处、用途各异：
 *
 * <table border="1">
 *   <tr><th>已有的东西</th><th>血缘拿它做什么</th></tr>
 *   <tr><td>{@code syncObjects} 的表清单、{@code tableMapping}、{@code targetDb}</td><td>表级与库级的边</td></tr>
 *   <tr><td>{@code columnMapping}</td><td>RENAME 边</td></tr>
 *   <tr><td>{@code columnMask}</td><td>MASK 边</td></tr>
 *   <tr><td>{@code columnFilter}</td><td>FILTER 边（行级，记在被引用的列上）</td></tr>
 *   <tr><td>{@code extraColumns}</td><td>DEFAULT_VALUE 边（目标端有、源端没有）</td></tr>
 *   <tr><td>{@code routeConfig}</td><td>ROUTE_SPLIT / ROUTE_MERGE 边</td></tr>
 * </table>
 *
 * <p>所以这里做的是<b>一个把配置归一成图的转换层</b>，而不是一套采集系统。
 * 这跟从零做血缘（要解析所有 SQL、要 hook 每条写入）差着一个量级，
 * 也是设计文档把它排在第一优先的理由。
 *
 * <h3>什么时候生成</h3>
 * <p>由调用方显式触发（建任务后、或手动重建），而不是运行期采集：
 * 血缘描述的是"数据<b>会</b>怎么流"，那是配置决定的；运行期采集只会把同一件事
 * 算很多遍，还得处理"任务没跑过就没有血缘"的空洞。
 */
@Service
public class LineageService {

    private static final Logger logger = LoggerFactory.getLogger(LineageService.class);
    private static final Gson GSON = new Gson();

    @Autowired
    private LineageNodeRepository nodeRepo;

    @Autowired
    private LineageEdgeRepository edgeRepo;

    @Autowired
    private MetadataService metadataService;

    // ================================================================ 生成

    /**
     * 按任务配置重建血缘。
     *
     * <p>先删该任务的旧边再重建——配置改过之后，旧边描述的是不存在的流向，
     * 留着比没有更糟。节点不删：它可能被别的任务的边引用着。
     *
     * @return 生成的边数
     */
    @Transactional
    public int rebuild(Workflow workflow) {
        String wid = workflow.getId();
        edgeRepo.deleteByWorkflowId(wid);

        Map<String, Object> syncObjects = parseSyncObjects(workflow.getSyncObjects());
        if (syncObjects.isEmpty()) {
            logger.info("任务 {} 无同步对象，血缘为空", wid);
            return 0;
        }

        int edges = 0;
        for (Map.Entry<String, Object> dbEntry : syncObjects.entrySet()) {
            String srcDb = dbEntry.getKey();
            if (!(dbEntry.getValue() instanceof Map)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> entry = (Map<String, Object>) dbEntry.getValue();

            // 库名映射：entry.targetDb ＞ 与源库同名（与 agent ConfigService 同一优先级）
            String tgtDb = str(entry.get("targetDb"));
            if (tgtDb == null || tgtDb.isEmpty()) {
                tgtDb = workflow.getTargetDbName() != null && !workflow.getTargetDbName().isEmpty()
                        ? workflow.getTargetDbName() : srcDb;
            }

            List<String> tables = strList(entry.get("tables"));
            Map<String, String> tableMapping = strMap(entry.get("tableMapping"));
            Map<String, Object> columnMapping = mapOf(entry.get("columnMapping"));
            Map<String, Object> columnMask = mapOf(entry.get("columnMask"));
            Map<String, Object> columnFilter = mapOf(entry.get("columnFilter"));
            Map<String, Object> extraColumns = mapOf(entry.get("extraColumns"));

            for (String srcTable : tables) {
                String tgtTable = tableMapping.getOrDefault(srcTable, srcTable);
                List<String> columns = probeColumns(workflow, srcDb, srcTable);
                if (columns.isEmpty()) {
                    // 探不到列就退化成表级：仍然记下"这张表流到那张表"，
                    // 好过整条链路在血缘里消失
                    logger.warn("探查列失败，{}.{} 退化为表级血缘", srcDb, srcTable);
                    columns = explicitColumnsOf(columnMapping, columnMask, srcTable);
                }

                Map<String, String> colMap = strMap(columnMapping.get(srcTable));
                Set<String> masked = maskedColumnsOf(columnMask.get(srcTable));
                Map<String, String> maskRules = maskRulesOf(columnMask.get(srcTable));
                Set<String> filterCols = filterColumnsOf(columnFilter.get(srcTable));

                for (String col : columns) {
                    String tgtCol = colMap.getOrDefault(col, col);
                    LineageNode src = ensureNode(LineageNode.Side.SOURCE, srcDb, srcTable, col);
                    LineageNode dst = ensureNode(LineageNode.Side.TARGET, tgtDb, tgtTable, tgtCol);

                    // 算子按"改动强度"取最强的那一个：脱敏 ＞ 改名 ＞ 原样。
                    // 一条边只记一个算子——同时改名又脱敏时，脱敏才是使用者需要知道的那件事
                    // （改名看目标列名就知道，而"值被改过"看不出来）。
                    LineageEdge.Operator op;
                    Map<String, Object> detail = new LinkedHashMap<>();
                    if (masked.contains(col.toLowerCase())) {
                        op = LineageEdge.Operator.MASK;
                        detail.put("rule", maskRules.get(col.toLowerCase()));
                        detail.put("comparable", false);   // 脱敏列不参与内容对比
                    } else if (!tgtCol.equals(col)) {
                        op = LineageEdge.Operator.RENAME;
                        detail.put("from", col);
                        detail.put("to", tgtCol);
                    } else {
                        op = LineageEdge.Operator.IDENTITY;
                    }
                    if (filterCols.contains(col.toLowerCase())) {
                        // 过滤是行级的，但它由某一列的取值决定——把这个事实记在那一列上，
                        // 影响面分析才能回答"改这一列会影响哪些行的同步"
                        detail.put("rowFilter", true);
                    }
                    edges += saveEdge(src, dst, op, detail, wid);
                }

                // 附加列：目标端有、源端没有。用一个虚拟源节点承接，
                // 否则这些列在血缘图里凭空出现，看图的人无从判断它们哪来的。
                for (Map.Entry<String, String> ex : extraColumnsOf(extraColumns.get(srcTable)).entrySet()) {
                    LineageNode src = ensureNode(LineageNode.Side.SOURCE, srcDb, srcTable, "(无源)");
                    LineageNode dst = ensureNode(LineageNode.Side.TARGET, tgtDb, tgtTable, ex.getKey());
                    Map<String, Object> detail = new LinkedHashMap<>();
                    detail.put("kind", ex.getValue());
                    edges += saveEdge(src, dst, LineageEdge.Operator.DEFAULT_VALUE, detail, wid);
                }
            }
        }

        // 分片路由：改变的是"落到哪张表"，独立于列级变换
        edges += buildRouteEdges(workflow, syncObjects);

        logger.info("任务 {} 血缘已重建：{} 条边", wid, edges);
        return edges;
    }

    /**
     * 分片路由的边。汇聚是多张源表 → 一张目标表，拆分是一张源表 → 多张分片表。
     * 记在<b>分片键列</b>上——那才是决定落点的那一列。
     */
    private int buildRouteEdges(Workflow workflow, Map<String, Object> syncObjects) {
        String routeJson = workflow.getRouteConfig();
        if (routeJson == null || routeJson.isEmpty()) {
            return 0;
        }
        int n = 0;
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> route = GSON.fromJson(routeJson, LinkedHashMap.class);
            if (route == null) {
                return 0;
            }
            String mode = str(route.get("mode"));
            List<?> rules = route.get("rules") instanceof List ? (List<?>) route.get("rules") : List.of();
            for (Object ro : rules) {
                if (!(ro instanceof Map)) continue;
                @SuppressWarnings("unchecked")
                Map<String, Object> rule = (Map<String, Object>) ro;
                String match = str(rule.get("match"));
                String shardKey = str(rule.get("shardKey"));
                String targetDb = str(rule.get("targetDb"));
                String targetTable = str(rule.get("targetTable"));
                if (match == null || match.isEmpty()) continue;

                String[] parts = match.split("\\.", 2);
                String db = parts.length > 1 ? parts[0] : "";
                String tbl = parts.length > 1 ? parts[1] : parts[0];
                String keyCol = (shardKey == null || shardKey.isEmpty()) ? "(整表)" : shardKey;

                LineageNode src = ensureNode(LineageNode.Side.SOURCE, db, tbl, keyCol);
                LineageNode dst = ensureNode(LineageNode.Side.TARGET,
                        targetDb == null ? db : targetDb,
                        targetTable == null ? tbl : targetTable, keyCol);
                Map<String, Object> detail = new LinkedHashMap<>();
                detail.put("mode", mode);
                detail.put("shardKey", shardKey);
                detail.put("algo", str(rule.get("algo")));
                n += saveEdge(src, dst,
                        "SPLIT".equalsIgnoreCase(mode)
                                ? LineageEdge.Operator.ROUTE_SPLIT : LineageEdge.Operator.ROUTE_MERGE,
                        detail, workflow.getId());
            }
        } catch (Exception e) {
            logger.warn("解析路由配置生成血缘失败: {}", e.getMessage());
        }
        return n;
    }

    // ================================================================ 查询

    /** 一次影响面查询的结果。 */
    public static class ImpactResult {
        public String origin;
        /** 下游受影响的列，按跳数分层 */
        public List<List<String>> downstreamByHop = new ArrayList<>();
        /** 展平后的下游列 */
        public List<String> downstream = new ArrayList<>();
        /** 路径上出现过的算子（去重）——告诉使用者"值在中途被改过没有" */
        public Set<String> operators = new LinkedHashSet<>();
        /** 路径上是否有脱敏：有的话下游那些列不参与内容对比 */
        public boolean hasMask;
    }

    /**
     * 影响面分析：改一个源列，下游哪些列会受影响。
     *
     * <p>这是运维最常问的那个问题，也是血缘最主要的用途。
     */
    public ImpactResult impact(String db, String table, String column, int maxDepth) {
        ImpactResult r = new ImpactResult();
        r.origin = db + "." + table + "." + column;

        List<LineageNode> starts =
                nodeRepo.findByDbNameAndTableNameAndColumnNameAndValidToSeqnoIsNull(db, table, column);
        if (starts.isEmpty()) {
            return r;
        }
        Set<Long> visited = new HashSet<>();
        List<LineageNode> frontier = new ArrayList<>(starts);
        starts.forEach(n -> visited.add(n.getId()));

        for (int hop = 0; hop < Math.max(1, maxDepth) && !frontier.isEmpty(); hop++) {
            List<LineageNode> next = new ArrayList<>();
            List<String> layer = new ArrayList<>();
            for (LineageNode node : frontier) {
                for (LineageEdge e : edgeRepo.findBySrcNodeId(node.getId())) {
                    r.operators.add(e.getOperator().name());
                    if (e.getOperator() == LineageEdge.Operator.MASK) {
                        r.hasMask = true;
                    }
                    // 环保护：血缘图在双向灾备下会成环（A→B 与 B→A）
                    if (!visited.add(e.getDstNodeId())) {
                        continue;
                    }
                    nodeRepo.findById(e.getDstNodeId()).ifPresent(dst -> {
                        layer.add(dst.qualifiedName());
                        next.add(dst);
                    });
                }
            }
            if (!layer.isEmpty()) {
                r.downstreamByHop.add(layer);
                r.downstream.addAll(layer);
            }
            frontier = next;
        }
        return r;
    }

    /** 上游追溯：这一列的数据从哪来。 */
    public ImpactResult upstream(String db, String table, String column, int maxDepth) {
        ImpactResult r = new ImpactResult();
        r.origin = db + "." + table + "." + column;
        List<LineageNode> starts =
                nodeRepo.findByDbNameAndTableNameAndColumnNameAndValidToSeqnoIsNull(db, table, column);
        if (starts.isEmpty()) {
            return r;
        }
        Set<Long> visited = new HashSet<>();
        List<LineageNode> frontier = new ArrayList<>(starts);
        starts.forEach(n -> visited.add(n.getId()));

        for (int hop = 0; hop < Math.max(1, maxDepth) && !frontier.isEmpty(); hop++) {
            List<LineageNode> next = new ArrayList<>();
            List<String> layer = new ArrayList<>();
            for (LineageNode node : frontier) {
                for (LineageEdge e : edgeRepo.findByDstNodeId(node.getId())) {
                    r.operators.add(e.getOperator().name());
                    if (e.getOperator() == LineageEdge.Operator.MASK) {
                        r.hasMask = true;
                    }
                    if (!visited.add(e.getSrcNodeId())) {
                        continue;
                    }
                    nodeRepo.findById(e.getSrcNodeId()).ifPresent(src -> {
                        layer.add(src.qualifiedName());
                        next.add(src);
                    });
                }
            }
            if (!layer.isEmpty()) {
                r.downstreamByHop.add(layer);
                r.downstream.addAll(layer);
            }
            frontier = next;
        }
        return r;
    }

    /** 某任务的全部边（供图展示）。 */
    public List<Map<String, Object>> graphOf(String workflowId) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (LineageEdge e : edgeRepo.findByWorkflowId(workflowId)) {
            Optional<LineageNode> s = nodeRepo.findById(e.getSrcNodeId());
            Optional<LineageNode> d = nodeRepo.findById(e.getDstNodeId());
            if (s.isEmpty() || d.isEmpty()) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("source", s.get().qualifiedName());
            m.put("target", d.get().qualifiedName());
            m.put("operator", e.getOperator().name());
            m.put("detail", e.getOperatorDetail());
            out.add(m);
        }
        return out;
    }

    // ================================================================ 内部

    @Transactional
    protected LineageNode ensureNode(LineageNode.Side side, String db, String table, String column) {
        return nodeRepo.findFirstBySideAndDbNameAndTableNameAndColumnNameAndValidToSeqnoIsNull(
                side, db, table, column).orElseGet(() -> {
            LineageNode n = new LineageNode();
            n.setSide(side);
            n.setDbName(db);
            n.setTableName(table);
            n.setColumnName(column);
            n.setValidFromSeqno(0L);
            return nodeRepo.save(n);
        });
    }

    private int saveEdge(LineageNode src, LineageNode dst, LineageEdge.Operator op,
                         Map<String, Object> detail, String workflowId) {
        try {
            LineageEdge e = new LineageEdge();
            e.setSrcNodeId(src.getId());
            e.setDstNodeId(dst.getId());
            e.setOperator(op);
            e.setOperatorDetail(detail == null || detail.isEmpty() ? null : GSON.toJson(detail));
            e.setWorkflowId(workflowId);
            edgeRepo.save(e);
            return 1;
        } catch (Exception ex) {
            // 唯一键冲突（同一任务同一对节点重复）不算错误，跳过即可
            return 0;
        }
    }

    /**
     * 探查表的列清单。
     *
     * <p>血缘要覆盖<b>每一列</b>才有用——只记配了规则的那几列，
     * 影响面分析会漏掉"原样搬运"的绝大多数列，而那些恰恰是最常被问到的。
     */
    private List<String> probeColumns(Workflow workflow, String db, String table) {
        try {
            List<Map<String, Object>> cols = metadataService.listColumns(
                    workflow.getSourceConnection(), db, table);
            List<String> out = new ArrayList<>();
            for (Map<String, Object> c : cols) {
                Object name = c.get("name");
                if (name != null) {
                    out.add(String.valueOf(name));
                }
            }
            return out;
        } catch (Exception e) {
            logger.debug("探查 {}.{} 的列失败: {}", db, table, e.getMessage());
            return List.of();
        }
    }

    /** 探不到列时的退化：至少把显式配了规则的列记下来。 */
    private List<String> explicitColumnsOf(Map<String, Object> columnMapping,
                                           Map<String, Object> columnMask, String table) {
        Set<String> s = new LinkedHashSet<>(strMap(columnMapping.get(table)).keySet());
        s.addAll(maskRulesOf(columnMask.get(table)).keySet());
        return new ArrayList<>(s);
    }

    // ---- JSON 解析小工具 ----

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseSyncObjects(String json) {
        if (json == null || json.isEmpty()) {
            return Map.of();
        }
        try {
            Map<String, Object> m = GSON.fromJson(json, LinkedHashMap.class);
            return m == null ? Map.of() : m;
        } catch (Exception e) {
            return Map.of();
        }
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static List<String> strList(Object o) {
        if (!(o instanceof List)) return List.of();
        List<String> out = new ArrayList<>();
        for (Object x : (List<?>) o) {
            if (x != null) out.add(String.valueOf(x));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapOf(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : Map.of();
    }

    private static Map<String, String> strMap(Object o) {
        if (!(o instanceof Map)) return Map.of();
        Map<String, String> out = new LinkedHashMap<>();
        ((Map<?, ?>) o).forEach((k, v) -> {
            if (k != null && v != null) out.put(String.valueOf(k), String.valueOf(v));
        });
        return out;
    }

    private static Set<String> maskedColumnsOf(Object o) {
        return maskRulesOf(o).keySet();
    }

    /** 表的脱敏规则：列名小写 → 规则名。 */
    private static Map<String, String> maskRulesOf(Object o) {
        Map<String, String> out = new LinkedHashMap<>();
        if (!(o instanceof List)) return out;
        for (Object item : (List<?>) o) {
            if (!(item instanceof Map)) continue;
            Object col = ((Map<?, ?>) item).get("column");
            Object rule = ((Map<?, ?>) item).get("rule");
            if (col != null) {
                out.put(String.valueOf(col).toLowerCase(), rule == null ? "" : String.valueOf(rule));
            }
        }
        return out;
    }

    /** 表的过滤条件引用了哪些列（小写）。 */
    private static Set<String> filterColumnsOf(Object o) {
        Set<String> out = new LinkedHashSet<>();
        if (!(o instanceof List)) return out;
        for (Object item : (List<?>) o) {
            if (item instanceof Map) {
                Object col = ((Map<?, ?>) item).get("column");
                if (col != null) out.add(String.valueOf(col).toLowerCase());
            }
        }
        return out;
    }

    /** 表的附加列：列名 → 类型。 */
    private static Map<String, String> extraColumnsOf(Object o) {
        Map<String, String> out = new LinkedHashMap<>();
        if (!(o instanceof List)) return out;
        for (Object item : (List<?>) o) {
            if (!(item instanceof Map)) continue;
            Object name = ((Map<?, ?>) item).get("name");
            Object kind = ((Map<?, ?>) item).get("kind");
            if (name != null) {
                out.put(String.valueOf(name), kind == null ? "" : String.valueOf(kind));
            }
        }
        return out;
    }
}
