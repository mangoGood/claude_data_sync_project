package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 一键诊断服务
 * 自动检查源库/目标库连接、权限、binlog配置、磁盘空间等前置条件
 */
@Service
public class DiagnosticService {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticService.class);

    @Autowired
    private WorkflowRepository workflowRepository;

    /**
     * 执行一键诊断
     */
    public Map<String, Object> diagnose(String workflowId, Long userId) {
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));
        if (!workflow.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此任务");
        }

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> checks = new ArrayList<>();
        int passed = 0;
        int failed = 0;
        int warnings = 0;

        // 1. 检查源库连接
        Map<String, Object> sourceConnCheck = checkDatabaseConnection("源库连接", workflow.getSourceConnection());
        checks.add(sourceConnCheck);
        String srcStatus = (String) sourceConnCheck.get("status");
        if ("PASS".equals(srcStatus)) passed++; else if ("FAIL".equals(srcStatus)) failed++; else warnings++;

        // 2. 检查目标库连接
        Map<String, Object> targetConnCheck = checkDatabaseConnection("目标库连接", workflow.getTargetConnection());
        checks.add(targetConnCheck);
        String tgtStatus = (String) targetConnCheck.get("status");
        if ("PASS".equals(tgtStatus)) passed++; else if ("FAIL".equals(tgtStatus)) failed++; else warnings++;

        // 3. 检查源库binlog配置
        if ("PASS".equals(srcStatus)) {
            Map<String, Object> binlogCheck = checkBinlogConfig(workflow.getSourceConnection());
            checks.add(binlogCheck);
            String bStatus = (String) binlogCheck.get("status");
            if ("PASS".equals(bStatus)) passed++; else if ("FAIL".equals(bStatus)) failed++; else warnings++;
        }

        // 4. 检查源库权限
        if ("PASS".equals(srcStatus)) {
            Map<String, Object> permCheck = checkDatabasePrivileges("源库权限", workflow.getSourceConnection());
            checks.add(permCheck);
            String pStatus = (String) permCheck.get("status");
            if ("PASS".equals(pStatus)) passed++; else if ("FAIL".equals(pStatus)) failed++; else warnings++;
        }

        // 5. 检查目标库权限
        if ("PASS".equals(tgtStatus)) {
            Map<String, Object> permCheck = checkDatabasePrivileges("目标库权限", workflow.getTargetConnection());
            checks.add(permCheck);
            String pStatus = (String) permCheck.get("status");
            if ("PASS".equals(pStatus)) passed++; else if ("FAIL".equals(pStatus)) failed++; else warnings++;
        }

        // 6. 检查磁盘空间
        Map<String, Object> diskCheck = checkDiskSpace();
        checks.add(diskCheck);
        String dStatus = (String) diskCheck.get("status");
        if ("PASS".equals(dStatus)) passed++; else if ("FAIL".equals(dStatus)) failed++; else warnings++;

        // 7. 检查任务配置完整性
        Map<String, Object> configCheck = checkTaskConfig(workflow);
        checks.add(configCheck);
        String cStatus = (String) configCheck.get("status");
        if ("PASS".equals(cStatus)) passed++; else if ("FAIL".equals(cStatus)) failed++; else warnings++;

        result.put("checks", checks);
        result.put("total", checks.size());
        result.put("passed", passed);
        result.put("failed", failed);
        result.put("warnings", warnings);
        result.put("overall", failed > 0 ? "FAIL" : (warnings > 0 ? "WARNING" : "PASS"));
        result.put("workflowId", workflowId);
        result.put("workflowName", workflow.getName());

        return result;
    }

    private Map<String, Object> checkDatabaseConnection(String checkName, String connectionStr) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", checkName);

        if (connectionStr == null || connectionStr.trim().isEmpty()) {
            result.put("status", "FAIL");
            result.put("message", "连接字符串为空");
            return result;
        }

        Connection conn = null;
        try {
            // 解析 mysql://user:pass@host:port/db 格式
            String[] parsed = parseConnectionUrl(connectionStr);
            String jdbcUrl = parsed[0];
            String username = parsed[1];
            String password = parsed[2];

            conn = DriverManager.getConnection(jdbcUrl, username, password);
            result.put("status", "PASS");
            result.put("message", "连接成功");
            result.put("detail", "JDBC: " + jdbcUrl.replaceAll(password, "***"));
        } catch (Exception e) {
            result.put("status", "FAIL");
            result.put("message", "连接失败: " + e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (Exception ignored) {}
        }
        return result;
    }

    private Map<String, Object> checkBinlogConfig(String connectionStr) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", "源库Binlog配置");

        Connection conn = null;
        try {
            String[] parsed = parseConnectionUrl(connectionStr);
            conn = DriverManager.getConnection(parsed[0], parsed[1], parsed[2]);

            // 检查 log_bin 是否开启
            try (PreparedStatement stmt = conn.prepareStatement("SHOW VARIABLES LIKE 'log_bin'");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String logBin = rs.getString(2);
                    if ("ON".equalsIgnoreCase(logBin)) {
                        result.put("status", "PASS");
                        result.put("message", "log_bin=ON");
                    } else {
                        result.put("status", "FAIL");
                        result.put("message", "log_bin=OFF，增量同步需要开启binlog");
                    }
                }
            }

            // 检查 binlog_format
            try (PreparedStatement stmt = conn.prepareStatement("SHOW VARIABLES LIKE 'binlog_format'");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String format = rs.getString(2);
                    if (!"ROW".equalsIgnoreCase(format)) {
                        result.put("status", "WARNING");
                        result.put("message", "binlog_format=" + format + "，建议设置为ROW");
                    }
                }
            }

            // 检查 binlog_row_image
            try (PreparedStatement stmt = conn.prepareStatement("SHOW VARIABLES LIKE 'binlog_row_image'");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String image = rs.getString(2);
                    if (!"FULL".equalsIgnoreCase(image)) {
                        String msg = "binlog_row_image=" + image + "，建议设置为FULL";
                        result.put("status", "WARNING");
                        result.put("message", result.get("message") + "; " + msg);
                    }
                }
            }
        } catch (Exception e) {
            result.put("status", "FAIL");
            result.put("message", "检查失败: " + e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (Exception ignored) {}
        }
        return result;
    }

    private Map<String, Object> checkDatabasePrivileges(String checkName, String connectionStr) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", checkName);

        Connection conn = null;
        try {
            String[] parsed = parseConnectionUrl(connectionStr);
            conn = DriverManager.getConnection(parsed[0], parsed[1], parsed[2]);

            try (PreparedStatement stmt = conn.prepareStatement("SHOW GRANTS FOR CURRENT_USER()");
                 ResultSet rs = stmt.executeQuery()) {
                StringBuilder grants = new StringBuilder();
                boolean hasAllPrivileges = false;
                while (rs.next()) {
                    String grant = rs.getString(1);
                    grants.append(grant).append("; ");
                    if (grant.contains("ALL PRIVILEGES")) hasAllPrivileges = true;
                }
                if (hasAllPrivileges) {
                    result.put("status", "PASS");
                    result.put("message", "拥有ALL PRIVILEGES权限");
                } else {
                    result.put("status", "WARNING");
                    result.put("message", "权限: " + grants);
                }
            }
        } catch (Exception e) {
            result.put("status", "FAIL");
            result.put("message", "权限检查失败: " + e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (Exception ignored) {}
        }
        return result;
    }

    private Map<String, Object> checkDiskSpace() {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", "本地磁盘空间");

        java.io.File disk = new java.io.File(".");
        long freeSpace = disk.getUsableSpace();
        long totalSpace = disk.getTotalSpace();
        long freeGB = freeSpace / (1024 * 1024 * 1024);

        if (freeGB > 1) {
            result.put("status", "PASS");
            result.put("message", "可用空间: " + freeGB + "GB");
        } else {
            result.put("status", "FAIL");
            result.put("message", "磁盘空间不足: 仅剩 " + freeGB + "GB");
        }
        return result;
    }

    private Map<String, Object> checkTaskConfig(Workflow workflow) {
        Map<String, Object> result = new HashMap<>();
        result.put("checkName", "任务配置完整性");

        List<String> issues = new ArrayList<>();
        if (workflow.getName() == null || workflow.getName().trim().isEmpty()) {
            issues.add("任务名称为空");
        }
        if (workflow.getSourceConnection() == null) {
            issues.add("源库连接未配置");
        }
        if (workflow.getTargetConnection() == null) {
            issues.add("目标库连接未配置");
        }
        if (workflow.getSyncObjects() == null || workflow.getSyncObjects().isEmpty()) {
            issues.add("同步对象未选择");
        }
        if (workflow.getMigrationMode() == null) {
            issues.add("迁移模式未选择");
        }

        if (issues.isEmpty()) {
            result.put("status", "PASS");
            result.put("message", "配置完整");
        } else {
            result.put("status", "FAIL");
            result.put("message", String.join("; ", issues));
        }
        return result;
    }

    // ==================== 启动前 schema 预检 ====================
    private static final com.google.gson.Gson PRECHECK_GSON = new com.google.gson.Gson();

    /**
     * 启动前 schema 预检：把"跑起来才暴露"的结构问题挡在 launch 之前。
     * 源表/库存在性（FAIL）、增量任务主键缺失（WARNING，无 PK 增量 UPDATE/DELETE 无法定位行）、
     * 列处理引用的源列存在性（FAIL）、目标同名表已存在（WARNING，全量可能冲突/重复）。
     * 目前仅 MySQL 源库；其它源类型返回单条 WARNING 跳过（不阻断启动）。
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> schemaPrecheck(String workflowId, Long userId) {
        Workflow workflow = workflowRepository.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));
        if (!workflow.getUserId().equals(userId)) {
            throw new RuntimeException("无权操作此任务");
        }

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> checks = new ArrayList<>();

        String srcConn = workflow.getSourceConnection();
        boolean mysqlSource = srcConn != null && srcConn.startsWith("mysql://");
        if (!mysqlSource) {
            checks.add(check("schema 预检", "WARNING",
                    "schema 预检当前仅支持 MySQL 源库，已跳过（不影响启动）", null));
            return summarize(result, checks, workflow);
        }

        List<DbEntry> entries;
        try {
            entries = parseSyncEntries(workflow);
        } catch (Exception e) {
            checks.add(check("同步对象解析", "FAIL", "无法解析同步对象配置: " + e.getMessage(), null));
            return summarize(result, checks, workflow);
        }
        if (entries.isEmpty()) {
            checks.add(check("同步对象", "FAIL", "未选择任何同步对象", null));
            return summarize(result, checks, workflow);
        }

        String mode = workflow.getMigrationMode();
        boolean needsIncrement = mode != null &&
                (mode.toLowerCase().contains("incre") || mode.equalsIgnoreCase("subscribe"));
        String tgtConn = workflow.getTargetConnection();
        boolean relationalTarget = tgtConn != null &&
                (tgtConn.startsWith("mysql://") || tgtConn.startsWith("postgresql://"));

        try (Connection src = openConn(srcConn)) {
            checks.add(checkSourceObjectsExist(src, entries));
            if (needsIncrement) {
                checks.add(checkPrimaryKeys(src, entries));
            }
            checks.add(checkColumnRefs(src, entries));
        } catch (Exception e) {
            checks.add(check("源库 schema 检查", "FAIL", "连接源库失败: " + e.getMessage(), null));
        }

        // 目标同名表预存在（仅关系型目标；异构/kafka 跳过）——目标库名按 per-db 映射解析
        if (relationalTarget && tgtConn.startsWith("mysql://")) {
            try (Connection tgt = openConn(tgtConn)) {
                checks.add(checkTargetConflicts(tgt, entries, workflow));
            } catch (Exception e) {
                checks.add(check("目标库 schema 检查", "WARNING", "连接目标库失败，跳过目标冲突检查: " + e.getMessage(), null));
            }
        }

        return summarize(result, checks, workflow);
    }

    /** 单库同步 entry（预检用）。tables 为空 + dbLevel=true 表示整库同步。 */
    private static final class DbEntry {
        String sourceDb;
        String targetDb;
        boolean dbLevel;
        List<String> tables = new ArrayList<>();
        Map<String, String> tableMapping = new HashMap<>();   // src表 -> tgt表
        // 表 -> 引用的源列集合（columnFilter 的 column、columnMapping 的 key）
        Map<String, java.util.Set<String>> referencedColumns = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    private List<DbEntry> parseSyncEntries(Workflow workflow) {
        List<DbEntry> out = new ArrayList<>();
        Map<String, Object> raw = PRECHECK_GSON.fromJson(workflow.getSyncObjects(), Map.class);
        if (raw == null) return out;
        String globalTargetDb = workflow.getTargetDbName();
        for (Map.Entry<String, Object> e : raw.entrySet()) {
            DbEntry de = new DbEntry();
            de.sourceDb = e.getKey();
            Object v = e.getValue();
            if (!(v instanceof Map)) {
                if (v instanceof List) for (Object t : (List<?>) v) de.tables.add(String.valueOf(t));
                de.targetDb = globalTargetDb != null ? globalTargetDb : de.sourceDb;
                out.add(de);
                continue;
            }
            Map<String, Object> m = (Map<String, Object>) v;
            Object td = m.get("targetDb");
            de.targetDb = (td instanceof String && !((String) td).isEmpty()) ? (String) td
                    : (globalTargetDb != null && !globalTargetDb.isEmpty() ? globalTargetDb : de.sourceDb);
            de.dbLevel = Boolean.TRUE.equals(m.get("dbLevel"));
            Object tables = m.get("tables");
            if (tables instanceof List) for (Object t : (List<?>) tables) de.tables.add(String.valueOf(t));
            Object tm = m.get("tableMapping");
            if (tm instanceof Map) ((Map<String, Object>) tm).forEach((k, val) -> de.tableMapping.put(k, String.valueOf(val)));
            // 收集列处理引用的源列
            Object cf = m.get("columnFilter");
            if (cf instanceof Map) ((Map<String, Object>) cf).forEach((table, items) -> {
                if (items instanceof List) for (Object it : (List<?>) items) {
                    if (it instanceof Map) {
                        Object col = ((Map<?, ?>) it).get("column");
                        if (col != null) de.referencedColumns.computeIfAbsent(table, x -> new java.util.HashSet<>()).add(String.valueOf(col));
                    }
                }
            });
            Object cm = m.get("columnMapping");
            if (cm instanceof Map) ((Map<String, Object>) cm).forEach((table, mp) -> {
                if (mp instanceof Map) ((Map<?, ?>) mp).keySet().forEach(srcCol ->
                        de.referencedColumns.computeIfAbsent(table, x -> new java.util.HashSet<>()).add(String.valueOf(srcCol)));
            });
            out.add(de);
        }
        return out;
    }

    private Map<String, Object> checkSourceObjectsExist(Connection src, List<DbEntry> entries) throws Exception {
        List<String> missing = new ArrayList<>();
        for (DbEntry de : entries) {
            if (!schemaExists(src, de.sourceDb)) {
                missing.add("库 " + de.sourceDb);
                continue;
            }
            if (de.dbLevel) continue; // 整库同步，不逐表校验
            for (String t : de.tables) {
                if (!tableExists(src, de.sourceDb, t)) missing.add(de.sourceDb + "." + t);
            }
        }
        if (missing.isEmpty()) {
            return check("源库对象存在性", "PASS", "所有同步对象均存在于源库", null);
        }
        return check("源库对象存在性", "FAIL",
                "源库不存在以下对象（" + missing.size() + " 个）", String.join(", ", missing));
    }

    private Map<String, Object> checkPrimaryKeys(Connection src, List<DbEntry> entries) throws Exception {
        List<String> noPk = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.dbLevel || !schemaExists(src, de.sourceDb)) continue;
            for (String t : de.tables) {
                if (tableExists(src, de.sourceDb, t) && !hasPrimaryKey(src, de.sourceDb, t)) {
                    noPk.add(de.sourceDb + "." + t);
                }
            }
        }
        if (noPk.isEmpty()) {
            return check("增量主键", "PASS", "增量同步的表均有主键", null);
        }
        return check("增量主键", "WARNING",
                "以下表无主键，增量 UPDATE/DELETE 无法按主键定位行，可能同步异常（" + noPk.size() + " 个）",
                String.join(", ", noPk));
    }

    private Map<String, Object> checkColumnRefs(Connection src, List<DbEntry> entries) throws Exception {
        List<String> missing = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.referencedColumns.isEmpty() || !schemaExists(src, de.sourceDb)) continue;
            for (Map.Entry<String, java.util.Set<String>> te : de.referencedColumns.entrySet()) {
                String table = te.getKey();
                if (!tableExists(src, de.sourceDb, table)) continue; // 表不存在已由存在性检查覆盖
                java.util.Set<String> cols = tableColumns(src, de.sourceDb, table);
                for (String ref : te.getValue()) {
                    if (!cols.contains(ref.toLowerCase())) {
                        missing.add(de.sourceDb + "." + table + "." + ref);
                    }
                }
            }
        }
        if (missing.isEmpty()) {
            return check("列处理引用列", "PASS", "列过滤/映射引用的源列均存在", null);
        }
        return check("列处理引用列", "FAIL",
                "列处理引用了不存在的源列（" + missing.size() + " 个）", String.join(", ", missing));
    }

    private Map<String, Object> checkTargetConflicts(Connection tgt, List<DbEntry> entries, Workflow workflow) throws Exception {
        List<String> existing = new ArrayList<>();
        for (DbEntry de : entries) {
            if (de.dbLevel) continue;
            if (!schemaExists(tgt, de.targetDb)) continue; // 目标库不存在→全量会建，无冲突
            for (String t : de.tables) {
                String tgtTable = de.tableMapping.getOrDefault(t, t);
                if (tableExists(tgt, de.targetDb, tgtTable)) {
                    existing.add(de.targetDb + "." + tgtTable);
                }
            }
        }
        if (existing.isEmpty()) {
            return check("目标表冲突", "PASS", "目标库无同名表，全量将新建", null);
        }
        return check("目标表冲突", "WARNING",
                "目标库已存在同名表，全量同步可能产生重复或冲突数据，请确认（" + existing.size() + " 个）",
                String.join(", ", existing));
    }

    // ---- information_schema 查询辅助（标识符经参数化，避免注入与 LIKE 通配符误匹配）----
    private boolean schemaExists(Connection conn, String schema) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private boolean tableExists(Connection conn, String schema, String table) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private boolean hasPrimaryKey(Connection conn, String schema, String table) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? " +
                "AND index_name = 'PRIMARY' LIMIT 1")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private java.util.Set<String> tableColumns(Connection conn, String schema, String table) throws Exception {
        java.util.Set<String> cols = new java.util.HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) cols.add(rs.getString(1).toLowerCase());
            }
        }
        return cols;
    }

    private Connection openConn(String connectionStr) throws Exception {
        String[] parsed = parseConnectionUrl(connectionStr);
        return DriverManager.getConnection(parsed[0], parsed[1], parsed[2]);
    }

    private Map<String, Object> check(String name, String status, String message, String detail) {
        Map<String, Object> m = new HashMap<>();
        m.put("checkName", name);
        m.put("status", status);
        m.put("message", message);
        if (detail != null) m.put("detail", detail);
        return m;
    }

    private Map<String, Object> summarize(Map<String, Object> result, List<Map<String, Object>> checks, Workflow workflow) {
        int passed = 0, failed = 0, warnings = 0;
        for (Map<String, Object> c : checks) {
            String s = (String) c.get("status");
            if ("PASS".equals(s)) passed++; else if ("FAIL".equals(s)) failed++; else warnings++;
        }
        result.put("checks", checks);
        result.put("total", checks.size());
        result.put("passed", passed);
        result.put("failed", failed);
        result.put("warnings", warnings);
        result.put("overall", failed > 0 ? "FAIL" : (warnings > 0 ? "WARNING" : "PASS"));
        result.put("workflowId", workflow.getId());
        result.put("workflowName", workflow.getName());
        return result;
    }

    /**
     * 解析 mysql://user:pass@host:port/db 格式为JDBC连接串
     */
    private String[] parseConnectionUrl(String connStr) {
        // mysql://root:rootpassword@192.168.107.6:3306/test_db1
        String url = connStr.replace("mysql://", "");
        int atIdx = url.indexOf('@');
        String userPass = url.substring(0, atIdx);
        String hostDb = url.substring(atIdx + 1);

        String[] up = userPass.split(":", 2);
        String username = up[0];
        String password = up.length > 1 ? up[1] : "";

        String jdbcUrl = "jdbc:mysql://" + hostDb + "?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";

        return new String[]{jdbcUrl, username, password};
    }
}
