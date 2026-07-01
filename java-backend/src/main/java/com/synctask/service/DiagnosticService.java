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
