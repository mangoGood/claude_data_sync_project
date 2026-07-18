package com.migration.agent.service;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.sqlobj.StoredObjectSyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * 库级同步的"任务结束"钩子：把源库的 TRIGGER / EVENT 同步到目标库。
 *
 * <p>为什么在任务结束时做：同步运行期间目标库若带触发器/事件，会对同步通道写入的行
 * 再次触发（双写/放大）；增量 DDL 通道对 trigger/event 一律运行期跳过，统一在任务结束
 * （用户点结束 terminate，或仅全量任务全量完成）时从源库读取最终定义一次性复制。
 *
 * <p>同步过程与逐对象结果（含失败原因）写入 workflow_logs，用户在任务详情"执行日志"
 * 中可见。仅对库级同步（config 里 sync.db.level=true）的任务生效；失败不阻断任务收尾。
 */
public class DbObjectsSyncService {

    private static final Logger logger = LoggerFactory.getLogger(DbObjectsSyncService.class);

    private DbObjectsSyncService() {}

    /**
     * 任务结束时同步 trigger/event（读 files/{taskId}/config.properties 自取连接信息）。
     * 非库级任务/配置缺失时静默跳过。
     */
    public static void syncTriggersAndEventsAtTaskEnd(String taskId) {
        try {
            File configFile = new File("files/" + taskId + "/config.properties");
            if (!configFile.exists()) {
                return;
            }
            Properties props = new Properties();
            try (FileInputStream in = new FileInputStream(configFile)) {
                props.load(in);
            }

            if (!Boolean.parseBoolean(props.getProperty("sync.db.level", "false"))) {
                return;
            }
            String dbsCsv = props.getProperty("sync.db.level.databases", "");
            if (dbsCsv.isEmpty()) {
                return;
            }
            if (!"mysql".equalsIgnoreCase(props.getProperty("source.db.type", "mysql"))
                    || !"mysql".equalsIgnoreCase(props.getProperty("target.db.type", "mysql"))) {
                logger.info("[{}] 库级 trigger/event 同步仅支持 MySQL，跳过", taskId);
                return;
            }

            logger.info("[{}] 任务结束：开始同步库级同步范围内的 TRIGGER/EVENT: {}", taskId, dbsCsv);
            addWorkflowLog(taskId, "INFO", "任务结束：开始同步源库的 TRIGGER/EVENT 到目标库（范围: " + dbsCsv + "）");

            StoredObjectSyncUtil.SyncReport total = new StoredObjectSyncUtil.SyncReport();
            try (Connection source = openConnection(props, "source");
                 Connection target = openConnection(props, "target")) {
                for (String db : dbsCsv.split(",")) {
                    String dbName = db.trim();
                    if (dbName.isEmpty()) {
                        continue;
                    }
                    // 库名映射：trigger/event 落到目标端映射库（未映射与源库同名）
                    String targetDbName = props.getProperty("schema.mapping.db." + dbName, dbName);
                    StoredObjectSyncUtil.SyncReport r =
                            StoredObjectSyncUtil.copyTriggersAndEventsDetailed(source, target, dbName, targetDbName);
                    total.succeeded.addAll(r.succeeded);
                    total.failed.addAll(r.failed);
                }
            }

            for (String ok : total.succeeded) {
                addWorkflowLog(taskId, "INFO", "已同步 " + ok);
            }
            for (String bad : total.failed) {
                addWorkflowLog(taskId, "ERROR", "同步失败 " + bad);
            }
            if (total.failed.isEmpty()) {
                addWorkflowLog(taskId, "INFO",
                        "TRIGGER/EVENT 同步完成：成功 " + total.succeeded.size() + " 个，全部成功");
            } else {
                addWorkflowLog(taskId, "WARNING",
                        "TRIGGER/EVENT 同步完成：成功 " + total.succeeded.size() + " 个，失败 "
                                + total.failed.size() + " 个（详见上方错误日志，可在源库修复后重新结束任务重试）");
            }
            logger.info("[{}] TRIGGER/EVENT 任务结束同步完成: 成功={}, 失败={}",
                    taskId, total.succeeded.size(), total.failed.size());
        } catch (Exception e) {
            logger.warn("[{}] 任务结束同步 TRIGGER/EVENT 失败（不影响任务收尾）: {}", taskId, e.getMessage());
            addWorkflowLog(taskId, "ERROR", "TRIGGER/EVENT 同步执行异常: " + e.getMessage());
        }
    }

    /** 把同步过程写入 backend 的 workflow_logs（任务详情"执行日志"展示）。写失败只记 agent 日志。 */
    private static void addWorkflowLog(String taskId, String level, String message) {
        try {
            AgentConfig agentConfig = new AgentConfig();
            String url = agentConfig.getMysqlDbUrl();
            if (url == null || url.isEmpty()) {
                return;
            }
            try (Connection conn = DriverManager.getConnection(url,
                    agentConfig.getMysqlDbUser(), agentConfig.getMysqlDbPassword());
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO workflow_logs (workflow_id, level, message) VALUES (?, ?, ?)")) {
                ps.setString(1, taskId);
                ps.setString(2, level);
                ps.setString(3, message);
                ps.executeUpdate();
            }
        } catch (Exception e) {
            logger.debug("[{}] 写 workflow_logs 失败: {}", taskId, e.getMessage());
        }
    }

    private static Connection openConnection(Properties props, String prefix) throws Exception {
        String host = props.getProperty(prefix + ".db.host", "localhost");
        String port = props.getProperty(prefix + ".db.port", "3306");
        String user = props.getProperty(prefix + ".db.username", "root");
        String password = CredentialCipher.decrypt(props.getProperty(prefix + ".db.password", ""));
        String url = "jdbc:mysql://" + host + ":" + port +
                "/?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8&allowPublicKeyRetrieval=true";
        return DriverManager.getConnection(url, user, password);
    }
}
