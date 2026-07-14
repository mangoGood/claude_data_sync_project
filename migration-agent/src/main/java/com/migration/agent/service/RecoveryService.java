package com.migration.agent.service;

import com.migration.agent.model.RecoveryTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class RecoveryService {
    private static final Logger logger = LoggerFactory.getLogger(RecoveryService.class);
    
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    
    public RecoveryService(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }
    
    public List<RecoveryTask> getUnfinishedTasks() {
        List<RecoveryTask> tasks = new ArrayList<>();
        
        String sql = "SELECT id, name, user_id, source_connection, target_connection, " +
                     "migration_mode, status, progress, created_at, sync_objects, source_db_name, " +
                     "source_type, target_type, task_type, dr_mode " +
                     "FROM workflows " +
                     "WHERE status IN ('STARTING', 'FULL_MIGRATING', 'FULL_COMPLETED', 'INCREMENT_RUNNING', 'SUBSCRIBE_RUNNING', 'SWITCHING') " +
                     "AND is_deleted = 0 " +
                     "ORDER BY created_at ASC";
        
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                RecoveryTask task = mapResultSetToTask(rs);
                tasks.add(task);
                logger.info("Found unfinished task: id={}, name={}, status={}, mode={}, sourceType={}, targetType={}", 
                    task.getTaskId(), task.getTaskName(), task.getStatus(), task.getMigrationMode(),
                    task.getSourceType(), task.getTargetType());
            }
            
            logger.info("Total unfinished tasks found: {}", tasks.size());
            
        } catch (SQLException e) {
            logger.error("Error querying unfinished tasks from database", e);
        }
        
        return tasks;
    }
    
    public RecoveryTask getTaskById(String taskId) {
        String sql = "SELECT id, name, user_id, source_connection, target_connection, " +
                     "migration_mode, status, progress, created_at, sync_objects, source_db_name, " +
                     "source_type, target_type, task_type, dr_mode " +
                     "FROM workflows WHERE id = ?";
        
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, taskId);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                return mapResultSetToTask(rs);
            }
            
        } catch (SQLException e) {
            logger.error("Error querying task by id: {}", taskId, e);
        }
        
        return null;
    }

    private RecoveryTask mapResultSetToTask(ResultSet rs) throws SQLException {
        RecoveryTask task = new RecoveryTask();
        task.setTaskId(rs.getString("id"));
        task.setTaskName(rs.getString("name"));
        task.setUserId(rs.getLong("user_id"));
        // workflows 表的连接串是 AES-GCM 静态加密的（backend JPA converter 只在后端侧解密），
        // agent 直连 DB 读出的是 ENC: 密文，恢复前必须解密，否则连接串解析失败、任务恢复即失败
        task.setSourceConnection(com.migration.common.crypto.CredentialCipher.decrypt(rs.getString("source_connection")));
        task.setTargetConnection(com.migration.common.crypto.CredentialCipher.decrypt(rs.getString("target_connection")));
        task.setMigrationMode(rs.getString("migration_mode"));
        task.setStatus(rs.getString("status"));
        task.setProgress(rs.getInt("progress"));
        
        Timestamp createdAt = rs.getTimestamp("created_at");
        if (createdAt != null) {
            task.setCreatedAt(createdAt.toLocalDateTime());
        }
        
        task.setSyncObjects(rs.getString("sync_objects"));
        task.setSourceDbName(rs.getString("source_db_name"));

        try {
            String sourceType = rs.getString("source_type");
            String targetType = rs.getString("target_type");
            task.setSourceType(sourceType != null ? sourceType : "mysql");
            task.setTargetType(targetType != null ? targetType : "mysql");
        } catch (SQLException e) {
            logger.debug("source_type/target_type columns not found in workflows table, using defaults");
            task.setSourceType("mysql");
            task.setTargetType("mysql");
        }

        try {
            String taskType = rs.getString("task_type");
            task.setTaskType(taskType != null ? taskType : "SYNC");
        } catch (SQLException e) {
            logger.debug("task_type column not found in workflows table, using default SYNC");
            task.setTaskType("SYNC");
        }

        try {
            task.setDrMode(rs.getString("dr_mode"));
        } catch (SQLException e) {
            logger.debug("dr_mode column not found in workflows table, leaving null");
        }
        
        return task;
    }
}
