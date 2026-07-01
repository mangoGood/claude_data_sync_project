package com.synctask.repository;

import com.synctask.entity.AuditLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 审计日志 Repository
 */
@Repository
public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {

    /** 按用户查询 */
    Page<AuditLog> findByUserIdOrderByCreatedAtDesc(Long userId, Pageable pageable);

    /** 按工作流查询 */
    List<AuditLog> findByWorkflowIdOrderByCreatedAtDesc(String workflowId);

    /** 按操作类型查询 */
    Page<AuditLog> findByActionOrderByCreatedAtDesc(AuditLog.Action action, Pageable pageable);

    /** 按时间范围查询 */
    List<AuditLog> findByCreatedAtBetweenOrderByCreatedAtDesc(LocalDateTime start, LocalDateTime end);

    /** 按用户和操作类型查询 */
    List<AuditLog> findByUserIdAndAction(Long userId, AuditLog.Action action);
}
