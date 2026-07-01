package com.synctask.repository;

import com.synctask.entity.SlowSqlRecord;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface SlowSqlRecordRepository extends JpaRepository<SlowSqlRecord, Long> {
    Page<SlowSqlRecord> findByUserIdOrderByCreatedAtDesc(Long userId, Pageable pageable);
    List<SlowSqlRecord> findByWorkflowIdOrderByCreatedAtDesc(String workflowId);
    List<SlowSqlRecord> findByWorkflowIdAndExecutionTimeMsGreaterThanEqualOrderByCreatedAtDesc(String workflowId, Long thresholdMs);
}
