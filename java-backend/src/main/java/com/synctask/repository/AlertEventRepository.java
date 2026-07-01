package com.synctask.repository;

import com.synctask.entity.AlertEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface AlertEventRepository extends JpaRepository<AlertEvent, Long> {
    Page<AlertEvent> findByUserIdOrderByCreatedAtDesc(Long userId, Pageable pageable);
    List<AlertEvent> findByWorkflowIdOrderByCreatedAtDesc(String workflowId);
    List<AlertEvent> findByStatus(String status);
}
