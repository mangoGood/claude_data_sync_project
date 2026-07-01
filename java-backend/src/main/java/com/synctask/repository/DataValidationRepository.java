package com.synctask.repository;

import com.synctask.entity.DataValidation;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface DataValidationRepository extends JpaRepository<DataValidation, Long> {
    Page<DataValidation> findByUserIdOrderByCreatedAtDesc(Long userId, Pageable pageable);
    List<DataValidation> findByWorkflowIdOrderByCreatedAtDesc(String workflowId);
    List<DataValidation> findByWorkflowIdAndStatus(String workflowId, String status);
}
