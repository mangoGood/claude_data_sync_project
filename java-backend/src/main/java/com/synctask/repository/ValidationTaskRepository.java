package com.synctask.repository;

import com.synctask.entity.ValidationTask;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ValidationTaskRepository extends JpaRepository<ValidationTask, String> {
    
    Page<ValidationTask> findByUserIdAndIsDeletedFalseOrderByCreatedAtDesc(Long userId, Pageable pageable);
    
    List<ValidationTask> findByUserIdAndIsDeletedFalseOrderByCreatedAtDesc(Long userId);
    
    Optional<ValidationTask> findByIdAndUserIdAndIsDeletedFalse(String id, Long userId);
    
    Optional<ValidationTask> findByIdAndIsDeletedFalse(String id);

    List<ValidationTask> findByWorkflowIdAndStatusInAndIsDeletedFalse(String workflowId, List<ValidationTask.ValidationStatus> statuses);
}
