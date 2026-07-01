package com.synctask.repository;

import com.synctask.entity.RetryPolicy;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface RetryPolicyRepository extends JpaRepository<RetryPolicy, Long> {
    List<RetryPolicy> findByUserId(Long userId);
    List<RetryPolicy> findByWorkflowIdAndUserId(String workflowId, Long userId);
    Optional<RetryPolicy> findByWorkflowIdAndErrorTypeAndEnabledTrue(String workflowId, String errorType);
    List<RetryPolicy> findByUserIdAndEnabledTrue(Long userId);
}
