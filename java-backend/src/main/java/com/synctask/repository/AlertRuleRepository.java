package com.synctask.repository;

import com.synctask.entity.AlertRule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface AlertRuleRepository extends JpaRepository<AlertRule, Long> {
    List<AlertRule> findByUserId(Long userId);
    List<AlertRule> findByUserIdAndEnabledTrue(Long userId);
    List<AlertRule> findByWorkflowIdAndUserId(String workflowId, Long userId);
    List<AlertRule> findByEnabledTrue();
}
