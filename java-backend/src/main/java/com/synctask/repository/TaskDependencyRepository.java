package com.synctask.repository;

import com.synctask.entity.TaskDependency;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface TaskDependencyRepository extends JpaRepository<TaskDependency, Long> {
    List<TaskDependency> findByUpstreamWorkflowIdAndEnabledTrue(String upstreamWorkflowId);
    List<TaskDependency> findByUserId(Long userId);
    List<TaskDependency> findByDownstreamWorkflowIdAndUserId(String downstreamWorkflowId, Long userId);
}
