package com.synctask.repository;

import com.synctask.entity.TaskSchedule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface TaskScheduleRepository extends JpaRepository<TaskSchedule, Long> {
    List<TaskSchedule> findByUserIdAndEnabledTrue(Long userId);
    List<TaskSchedule> findByWorkflowIdAndUserId(String workflowId, Long userId);
    List<TaskSchedule> findByEnabledTrue();
}
