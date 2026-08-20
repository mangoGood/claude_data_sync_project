package com.synctask.repository;

import com.synctask.entity.TrafficTaskConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TrafficTaskConfigRepository extends JpaRepository<TrafficTaskConfig, String> {

    /** 源库开关还没确认还原的捕获任务——agent 扫尾与告警要盯着它们。 */
    List<TrafficTaskConfig> findBySrcRestorePendingTrue();
}
