package com.synctask.repository;

import com.synctask.entity.AgentNode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface AgentNodeRepository extends JpaRepository<AgentNode, String> {

    /** 心跳仍新鲜的 agent（容量占比排序放在服务层做，避免依赖方言函数）。 */
    @Query("SELECT a FROM AgentNode a WHERE a.status = 'ONLINE' AND a.heartbeatAt >= :since")
    List<AgentNode> findAlive(@Param("since") LocalDateTime since);
}
