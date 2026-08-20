package com.synctask.repository;

import com.synctask.entity.LineageEdge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LineageEdgeRepository extends JpaRepository<LineageEdge, Long> {

    java.util.List<LineageEdge> findBySrcNodeId(Long srcNodeId);
    java.util.List<LineageEdge> findByDstNodeId(Long dstNodeId);
    java.util.List<LineageEdge> findByWorkflowId(String workflowId);
    void deleteByWorkflowId(String workflowId);
}
