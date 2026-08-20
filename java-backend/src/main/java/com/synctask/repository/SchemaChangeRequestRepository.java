package com.synctask.repository;

import com.synctask.entity.SchemaChangeRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SchemaChangeRequestRepository extends JpaRepository<SchemaChangeRequest, Long> {

    java.util.List<SchemaChangeRequest> findByStatusOrderByCreatedAtDesc(SchemaChangeRequest.Status status);
    java.util.List<SchemaChangeRequest> findByWorkflowIdOrderByCreatedAtDesc(String workflowId);
    java.util.Optional<SchemaChangeRequest> findByWorkflowIdAndSeqno(String workflowId, Long seqno);
    java.util.List<SchemaChangeRequest> findByWorkflowIdAndStatus(String workflowId, SchemaChangeRequest.Status status);
}
