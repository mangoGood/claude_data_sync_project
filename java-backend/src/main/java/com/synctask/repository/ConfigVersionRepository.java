package com.synctask.repository;

import com.synctask.entity.ConfigVersion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface ConfigVersionRepository extends JpaRepository<ConfigVersion, Long> {
    List<ConfigVersion> findByWorkflowIdOrderByVersionNumberDesc(String workflowId);
    Optional<ConfigVersion> findTopByWorkflowIdOrderByVersionNumberDesc(String workflowId);
    Optional<ConfigVersion> findByWorkflowIdAndVersionNumber(String workflowId, Integer versionNumber);
}
