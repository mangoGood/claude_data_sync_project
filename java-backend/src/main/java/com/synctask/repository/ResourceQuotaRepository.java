package com.synctask.repository;

import com.synctask.entity.ResourceQuota;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface ResourceQuotaRepository extends JpaRepository<ResourceQuota, Long> {
    Optional<ResourceQuota> findByUserId(Long userId);
}
