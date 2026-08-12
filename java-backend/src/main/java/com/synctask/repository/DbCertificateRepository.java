package com.synctask.repository;

import com.synctask.entity.DbCertificate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DbCertificateRepository extends JpaRepository<DbCertificate, String> {

    List<DbCertificate> findByUserIdOrderByCreatedAtDesc(Long userId);

    Optional<DbCertificate> findByIdAndUserId(String id, Long userId);

    boolean existsByUserIdAndName(Long userId, String name);

    /**
     * 引用计数：证书被任务引用时不允许删除。
     *
     * <p>删掉正在被引用的证书，任务会在**下一次重启/续传**时才失败——那时没人会把它
     * 和几天前的一次删除联系起来。所以在删除这一侧挡住。
     */
    @Query("SELECT COUNT(w) FROM Workflow w WHERE w.isDeleted = false "
            + "AND (w.sourceSslCertId = :certId OR w.targetSslCertId = :certId)")
    long countReferencingWorkflows(@Param("certId") String certId);
}
