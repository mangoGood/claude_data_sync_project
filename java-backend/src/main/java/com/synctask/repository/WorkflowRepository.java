package com.synctask.repository;

import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface WorkflowRepository extends JpaRepository<Workflow, String>, JpaSpecificationExecutor<Workflow> {
    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false")
    Page<Workflow> findByUserId(@Param("userId") Long userId, Pageable pageable);
    
    List<Workflow> findByUserId(Long userId);
    List<Workflow> findByUserIdAndStatus(Long userId, WorkflowStatus status);
    
    List<Workflow> findByUserIdAndIsDeletedFalse(Long userId);
    
    List<Workflow> findByUserIdAndIsDeletedFalseOrderByCreatedAtDesc(Long userId);
    
    Optional<Workflow> findByIdAndUserIdAndIsDeletedFalse(String id, Long userId);
    
    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false AND " +
           "(LOWER(w.name) LIKE LOWER(CONCAT('%', :keyword, '%')) OR LOWER(w.id) LIKE LOWER(CONCAT('%', :keyword, '%')))")
    Page<Workflow> findByUserIdAndKeyword(@Param("userId") Long userId, @Param("keyword") String keyword, Pageable pageable);
    
    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false AND w.status = :status")
    Page<Workflow> findByUserIdAndStatus(@Param("userId") Long userId, @Param("status") WorkflowStatus status, Pageable pageable);
    
    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false AND " +
           "(LOWER(w.name) LIKE LOWER(CONCAT('%', :keyword, '%')) OR LOWER(w.id) LIKE LOWER(CONCAT('%', :keyword, '%'))) AND w.status = :status")
    Page<Workflow> findByUserIdAndKeywordAndStatus(@Param("userId") Long userId, @Param("keyword") String keyword, @Param("status") WorkflowStatus status, Pageable pageable);
    
    List<Workflow> findByUserIdAndStatusAndIsDeletedFalse(Long userId, WorkflowStatus status);

    /** 按状态取任务，排除指定 taskType（用于异常任务列表过滤掉 DR_SHADOW 隐藏影子任务），下推到 DB 查询。 */
    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false " +
           "AND w.status = :status AND w.taskType <> :excludeTaskType")
    List<Workflow> findByUserIdAndStatusExcludingTaskType(@Param("userId") Long userId,
                                                          @Param("status") WorkflowStatus status,
                                                          @Param("excludeTaskType") String excludeTaskType);

    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false AND w.taskType = :taskType")
    Page<Workflow> findByUserIdAndTaskType(@Param("userId") Long userId, @Param("taskType") String taskType, Pageable pageable);

    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false AND w.taskType = :taskType AND " +
           "(LOWER(w.name) LIKE LOWER(CONCAT('%', :keyword, '%')) OR LOWER(w.id) LIKE LOWER(CONCAT('%', :keyword, '%')))")
    Page<Workflow> findByUserIdAndTaskTypeAndKeyword(@Param("userId") Long userId, @Param("taskType") String taskType, @Param("keyword") String keyword, Pageable pageable);

    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false AND w.taskType = :taskType AND w.status = :status")
    Page<Workflow> findByUserIdAndTaskTypeAndStatus(@Param("userId") Long userId, @Param("taskType") String taskType, @Param("status") WorkflowStatus status, Pageable pageable);

    @Query("SELECT w FROM Workflow w WHERE w.userId = :userId AND w.isDeleted = false AND w.taskType = :taskType AND " +
           "(LOWER(w.name) LIKE LOWER(CONCAT('%', :keyword, '%')) OR LOWER(w.id) LIKE LOWER(CONCAT('%', :keyword, '%'))) AND w.status = :status")
    Page<Workflow> findByUserIdAndTaskTypeAndKeywordAndStatus(@Param("userId") Long userId, @Param("taskType") String taskType, @Param("keyword") String keyword, @Param("status") WorkflowStatus status, Pageable pageable);

    boolean existsByUserIdAndTaskTypeAndNameAndIsDeletedFalse(Long userId, String taskType, String name);
}
