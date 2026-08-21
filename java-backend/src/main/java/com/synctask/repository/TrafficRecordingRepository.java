package com.synctask.repository;

import com.synctask.entity.TrafficRecording;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TrafficRecordingRepository extends JpaRepository<TrafficRecording, String> {

    Page<TrafficRecording> findByUserIdAndIsDeletedFalseOrderByCreatedAtDesc(Long userId, Pageable pageable);

    List<TrafficRecording> findByUserIdAndIsDeletedFalseAndSealedTrueOrderByCreatedAtDesc(Long userId);

    Optional<TrafficRecording> findByIdAndIsDeletedFalse(String id);

    Optional<TrafficRecording> findFirstByCaptureTaskIdAndIsDeletedFalse(String captureTaskId);
}
