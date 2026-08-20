package com.synctask.repository;

import com.synctask.entity.DataClassification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataClassificationRepository extends JpaRepository<DataClassification, Long> {

    java.util.Optional<DataClassification> findByDbNameAndTableNameAndColumnName(String db, String table, String column);
    java.util.List<DataClassification> findByDbNameAndTableName(String db, String table);
    java.util.List<DataClassification> findByLevel(DataClassification.Level level);
}
