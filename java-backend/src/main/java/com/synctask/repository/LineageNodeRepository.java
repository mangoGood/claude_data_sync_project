package com.synctask.repository;

import com.synctask.entity.LineageNode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LineageNodeRepository extends JpaRepository<LineageNode, Long> {

    java.util.List<LineageNode> findByDbNameAndTableNameAndValidToSeqnoIsNull(String dbName, String tableName);
    java.util.Optional<LineageNode> findFirstBySideAndDbNameAndTableNameAndColumnNameAndValidToSeqnoIsNull(
            LineageNode.Side side, String dbName, String tableName, String columnName);
    java.util.List<LineageNode> findByDbNameAndTableNameAndColumnNameAndValidToSeqnoIsNull(
            String dbName, String tableName, String columnName);
}
