package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 血缘节点：某个字段的一个版本。
 *
 * <p>有效区间用 seqno 而不是墙上时间——与 {@code schema-timeline} 同一把尺子，
 * 才能回答"某个位点时这一列从哪来"。列改过名之后，旧节点闭合、新节点开出，
 * 两者之间由一条 RENAME 边相连。
 */
@Entity
@Table(name = "lineage_node")
public class LineageNode {

    public enum Side { SOURCE, TARGET }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Side side;

    @Column(name = "db_name", nullable = false, length = 128)
    private String dbName;

    @Column(name = "table_name", nullable = false, length = 128)
    private String tableName;

    @Column(name = "column_name", nullable = false, length = 128)
    private String columnName;

    @Column(name = "valid_from_seqno", nullable = false)
    private Long validFromSeqno = 0L;

    /** NULL = 当前有效 */
    @Column(name = "valid_to_seqno")
    private Long validToSeqno;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    void onCreate() {
        createdAt = LocalDateTime.now();
    }

    /** "库.表.列"，日志与 API 里用它做人类可读标识。 */
    public String qualifiedName() {
        return dbName + "." + tableName + "." + columnName;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Side getSide() { return side; }
    public void setSide(Side side) { this.side = side; }
    public String getDbName() { return dbName; }
    public void setDbName(String dbName) { this.dbName = dbName; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
    public Long getValidFromSeqno() { return validFromSeqno; }
    public void setValidFromSeqno(Long v) { this.validFromSeqno = v; }
    public Long getValidToSeqno() { return validToSeqno; }
    public void setValidToSeqno(Long v) { this.validToSeqno = v; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime t) { this.createdAt = t; }
}
