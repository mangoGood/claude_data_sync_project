package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/** 列级数据分级与标签。 */
@Entity
@Table(name = "data_classification")
public class DataClassification {

    /**
     * 敏感级别。<b>顺序有意义</b>——策略校验"目标端级别不得低于源端"靠这个序，
     * 因此新增级别时必须插到正确的位置上，不能追加到末尾了事。
     */
    public enum Level {
        PUBLIC, INTERNAL, SENSITIVE, RESTRICTED;

        /** 本级别是否不低于另一个。 */
        public boolean atLeast(Level other) {
            return this.ordinal() >= other.ordinal();
        }
    }

    /** 打标来源：人工优先于规则，重跑规则不覆盖人工值。 */
    public enum Source { MANUAL, RULE }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "db_name", nullable = false, length = 128)
    private String dbName;

    @Column(name = "table_name", nullable = false, length = 128)
    private String tableName;

    @Column(name = "column_name", nullable = false, length = 128)
    private String columnName;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Level level = Level.INTERNAL;

    @Column(length = 512)
    private String tags;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Source source = Source.MANUAL;

    @Column(name = "matched_rule", length = 128)
    private String matchedRule;

    @Column(name = "updated_by")
    private Long updatedBy;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public String qualifiedName() {
        return dbName + "." + tableName + "." + columnName;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getDbName() { return dbName; }
    public void setDbName(String v) { this.dbName = v; }
    public String getTableName() { return tableName; }
    public void setTableName(String v) { this.tableName = v; }
    public String getColumnName() { return columnName; }
    public void setColumnName(String v) { this.columnName = v; }
    public Level getLevel() { return level; }
    public void setLevel(Level v) { this.level = v; }
    public String getTags() { return tags; }
    public void setTags(String v) { this.tags = v; }
    public Source getSource() { return source; }
    public void setSource(Source v) { this.source = v; }
    public String getMatchedRule() { return matchedRule; }
    public void setMatchedRule(String v) { this.matchedRule = v; }
    public Long getUpdatedBy() { return updatedBy; }
    public void setUpdatedBy(Long v) { this.updatedBy = v; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime t) { this.createdAt = t; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime t) { this.updatedAt = t; }
}
