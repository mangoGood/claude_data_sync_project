package com.synctask.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/** 自动分级规则：按列名正则命中。 */
@Entity
@Table(name = "classification_rule")
public class ClassificationRule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 128, unique = true)
    private String name;

    @Column(name = "column_pattern", nullable = false, length = 256)
    private String columnPattern;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private DataClassification.Level level;

    @Column(length = 512)
    private String tags;

    @Column(nullable = false)
    private Boolean enabled = true;

    /** 数值小的先匹配：更具体的规则应当排在更宽泛的前面。 */
    @Column(nullable = false)
    private Integer priority = 100;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String v) { this.name = v; }
    public String getColumnPattern() { return columnPattern; }
    public void setColumnPattern(String v) { this.columnPattern = v; }
    public DataClassification.Level getLevel() { return level; }
    public void setLevel(DataClassification.Level v) { this.level = v; }
    public String getTags() { return tags; }
    public void setTags(String v) { this.tags = v; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean v) { this.enabled = v; }
    public Integer getPriority() { return priority; }
    public void setPriority(Integer v) { this.priority = v; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime t) { this.createdAt = t; }
}
