package com.synctask.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import java.time.LocalDateTime;

/** 血缘边：一次字段级变换。 */
@Entity
@Table(name = "lineage_edge")
public class LineageEdge {

    /**
     * 变换算子。
     *
     * <p>这些不是凭空定义的，每一个都对应平台已有的一项能力——
     * 血缘的采集<b>不需要新探针</b>，只需要把这些配置归一成图。
     */
    public enum Operator {
        /** 原样搬运 */
        IDENTITY,
        /** 列名映射（column.mapping） */
        RENAME,
        /** 脱敏（column.mask） */
        MASK,
        /** 行过滤（column.filter）——整表口径，记在受影响的列上 */
        FILTER,
        /** 附加列（column.extra）：目标端有、源端没有 */
        DEFAULT_VALUE,
        /** 分片拆分（route.split） */
        ROUTE_SPLIT,
        /** 分片汇聚（route.merge） */
        ROUTE_MERGE,
        /** 不同步 */
        DROP
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "src_node_id", nullable = false)
    private Long srcNodeId;

    @Column(name = "dst_node_id", nullable = false)
    private Long dstNodeId;

    /**
     * 存成 VARCHAR 而不是 MySQL 原生 ENUM。
     *
     * <p>Hibernate 6 默认把 Java 枚举映射成 MySQL ENUM，而<b>算子集合是会长的</b>——
     * 将来新增一种变换（比如加密、聚合）就得先跑一次 ALTER 才能写入。
     * 其余几个枚举（敏感级别、审批状态）取值封闭，用 ENUM 合适；这一个不是。
     * 不显式指定的话，{@code ddl-auto: validate} 会直接拒绝启动
     * （建表脚本写 VARCHAR、实体期望 ENUM）。
     */
    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(org.hibernate.type.SqlTypes.VARCHAR)
    @Column(nullable = false, length = 32)
    private Operator operator;

    @Column(name = "operator_detail", columnDefinition = "JSON")
    private String operatorDetail;

    @Column(name = "workflow_id", nullable = false, length = 36)
    private String workflowId;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getSrcNodeId() { return srcNodeId; }
    public void setSrcNodeId(Long v) { this.srcNodeId = v; }
    public Long getDstNodeId() { return dstNodeId; }
    public void setDstNodeId(Long v) { this.dstNodeId = v; }
    public Operator getOperator() { return operator; }
    public void setOperator(Operator o) { this.operator = o; }
    public String getOperatorDetail() { return operatorDetail; }
    public void setOperatorDetail(String d) { this.operatorDetail = d; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String id) { this.workflowId = id; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime t) { this.createdAt = t; }
}
