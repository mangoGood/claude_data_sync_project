package com.synctask.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/**
 * 一份流量录制文件的目录项。
 *
 * <p>文件本体躺在产出它的那台 agent 上（{@code files/<captureTaskId>/traffic/}），
 * 这里只记元数据 + {@code agentId}——集群里不记归属就不知道该去哪台机器取文件。
 */
@Entity
@Table(name = "traffic_recordings")
public class TrafficRecording {

    @Id
    @Column(length = 36)
    private String id;

    /** 产出它的捕获任务；上传进来的录制为 null。 */
    @Column(name = "capture_task_id", length = 36)
    private String captureTaskId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "agent_id", length = 64)
    private String agentId;

    @Column(nullable = false)
    private String name;

    @Column(name = "t0_wall")
    private LocalDateTime t0Wall;

    @Column(name = "end_wall")
    private LocalDateTime endWall;

    @Column(name = "duration_ms")
    private Long durationMs = 0L;

    @Column(name = "record_count")
    private Long recordCount = 0L;

    @Column(name = "byte_size")
    private Long byteSize = 0L;

    @Column(name = "session_count")
    private Integer sessionCount = 0;

    /** 时间轴空洞数。非 0 表示捕获中断过，那段时间源库的语句<b>永久丢失</b>。 */
    @Column(name = "gap_count")
    private Integer gapCount = 0;

    @Column(name = "stats_json", columnDefinition = "TEXT")
    private String statsJson;

    /** {@code manifest.source} 原样：回放前逐项比对（尤其是 server_uuid）的依据。 */
    @Column(name = "source_fingerprint", columnDefinition = "TEXT")
    private String sourceFingerprint;

    @Column(length = 64)
    private String sha256;

    /** 未封口的录制不允许回放：它的分段清单和统计都还不完整。 */
    @Column
    private Boolean sealed = false;

    @Column(name = "is_deleted")
    private Boolean isDeleted = false;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) createdAt = LocalDateTime.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getCaptureTaskId() { return captureTaskId; }
    public void setCaptureTaskId(String v) { this.captureTaskId = v; }
    public Long getUserId() { return userId; }
    public void setUserId(Long v) { this.userId = v; }
    public String getAgentId() { return agentId; }
    public void setAgentId(String v) { this.agentId = v; }
    public String getName() { return name; }
    public void setName(String v) { this.name = v; }
    public LocalDateTime getT0Wall() { return t0Wall; }
    public void setT0Wall(LocalDateTime v) { this.t0Wall = v; }
    public LocalDateTime getEndWall() { return endWall; }
    public void setEndWall(LocalDateTime v) { this.endWall = v; }
    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long v) { this.durationMs = v; }
    public Long getRecordCount() { return recordCount; }
    public void setRecordCount(Long v) { this.recordCount = v; }
    public Long getByteSize() { return byteSize; }
    public void setByteSize(Long v) { this.byteSize = v; }
    public Integer getSessionCount() { return sessionCount; }
    public void setSessionCount(Integer v) { this.sessionCount = v; }
    public Integer getGapCount() { return gapCount; }
    public void setGapCount(Integer v) { this.gapCount = v; }
    public String getStatsJson() { return statsJson; }
    public void setStatsJson(String v) { this.statsJson = v; }
    public String getSourceFingerprint() { return sourceFingerprint; }
    public void setSourceFingerprint(String v) { this.sourceFingerprint = v; }
    public String getSha256() { return sha256; }
    public void setSha256(String v) { this.sha256 = v; }
    public Boolean getSealed() { return sealed; }
    public void setSealed(Boolean v) { this.sealed = v; }
    public Boolean getIsDeleted() { return isDeleted; }
    public void setIsDeleted(Boolean v) { this.isDeleted = v; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime v) { this.createdAt = v; }
}
