package com.synctask.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 流量复制/回放任务的专属配置（与 {@link Workflow} 1:1）。
 *
 * <p>不并进 {@code workflows}：那张表已有 60+ 列，而这批字段只对
 * {@code TRAFFIC_CAPTURE}/{@code TRAFFIC_REPLAY} 两种任务类型有意义。
 */
@Entity
@Table(name = "traffic_task_config")
public class TrafficTaskConfig {

    public static final String TYPE_CAPTURE = "TRAFFIC_CAPTURE";
    public static final String TYPE_REPLAY = "TRAFFIC_REPLAY";

    @Id
    @Column(name = "task_id", length = 36)
    private String taskId;

    // ===== 捕获侧 =====
    @Column(name = "capture_backend", length = 20)
    private String captureBackend = "GENERAL_LOG";

    /**
     * 源端/目标端引擎：{@code mysql} / {@code postgresql} / {@code oracle}。
     *
     * <p>三种引擎的捕获通道没有一行共用代码，录制格式也自带引擎标记——
     * 回放向导要靠它把异引擎的录制过滤掉（跨引擎回放是硬拦的，见 E3131）。
     */
    @Column(length = 20)
    private String engine = "mysql";

    /**
     * 源端被改动的原始状态（JSON），兜底还原的依据。
     *
     * <p>MySQL 是两个全局变量、PG 是四个 GUC 的原值与来源、
     * Oracle 是两条审计策略名与启用范围——形状完全不同，所以不再往表上加列。
     */
    @Column(name = "src_state_before", columnDefinition = "TEXT")
    private String srcStateBefore;

    @Column(name = "capture_databases", columnDefinition = "TEXT")
    private String captureDatabases;

    @Column(name = "capture_classes", length = 100)
    private String captureClasses = "SELECT,DML,DDL";

    @Column(name = "capture_users", columnDefinition = "TEXT")
    private String captureUsers;

    @Column(name = "capture_sample_rate", precision = 5, scale = 4)
    private BigDecimal captureSampleRate = BigDecimal.ONE;

    @Column(name = "capture_enrich")
    private Boolean captureEnrich = false;

    @Column(name = "capture_max_duration_ms")
    private Long captureMaxDurationMs = 7_200_000L;

    @Column(name = "capture_max_bytes")
    private Long captureMaxBytes = 21_474_836_480L;

    @Column(name = "capture_max_records")
    private Long captureMaxRecords = 100_000_000L;

    /**
     * 源库 {@code general_log} 的原值。
     *
     * <p>必须落库：捕获进程被 {@code kill -9}、agent 硬崩时，没有任何活着的进程
     * 记得该把它还原成什么，而不还原会让源库把语句日志一直写下去、直到磁盘满。
     */
    @Column(name = "src_general_log_before", length = 10)
    private String srcGeneralLogBefore;

    @Column(name = "src_log_output_before", length = 30)
    private String srcLogOutputBefore;

    /** 源库开关尚未确认还原。为 true 时 agent 扫尾与告警都要盯着它。 */
    @Column(name = "src_restore_pending")
    private Boolean srcRestorePending = false;

    // ===== 回放侧 =====
    @Column(name = "replay_recording_id", length = 36)
    private String replayRecordingId;

    @Column(name = "replay_speed", precision = 6, scale = 3)
    private BigDecimal replaySpeed = BigDecimal.ONE;

    @Column(name = "replay_classes", length = 100)
    private String replayClasses = "SELECT,DML";

    @Column(name = "replay_lag_policy", length = 20)
    private String replayLagPolicy = "WAIT";

    @Column(name = "replay_lag_skip_ms")
    private Long replayLagSkipMs = 5000L;

    @Column(name = "replay_gap_policy", length = 20)
    private String replayGapPolicy = "PRESERVE";

    @Column(name = "replay_max_sessions")
    private Integer replayMaxSessions = 200;

    @Column(name = "replay_compare", length = 20)
    private String replayCompare = "NONE";

    @Column(name = "replay_allow_dcl")
    private Boolean replayAllowDcl = false;

    @Column(name = "replay_allow_dangerous")
    private Boolean replayAllowDangerous = false;

    @Column(name = "replay_allow_same_instance")
    private Boolean replayAllowSameInstance = false;

    @Column(name = "replay_abort_error_rate", precision = 5, scale = 4)
    private BigDecimal replayAbortErrorRate = new BigDecimal("0.5");

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getEngine() { return engine; }
    public void setEngine(String v) { this.engine = v; }

    public String getSrcStateBefore() { return srcStateBefore; }
    public void setSrcStateBefore(String v) { this.srcStateBefore = v; }

    public String getCaptureBackend() { return captureBackend; }
    public void setCaptureBackend(String v) { this.captureBackend = v; }
    public String getCaptureDatabases() { return captureDatabases; }
    public void setCaptureDatabases(String v) { this.captureDatabases = v; }
    public String getCaptureClasses() { return captureClasses; }
    public void setCaptureClasses(String v) { this.captureClasses = v; }
    public String getCaptureUsers() { return captureUsers; }
    public void setCaptureUsers(String v) { this.captureUsers = v; }
    public BigDecimal getCaptureSampleRate() { return captureSampleRate; }
    public void setCaptureSampleRate(BigDecimal v) { this.captureSampleRate = v; }
    public Boolean getCaptureEnrich() { return captureEnrich; }
    public void setCaptureEnrich(Boolean v) { this.captureEnrich = v; }
    public Long getCaptureMaxDurationMs() { return captureMaxDurationMs; }
    public void setCaptureMaxDurationMs(Long v) { this.captureMaxDurationMs = v; }
    public Long getCaptureMaxBytes() { return captureMaxBytes; }
    public void setCaptureMaxBytes(Long v) { this.captureMaxBytes = v; }
    public Long getCaptureMaxRecords() { return captureMaxRecords; }
    public void setCaptureMaxRecords(Long v) { this.captureMaxRecords = v; }
    public String getSrcGeneralLogBefore() { return srcGeneralLogBefore; }
    public void setSrcGeneralLogBefore(String v) { this.srcGeneralLogBefore = v; }
    public String getSrcLogOutputBefore() { return srcLogOutputBefore; }
    public void setSrcLogOutputBefore(String v) { this.srcLogOutputBefore = v; }
    public Boolean getSrcRestorePending() { return srcRestorePending; }
    public void setSrcRestorePending(Boolean v) { this.srcRestorePending = v; }
    public String getReplayRecordingId() { return replayRecordingId; }
    public void setReplayRecordingId(String v) { this.replayRecordingId = v; }
    public BigDecimal getReplaySpeed() { return replaySpeed; }
    public void setReplaySpeed(BigDecimal v) { this.replaySpeed = v; }
    public String getReplayClasses() { return replayClasses; }
    public void setReplayClasses(String v) { this.replayClasses = v; }
    public String getReplayLagPolicy() { return replayLagPolicy; }
    public void setReplayLagPolicy(String v) { this.replayLagPolicy = v; }
    public Long getReplayLagSkipMs() { return replayLagSkipMs; }
    public void setReplayLagSkipMs(Long v) { this.replayLagSkipMs = v; }
    public String getReplayGapPolicy() { return replayGapPolicy; }
    public void setReplayGapPolicy(String v) { this.replayGapPolicy = v; }
    public Integer getReplayMaxSessions() { return replayMaxSessions; }
    public void setReplayMaxSessions(Integer v) { this.replayMaxSessions = v; }
    public String getReplayCompare() { return replayCompare; }
    public void setReplayCompare(String v) { this.replayCompare = v; }
    public Boolean getReplayAllowDcl() { return replayAllowDcl; }
    public void setReplayAllowDcl(Boolean v) { this.replayAllowDcl = v; }
    public Boolean getReplayAllowDangerous() { return replayAllowDangerous; }
    public void setReplayAllowDangerous(Boolean v) { this.replayAllowDangerous = v; }
    public Boolean getReplayAllowSameInstance() { return replayAllowSameInstance; }
    public void setReplayAllowSameInstance(Boolean v) { this.replayAllowSameInstance = v; }
    public BigDecimal getReplayAbortErrorRate() { return replayAbortErrorRate; }
    public void setReplayAbortErrorRate(BigDecimal v) { this.replayAbortErrorRate = v; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
}
