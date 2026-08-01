package com.synctask.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/** 执行面 agent 注册表的一行（agent 自己写入并心跳，后端只读 + 选派时参考负载）。 */
@Entity
@Table(name = "agents")
public class AgentNode {

    @Id
    @Column(name = "agent_id", length = 64)
    private String agentId;

    @Column(name = "host", nullable = false)
    private String host;

    @Column(name = "port", nullable = false)
    private Integer port;

    @Column(name = "capacity", nullable = false)
    private Integer capacity = 10;

    @Column(name = "running_tasks", nullable = false)
    private Integer runningTasks = 0;

    @Column(name = "status", nullable = false, length = 20)
    private String status = "ONLINE";

    @Column(name = "version", length = 50)
    private String version;

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    @Column(name = "heartbeat_at")
    private LocalDateTime heartbeatAt;

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    public Integer getRunningTasks() {
        return runningTasks;
    }

    public void setRunningTasks(Integer runningTasks) {
        this.runningTasks = runningTasks;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public LocalDateTime getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(LocalDateTime startedAt) {
        this.startedAt = startedAt;
    }

    public LocalDateTime getHeartbeatAt() {
        return heartbeatAt;
    }

    public void setHeartbeatAt(LocalDateTime heartbeatAt) {
        this.heartbeatAt = heartbeatAt;
    }
}
