package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * agent 在元数据库里的注册与心跳（P1-1 集群化）。
 *
 * <p>原来的下发是「Kafka 广播 + 谁抢到算谁」：agent 硬崩后它名下的任务<b>没有人接管</b>，
 * 只能等这台机器被人拉起来。恢复能力本身早就有（子进程各自 checkpoint 续传），
 * 缺的只是「谁负责这个任务」这条信息——这个类负责把它写进 {@code agents} 表。
 *
 * <p>心跳同时做两件事：刷新 {@code agents.heartbeat_at}，以及给本 agent 正在跑的任务
 * <b>续租</b>（{@code workflows.lease_expire_at}）。两者缺一不可：
 * 只刷 agent 心跳，进程活着但任务线程死了照样没人发现；只续任务租约，
 * 后端就无从知道这台机器还在不在、该把任务改派给谁。
 *
 * <p>agent_id 必须<b>跨重启稳定</b>（否则每次重启都变成一台"新机器"，旧记录永远躺在表里
 * 且它名下的任务会被判成失联而被抢占）：优先取 {@code MIGRATION_AGENT_ID}，
 * 否则落在 {@code files/.agent_id} 里，首次生成后一直复用。
 */
public class AgentRegistryService {
    private static final Logger logger = LoggerFactory.getLogger(AgentRegistryService.class);

    /** 租约时长：心跳间隔的数倍，容忍偶发的 GC / 元数据库抖动。 */
    static final int LEASE_SECONDS = 90;
    private static final long HEARTBEAT_INTERVAL_MS = 15000;
    private static final String AGENT_ID_FILE = "files/.agent_id";

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String agentId;
    private final String host;
    private final int port;
    private final int capacity;
    /** 当前在跑的任务 id（心跳时用来刷新 running_tasks 与续租）。 */
    private final Supplier<java.util.Set<String>> runningTasks;

    private ScheduledExecutorService heartbeatExecutor;

    public AgentRegistryService(AgentConfig config, Supplier<java.util.Set<String>> runningTasks) {
        this.dbUrl = config.getMysqlDbUrl();
        this.dbUser = config.getMysqlDbUser();
        this.dbPassword = config.getMysqlDbPassword();
        this.agentId = resolveAgentId();
        this.host = resolveHost();
        this.port = config.getHttpServerPort();
        this.capacity = Integer.parseInt(config.getRawProperty("agent.capacity", "10"));
        this.runningTasks = runningTasks;
    }

    public String getAgentId() {
        return agentId;
    }

    /** 注册（存在即更新）并启动心跳。元数据库不可达时只告警不阻断——单机部署照常跑。 */
    public void start() {
        try {
            register();
        } catch (SQLException e) {
            logger.warn("agent 注册失败（集群功能降级，仍按单机运行）: {}", e.getMessage());
        }
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "agent-heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeatExecutor.scheduleAtFixedRate(this::heartbeatQuietly,
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        logger.info("agent 已注册: agentId={} {}:{} capacity={}", agentId, host, port, capacity);
    }

    /** 优雅停机：置 OFFLINE 并释放租约，后端立刻就能改派，不用等心跳超时。 */
    public void stop() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        try (Connection conn = connect()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE agents SET status='OFFLINE', heartbeat_at=? WHERE agent_id=?")) {
                ps.setTimestamp(1, now());
                ps.setString(2, agentId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE workflows SET lease_expire_at=? WHERE agent_id=?")) {
                ps.setTimestamp(1, now());
                ps.setString(2, agentId);
                ps.executeUpdate();
            }
            logger.info("agent 已下线: agentId={}", agentId);
        } catch (SQLException e) {
            logger.warn("agent 下线登记失败: {}", e.getMessage());
        }
    }

    /**
     * 时间戳一律由 <b>JVM 侧</b>绑定，绝不用 SQL 的 {@code NOW()}。
     *
     * <p>元数据库跑在容器里（UTC），backend 的存活判定用的是 {@code LocalDateTime.now()}（JVM 时区）。
     * 用 {@code NOW()} 写心跳，两边差一个时区偏移——实测差 8 小时，于是<b>任何 agent 都永远显示"不在线"</b>，
     * 选派退回广播、故障转移整个不工作，而日志里只有一句"没有存活的 agent 注册"。
     */
    private static Timestamp now() {
        return new Timestamp(System.currentTimeMillis());
    }

    private void register() throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO agents (agent_id, host, port, capacity, running_tasks, status, version, started_at, heartbeat_at) " +
                     "VALUES (?, ?, ?, ?, 0, 'ONLINE', ?, ?, ?) " +
                     "ON DUPLICATE KEY UPDATE host=VALUES(host), port=VALUES(port), capacity=VALUES(capacity), " +
                     "status='ONLINE', version=VALUES(version), started_at=VALUES(started_at), heartbeat_at=VALUES(heartbeat_at)")) {
            ps.setString(1, agentId);
            ps.setString(2, host);
            ps.setInt(3, port);
            ps.setInt(4, capacity);
            ps.setString(5, getClass().getPackage().getImplementationVersion());
            ps.setTimestamp(6, now());
            ps.setTimestamp(7, now());
            ps.executeUpdate();
        }
    }

    private void heartbeatQuietly() {
        try {
            heartbeat();
        } catch (Exception e) {
            // 元数据库抖动不该把 agent 拖垮：心跳丢几拍最坏结果是任务被改派，
            // 而改派后新 agent 起进程时会被任务实例锁挡住（P0-3），不会双写
            logger.warn("agent 心跳失败: {}", e.toString());
        }
    }

    void heartbeat() throws SQLException {
        java.util.Set<String> tasks = runningTasks != null ? runningTasks.get() : java.util.Collections.emptySet();
        try (Connection conn = connect()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE agents SET heartbeat_at=?, status='ONLINE', running_tasks=? WHERE agent_id=?")) {
                ps.setTimestamp(1, now());
                ps.setInt(2, tasks.size());
                ps.setString(3, agentId);
                if (ps.executeUpdate() == 0) {
                    register();   // 表被清过 / 首次注册失败：补一次
                }
            }
            if (tasks.isEmpty()) {
                return;
            }
            // 续租：只给"确实还在本 agent 上跑"的任务续，进程活着但任务线程没了的照样会过期被接管
            StringBuilder sql = new StringBuilder(
                    "UPDATE workflows SET lease_expire_at = ?, agent_id=? WHERE id IN (");
            for (int i = 0; i < tasks.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(')');
            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                ps.setTimestamp(idx++, leaseDeadline());
                ps.setString(idx++, agentId);
                for (String taskId : tasks) {
                    ps.setString(idx++, taskId);
                }
                ps.executeUpdate();
            }
        }
    }

    private static Timestamp leaseDeadline() {
        return new Timestamp(System.currentTimeMillis() + LEASE_SECONDS * 1000L);
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    /**
     * 本 agent 是否有权执行该任务：未指派（NULL）或指派给自己都算。
     *
     * <p>启动恢复必须过这道闸——否则集群里每台 agent 都会把<b>所有</b>未完成任务捞起来重跑，
     * 变成人为的双写。查不到记录（元数据库不可达）时返回 true：宁可退回旧的单机语义，
     * 也不能因为查不到库就让任务全体停摆。
     */
    public boolean ownsTask(String taskId) {
        String sql = "SELECT agent_id, lease_expire_at FROM workflows WHERE id=?";
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return true;
                }
                String owner = rs.getString("agent_id");
                if (owner == null || owner.isEmpty() || owner.equals(agentId)) {
                    return true;
                }
                java.sql.Timestamp lease = rs.getTimestamp("lease_expire_at");
                boolean expired = lease == null || lease.getTime() < System.currentTimeMillis();
                if (expired) {
                    logger.info("[{}] 任务归属 {} 但租约已过期，本 agent 接管", taskId, owner);
                }
                return expired;
            }
        } catch (SQLException e) {
            logger.warn("[{}] 查询任务归属失败，按可执行处理: {}", taskId, e.getMessage());
            return true;
        }
    }

    /** 抢占/接管一个任务：把归属改成自己并续上租约。 */
    public void claimTask(String taskId) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE workflows SET agent_id=?, lease_expire_at=?, lease_epoch=lease_epoch+1 WHERE id=?")) {
            ps.setString(1, agentId);
            ps.setTimestamp(2, leaseDeadline());
            ps.setString(3, taskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.warn("[{}] 认领任务失败: {}", taskId, e.getMessage());
        }
    }

    private static String resolveAgentId() {
        String fromEnv = System.getenv("MIGRATION_AGENT_ID");
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return fromEnv.trim();
        }
        File f = new File(AGENT_ID_FILE);
        try {
            if (f.isFile()) {
                String id = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8).trim();
                if (!id.isEmpty()) {
                    return id;
                }
            }
            String id = "agent-" + UUID.randomUUID().toString().substring(0, 8);
            if (f.getParentFile() != null) {
                f.getParentFile().mkdirs();
            }
            Files.write(f.toPath(), id.getBytes(StandardCharsets.UTF_8));
            return id;
        } catch (IOException e) {
            // 落不了盘就退化成本次进程内唯一：功能可用，只是重启后会换个身份
            logger.warn("生成 agent_id 失败，使用临时 id: {}", e.getMessage());
            return "agent-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }

    private static String resolveHost() {
        String fromEnv = System.getenv("MIGRATION_AGENT_HOST");
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return fromEnv.trim();
        }
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}
