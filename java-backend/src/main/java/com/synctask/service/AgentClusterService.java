package com.synctask.service;

import com.synctask.entity.AgentNode;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowLog;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.AgentNodeRepository;
import com.synctask.repository.WorkflowLogRepository;
import com.synctask.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 执行面选派与故障转移（P1-1）。
 *
 * <p>原来的下发是「Kafka 广播 + 谁抢到算谁」：同一个消费组里谁拿到分区谁执行，
 * 后端不知道任务落在哪台机器上，agent 硬崩后它名下的任务<b>没有人接管</b>。
 * 恢复能力早就有（子进程各自 checkpoint 续传），缺的只是「谁负责」这条信息。
 *
 * <p>现在：启动时按负载挑一台存活 agent 写进 {@code workflows.agent_id} 再投 Kafka，
 * 消息带 {@code targetAgentId}，其它 agent 见到不是自己就放行；
 * agent 每 15s 心跳并给自己在跑的任务续租，租约过期即由本服务改派给另一台。
 *
 * <p><b>为什么改派是安全的</b>：接管方走的就是既有的"崩溃恢复"路径（从各自 checkpoint 续传），
 * 而 P0-3 的任务级文件锁保证同一 taskId 的子进程在全局只有一套能真正跑起来——
 * 万一老 agent 只是网络分区、并没有真死，新老两套进程也不会同时写目标库。
 */
@Service
public class AgentClusterService {
    private static final Logger logger = LoggerFactory.getLogger(AgentClusterService.class);

    /** 心跳超时：agent 侧每 15s 一跳，90s 容忍 5 次丢跳（GC / 元数据库抖动）。 */
    private static final int HEARTBEAT_TIMEOUT_SECONDS = 90;
    private static final int LEASE_SECONDS = 90;

    /** 需要有人负责的运行态（终态与 CONFIGURING/PAUSED 不参与改派）。 */
    private static final Set<WorkflowStatus> ACTIVE_STATES = EnumSet.of(
            WorkflowStatus.PENDING, WorkflowStatus.RECEIVED, WorkflowStatus.STARTING,
            WorkflowStatus.FULL_MIGRATING, WorkflowStatus.FULL_COMPLETED,
            WorkflowStatus.INCREMENT_RUNNING, WorkflowStatus.SUBSCRIBE_RUNNING,
            WorkflowStatus.RECONNECTING);

    @Autowired
    private AgentNodeRepository agentNodeRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private WorkflowLogRepository workflowLogRepository;

    /** 心跳新鲜的 agent，按容量占用率升序（最闲的在前）。 */
    public List<AgentNode> aliveAgents() {
        LocalDateTime since = LocalDateTime.now().minusSeconds(HEARTBEAT_TIMEOUT_SECONDS);
        // 必须拷贝再排序：仓储返回的列表不保证可变，就地 sort 会抛 UnsupportedOperationException，
        // 而它会被巡检的兜底 catch 吞掉——表现为"故障转移悄悄不工作了"，最难查的那种。
        List<AgentNode> alive = new java.util.ArrayList<>(agentNodeRepository.findAlive(since));
        alive.sort(Comparator.comparingDouble(a ->
                (double) nz(a.getRunningTasks()) / Math.max(1, nz(a.getCapacity()))));
        return alive;
    }

    /**
     * 给任务指派一个 agent 并写上租约。集群里一台 agent 都没注册时返回 false，
     * 任务照旧走广播语义——单机部署不该因为"集群功能"而起不来。
     */
    public boolean assign(Workflow workflow) {
        Optional<AgentNode> picked = aliveAgents().stream()
                .filter(a -> nz(a.getRunningTasks()) < nz(a.getCapacity()))
                .findFirst();
        if (picked.isEmpty()) {
            // 全都满载时也别把任务卡死：退回最闲的一台，容量只是调度倾向不是硬闸
            picked = aliveAgents().stream().findFirst();
        }
        if (picked.isEmpty()) {
            logger.info("没有存活的 agent 注册，任务 {} 按广播语义下发", workflow.getId());
            workflow.setAgentId(null);
            return false;
        }
        AgentNode agent = picked.get();
        workflow.setAgentId(agent.getAgentId());
        workflow.setLeaseExpireAt(LocalDateTime.now().plusSeconds(LEASE_SECONDS));
        workflow.setLeaseEpoch(nz(workflow.getLeaseEpoch()) + 1);
        logger.info("任务 {} 指派给 agent {}（{} 负载 {}/{}）", workflow.getId(), agent.getAgentId(),
                agent.baseUrl(), agent.getRunningTasks(), agent.getCapacity());
        return true;
    }

    /** 任务当前归属 agent 的 HTTP 地址；未指派/查不到时返回空（调用方回退默认地址）。 */
    public Optional<String> agentBaseUrl(String agentId) {
        if (agentId == null || agentId.isEmpty()) {
            return Optional.empty();
        }
        return agentNodeRepository.findById(agentId).map(AgentNode::baseUrl);
    }

    /**
     * 租约巡检：把失联 agent 名下仍在运行的任务改派给其它存活 agent。
     *
     * <p>判定失联同时看两处，缺一不可：agent 的心跳过期（进程没了），
     * 或任务租约过期（进程还在但这个任务的线程已经不在跑了）。
     */
    @Scheduled(fixedDelay = 20000, initialDelay = 60000)
    public void reapExpiredLeases() {
        try {
            List<AgentNode> alive = aliveAgents();
            if (alive.isEmpty()) {
                return;   // 一台活的都没有，改派给谁都没意义，等 agent 回来自己恢复
            }
            Set<String> aliveIds = new java.util.HashSet<>();
            alive.forEach(a -> aliveIds.add(a.getAgentId()));

            LocalDateTime now = LocalDateTime.now();
            for (Workflow w : workflowRepository.findAll()) {
                if (Boolean.TRUE.equals(w.getIsDeleted()) || !ACTIVE_STATES.contains(w.getStatus())) {
                    continue;
                }
                String owner = w.getAgentId();
                if (owner == null || owner.isEmpty()) {
                    continue;   // 从没指派过（广播语义），不介入
                }
                boolean ownerAlive = aliveIds.contains(owner);
                boolean leaseValid = w.getLeaseExpireAt() != null && w.getLeaseExpireAt().isAfter(now);
                if (ownerAlive && leaseValid) {
                    continue;
                }
                Optional<AgentNode> takeover = alive.stream()
                        .filter(a -> !a.getAgentId().equals(owner))
                        .findFirst();
                if (takeover.isEmpty()) {
                    continue;   // 只剩原 agent 自己（心跳刚回来），交给它自己续跑
                }
                reassign(w, owner, takeover.get());
            }
        } catch (Exception e) {
            logger.warn("租约巡检出错: {}", e.toString());
        }
    }

    private void reassign(Workflow w, String oldOwner, AgentNode target) {
        w.setAgentId(target.getAgentId());
        w.setLeaseExpireAt(LocalDateTime.now().plusSeconds(LEASE_SECONDS));
        w.setLeaseEpoch(nz(w.getLeaseEpoch()) + 1);
        workflowRepository.save(w);

        String msg = String.format("agent %s 失联，任务已改派给 %s（租约代次 %d），将从各自 checkpoint 续跑",
                oldOwner, target.getAgentId(), nz(w.getLeaseEpoch()));
        logger.warn("[{}] {}", w.getId(), msg);
        WorkflowLog log = new WorkflowLog();
        log.setWorkflowId(w.getId());
        log.setLevel(WorkflowLog.LogLevel.WARNING);
        log.setMessage(msg);
        workflowLogRepository.save(log);

        try {
            // 用 resume 让接管方走既有的"恢复任务"路径：读 H2/checkpoint 续传，
            // 与一次进程崩溃恢复等价，不会重做全量
            kafkaProducerService.sendControlMessage(
                    buildTakeoverMessage(w, target.getAgentId()));
        } catch (Exception e) {
            logger.error("[{}] 改派消息发送失败: {}", w.getId(), e.toString());
        }
    }

    private com.synctask.dto.TaskCreatedMessage buildTakeoverMessage(Workflow w, String agentId) {
        com.synctask.dto.TaskCreatedMessage m = new com.synctask.dto.TaskCreatedMessage();
        m.setTaskId(w.getId());
        m.setTaskName(w.getName());
        m.setUserId(w.getUserId());
        m.setSourceConnection(w.getSourceConnection());
        m.setTargetConnection(w.getTargetConnection());
        m.setMigrationMode(w.getMigrationMode());
        m.setCreatedAt(w.getCreatedAt());
        m.setMessageType("resume");
        m.setCurrentStatus(w.getStatus() != null ? w.getStatus().name() : null);
        m.setSourceType(w.getSourceType());
        m.setTargetType(w.getTargetType());
        m.setSourceDbName(w.getSourceDbName());
        m.setTargetDbName(w.getTargetDbName());
        m.setTaskType(w.getTaskType());
        m.setConsistencyMode(w.getConsistencyMode());
        m.setDrMode(w.getDrMode());
        m.setTargetAgentId(agentId);
        return m;
    }

    private static int nz(Integer v) {
        return v == null ? 0 : v;
    }
}
