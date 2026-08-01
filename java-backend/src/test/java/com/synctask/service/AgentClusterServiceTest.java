package com.synctask.service;

import com.synctask.dto.TaskCreatedMessage;
import com.synctask.entity.AgentNode;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowLog;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.AgentNodeRepository;
import com.synctask.repository.WorkflowLogRepository;
import com.synctask.repository.WorkflowRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 执行面选派与故障转移（P1-1）。
 *
 * <p>这里守的是两条相反方向的错误：**该接管时没人接管**（agent 硬崩后任务永久停摆，
 * 也就是改造前的行为），和**不该接管时抢了过来**（原 agent 还活着、租约还有效却被改派，
 * 等于人为制造双写——虽然任务级文件锁兜得住，但会让任务在两台机器之间来回抖）。
 */
@DisplayName("集群选派与租约抢占")
class AgentClusterServiceTest {

    @Mock
    private AgentNodeRepository agentNodeRepository;
    @Mock
    private WorkflowRepository workflowRepository;
    @Mock
    private KafkaProducerService kafkaProducerService;
    @Mock
    private WorkflowLogRepository workflowLogRepository;

    @InjectMocks
    private AgentClusterService service;

    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    private AgentNode agent(String id, int running, int capacity) {
        AgentNode a = new AgentNode();
        a.setAgentId(id);
        a.setHost("10.0.0." + (id.hashCode() & 0x7f));
        a.setPort(8083);
        a.setCapacity(capacity);
        a.setRunningTasks(running);
        a.setStatus("ONLINE");
        a.setHeartbeatAt(LocalDateTime.now());
        return a;
    }

    private Workflow workflow(String id, String owner, LocalDateTime leaseExpire, WorkflowStatus status) {
        Workflow w = new Workflow();
        w.setId(id);
        w.setName("t-" + id);
        w.setUserId(1L);
        w.setStatus(status);
        w.setAgentId(owner);
        w.setLeaseExpireAt(leaseExpire);
        w.setLeaseEpoch(3);
        w.setIsDeleted(false);
        return w;
    }

    @Test
    @DisplayName("指派挑负载最轻的存活 agent，并写上租约")
    void assignsLeastLoadedAgent() {
        when(agentNodeRepository.findAlive(any())).thenReturn(List.of(
                agent("busy", 8, 10), agent("idle", 1, 10)));

        Workflow w = workflow("w1", null, null, WorkflowStatus.PENDING);
        assertTrue(service.assign(w));

        assertEquals("idle", w.getAgentId());
        assertTrue(w.getLeaseExpireAt().isAfter(LocalDateTime.now()), "指派后必须带上未来的租约到期时刻");
        assertEquals(4, w.getLeaseEpoch(), "每次指派租约代次 +1，便于排查谁在什么时候接管过");
    }

    @Test
    @DisplayName("一台 agent 都没注册时退回广播语义，不把任务卡死")
    void fallsBackToBroadcastWhenNoAgents() {
        when(agentNodeRepository.findAlive(any())).thenReturn(List.of());

        Workflow w = workflow("w1", null, null, WorkflowStatus.PENDING);
        assertFalse(service.assign(w));
        assertNull(w.getAgentId(), "没有 agent 注册时不能留下一个指向空的归属");
    }

    @Test
    @DisplayName("owner 失联 → 任务改派给存活 agent 并下发 resume")
    void reassignsWhenOwnerIsGone() {
        when(agentNodeRepository.findAlive(any())).thenReturn(List.of(agent("live", 0, 10)));
        Workflow w = workflow("w1", "dead", LocalDateTime.now().minusMinutes(5), WorkflowStatus.INCREMENT_RUNNING);
        when(workflowRepository.findAll()).thenReturn(List.of(w));

        service.reapExpiredLeases();

        assertEquals("live", w.getAgentId());
        verify(workflowRepository).save(w);

        ArgumentCaptor<TaskCreatedMessage> sent = ArgumentCaptor.forClass(TaskCreatedMessage.class);
        verify(kafkaProducerService).sendControlMessage(sent.capture());
        assertEquals("resume", sent.getValue().getMessageType(), "接管要走既有的恢复路径，不能重做全量");
        assertEquals("live", sent.getValue().getTargetAgentId(), "消息必须定向，否则老 agent 也会接");
        assertEquals("INCREMENT_RUNNING", sent.getValue().getCurrentStatus(),
                "带上当前阶段，接管方才知道要跳过全量");

        ArgumentCaptor<WorkflowLog> log = ArgumentCaptor.forClass(WorkflowLog.class);
        verify(workflowLogRepository).save(log.capture());
        assertTrue(log.getValue().getMessage().contains("改派"));
    }

    @Test
    @DisplayName("owner 活着且租约有效 → 不动（避免把在跑的任务抢来抢去）")
    void keepsTaskWhenOwnerHealthy() {
        when(agentNodeRepository.findAlive(any())).thenReturn(List.of(
                agent("owner", 1, 10), agent("other", 0, 10)));
        Workflow w = workflow("w1", "owner", LocalDateTime.now().plusMinutes(1), WorkflowStatus.INCREMENT_RUNNING);
        when(workflowRepository.findAll()).thenReturn(List.of(w));

        service.reapExpiredLeases();

        assertEquals("owner", w.getAgentId());
        verify(workflowRepository, never()).save(any());
        verify(kafkaProducerService, never()).sendControlMessage(any());
    }

    @Test
    @DisplayName("租约过期但 agent 还活着（任务线程死了）→ 照样改派")
    void reassignsWhenLeaseExpiredEvenIfAgentAlive() {
        when(agentNodeRepository.findAlive(any())).thenReturn(List.of(
                agent("owner", 1, 10), agent("other", 0, 10)));
        Workflow w = workflow("w1", "owner", LocalDateTime.now().minusSeconds(30), WorkflowStatus.INCREMENT_RUNNING);
        when(workflowRepository.findAll()).thenReturn(List.of(w));

        service.reapExpiredLeases();

        assertEquals("other", w.getAgentId(), "进程活着但这个任务没在跑，也得有人接");
    }

    @Test
    @DisplayName("终态/未指派的任务不参与改派")
    void ignoresTerminalAndUnassigned() {
        when(agentNodeRepository.findAlive(any())).thenReturn(List.of(agent("live", 0, 10)));
        Workflow done = workflow("w1", "dead", LocalDateTime.now().minusMinutes(5), WorkflowStatus.COMPLETED);
        Workflow broadcast = workflow("w2", null, null, WorkflowStatus.INCREMENT_RUNNING);
        when(workflowRepository.findAll()).thenReturn(List.of(done, broadcast));

        service.reapExpiredLeases();

        verify(workflowRepository, never()).save(any());
        verify(kafkaProducerService, never()).sendControlMessage(any());
    }
}
