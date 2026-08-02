package com.synctask.service;

import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowLogRepository;
import com.synctask.repository.WorkflowRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

/**
 * 任务生命周期状态的单调性（P2-2）。
 *
 * <p>实测过的现象：{@code FULL_MIGRATING → FULL_COMPLETED → FULL_MIGRATING → INCREMENT_RUNNING}。
 * agent 侧的进度监控线程与"全量完成"是两个发送方，监控线程被 interrupt 之前可能已经在构造
 * 一条 FULL_MIGRATING，于是它后到、把 FULL_COMPLETED 顶了回去。UI、告警、
 * {@code getIncrementalWorkflows()} 这类按状态取任务的逻辑都会短暂看到错误状态。
 *
 * <p>agent 侧补了发送前二次确认，这里守的是另一半：消息乱序/重投/多 agent 接管都可能再次
 * 制造倒退，落库前自己挡一次。
 */
@DisplayName("任务状态单调性")
class StatusMonotonicityTest {

    @Mock
    private WorkflowRepository workflowRepository;
    @Mock
    private WorkflowLogRepository workflowLogRepository;
    @Mock
    private KafkaProducerService kafkaProducerService;
    @Mock
    private MessageChannel messageChannel;

    private KafkaConsumerService service;

    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
        // SimpMessagingTemplate 在本 JDK 上 mock 不了，用真实实例 + mock 的 channel
        // （send 必须返回 true，否则模板会抛 MessageDeliveryException）
        when(messageChannel.send(any(), anyLong())).thenReturn(true);
        SimpMessagingTemplate template = new SimpMessagingTemplate(messageChannel);

        service = new KafkaConsumerService();
        ReflectionTestUtils.setField(service, "workflowRepository", workflowRepository);
        ReflectionTestUtils.setField(service, "workflowLogRepository", workflowLogRepository);
        ReflectionTestUtils.setField(service, "kafkaProducerService", kafkaProducerService);
        ReflectionTestUtils.setField(service, "messagingTemplate", template);
        service.init();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    private Workflow workflow(WorkflowStatus status) {
        Workflow w = new Workflow();
        w.setId("task-1");
        w.setUserId(1L);
        w.setStatus(status);
        w.setMigrationMode("fullAndIncre");
        w.setTaskType("SYNC");
        when(workflowRepository.findById("task-1")).thenReturn(Optional.of(w));
        when(workflowRepository.save(any(Workflow.class))).thenAnswer(inv -> inv.getArgument(0));
        return w;
    }

    private Map<String, Object> message(String status, Integer progress) {
        Map<String, Object> m = new HashMap<>();
        m.put("taskId", "task-1");
        m.put("status", status);
        m.put("timestamp", System.currentTimeMillis() + 1000);
        if (progress != null) {
            m.put("progress", progress);
        }
        return m;
    }

    @Test
    @DisplayName("迟到的 FULL_MIGRATING 不能把 FULL_COMPLETED 顶回去")
    void staleFullMigratingIsRejected() {
        Workflow w = workflow(WorkflowStatus.FULL_COMPLETED);

        service.consumeTaskStatusMessage(message("FULL_MIGRATING", 60));

        assertEquals(WorkflowStatus.FULL_COMPLETED, w.getStatus(), "状态不得倒退");
    }

    @Test
    @DisplayName("被挡下的消息里的进度仍要生效——只丢状态这一个字段")
    void rejectedStatusStillAppliesProgress() {
        Workflow w = workflow(WorkflowStatus.INCREMENT_RUNNING);

        service.consumeTaskStatusMessage(message("FULL_MIGRATING", 77));

        assertEquals(WorkflowStatus.INCREMENT_RUNNING, w.getStatus());
        assertEquals(77, w.getProgress(), "进度是真实观测值，不能连带丢掉");
    }

    @Test
    @DisplayName("正常前进照常放行")
    void forwardTransitionsPass() {
        Workflow w = workflow(WorkflowStatus.FULL_COMPLETED);

        service.consumeTaskStatusMessage(message("INCREMENT_RUNNING", 100));

        assertEquals(WorkflowStatus.INCREMENT_RUNNING, w.getStatus());
        assertTrue(w.getIncrementStarted());
    }

    @Test
    @DisplayName("控制态（FAILED/RECONNECTING/PAUSED）任何阶段都能进，不受单调性约束")
    void controlStatesAlwaysPass() {
        Workflow failed = workflow(WorkflowStatus.INCREMENT_RUNNING);
        service.consumeTaskStatusMessage(message("RECONNECTING", 100));
        assertEquals(WorkflowStatus.RECONNECTING, failed.getStatus(), "长期重连不是倒退");

        // 从控制态回到运行态同样放行（重连成功）
        service.consumeTaskStatusMessage(message("INCREMENT_RUNNING", 100));
        assertEquals(WorkflowStatus.INCREMENT_RUNNING, failed.getStatus());
    }

    @Test
    @DisplayName("阶段序号本身：运行态高于全量完成，控制态不参与比较")
    void phaseOrdering() {
        assertTrue(WorkflowStatus.FULL_COMPLETED.phase() > WorkflowStatus.FULL_MIGRATING.phase());
        assertTrue(WorkflowStatus.INCREMENT_RUNNING.phase() > WorkflowStatus.FULL_COMPLETED.phase());
        assertTrue(WorkflowStatus.COMPLETED.phase() > WorkflowStatus.INCREMENT_RUNNING.phase());
        // 增量与订阅是并列的运行态，同阶段
        assertEquals(WorkflowStatus.INCREMENT_RUNNING.phase(), WorkflowStatus.SUBSCRIBE_RUNNING.phase());
        for (WorkflowStatus s : new WorkflowStatus[]{WorkflowStatus.SWITCHING, WorkflowStatus.RECONNECTING,
                WorkflowStatus.FAILED, WorkflowStatus.PAUSED}) {
            assertEquals(WorkflowStatus.PHASE_CONTROL, s.phase(), s + " 必须是控制态");
            assertTrue(!s.isLifecyclePhase());
        }
    }
}
