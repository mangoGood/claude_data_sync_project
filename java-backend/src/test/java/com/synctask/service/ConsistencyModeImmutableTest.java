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
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 一致性语义：建任务时选，之后不可改。
 *
 * <p>为什么必须在服务端挡：前端只是把它渲染成只读徽章，直接调 {@code PUT /workflows/{id}/config}
 * 就能绕过去。而这个字段一改，整条增量管线的编排就换了一套（串行按源事务提交 ↔ 冲突矩阵并发投递），
 * 同一条链路前后半段语义不一致，位点回放时更没法解释目标端到底应该是什么状态。
 */
@DisplayName("一致性语义：创建时选定、之后不可修改")
class ConsistencyModeImmutableTest {

    @Mock
    private WorkflowRepository workflowRepository;
    @Mock
    private WorkflowLogRepository workflowLogRepository;
    @Mock
    private KafkaProducerService kafkaProducerService;
    @Mock
    private AgentClusterService agentClusterService;

    private WorkflowService service;
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
        service = new WorkflowService();
        ReflectionTestUtils.setField(service, "workflowRepository", workflowRepository);
        ReflectionTestUtils.setField(service, "workflowLogRepository", workflowLogRepository);
        ReflectionTestUtils.setField(service, "kafkaProducerService", kafkaProducerService);
        ReflectionTestUtils.setField(service, "agentClusterService", agentClusterService);
        when(workflowRepository.save(any(Workflow.class))).thenAnswer(inv -> inv.getArgument(0));
        when(workflowRepository.existsByUserIdAndTaskTypeAndNameAndIsDeletedFalse(
                anyLong(), anyString(), anyString())).thenReturn(false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    private Workflow existing(String consistencyMode) {
        Workflow w = new Workflow();
        w.setId("task-1");
        w.setUserId(1L);
        w.setStatus(WorkflowStatus.CONFIGURING);
        w.setTaskType("SYNC");
        w.setSourceType("mysql");
        w.setTargetType("mysql");
        w.setConsistencyMode(consistencyMode);
        when(workflowRepository.findById("task-1")).thenReturn(Optional.of(w));
        return w;
    }

    @Test
    @DisplayName("同步任务默认最终一致，订阅/灾备默认事务一致")
    void defaultsFollowTaskType() {
        assertEquals("EVENTUAL", WorkflowService.defaultConsistencyMode("SYNC"));
        assertEquals("TRANSACTIONAL", WorkflowService.defaultConsistencyMode("SUBSCRIBE"));
        assertEquals("TRANSACTIONAL", WorkflowService.defaultConsistencyMode("DR"));
        assertEquals("TRANSACTIONAL", WorkflowService.defaultConsistencyMode("DR_SHADOW"));
    }

    @Test
    @DisplayName("不传时按任务类型落默认值")
    void createAppliesTypeDefault() {
        Workflow sync = service.createWorkflow("t-sync", "mysql", "mysql", 1L, "SYNC", null, null);
        assertEquals("EVENTUAL", sync.getConsistencyMode());

        Workflow sub = service.createWorkflow("t-sub", "mysql", "mysql", 1L, "SUBSCRIBE", null, null);
        assertEquals("TRANSACTIONAL", sub.getConsistencyMode());
    }

    @Test
    @DisplayName("用户显式选择优先于类型默认")
    void createHonoursExplicitChoice() {
        Workflow w = service.createWorkflow("t1", "mysql", "mysql", 1L, "SYNC", null, "transactional");
        assertEquals("TRANSACTIONAL", w.getConsistencyMode(), "大小写不敏感，落库归一成大写");

        Workflow w2 = service.createWorkflow("t2", "mysql", "mysql", 1L, "SUBSCRIBE", null, "EVENTUAL");
        assertEquals("EVENTUAL", w2.getConsistencyMode());
    }

    @Test
    @DisplayName("非法取值直接拒绝，不静默落成默认值")
    void createRejectsIllegalValue() {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> service.createWorkflow("t3", "mysql", "mysql", 1L, "SYNC", null, "STRONG"));
        assertTrue(e.getMessage().contains("一致性模式"), e.getMessage());
    }

    @Test
    @DisplayName("改配置时试图改一致性语义 → 报错，已存值不动")
    void updateConfigRejectsChange() {
        Workflow w = existing("EVENTUAL");

        RuntimeException e = assertThrows(RuntimeException.class, () -> service.updateConfig(
                "task-1", 1L, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, "TRANSACTIONAL"));

        assertTrue(e.getMessage().contains("不可修改"), e.getMessage());
        assertEquals("EVENTUAL", w.getConsistencyMode());
    }

    @Test
    @DisplayName("回传相同的值（前端只读字段照原样提交）不算修改")
    void updateConfigAcceptsSameValue() {
        Workflow w = existing("EVENTUAL");

        service.updateConfig("task-1", 1L, null, null, "full", null, null, null, null, null,
                null, null, null, null, null, null, null, null, "EVENTUAL");

        assertEquals("EVENTUAL", w.getConsistencyMode());
        assertEquals("full", w.getMigrationMode());
    }
}
