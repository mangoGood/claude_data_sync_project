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
 * 全量装载通道与一致性快照档位：建任务时按<b>源端</b>取默认，任务启动前可改、启动后不可改。
 *
 * <p>为什么默认值要按源端分：各家真快照的代价差一个数量级。MySQL 要 RELOAD 权限 +
 * 一段全局读锁（源库短暂只读），默认替用户承担不合适；TiDB/PG/Oracle/Mongo/Redis 的快照
 * 分别是 MVCC 历史读、导出快照、闪回查询、快照会话与 RDB，都不加全局锁，默认开着才让
 * "全量结束点"这个语义默认可用。
 *
 * <p>为什么"启动后不可改"不需要单独判断：这两项只在全量阶段生效，改它等于改一次已经跑过的
 * 全量，没有意义。统一由 updateConfig 的 CONFIGURING 状态校验挡住。
 */
@DisplayName("全量装载/快照档位：源端默认 + 未启动可改")
class FullLoadOptionsTest {

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

    private Workflow existing(WorkflowStatus status) {
        Workflow w = new Workflow();
        w.setId("task-1");
        w.setUserId(1L);
        w.setStatus(status);
        w.setTaskType("SYNC");
        w.setSourceType("mysql");
        w.setTargetType("mysql");
        w.setConsistencyMode("EVENTUAL");
        w.setBulkLoadEnabled(true);
        w.setBulkLoadMode("AUTO");
        w.setSnapshotMode("GTID_ONLY");
        when(workflowRepository.findById("task-1")).thenReturn(Optional.of(w));
        return w;
    }

    private Workflow update(String bulkMode, String snapshotMode, Boolean bulkEnabled) {
        return service.updateConfig("task-1", 1L, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null,
                new WorkflowService.FullLoadOptions(bulkEnabled, bulkMode, snapshotMode));
    }

    @Test
    @DisplayName("快照默认按源端给：MySQL 只记位点，其余源端给真快照")
    void snapshotDefaultsPerSourceType() {
        assertEquals("GTID_ONLY", WorkflowService.defaultSnapshotMode("mysql"));
        assertEquals("GTID_ONLY", WorkflowService.defaultSnapshotMode(null));
        assertEquals("GTID_ONLY", WorkflowService.defaultSnapshotMode("elasticsearch"));
        assertEquals("CONSISTENT", WorkflowService.defaultSnapshotMode("tidb"));
        assertEquals("CONSISTENT", WorkflowService.defaultSnapshotMode("postgresql"));
        assertEquals("CONSISTENT", WorkflowService.defaultSnapshotMode("oracle"));
        assertEquals("CONSISTENT", WorkflowService.defaultSnapshotMode("mongodb"));
        assertEquals("CONSISTENT", WorkflowService.defaultSnapshotMode("redis"));
    }

    @Test
    @DisplayName("建任务：装载默认 AUTO+启用，快照按源端落默认")
    void createAppliesDefaults() {
        Workflow mysql = service.createWorkflow("t-mysql", "mysql", "mysql", 1L, "SYNC", null, null);
        assertEquals("AUTO", mysql.getBulkLoadMode());
        assertTrue(mysql.getBulkLoadEnabled());
        assertEquals("GTID_ONLY", mysql.getSnapshotMode());

        Workflow pg = service.createWorkflow("t-pg", "postgresql", "postgresql", 1L, "SYNC", null, null);
        assertEquals("CONSISTENT", pg.getSnapshotMode());

        Workflow dr = service.createWorkflow("t-dr", "mongodb", "mongodb", 1L, "DR", null, null);
        assertEquals("CONSISTENT", dr.getSnapshotMode(), "灾备任务的全量走同一套链路，规则相同");
    }

    @Test
    @DisplayName("未启动（配置中）可以改档位")
    void updateAllowedWhileConfiguring() {
        Workflow w = existing(WorkflowStatus.CONFIGURING);

        update("COPY", "CONSISTENT", false);

        assertEquals("COPY", w.getBulkLoadMode());
        assertEquals("CONSISTENT", w.getSnapshotMode());
        assertEquals(Boolean.FALSE, w.getBulkLoadEnabled());
    }

    @Test
    @DisplayName("不传的字段保持原值（配置页只提交改动的那部分）")
    void updateKeepsUntouchedFields() {
        Workflow w = existing(WorkflowStatus.CONFIGURING);

        update(null, "NONE", null);

        assertEquals("AUTO", w.getBulkLoadMode(), "没传就不该动");
        assertEquals(Boolean.TRUE, w.getBulkLoadEnabled());
        assertEquals("NONE", w.getSnapshotMode());
    }

    @Test
    @DisplayName("已启动的任务改不动（整个配置接口只在 CONFIGURING 放行）")
    void updateRejectedAfterLaunch() {
        Workflow w = existing(WorkflowStatus.INCREMENT_RUNNING);

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> update("COPY", "CONSISTENT", null));

        assertTrue(e.getMessage().contains("只能修改配置中的任务"), e.getMessage());
        assertEquals("AUTO", w.getBulkLoadMode());
        assertEquals("GTID_ONLY", w.getSnapshotMode());
    }

    @Test
    @DisplayName("非法档位直接拒绝，不静默落成默认值")
    void rejectsIllegalValues() {
        existing(WorkflowStatus.CONFIGURING);

        RuntimeException bulk = assertThrows(RuntimeException.class,
                () -> update("LOAD_DATA_INFILE", null, null));
        assertTrue(bulk.getMessage().contains("批量装载档位"), bulk.getMessage());

        RuntimeException snapshot = assertThrows(RuntimeException.class,
                () -> update(null, "STRONG", null));
        assertTrue(snapshot.getMessage().contains("快照档位"), snapshot.getMessage());
    }

    @Test
    @DisplayName("档位大小写不敏感，落库统一大写")
    void normalisesCase() {
        Workflow w = existing(WorkflowStatus.CONFIGURING);

        update("copy", "consistent", null);

        assertEquals("COPY", w.getBulkLoadMode());
        assertEquals("CONSISTENT", w.getSnapshotMode());
    }
}
