package com.synctask.service;

import com.synctask.entity.SyncErrorCode;
import com.synctask.entity.Workflow;
import com.synctask.entity.WorkflowLog;
import com.synctask.entity.WorkflowStatus;
import com.synctask.repository.WorkflowLogRepository;
import com.synctask.repository.WorkflowRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 派发消息发不出去时，任务必须置 FAILED 并带错误码。
 *
 * <p>改造前只 {@code addLog(WARNING, "Kafka 消息发送失败")}，任务留在 PENDING、HTTP 照常返回成功。
 * 而"消息没投出去"意味着执行端从未收到它，任务**永远不会开始跑** —— 页面上只看到一个永远
 * "启动中"的任务，没人知道该重启它。实际发生过：Kafka 比后端晚起了一分钟
 * （{@code Topic sync-task-created not present in metadata after 60000 ms}），任务卡在 PENDING。
 */
@DisplayName("派发失败即置 FAILED（不再留在 PENDING）")
class TaskDispatchFailureTest {

    @Mock
    private WorkflowRepository workflowRepository;
    @Mock
    private WorkflowLogRepository workflowLogRepository;

    private WorkflowService service;
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
        service = new WorkflowService();
        ReflectionTestUtils.setField(service, "workflowRepository", workflowRepository);
        ReflectionTestUtils.setField(service, "workflowLogRepository", workflowLogRepository);
    }

    @AfterEach
    void tearDown() throws Exception {
        mocks.close();
    }

    private Workflow workflow(String id, WorkflowStatus status) {
        Workflow w = new Workflow();
        w.setId(id);
        w.setStatus(status);
        return w;
    }

    @Test
    @DisplayName("PENDING 的任务：置 FAILED + E5004 + 原因写进 error_message")
    void pendingTaskBecomesFailed() {
        Workflow w = workflow("t1", WorkflowStatus.PENDING);
        when(workflowRepository.findById("t1")).thenReturn(Optional.of(w));

        service.markDispatchFailed("t1", new org.apache.kafka.common.errors.TimeoutException(
                "Topic sync-task-created not present in metadata after 60000 ms."));

        ArgumentCaptor<Workflow> saved = ArgumentCaptor.forClass(Workflow.class);
        verify(workflowRepository).save(saved.capture());
        assertEquals(WorkflowStatus.FAILED, saved.getValue().getStatus());
        assertEquals(SyncErrorCode.TASK_DISPATCH_FAILED.getCode(), saved.getValue().getErrorCode());
        assertEquals("E5004", saved.getValue().getErrorCode());
        assertTrue(saved.getValue().getErrorMessage().contains("not present in metadata"),
                "原因要留在 error_message 里，否则排障还得去翻日志: "
                        + saved.getValue().getErrorMessage());
    }

    @Test
    @DisplayName("已经跑起来的任务不能被迟到的失败回调打成 FAILED")
    void runningTaskIsNotClobbered() {
        Workflow w = workflow("t2", WorkflowStatus.INCREMENT_RUNNING);
        when(workflowRepository.findById("t2")).thenReturn(Optional.of(w));

        service.markDispatchFailed("t2", new RuntimeException("late async failure"));

        verify(workflowRepository, never()).save(any(Workflow.class));
        assertEquals(WorkflowStatus.INCREMENT_RUNNING, w.getStatus(),
                "异步回调可能很晚才到，那时任务也许已被执行端接走并跑起来了");
    }

    @Test
    @DisplayName("任务已被删除：不抛异常（回调线程不能被带走）")
    void missingTaskIsTolerated() {
        when(workflowRepository.findById("gone")).thenReturn(Optional.empty());
        service.markDispatchFailed("gone", new RuntimeException("boom"));
        verify(workflowRepository, never()).save(any(Workflow.class));
    }

    @Test
    @DisplayName("异常没有 message 时用类名兜底，不能写出 null")
    void nullMessageFallsBackToClassName() {
        Workflow w = workflow("t3", WorkflowStatus.PENDING);
        when(workflowRepository.findById("t3")).thenReturn(Optional.of(w));

        service.markDispatchFailed("t3", new IllegalStateException());

        ArgumentCaptor<Workflow> saved = ArgumentCaptor.forClass(Workflow.class);
        verify(workflowRepository).save(saved.capture());
        assertTrue(saved.getValue().getErrorMessage().contains("IllegalStateException"),
                saved.getValue().getErrorMessage());
    }

    @Test
    @DisplayName("落库失败也不抛出去（调用方可能是 Kafka 生产者的回调线程）")
    void repositoryFailureIsSwallowed() {
        Workflow w = workflow("t4", WorkflowStatus.PENDING);
        when(workflowRepository.findById("t4")).thenReturn(Optional.of(w));
        when(workflowRepository.save(any(Workflow.class))).thenThrow(new RuntimeException("db down"));

        service.markDispatchFailed("t4", new RuntimeException("send failed"));
        // 没有异常抛到这里就算通过
    }

    @Test
    @DisplayName("会留一条 ERROR 级日志（WARNING 在页面上不够醒目）")
    void writesErrorLevelLog() {
        Workflow w = workflow("t5", WorkflowStatus.PENDING);
        when(workflowRepository.findById("t5")).thenReturn(Optional.of(w));

        service.markDispatchFailed("t5", new RuntimeException("send failed"));

        ArgumentCaptor<WorkflowLog> log = ArgumentCaptor.forClass(WorkflowLog.class);
        verify(workflowLogRepository).save(log.capture());
        assertEquals(WorkflowLog.LogLevel.ERROR, log.getValue().getLevel());
        assertTrue(log.getValue().getMessage().contains("E5004"), log.getValue().getMessage());
    }
}
