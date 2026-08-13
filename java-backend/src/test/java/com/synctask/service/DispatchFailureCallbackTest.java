package com.synctask.service;

import com.synctask.entity.Workflow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 派发消息投递失败时，失败回调必须被调到。
 *
 * <p>失败有两条路径，改造前**两条都只留一行日志**：
 * <ul>
 *   <li><b>同步抛出</b>：broker 不可达时 {@code send()} 自己阻塞到 {@code max.block.ms}（默认 60s）
 *       再抛 TimeoutException —— 真实故障里就是这条；</li>
 *   <li><b>异步失败</b>：连上了但 ack 失败，只走 future 的回调。</li>
 * </ul>
 * 本测试守的是异步那条（同步那条由调用方的 try/catch 接住）。
 */
@DisplayName("派发失败回调")
class DispatchFailureCallbackTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaProducerService producer;
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
        producer = new KafkaProducerService();
        ReflectionTestUtils.setField(producer, "kafkaTemplate", kafkaTemplate);
        ReflectionTestUtils.setField(producer, "taskCreatedTopic", "sync-task-created");
    }

    @AfterEach
    void tearDown() throws Exception {
        mocks.close();
    }

    private Workflow workflow() {
        Workflow w = new Workflow();
        w.setId("t1");
        w.setName("dispatch-test");
        return w;
    }

    @Test
    @DisplayName("异步 ack 失败：回调被调到，且拿到原始异常")
    void asyncFailureInvokesCallback() {
        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        RuntimeException cause = new RuntimeException("broker gone");
        failed.completeExceptionally(cause);
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);

        AtomicReference<Throwable> seen = new AtomicReference<>();
        producer.sendTaskCreatedMessage(workflow(), seen::set);

        assertNotNull(seen.get(), "异步失败只走 future 回调，不接住就只剩一行日志");
        // CompletableFuture 会把异常包一层 CompletionException
        Throwable root = seen.get().getCause() != null ? seen.get().getCause() : seen.get();
        assertEquals("broker gone", root.getMessage());
    }

    @Test
    @DisplayName("发送成功：不调失败回调")
    void successDoesNotInvokeCallback() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(new CompletableFuture<>());   // 一直未完成 = 尚未失败

        AtomicReference<Throwable> seen = new AtomicReference<>();
        producer.sendTaskCreatedMessage(workflow(), seen::set);

        assertNull(seen.get());
    }

    @Test
    @DisplayName("同步抛出（broker 不可达）：异常照常往上抛，由调用方的 try/catch 接住")
    void synchronousThrowPropagates() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException(
                        "Topic sync-task-created not present in metadata after 60000 ms."));

        assertThrows(org.apache.kafka.common.errors.TimeoutException.class,
                () -> producer.sendTaskCreatedMessage(workflow(), t -> { }));
    }

    @Test
    @DisplayName("回调自己抛异常不能把生产者线程带走")
    void throwingCallbackIsContained() {
        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker gone"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);

        producer.sendTaskCreatedMessage(workflow(), t -> {
            throw new IllegalStateException("callback blew up");
        });
        // 没有异常逃出来就算通过
    }

    @Test
    @DisplayName("不传回调（老调用方）仍然可用")
    void nullCallbackIsAllowed() {
        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker gone"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);

        producer.sendTaskCreatedMessage(workflow());
    }
}
