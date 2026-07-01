package com.migration.extract;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link BackpressureController} 单元测试。
 *
 * <p>验证高/低水位触发逻辑、信号文件读写、强制恢复等行为。
 */
@DisplayName("BackpressureController 背压控制器测试")
class BackpressureControllerTest {

    @TempDir
    Path tempDir;

    private BackpressureController controller;

    @BeforeEach
    void setUp() throws Exception {
        // 使用临时目录作为 files 根目录，避免污染工作区
        controller = new BackpressureController("test-task", 50, 20);
        // 通过反射改写 signalFilePath 指向临时目录
        Field signalPathField = BackpressureController.class.getDeclaredField("signalFilePath");
        signalPathField.setAccessible(true);
        String tempSignalPath = tempDir.resolve("backpressure.signal").toString();
        signalPathField.set(controller, tempSignalPath);
    }

    @AfterEach
    void tearDown() {
        if (controller != null) {
            controller.forceResume();
        }
    }

    @Test
    @DisplayName("初始状态应为 RESUME，未触发背压")
    void initialSignalShouldBeResume() {
        assertEquals(BackpressureController.Signal.RESUME, controller.getCurrentSignal());
        assertFalse(controller.isPaused());
    }

    @Test
    @DisplayName("积压量低于高水位时保持 RESUME")
    void shouldStayResumeWhenBelowHighWatermark() {
        BackpressureController.Signal signal = controller.checkAndApplyBackpressure(30);
        assertEquals(BackpressureController.Signal.RESUME, signal);
        assertFalse(controller.isPaused());
    }

    @Test
    @DisplayName("积压量超过高水位时触发 PAUSE")
    void shouldPauseWhenExceedHighWatermark() {
        BackpressureController.Signal signal = controller.checkAndApplyBackpressure(60);
        assertEquals(BackpressureController.Signal.PAUSE, signal);
        assertTrue(controller.isPaused());
    }

    @Test
    @DisplayName("积压量等于高水位时不触发 PAUSE（严格大于）")
    void shouldNotPauseWhenEqualsHighWatermark() {
        BackpressureController.Signal signal = controller.checkAndApplyBackpressure(50);
        assertEquals(BackpressureController.Signal.RESUME, signal);
        assertFalse(controller.isPaused());
    }

    @Test
    @DisplayName("PAUSE 状态下积压量介于高低水位之间时保持 PAUSE（迟滞）")
    void shouldStayPauseWhenBetweenWatermarks() {
        controller.checkAndApplyBackpressure(60);
        assertEquals(BackpressureController.Signal.PAUSE, controller.getCurrentSignal());

        // 介于 20 和 50 之间，应保持 PAUSE
        BackpressureController.Signal signal = controller.checkAndApplyBackpressure(30);
        assertEquals(BackpressureController.Signal.PAUSE, signal);
        assertTrue(controller.isPaused());
    }

    @Test
    @DisplayName("PAUSE 状态下积压量低于低水位时恢复 RESUME")
    void shouldResumeWhenBelowLowWatermark() {
        controller.checkAndApplyBackpressure(60);
        assertEquals(BackpressureController.Signal.PAUSE, controller.getCurrentSignal());

        BackpressureController.Signal signal = controller.checkAndApplyBackpressure(10);
        assertEquals(BackpressureController.Signal.RESUME, signal);
        assertFalse(controller.isPaused());
    }

    @Test
    @DisplayName("PAUSE 状态下积压量等于低水位时保持 PAUSE（严格小于）")
    void shouldStayPauseWhenEqualsLowWatermark() {
        controller.checkAndApplyBackpressure(60);
        BackpressureController.Signal signal = controller.checkAndApplyBackpressure(20);
        assertEquals(BackpressureController.Signal.PAUSE, signal);
    }

    @Test
    @DisplayName("完整迟滞循环：RESUME→PAUSE→RESUME→PAUSE")
    void completeHysteresisLoop() {
        // 初始 RESUME
        assertEquals(BackpressureController.Signal.RESUME, controller.checkAndApplyBackpressure(0));

        // 升至高水位以上 → PAUSE
        assertEquals(BackpressureController.Signal.PAUSE, controller.checkAndApplyBackpressure(100));
        assertTrue(controller.isPaused());

        // 降至低水位以下 → RESUME
        assertEquals(BackpressureController.Signal.RESUME, controller.checkAndApplyBackpressure(0));
        assertFalse(controller.isPaused());

        // 再次升高 → PAUSE
        assertEquals(BackpressureController.Signal.PAUSE, controller.checkAndApplyBackpressure(80));
        assertTrue(controller.isPaused());
    }

    @Test
    @DisplayName("forceResume 应写入 RESUME 信号")
    void forceResumeShouldWriteResumeSignal() {
        controller.checkAndApplyBackpressure(100);
        assertTrue(controller.isPaused());

        controller.forceResume();
        assertEquals(BackpressureController.Signal.RESUME, controller.getCurrentSignal());
        assertFalse(controller.isPaused());
    }

    @Test
    @DisplayName("信号文件不存在时默认为 RESUME")
    void shouldDefaultResumeWhenSignalFileMissing() throws Exception {
        // 删除信号文件
        Field signalPathField = BackpressureController.class.getDeclaredField("signalFilePath");
        signalPathField.setAccessible(true);
        File signalFile = new File((String) signalPathField.get(controller));
        if (signalFile.exists()) {
            signalFile.delete();
        }

        assertFalse(controller.isPaused());
    }

    @Test
    @DisplayName("高/低水位配置应正确返回")
    void watermarkConfigShouldBeCorrect() {
        assertEquals(50, controller.getHighWatermark());
        assertEquals(20, controller.getLowWatermark());
    }
}
