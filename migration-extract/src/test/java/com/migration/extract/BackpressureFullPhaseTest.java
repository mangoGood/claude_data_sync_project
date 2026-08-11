package com.migration.extract;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 全量期间不施加背压。
 *
 * <p>extract 与全量并行跑之后，THL 会在整个全量期间只进不出（increment 按设计要等
 * FULL_COMPLETED）。此时按积压量去暂停 capture，是把"还没有消费者"误判成"消费不过来"——
 * 而 capture 一暂停，源库 binlog 继续前进、capture 位点原地不动，长全量下有被 purge 的风险。
 */
@DisplayName("背压：全量期间不暂停 capture")
class BackpressureFullPhaseTest {

    private static final String TASK = "bp-full-phase-test";

    private Path marker() {
        return Paths.get("files", TASK, "full_running");
    }

    private Path signal() {
        return Paths.get("files", TASK, "backpressure.signal");
    }

    @AfterEach
    void cleanup() throws IOException {
        Files.deleteIfExists(marker());
        Files.deleteIfExists(signal());
    }

    private void setFullRunning(boolean running) throws IOException {
        if (running) {
            Files.createDirectories(marker().getParent());
            Files.writeString(marker(), "1");
        } else {
            Files.deleteIfExists(marker());
        }
    }

    @Test
    @DisplayName("全量进行中：积压再高也不暂停")
    void noPauseWhileFullRunning() throws IOException {
        setFullRunning(true);
        BackpressureController bp = new BackpressureController(TASK, 10, 2);

        assertEquals(BackpressureController.Signal.RESUME, bp.checkAndApplyBackpressure(99999));
        assertFalse(bp.isPaused(), "全量期间 THL 只进不出是设计使然，不该把它当积压");
    }

    @Test
    @DisplayName("全量开始时若已处于暂停，要主动解除——否则 capture 被冻在全量全程")
    void resumesIfAlreadyPaused() throws IOException {
        BackpressureController bp = new BackpressureController(TASK, 10, 2);
        bp.checkAndApplyBackpressure(50);          // 先触发暂停
        assertTrue(bp.isPaused());

        setFullRunning(true);
        bp.checkAndApplyBackpressure(50);
        assertFalse(bp.isPaused(), "全量一开始就该解除既有的暂停");
    }

    @Test
    @DisplayName("全量结束后恢复正常背压")
    void normalAfterFull() throws IOException {
        setFullRunning(true);
        BackpressureController bp = new BackpressureController(TASK, 10, 2);
        bp.checkAndApplyBackpressure(99999);
        assertFalse(bp.isPaused());

        setFullRunning(false);
        bp.checkAndApplyBackpressure(99999);
        assertTrue(bp.isPaused(), "全量结束、increment 起来之后，积压就是真积压了");

        bp.checkAndApplyBackpressure(1);
        assertFalse(bp.isPaused());
    }
}
