package com.migration.extract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 背压控制器：基于文件信号实现 extract → capture 的反压。
 *
 * <p>工作原理：
 * <ul>
 *   <li>extract 进程通过 {@link #checkAndApplyBackpressure(int)} 检测 THL 输出目录积压量，
 *       超过高水位时写入 PAUSE 信号文件，低于低水位时写入 RESUME 信号文件。</li>
 *   <li>capture 进程通过 {@link #isPaused()} 轮询信号文件，暂停或恢复 binlog 拉取。</li>
 * </ul>
 *
 * <p>信号文件位于 {@code files/<taskId>/backpressure.signal}，内容为 PAUSE 或 RESUME。
 * 使用文件通信是因为 capture 和 extract 是独立 JVM 进程，无法共享内存。
 */
public class BackpressureController {
    private static final Logger logger = LoggerFactory.getLogger(BackpressureController.class);

    public enum Signal { PAUSE, RESUME }

    private final String signalFilePath;
    private final int highWatermark;
    private final int lowWatermark;
    private volatile Signal lastSignal = Signal.RESUME;

    /**
     * @param taskId         任务 ID
     * @param highWatermark  高水位（THL 积压文件数超过此值时暂停 capture）
     * @param lowWatermark   低水位（积压降到此值以下时恢复 capture）
     */
    public BackpressureController(String taskId, int highWatermark, int lowWatermark) {
        this.signalFilePath = "files/" + taskId + "/backpressure.signal";
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;
    }

    /**
     * extract 调用：根据当前积压量自动施加或解除背压。
     *
     * @param pendingThlFileCount 待处理的 THL 文件数量
     * @return 当前生效的信号
     */
    public Signal checkAndApplyBackpressure(int pendingThlFileCount) {
        if (lastSignal == Signal.RESUME && pendingThlFileCount > highWatermark) {
            writeSignal(Signal.PAUSE);
            logger.warn("背压触发：THL积压 {} 超过高水位 {}，暂停 capture", pendingThlFileCount, highWatermark);
        } else if (lastSignal == Signal.PAUSE && pendingThlFileCount < lowWatermark) {
            writeSignal(Signal.RESUME);
            logger.info("背压解除：THL积压 {} 低于低水位 {}，恢复 capture", pendingThlFileCount, lowWatermark);
        }
        return lastSignal;
    }

    /**
     * capture 调用：检查是否应暂停拉取。
     */
    public boolean isPaused() {
        Signal current = readSignal();
        return current == Signal.PAUSE;
    }

    /**
     * 强制写入 RESUME 信号（进程退出时调用）。
     */
    public void forceResume() {
        writeSignal(Signal.RESUME);
    }

    private void writeSignal(Signal signal) {
        lastSignal = signal;
        Path path = Paths.get(signalFilePath);
        try {
            Files.createDirectories(path.getParent());
            try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
                pw.println(signal.name());
                pw.println(System.currentTimeMillis());
            }
        } catch (IOException e) {
            logger.warn("写入背压信号文件失败: {}", e.getMessage());
        }
    }

    private Signal readSignal() {
        File file = new File(signalFilePath);
        if (!file.exists()) {
            return Signal.RESUME;
        }
        try {
            String firstLine = Files.readAllLines(file.toPath()).stream().findFirst().orElse("RESUME");
            return "PAUSE".equalsIgnoreCase(firstLine.trim()) ? Signal.PAUSE : Signal.RESUME;
        } catch (IOException e) {
            logger.debug("读取背压信号文件失败: {}", e.getMessage());
            return Signal.RESUME;
        }
    }

    public Signal getCurrentSignal() {
        return lastSignal;
    }

    public int getHighWatermark() {
        return highWatermark;
    }

    public int getLowWatermark() {
        return lowWatermark;
    }
}
