package com.migration.traffic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 指标/活性文件的写出。与既有引擎同一套约定：{@code files/<taskId>/metrics/<name>}，
 * agent 侧按 mtime 判活性、按内容取数值。
 *
 * <p><b>活性文件与"有没有干活"无关</b>：{@link #liveness()} 每轮循环无条件调用。
 * 拿"有数据才更新"的业务指标当活性判据，源库空闲时会误杀任务；这是本仓库反复踩过的坑。
 */
public final class TrafficMetrics {

    private static final Logger logger = LoggerFactory.getLogger(TrafficMetrics.class);

    /** mtime 在部分文件系统上只有秒级精度，刷新间隔必须明显大于 1s 才有区分度。 */
    private static final long LIVENESS_INTERVAL_MS = 2000L;

    private final File dir;
    private final String livenessName;
    private long lastLivenessAt;

    public TrafficMetrics(String taskId, String livenessName) {
        this.dir = new File("./files/" + taskId + "/metrics");
        this.livenessName = livenessName;
        if (!dir.exists() && !dir.mkdirs()) {
            logger.warn("指标目录创建失败: {}", dir.getAbsolutePath());
        }
    }

    public void set(String name, long value) {
        write(name, String.valueOf(value));
    }

    public void set(String name, String value) {
        write(name, value);
    }

    /** 无条件刷新活性（内部限流到 ~2s 一次）。 */
    public void liveness() {
        long now = System.currentTimeMillis();
        if (now - lastLivenessAt < LIVENESS_INTERVAL_MS) return;
        lastLivenessAt = now;
        write(livenessName, String.valueOf(now));
    }

    private void write(String name, String value) {
        try (BufferedWriter w = new BufferedWriter(new FileWriter(new File(dir, name), false))) {
            w.write(value);
        } catch (IOException e) {
            logger.debug("写指标 {} 失败: {}", name, e.getMessage());
        }
    }
}
