package com.migration.agent.thread;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 指标文件的新鲜度判断。
 *
 * <p>链路停了之后指标文件不再更新，若照旧返回最后一次写入的数字，面板上看到的是一个
 * **过期但看着正常**的延迟。"量不出来"和"延迟很低"是两件完全不同的事，必须能区分开 ——
 * 指标行的第一个字段就是写入时刻，判据现成。
 */
@DisplayName("指标文件过期即按无数据处理")
class MetricStalenessTest {

    @TempDir
    Path tempDir;

    /** 构造一个 executor 需要一整套依赖，指标读取本身与实例状态无关，直接调静态实现。 */
    private Long read(File file, long maxAgeMs) {
        return AbstractTaskExecutor.readFreshMetric(file.getAbsolutePath(), maxAgeMs);
    }

    private File metric(long writtenAt, long value) throws Exception {
        File f = tempDir.resolve("rto_metric").toFile();
        Files.write(f.toPath(), (writtenAt + "|" + value + "|" + (writtenAt - value) + "\n")
                .getBytes(StandardCharsets.UTF_8));
        return f;
    }

    @Test
    @DisplayName("刚写的样本正常返回")
    void freshSampleIsReturned() throws Exception {
        File f = metric(System.currentTimeMillis(), 1234L);
        assertEquals(1234L, read(f, 60_000L));
    }

    @Test
    @DisplayName("过期的样本按无数据处理，而不是返回那个旧值")
    void staleSampleReadsAsNoData() throws Exception {
        File f = metric(System.currentTimeMillis() - 10 * 60_000L, 5L);
        assertNull(read(f, 60_000L),
                "链路早停了还返回 5ms，面板上就是一个过期但看着正常的延迟");
    }

    @Test
    @DisplayName("maxAge<=0 关闭新鲜度判断（兼容没有时间戳的老指标）")
    void ageCheckCanBeDisabled() throws Exception {
        File f = metric(System.currentTimeMillis() - 10 * 60_000L, 5L);
        assertEquals(5L, read(f, 0L));
    }

    @Test
    @DisplayName("文件不存在返回 null")
    void missingFileIsNull() throws Exception {
        assertNull(read(tempDir.resolve("nope").toFile(), 60_000L));
    }

    @Test
    @DisplayName("内容坏掉不抛异常，按无数据处理")
    void brokenContentIsNull() throws Exception {
        File f = tempDir.resolve("rto_metric").toFile();
        Files.write(f.toPath(), "garbage".getBytes(StandardCharsets.UTF_8));
        assertNull(read(f, 60_000L));
    }
}
