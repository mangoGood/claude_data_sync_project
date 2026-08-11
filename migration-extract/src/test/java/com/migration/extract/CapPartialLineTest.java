package com.migration.extract;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code .cap} 半行保护：只消费已经写完整（以换行结束）的行。
 *
 * <p>capture 是一边写一边被读的。一条记录写进 {@code BufferedWriter} 时若跨过 8192 字符的
 * 缓冲边界，它会分两次落到文件上；读取方夹在两次之间扫到文件就会看到半条记录，而
 * {@code readLine()} 对"文件末尾没有换行符"和"一行正常结束"给出的结果一模一样。
 * 把半行当完整事件消费掉，前半截会被写进 THL（字段够数时下游发现不了），后半截在下一轮
 * 变成孤行被丢掉 —— 两头都是静默的。
 */
@DisplayName(".cap 半行保护：countLines 只数写完整的行")
class CapPartialLineTest {

    @TempDir
    Path tempDir;

    private int countLines(File file) throws Exception {
        ContinuousExtractMain main = new ContinuousExtractMain();
        Method m = ContinuousExtractMain.class.getDeclaredMethod("countLines", File.class);
        m.setAccessible(true);
        return (int) m.invoke(main, file);
    }

    private File write(String content) throws Exception {
        File f = tempDir.resolve("binlog_test_0000.cap").toFile();
        try (Writer w = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)) {
            w.write(content);
        }
        return f;
    }

    @Test
    @DisplayName("以换行结束：所有行都算完整")
    void countsAllWhenTerminated() throws Exception {
        assertEquals(3, countLines(write("a\nb\nc\n")));
    }

    @Test
    @DisplayName("末行没写完：不计入，留到下一轮重读")
    void lastPartialLineIsNotCounted() throws Exception {
        assertEquals(2, countLines(write("a\nb\nhalf-writ")));
    }

    @Test
    @DisplayName("只有半行时一行都不消费")
    void singlePartialLineCountsZero() throws Exception {
        assertEquals(0, countLines(write("half-writ")));
    }

    @Test
    @DisplayName("空文件为 0（不能减成 -1）")
    void emptyFileIsZero() throws Exception {
        assertEquals(0, countLines(write("")));
    }

    @Test
    @DisplayName("半行补齐之后即可被消费")
    void partialLineBecomesReadableOnceCompleted() throws Exception {
        File f = write("a\nb\nhalf");
        assertEquals(2, countLines(f));
        // capture 把这条记录的后半截也写完了
        try (Writer w = new OutputStreamWriter(new FileOutputStream(f, true), StandardCharsets.UTF_8)) {
            w.write("-rest\n");
        }
        assertEquals(3, countLines(f));
    }
}
