package com.migration.common.position;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 位点保留期巡检结果的落盘与读取。
 *
 * <p>这个文件是 agent 与 capture 之间唯一的约定（跨进程、无依赖），
 * 所以格式必须稳：说明里带上 {@code |} 或换行也不能把这一行撑破，
 * 否则读侧解析出来的状态就是错的——而它恰恰是用来报警的。
 */
@DisplayName("位点保留期巡检结果")
class RetentionStatusTest {

    private final String dir = "files/unit-test-retention-" + System.nanoTime();

    @AfterEach
    void cleanup() {
        File d = new File(dir);
        File[] files = d.listFiles();
        if (files != null) {
            for (File f : files) f.delete();
        }
        d.delete();
    }

    @Test
    @DisplayName("写读 round-trip")
    void roundTrip() {
        RetentionStatus.write(dir, RetentionStatus.State.WARN, 0, "位点文件 mysql-bin.000001，其前尚存 0 个文件");
        RetentionStatus.Record rec = RetentionStatus.read(dir);
        assertEquals(RetentionStatus.State.WARN, rec.state);
        assertEquals(0, rec.headroom);
        assertTrue(rec.detail.contains("mysql-bin.000001"));
        assertTrue(rec.timestamp > 0);
    }

    @Test
    @DisplayName("说明里的分隔符和换行必须被消化掉，不能撑破单行格式")
    void detailWithSeparatorsStaysOnOneLine() {
        RetentionStatus.write(dir, RetentionStatus.State.LOST, 0, "查询失败: a|b\nc|d");
        RetentionStatus.Record rec = RetentionStatus.read(dir);
        assertEquals(RetentionStatus.State.LOST, rec.state);
        assertEquals("查询失败: a/b c/d", rec.detail);
    }

    @Test
    @DisplayName("没有文件就是没跑过巡检，返回 null 而不是假状态")
    void missingFileReturnsNull() {
        assertNull(RetentionStatus.read(dir));
    }

    @Test
    @DisplayName("残缺行按没有结果处理：宁可显示未知，也不能报一个错的状态")
    void brokenLineReturnsNull() {
        com.migration.common.io.AtomicFileWriter.writeStringQuietly(
                RetentionStatus.fileIn(dir), "1754000000000|WARN\n");
        assertNull(RetentionStatus.read(dir));
    }
}
