package com.migration.common.position;

import com.migration.common.io.AtomicFileWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 位点持久化的关键不变量：
 * <ul>
 *   <li>已落盘位点优先于 config.properties 的起始位点 —— 崩溃重启不再从任务起点整段重放；</li>
 *   <li>位点文件缺失/损坏时干净回退到起始位点，而不是抛异常或用半个值定位；</li>
 *   <li>写入是原子的 —— 不会出现"读到半个文件"。</li>
 * </ul>
 */
@DisplayName("CapturePositionStore 位点持久化")
class CapturePositionStoreTest {

    @Test
    @DisplayName("无落盘位点时用 config 起始位点")
    void fallsBackToConfigWhenNoPersistedFile(@TempDir Path dir) {
        Properties persisted = CapturePositionStore.load(dir.toString());
        assertTrue(persisted.isEmpty());
        assertEquals("binlog.000001",
                CapturePositionStore.prefer(persisted, "binlog.file", "binlog.000001", "binlog 文件"));
    }

    @Test
    @DisplayName("有落盘位点时优先用它，而不是 config 起始位点")
    void prefersPersistedOverConfig(@TempDir Path dir) {
        Properties pos = new Properties();
        pos.setProperty("binlog.file", "binlog.000042");
        pos.setProperty("binlog.position", "43310545");
        CapturePositionStore.save(dir.toString(), pos, "test");

        Properties persisted = CapturePositionStore.load(dir.toString());
        assertEquals("binlog.000042",
                CapturePositionStore.prefer(persisted, "binlog.file", "binlog.000001", "binlog 文件"));
        assertEquals("43310545",
                CapturePositionStore.prefer(persisted, "binlog.position", "31487318", "binlog 位点"));
    }

    @Test
    @DisplayName("落盘位点里的空值不算数，回退 config")
    void blankPersistedValueFallsBack(@TempDir Path dir) {
        Properties pos = new Properties();
        pos.setProperty("gtid.set", "   ");
        CapturePositionStore.save(dir.toString(), pos, "test");

        Properties persisted = CapturePositionStore.load(dir.toString());
        assertEquals("uuid:1-5",
                CapturePositionStore.prefer(persisted, "gtid.set", "uuid:1-5", "GTID 集"));
    }

    @Test
    @DisplayName("位点文件损坏时回退起始位点，不抛异常")
    void corruptedFileFallsBackQuietly(@TempDir Path dir) throws Exception {
        File f = CapturePositionStore.fileIn(dir.toString());
        // properties 解析很宽容，用非法 unicode 转义制造真正的解析失败
        try (FileOutputStream fos = new FileOutputStream(f)) {
            fos.write("binlog.file=\\uZZZZ\n".getBytes("UTF-8"));
        }
        Properties persisted = CapturePositionStore.load(dir.toString());
        assertEquals("binlog.000001",
                CapturePositionStore.prefer(persisted, "binlog.file", "binlog.000001", "binlog 文件"));
    }

    @Test
    @DisplayName("preferPersisted 可用配置项关掉，回到只认 config 的旧行为")
    void preferPersistedCanBeDisabled() {
        Properties cfg = new Properties();
        assertTrue(CapturePositionStore.preferPersisted(cfg));
        cfg.setProperty(CapturePositionStore.PREFER_PERSISTED_KEY, "false");
        assertFalse(CapturePositionStore.preferPersisted(cfg));
    }

    @Test
    @DisplayName("原子写不留临时文件，覆盖写后内容完整")
    void atomicWriteLeavesNoTempAndFullyReplaces(@TempDir Path dir) throws Exception {
        File target = new File(dir.toFile(), "pos.properties");
        Properties first = new Properties();
        first.setProperty("k", "a-very-long-value-".repeat(200));
        AtomicFileWriter.writeProperties(target, first, "first");

        Properties second = new Properties();
        second.setProperty("k", "short");
        AtomicFileWriter.writeProperties(target, second, "second");

        Properties read = new Properties();
        try (java.io.FileInputStream in = new java.io.FileInputStream(target)) {
            read.load(in);
        }
        assertEquals("short", read.getProperty("k"));

        File[] leftovers = dir.toFile().listFiles((d, n) -> n.endsWith(".tmp"));
        assertEquals(0, leftovers == null ? 0 : leftovers.length, "原子写不应留下临时文件");
    }
}
