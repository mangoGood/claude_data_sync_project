package com.migration.common.lob;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 大字段落盘文件的回收。
 *
 * <p>两个方向都会出事，所以两边都要锁：删早了，重放时取不到内容（要么 fail-stop，
 * 要么更糟——静默跳过那一行的大字段）；不删，一个文件 1GB，攒几十个就把盘吃光。
 * 判据只能是位点，不能是时间或数量。
 */
@DisplayName("大字段落盘回收")
class LobSpillJanitorTest {

    @TempDir
    File dir;

    private File touch(String name) throws IOException {
        File f = new File(dir, name);
        assertTrue(f.createNewFile());
        return f;
    }

    @Test
    @DisplayName("文件名解析：位点 / 序号 / binlog 文件名")
    void parsesNames() {
        LobSpillNaming.Parsed p = LobSpillNaming.parse("mysql-bin.000042#1234567#c3.lob");
        assertNotNull(p);
        assertEquals("mysql-bin.000042", p.binlogFile);
        assertEquals(1234567L, p.position);
        assertEquals(3, p.cellIndex);

        assertNull(LobSpillNaming.parse("random.lob"));
        assertNull(LobSpillNaming.parse("mysql-bin.000042#1234567#c3.lobtmp"));
        assertNull(LobSpillNaming.parse(null));
    }

    @Test
    @DisplayName("只删位点严格早于已应用位点的文件")
    void deletesOnlyBeforeAppliedPosition() throws IOException {
        File old1 = touch(LobSpillNaming.fileName("mysql-bin.000001", 100, 0));
        File old2 = touch(LobSpillNaming.fileName("mysql-bin.000001", 500, 1));
        File current = touch(LobSpillNaming.fileName("mysql-bin.000001", 900, 0));
        File future = touch(LobSpillNaming.fileName("mysql-bin.000001", 1500, 0));

        int removed = new LobSpillJanitor(dir.getAbsolutePath())
                .cleanupUpTo("mysql-bin.000001", 900);

        assertEquals(2, removed);
        assertFalse(old1.exists());
        assertFalse(old2.exists());
        // 同位点的就是当前这一批，可能还要重放，绝不能删
        assertTrue(current.exists(), "位点相等的文件不能删——那正是当前这批，可能重放");
        assertTrue(future.exists());
    }

    @Test
    @DisplayName("跨 binlog 文件按文件名序比较（带序号，字典序即时间序）")
    void comparesAcrossBinlogFiles() throws IOException {
        File older = touch(LobSpillNaming.fileName("mysql-bin.000001", 999999, 0));
        File newer = touch(LobSpillNaming.fileName("mysql-bin.000003", 4, 0));

        int removed = new LobSpillJanitor(dir.getAbsolutePath())
                .cleanupUpTo("mysql-bin.000002", 4);

        assertEquals(1, removed);
        assertFalse(older.exists(), "旧 binlog 文件里的落盘文件应被回收");
        assertTrue(newer.exists(), "更新的 binlog 文件里的不能删");
    }

    @Test
    @DisplayName("名字不认识的文件一律不碰")
    void leavesUnknownFilesAlone() throws IOException {
        File stranger = touch("something-else.lob");
        File tmp = touch("spill-1-2.lobtmp");
        new LobSpillJanitor(dir.getAbsolutePath()).cleanupUpTo("mysql-bin.000009", 999999);
        // 宁可留着占点盘，也不能误删别人的东西
        assertTrue(stranger.exists());
        assertTrue(tmp.exists());
    }

    @Test
    @DisplayName("同一位点重复调用不重复扫目录（apply 热路径每事件都会调）")
    void skipsRepeatedCleanupAtSamePosition() throws IOException {
        touch(LobSpillNaming.fileName("mysql-bin.000001", 10, 0));
        LobSpillJanitor janitor = new LobSpillJanitor(dir.getAbsolutePath());
        assertEquals(1, janitor.cleanupUpTo("mysql-bin.000001", 100));
        touch(LobSpillNaming.fileName("mysql-bin.000001", 20, 0));
        assertEquals(0, janitor.cleanupUpTo("mysql-bin.000001", 100), "同位点应短路，不再扫目录");
        assertEquals(1, janitor.cleanupUpTo("mysql-bin.000001", 101), "位点推进后才重新扫");
    }

    @Test
    @DisplayName("目录不存在 / 位点为空时安全返回")
    void safeWhenNothingToDo() {
        assertEquals(0, new LobSpillJanitor(null).cleanupUpTo("mysql-bin.000001", 1));
        assertEquals(0, new LobSpillJanitor("/nonexistent/path/xyz").cleanupUpTo("mysql-bin.000001", 1));
        assertEquals(0, new LobSpillJanitor(dir.getAbsolutePath()).cleanupUpTo("", 1));
        assertEquals(0, new LobSpillJanitor(dir.getAbsolutePath()).cleanupUpTo(null, 1));
    }
}
