package com.migration.common.snapshot;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 全量快照位点的文件契约。
 *
 * <p>五条链路（SQL / Mongo / ES / Redis / TiDB）写的是同一个文件、同一种格式，agent 只读一处，
 * 所以这个格式一旦解析错，受影响的是全部链路的"全量对应源端哪个点"。
 *
 * <p>最容易踩的是<b>位点里自带分隔符</b>：PG 的位点是 {@code lsn:...;snapshot:...}，
 * MySQL 的 GTID 集合里带逗号与冒号。按分隔符全切会把位点截断，所以只切前三个。
 */
@DisplayName("全量快照位点文件契约")
class SnapshotPositionTest {

    private final String taskId = "unit-test-snapshot-" + System.nanoTime();

    @AfterEach
    void cleanup() {
        File dir = new File("files/" + taskId);
        File f = new File(dir, SnapshotPosition.FILE_NAME);
        f.delete();
        dir.delete();
    }

    @Test
    @DisplayName("写入后能原样读回")
    void roundTrip() {
        SnapshotPosition.write(taskId, "CONSISTENT", "mysql", "gtid:aaaa-bbbb:1-100");

        SnapshotPosition.Record r = SnapshotPosition.read(taskId);
        assertNotNull(r);
        assertEquals("CONSISTENT", r.mode);
        assertEquals("mysql", r.dbType);
        assertEquals("gtid:aaaa-bbbb:1-100", r.position);
        assertTrue(r.timestamp > 0);
    }

    @Test
    @DisplayName("位点里带分隔符（PG 的 lsn;snapshot）不能被截断")
    void positionMayContainSeparators() {
        SnapshotPosition.write(taskId, "CONSISTENT", "postgresql", "lsn:0/1A2B3C;snapshot:00000003-0000001F-1");

        SnapshotPosition.Record r = SnapshotPosition.read(taskId);
        assertNotNull(r);
        assertEquals("lsn:0/1A2B3C;snapshot:00000003-0000001F-1", r.position);
    }

    @Test
    @DisplayName("没有文件（NONE 档/老任务/仅增量）→ 返回 null，不抛异常")
    void missingFileReadsAsNull() {
        assertNull(SnapshotPosition.read(taskId));
    }

    @Test
    @DisplayName("taskId 或位点为空时不写文件（位点是增强项，不能反过来影响全量）")
    void ignoresIncompleteInput() {
        SnapshotPosition.write(null, "CONSISTENT", "mysql", "gtid:x");
        SnapshotPosition.write(taskId, "CONSISTENT", "mysql", null);
        assertNull(SnapshotPosition.read(taskId));
    }
}
