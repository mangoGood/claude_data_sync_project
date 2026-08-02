package com.migration.increment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双向写写冲突消解（P1-4）。
 *
 * <p>最关键的一条是<b>两个方向必须选出同一个赢家</b>：A→B 与 B→A 是两个独立进程、各算各的，
 * 一旦裁决规则不是对称的（比如平局时各自留自己的值），两端就会各自收敛到不同的值且永远不再一致——
 * 那比"谁后到谁覆盖"更糟，因为它看起来还是"成功同步"的。
 */
@DisplayName("双向写写冲突消解：时间戳裁决 + 平局对称")
class ConflictResolverTest {

    private Connection conn;
    private String taskId;

    @BeforeEach
    void setUp() throws Exception {
        taskId = "cdr-test-" + System.nanoTime();
        conn = DriverManager.getConnection("jdbc:h2:mem:" + taskId + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP ALL OBJECTS");
        }
        conn.close();
        File dir = new File("files", taskId);
        if (dir.isDirectory()) {
            try (Stream<java.nio.file.Path> walk = Files.walk(dir.toPath())) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private ConflictResolver resolver(String nodeId, String policy, String primary) {
        Properties props = new Properties();
        props.setProperty(ConflictResolver.KEY_POLICY, policy);
        if (primary != null) {
            props.setProperty(ConflictResolver.KEY_PRIMARY_NODE, primary);
        }
        ConflictResolver r = new ConflictResolver(props, taskId, nodeId, false);
        r.ensureTable(conn);
        return r;
    }

    @Test
    @DisplayName("首次写入直接应用；更新的时间戳照常推进")
    void firstWriteAndNewerWins() throws Exception {
        ConflictResolver a = resolver("dbA", "LWW_SOURCE_TS", null);

        assertEquals(ConflictResolver.Decision.APPLY, a.decide(conn, "t", "1", 1000));
        a.record(conn, "t", "1", 1000);

        assertEquals(ConflictResolver.Decision.APPLY, a.decide(conn, "t", "1", 2000));
        a.record(conn, "t", "1", 2000);
        assertEquals(0, a.getConflictCount(), "同一节点的连续写入不是冲突");
    }

    @Test
    @DisplayName("LWW：更旧的对端写入被判输并计入冲突")
    void olderPeerWriteLoses() throws Exception {
        ConflictResolver a = resolver("dbA", "LWW_SOURCE_TS", null);
        ConflictResolver b = resolver("dbB", "LWW_SOURCE_TS", null);

        a.decide(conn, "t", "1", 5000);
        a.record(conn, "t", "1", 5000);

        assertEquals(ConflictResolver.Decision.SKIP, b.decide(conn, "t", "1", 4000),
                "对端更早的写入必须被丢弃，否则新值会被旧值覆盖");
        assertEquals(1, b.getConflictCount());
        assertEquals(1, b.getSkippedCount());
    }

    @Test
    @DisplayName("LWW 平局：两个方向各自算，必须选出同一个赢家")
    void tieBreakIsSymmetric() throws Exception {
        // 方向1：B 的写入撞上已记录的 A（同一毫秒）
        ConflictResolver bIntoA = resolver("dbB", "LWW_SOURCE_TS", null);
        bIntoA.record(conn, "t", "1", 7000);          // 先记一条 dbB 的
        ConflictResolver aIntoB = resolver("dbA", "LWW_SOURCE_TS", null);
        ConflictResolver.Decision aResult = aIntoB.decide(conn, "t", "1", 7000);

        // 方向2：反过来——已记录的是 dbA，来的是 dbB
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM `" + ConflictResolver.TABLE + "`");
        }
        ConflictResolver aFirst = resolver("dbA", "LWW_SOURCE_TS", null);
        aFirst.record(conn, "t", "1", 7000);
        ConflictResolver bSecond = resolver("dbB", "LWW_SOURCE_TS", null);
        ConflictResolver.Decision bResult = bSecond.decide(conn, "t", "1", 7000);

        // 两边独立裁决，结果必须一致地指向同一个节点的值（这里按节点 id 字典序，dbB 胜）
        assertEquals(ConflictResolver.Decision.SKIP, aResult, "dbA 平局应判输（字典序小）");
        assertEquals(ConflictResolver.Decision.APPLY, bResult, "dbB 平局应判赢（字典序大）");
    }

    @Test
    @DisplayName("NODE_PRIORITY：主端恒赢，从端的并发写被丢弃")
    void nodePriorityAlwaysPrefersPrimary() throws Exception {
        ConflictResolver secondary = resolver("dbB", "NODE_PRIORITY", "dbA");
        ConflictResolver primary = resolver("dbA", "NODE_PRIORITY", "dbA");

        secondary.record(conn, "t", "9", 9000);
        // 主端更早的写入照样赢（策略就是"主端说了算"）
        assertEquals(ConflictResolver.Decision.APPLY, primary.decide(conn, "t", "9", 8000));

        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM `" + ConflictResolver.TABLE + "`");
        }
        primary.record(conn, "t", "9", 9000);
        assertEquals(ConflictResolver.Decision.SKIP, secondary.decide(conn, "t", "9", 8000));
    }

    @Test
    @DisplayName("ERROR 策略：冲突即 fail-stop，交人工")
    void errorPolicyFailsFast() throws Exception {
        ConflictResolver a = resolver("dbA", "ERROR", null);
        ConflictResolver b = resolver("dbB", "ERROR", null);
        a.record(conn, "t", "1", 3000);
        assertEquals(ConflictResolver.Decision.FAIL, b.decide(conn, "t", "1", 2000));
    }

    @Test
    @DisplayName("冲突落 conflict.jsonl，带得上两端节点与时间戳")
    void conflictIsRecordedToFile() throws Exception {
        ConflictResolver a = resolver("dbA", "LWW_SOURCE_TS", null);
        ConflictResolver b = resolver("dbB", "LWW_SOURCE_TS", null);
        a.record(conn, "t", "42", 6000);
        b.decide(conn, "t", "42", 5000);

        File f = new File("files/" + taskId + "/conflict.jsonl");
        assertTrue(f.isFile(), "应写出冲突记录供 UI 展示");
        String content = Files.readString(f.toPath());
        assertTrue(content.contains("\"incomingNode\":\"dbB\""), content);
        assertTrue(content.contains("\"storedNode\":\"dbA\""), content);
        assertTrue(content.contains("\"winner\":\"stored\""), content);
    }

    @Test
    @DisplayName("无主键/无时间戳的事件不参与裁决，直接放行")
    void missingIdentityIsAlwaysApplied() throws Exception {
        ConflictResolver a = resolver("dbA", "LWW_SOURCE_TS", null);
        a.record(conn, "t", "1", 5000);
        assertEquals(ConflictResolver.Decision.APPLY, a.decide(conn, "t", null, 1000));
        assertEquals(ConflictResolver.Decision.APPLY, a.decide(conn, "t", "1", 0));
    }
}
