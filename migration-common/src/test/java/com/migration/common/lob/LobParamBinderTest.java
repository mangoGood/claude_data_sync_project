package com.migration.common.lob;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 增量 apply 侧的大字段参数绑定。
 *
 * <p>三个失败面各测一遍，因为它们的后果都是"静默"的：
 * 文件缺失（跨机接管）、文件被截断、以及把"只留身份"的前镜像引用当成内容用。
 * 这三种情况如果不显式报错，表现都是目标端多了一行看似正常、内容却是错的数据。
 */
@DisplayName("大字段参数绑定")
class LobParamBinderTest {

    @TempDir
    File spillDir;

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:bind_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE t (id INT PRIMARY KEY, payload VARBINARY(1000000))");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.close();
    }

    private static byte[] payload(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) (i % 251);
        }
        return b;
    }

    private LobRef spill(String name, byte[] content) throws Exception {
        Files.write(new File(spillDir, name).toPath(), content);
        return LobRef.spilled(name, content.length, "md5-not-checked-here");
    }

    private byte[] readBack(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT payload FROM t WHERE id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getBytes(1) : null;
            }
        }
    }

    @Test
    @DisplayName("引用绑成流式参数，内容逐字节写入目标")
    void bindsRefAsStream() throws Exception {
        byte[] content = payload(50_000);
        LobRef ref = spill("mysql-bin.000001#100#c0.lob", content);

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (id, payload) VALUES (?, ?)");
             LobParamBinder binder = new LobParamBinder(spillDir.getAbsolutePath())) {
            assertTrue(binder.bindAll(ps, Arrays.asList(1, ref)), "应报告绑定了大字段");
            ps.executeUpdate();
        }
        assertArrayEquals(content, readBack(1));
    }

    @Test
    @DisplayName("普通参数照旧 setObject，行为不变")
    void bindsPlainParams() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (id, payload) VALUES (?, ?)");
             LobParamBinder binder = new LobParamBinder(spillDir.getAbsolutePath())) {
            assertFalse(binder.bindAll(ps, Arrays.asList(2, payload(10))));
            ps.executeUpdate();
        }
        assertArrayEquals(payload(10), readBack(2));
    }

    @Test
    @DisplayName("落盘文件不存在（典型：跨机接管）→ 明确报错并说清怎么办")
    void missingFileFailsLoudly() {
        LobRef ref = LobRef.spilled("mysql-bin.000001#999#c0.lob", 123, "abc");
        SQLException e = assertThrows(SQLException.class, () -> {
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (id, payload) VALUES (?, ?)");
                 LobParamBinder binder = new LobParamBinder(spillDir.getAbsolutePath())) {
                binder.bindAll(ps, Arrays.asList(3, ref));
            }
        });
        assertTrue(e.getMessage().contains("SYNC_LOB_FILE_MISSING"), e.getMessage());
        // 落盘文件是本机状态，中心位点回灌到别的机器上时文件并不在——错误信息必须点出这一点
        assertTrue(e.getMessage().contains("跨机接管"), e.getMessage());
    }

    @Test
    @DisplayName("文件长度与引用不符 → 报错，不写半截内容")
    void truncatedFileFailsLoudly() throws Exception {
        Files.write(new File(spillDir, "mysql-bin.000001#100#c0.lob").toPath(), payload(100));
        LobRef ref = LobRef.spilled("mysql-bin.000001#100#c0.lob", 999, "abc");
        SQLException e = assertThrows(SQLException.class, () -> {
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (id, payload) VALUES (?, ?)");
                 LobParamBinder binder = new LobParamBinder(spillDir.getAbsolutePath())) {
                binder.bindAll(ps, Arrays.asList(4, ref));
            }
        });
        assertTrue(e.getMessage().contains("SYNC_LOB_FILE_TRUNCATED"), e.getMessage());
    }

    @Test
    @DisplayName("把只留身份的前镜像当参数用 → 报错（无主键大字段表的必然结局）")
    void discardedRefAsParamFails() {
        LobRef ref = LobRef.discarded(1024, "deadbeef");
        SQLException e = assertThrows(SQLException.class, () -> {
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (id, payload) VALUES (?, ?)");
                 LobParamBinder binder = new LobParamBinder(spillDir.getAbsolutePath())) {
                binder.bindAll(ps, Arrays.asList(5, ref));
            }
        });
        assertTrue(e.getMessage().contains("SYNC_LOB_DISCARDED_IN_PARAM"), e.getMessage());
        assertTrue(e.getMessage().contains("必须有主键"), e.getMessage());
    }

    @Test
    @DisplayName("containsLob 用于判定是否要禁用批量")
    void detectsLobParams() {
        assertTrue(LobParamBinder.containsLob(List.of(1, LobRef.discarded(1, "x"))));
        assertFalse(LobParamBinder.containsLob(List.of(1, "abc")));
        assertFalse(LobParamBinder.containsLob(null));
    }

    @Test
    @DisplayName("close 关掉打开的文件流（否则长跑的增量进程会耗尽 fd）")
    void closesOpenedStreams() throws Exception {
        byte[] content = payload(1000);
        LobRef ref = spill("mysql-bin.000001#200#c0.lob", content);
        LobParamBinder binder = new LobParamBinder(spillDir.getAbsolutePath());
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (id, payload) VALUES (?, ?)")) {
            binder.bindAll(ps, Arrays.asList(6, ref));
            ps.executeUpdate();
        }
        binder.close();
        // 关掉之后文件应可删除（Windows 上有 fd 就删不掉；这里主要是不抛异常即通过）
        assertTrue(new File(spillDir, ref.file()).delete());
    }
}
