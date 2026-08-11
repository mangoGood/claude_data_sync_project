package com.migration.extract;

import com.migration.common.lob.LobRef;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 大字段引用在 extract 侧的透传（纯单测，不连库）。
 *
 * <p>要守的两条：
 * <ol>
 *   <li><b>类型化路径</b>必须把 {@code @lob:} 还原成 {@link LobRef} 原样带下去，
 *       而且绝不能因为"认不出来"而返回 null——返回 null 会让整行回退文本路径；</li>
 *   <li><b>文本路径</b>必须放毒丸而不是引号包起来的字符串。后者会把
 *       "@lob:xxx" 这串字符当作内容写进目标 BLOB 列，是静默的数据损坏，
 *       比直接报错难查一个数量级。</li>
 * </ol>
 */
@DisplayName("大字段引用透传")
class LobRefPassthroughTest {

    private final MySQLBinlogExtractor extractor = new MySQLBinlogExtractor();

    @SuppressWarnings("unchecked")
    private ArrayList<Object> typeRow(List<String> values, String[] types, String[] fullTypes,
                                      String[] names) throws Exception {
        Method m = MySQLBinlogExtractor.class.getDeclaredMethod("typeRowValues",
                List.class, String[].class, String[].class, String[].class, Map.class);
        m.setAccessible(true);
        return (ArrayList<Object>) m.invoke(extractor, values, types, fullTypes, names, new HashMap<String, List<String>>());
    }

    private String formatRow(List<String> values, String[] types, String[] fullTypes,
                             String[] names) throws Exception {
        Method m = MySQLBinlogExtractor.class.getDeclaredMethod("formatRowData",
                List.class, String[].class, String[].class, String[].class, Map.class);
        m.setAccessible(true);
        return (String) m.invoke(extractor, values, types, fullTypes, names, new HashMap<String, List<String>>());
    }

    private static final String[] TYPES = {"int", "longblob"};
    private static final String[] FULL_TYPES = {"int", "longblob"};
    private static final String[] NAMES = {"id", "c_blob"};

    @Test
    @DisplayName("类型化路径把引用原样还原成 LobRef")
    void typedPathKeepsRef() throws Exception {
        LobRef ref = LobRef.spilled("mysql-bin.000042#1234#c1.lob", 1073741824L, "abc123");
        ArrayList<Object> typed = typeRow(Arrays.asList("7", ref.toMarker()), TYPES, FULL_TYPES, NAMES);

        assertNotNull(typed, "带大字段的行绝不能返回 null —— 那会让整行回退文本路径");
        assertEquals("7", typed.get(0));
        LobRef got = assertInstanceOf(LobRef.class, typed.get(1));
        assertEquals(ref.file(), got.file());
        assertEquals(1073741824L, got.length());
        assertEquals("abc123", got.md5());
    }

    @Test
    @DisplayName("只留身份的引用（前镜像）也能透传")
    void typedPathKeepsDiscardedRef() throws Exception {
        LobRef ref = LobRef.discarded(999, "def456");
        ArrayList<Object> typed = typeRow(Arrays.asList("7", ref.toMarker()), TYPES, FULL_TYPES, NAMES);
        LobRef got = assertInstanceOf(LobRef.class, typed.get(1));
        assertTrue(got.isDiscarded());
        assertEquals(999, got.length());
    }

    @Test
    @DisplayName("文本路径放毒丸，绝不把引用当成字符串内容")
    void textPathPoisons() throws Exception {
        LobRef ref = LobRef.spilled("mysql-bin.000042#1234#c1.lob", 1073741824L, "abc123");
        String sql = formatRow(Arrays.asList("7", ref.toMarker()), TYPES, FULL_TYPES, NAMES);

        assertTrue(sql.contains(MySQLBinlogExtractor.LOB_TEXT_PATH_POISON), sql);
        // 关键：不能出现 '@lob:...' 这种"看起来合法"的字符串字面量
        assertFalse(sql.contains("'" + LobRef.MARKER), "引用被当成字符串字面量了，会静默写坏目标列: " + sql);
    }

    @Test
    @DisplayName("普通十六进制值不受影响（阈值以下零回归）")
    void plainHexUnaffected() throws Exception {
        ArrayList<Object> typed = typeRow(Arrays.asList("7", "0x0102ff"), TYPES, FULL_TYPES, NAMES);
        assertArrayEqualsByte(new byte[]{1, 2, (byte) 0xff}, (byte[]) typed.get(1));

        String sql = formatRow(Arrays.asList("7", "0x0102ff"), TYPES, FULL_TYPES, NAMES);
        assertTrue(sql.contains("0x0102ff"), sql);
        assertFalse(sql.contains(MySQLBinlogExtractor.LOB_TEXT_PATH_POISON));
    }

    @Test
    @DisplayName("containsLobRef 用于给事件打 has_lob 标记")
    void detectsLobInRow() {
        assertTrue(MySQLBinlogExtractor.containsLobRef(
                Arrays.asList("1", LobRef.discarded(5, "x").toMarker())));
        assertFalse(MySQLBinlogExtractor.containsLobRef(Arrays.asList("1", "0xdeadbeef", null)));
    }

    private static void assertArrayEqualsByte(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i], "第 " + i + " 字节");
        }
    }
}
