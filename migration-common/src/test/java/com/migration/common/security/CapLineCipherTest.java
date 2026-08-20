package com.migration.common.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code .cap} 逐行加密判据。
 *
 * <p>这条链路上真正要守的不是"密文对不对"，而是<b>行边界一个都不能动</b>：
 * extract 按已读<b>行数</b>记断点，且用"文件是否以换行结尾"检测半行。
 * 仓库里"{@code .cap} 半行"是修过的静默丢数缺陷——任何动摇行边界的改法
 * 都在拿最脆弱的续传逻辑冒险。
 */
class CapLineCipherTest {

    @TempDir
    Path tmp;

    private static Properties enabled() {
        Properties p = new Properties();
        p.setProperty("capture.encryption.enabled", "true");
        p.setProperty("capture.encryption.password", "a-real-secret");
        return p;
    }

    /** 一条真实形态的记录：字段用  分隔，内部没有换行。 */
    private static String record(String type, String payload) {
        return type + '\001' + "binlog.000035" + '\001' + "172573" + '\001' + payload;
    }

    // ---------------- 行边界 ----------------

    @Test
    @DisplayName("行数完全不变——这是 extract 断点的基础")
    void lineCountUnchanged() throws IOException {
        CapLineCipher c = new CapLineCipher(enabled());
        Path f = tmp.resolve("a.cap");
        List<String> records = List.of(
                record("ROTATE", "x"),
                record("QUERY", "INSERT INTO t VALUES (1)"),
                record("WRITE_ROWS", "1|abc|2026-08-19"),
                record("XID", "9988"));

        try (CapFileWriter w = new CapFileWriter(new FileWriter(f.toFile()), c)) {
            for (String r : records) {
                w.write(r + "\n");   // capture 的写法：一次一条完整记录（带换行）
            }
        }

        List<String> lines = Files.readAllLines(f);
        assertEquals(records.size(), lines.size(), "加密后行数必须与记录数一致");
        for (String line : lines) {
            assertTrue(CapLineCipher.isEncryptedLine(line), "每行都应是密文行: " + line);
        }
    }

    @Test
    @DisplayName("解密后逐条还原原始记录")
    void roundTripsEveryRecord() throws IOException {
        CapLineCipher c = new CapLineCipher(enabled());
        Path f = tmp.resolve("b.cap");
        List<String> records = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            records.add(record("WRITE_ROWS", i + "|payload-" + i + "|中文与符号 &<>\"'"));
        }
        try (CapFileWriter w = new CapFileWriter(new FileWriter(f.toFile()), c)) {
            for (String r : records) {
                w.write(r + "\n");
            }
        }
        List<String> got = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new FileReader(f.toFile()))) {
            String line;
            while ((line = r.readLine()) != null) {
                got.add(c.decryptLine(line));
            }
        }
        assertEquals(records, got);
    }

    @Test
    @DisplayName("文件仍以换行结尾——extract 的半行检测靠它")
    void fileStillEndsWithNewline() throws IOException {
        CapLineCipher c = new CapLineCipher(enabled());
        Path f = tmp.resolve("c.cap");
        try (CapFileWriter w = new CapFileWriter(new FileWriter(f.toFile()), c)) {
            w.write(record("QUERY", "x") + "\n");
        }
        byte[] bytes = Files.readAllBytes(f);
        assertEquals('\n', (char) bytes[bytes.length - 1]);
    }

    // ---------------- 明文/密文共存 ----------------

    @Test
    @DisplayName("同一文件里明文行与密文行共存——升级瞬间就是这样，断点不能错")
    void mixedPlainAndEncryptedLines() throws IOException {
        CapLineCipher c = new CapLineCipher(enabled());
        Path f = tmp.resolve("d.cap");
        String plain1 = record("ROTATE", "old-1");
        String plain2 = record("QUERY", "old-2");
        String enc = c.encryptRecord(record("XID", "new-1"));

        Files.writeString(f, plain1 + "\n" + plain2 + "\n" + enc + "\n");

        List<String> got = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new FileReader(f.toFile()))) {
            String line;
            while ((line = r.readLine()) != null) {
                got.add(c.decryptLine(line));
            }
        }
        assertEquals(List.of(plain1, plain2, record("XID", "new-1")), got,
                "明文行必须原样透传，密文行必须解开");
    }

    @Test
    @DisplayName("行首标记不会与明文歧义：明文行首字符恒为事件类型名的大写字母")
    void markerCannotCollideWithPlaintext() {
        for (String type : new String[]{"ROTATE", "QUERY", "WRITE_ROWS", "UPDATE_ROWS",
                "DELETE_ROWS", "XID", "FORMAT_DESCRIPTION", "ANONYMOUS_GTID", "TABLE_MAP"}) {
            String line = record(type, "x");
            assertNotEquals(CapLineCipher.ENC_MARK, line.charAt(0),
                    "明文行首不应等于密文标记: " + type);
            assertFalse(CapLineCipher.isEncryptedLine(line));
        }
    }

    // ---------------- 失败必须响 ----------------

    @Test
    @DisplayName("密钥不对时抛错，不静默跳过——静默跳过就是丢数据")
    void wrongKeyThrows() {
        Properties a = enabled();
        Properties b = enabled();
        b.setProperty("capture.encryption.password", "another-secret");
        String enc = new CapLineCipher(a).encryptRecord(record("QUERY", "secret"));
        assertThrows(IllegalStateException.class, () -> new CapLineCipher(b).decryptLine(enc));
    }

    @Test
    @DisplayName("没开加密却读到密文行 → 抛错并说清原因")
    void encryptedLineWithoutKeyThrows() {
        String enc = new CapLineCipher(enabled()).encryptRecord(record("QUERY", "x"));
        CapLineCipher off = new CapLineCipher(new Properties());
        assertFalse(off.isEnabled());
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> off.decryptLine(enc));
        assertTrue(e.getMessage().contains("capture.encryption"), "报错要指向配置: " + e.getMessage());
    }

    @Test
    @DisplayName("开了加密但无任何密钥来源 → 拒绝启动")
    void refusesPredictableKey() {
        String saved = System.getProperty("synctask.master.key");
        System.clearProperty("synctask.master.key");
        try {
            if (System.getenv("SYNCTASK_MASTER_KEY") == null
                    || System.getenv("SYNCTASK_MASTER_KEY").isEmpty()) {
                Properties p = new Properties();
                p.setProperty("capture.encryption.enabled", "true");
                assertThrows(IllegalStateException.class, () -> new CapLineCipher(p));
            }
        } finally {
            if (saved != null) System.setProperty("synctask.master.key", saved);
        }
    }

    // ---------------- 默认行为 ----------------

    @Test
    @DisplayName("默认关闭时写入完全透传，与改造前逐字节一致")
    void disabledIsBytewiseIdentical() throws IOException {
        CapLineCipher off = new CapLineCipher(new Properties());
        assertFalse(off.isEnabled());

        Path a = tmp.resolve("plain-new.cap");
        Path b = tmp.resolve("plain-old.cap");
        String[] recs = {record("ROTATE", "1"), record("QUERY", "2"), record("XID", "3")};

        try (CapFileWriter w = new CapFileWriter(new FileWriter(a.toFile()), off)) {
            for (String r : recs) w.write(r + "\n");
        }
        // 改造前的写法：裸 BufferedWriter
        try (java.io.BufferedWriter w = new java.io.BufferedWriter(new FileWriter(b.toFile()))) {
            for (String r : recs) w.write(r + "\n");
        }
        assertArrayEquals(Files.readAllBytes(b), Files.readAllBytes(a),
                "未开加密时新写入器必须与旧写法产出完全相同的字节");
    }

    @Test
    @DisplayName("capture 加密开关缺省继承 thl.encryption.enabled")
    void inheritsThlSwitch() {
        Properties p = new Properties();
        p.setProperty("thl.encryption.enabled", "true");
        p.setProperty("thl.encryption.password", "shared-secret");
        assertTrue(new CapLineCipher(p).isEnabled(),
                "\"落盘加密\"对使用者是一个概念，不该出现 THL 加了、.cap 没加");
    }

    @Test
    @DisplayName("一次写入多条记录也逐条加密")
    void handlesMultiRecordWrite() throws IOException {
        CapLineCipher c = new CapLineCipher(enabled());
        Path f = tmp.resolve("e.cap");
        try (CapFileWriter w = new CapFileWriter(new FileWriter(f.toFile()), c)) {
            w.write(record("A", "1") + "\n" + record("B", "2") + "\n");
        }
        List<String> lines = Files.readAllLines(f);
        assertEquals(2, lines.size());
        assertEquals(record("A", "1"), c.decryptLine(lines.get(0)));
        assertEquals(record("B", "2"), c.decryptLine(lines.get(1)));
    }
}
