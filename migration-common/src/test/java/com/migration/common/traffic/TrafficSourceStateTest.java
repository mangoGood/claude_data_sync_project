package com.migration.common.traffic;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("流量复制：源库开关状态的落盘与读回")
class TrafficSourceStateTest {

    private static TrafficSourceState sample() {
        TrafficSourceState s = new TrafficSourceState();
        s.host = "10.0.0.7";
        s.port = "3306";
        s.username = "dts";
        s.password = "S3cr3t#pw";
        s.urlParams = "useSSL=true&requireSSL=true";
        s.generalLog = "0";
        s.logOutput = "FILE";
        return s;
    }

    @Test
    @DisplayName("往返一致：原始开关值必须原样读回，否则还原会把源库设成错的档位")
    void roundTrip(@TempDir Path tmp) throws IOException {
        File dir = tmp.toFile();
        sample().save(dir);

        TrafficSourceState back = TrafficSourceState.load(dir);
        assertNotNull(back);
        assertEquals("10.0.0.7", back.host);
        assertEquals("3306", back.port);
        assertEquals("dts", back.username);
        assertEquals("S3cr3t#pw", back.password);
        assertEquals("useSSL=true&requireSSL=true", back.urlParams);
        assertEquals("0", back.generalLog);
        assertEquals("FILE", back.logOutput);
    }

    @Test
    @DisplayName("口令不得明文落盘 —— 这份文件是给 agent 兜底用的，会长期留在磁盘上")
    void passwordEncryptedAtRest(@TempDir Path tmp) throws IOException {
        File dir = tmp.toFile();
        sample().save(dir);
        String raw = new String(Files.readAllBytes(
                TrafficSourceState.fileIn(dir).toPath()), StandardCharsets.UTF_8);
        assertFalse(raw.contains("S3cr3t#pw"), "落盘内容里不能出现明文口令");
        assertTrue(raw.contains("ENC"), "口令应为 ENC/ENC2 密文");
    }

    @Test
    @DisplayName("文件不存在 = 没有待还原的状态")
    void missingFile(@TempDir Path tmp) {
        assertNull(TrafficSourceState.load(tmp.toFile()));
    }

    @Test
    @DisplayName("缺连接信息的残缺文件不能被当成可用状态（否则还原会连到空地址）")
    void incompleteFile(@TempDir Path tmp) throws IOException {
        File f = TrafficSourceState.fileIn(tmp.toFile());
        Files.write(f.toPath(), "original.general_log=0\n".getBytes(StandardCharsets.UTF_8));
        assertNull(TrafficSourceState.load(tmp.toFile()));
    }

    @Test
    @DisplayName("clear 之后不再有待还原状态")
    void clear(@TempDir Path tmp) throws IOException {
        File dir = tmp.toFile();
        sample().save(dir);
        assertNotNull(TrafficSourceState.load(dir));
        TrafficSourceState.clear(dir);
        assertNull(TrafficSourceState.load(dir));
    }
}
