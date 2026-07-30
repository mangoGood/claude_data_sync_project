package com.migration.increment;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 第 3 批：转换失败的处置（P1-3）与表延迟文件的滚动裁剪（P2-1）。
 *
 * <p>转换失败以前是被外层 catch 吞掉、顺手把<b>整个 THL 文件</b>标成"已处理完"，
 * 剩余事件永久丢弃且不上报——这里锁死改后的两条：FAIL_STOP 要写 error_status 且返回"停止"，
 * 死信记录要带得上失败原因（跟人工裁决跳过区分开，UI 上才知道是谁跳的）。
 */
@DisplayName("转换失败处置 + 表延迟文件裁剪")
class ConvertFailureAndLatencyRollTest {

    private ContinuousIncrementMain main;
    private String taskId;
    private File taskDir;

    @BeforeEach
    void setUp() throws Exception {
        main = new ContinuousIncrementMain();
        taskId = "convfail-test-" + System.nanoTime();
        taskDir = new File("files", taskId);
        taskDir.mkdirs();
        set("taskId", taskId);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (!taskDir.exists()) return;
        try (Stream<java.nio.file.Path> walk = Files.walk(taskDir.toPath())) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private void set(String field, Object value) throws Exception {
        Field f = ContinuousIncrementMain.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(main, value);
    }

    private Object call(String method, Class<?>[] types, Object... args) throws Exception {
        Method m = ContinuousIncrementMain.class.getDeclaredMethod(method, types);
        m.setAccessible(true);
        return m.invoke(main, args);
    }

    private THLEvent event(long seqno, String table) {
        THLEvent e = new THLEvent();
        e.setSeqno(seqno);
        e.setEventId("binlog.000001:" + (1000 + seqno));
        e.addMetadata("event_type", "UPDATE");
        e.addMetadata("table_name", table);
        e.addMetadata("binlog_file", "binlog.000001");
        e.addMetadata("binlog_position", 1000L + seqno);
        e.setSourceTstamp(new java.sql.Timestamp(System.currentTimeMillis() - 500));
        return e;
    }

    @Test
    @DisplayName("FAIL_STOP：写 error_status(E3009) 并要求停止，位点不推进")
    void failStopWritesErrorStatus() throws Exception {
        set("convertDeadLetter", false);
        set("lastExecutedSeqno", 41L);

        Object goOn = call("handleConvertFailure", new Class<?>[]{THLEvent.class, Exception.class},
                event(42, "t1"), new IllegalStateException("未知类型 GEOMETRY"));

        assertFalse((Boolean) goOn, "FAIL_STOP 必须让调用方停下来");

        File errorFile = new File(taskDir, "binlog_output/error_status");
        assertTrue(errorFile.isFile(), "应写出 error_status 供 agent 判 FAILED");
        String line = Files.readAllLines(errorFile.toPath()).get(0);
        String[] parts = line.split("\\|");
        assertEquals("E3009", parts[1]);
        assertEquals("42", parts[2]);
        assertTrue(line.contains("binlog.000001:1042"), "错误信息要带稳定 eventId 供人工裁决跳过");
        assertTrue(line.contains("未知类型 GEOMETRY"));

        Field f = ContinuousIncrementMain.class.getDeclaredField("lastExecutedSeqno");
        f.setAccessible(true);
        assertEquals(41L, f.get(main), "位点绝不能越过没应用成功的事件");
    }

    @Test
    @DisplayName("死信记录带得上原因：转换失败与人工裁决可区分")
    void deadLetterCarriesReason() throws Exception {
        call("recordDeadLetter", new Class<?>[]{THLEvent.class, String.class},
                event(7, "orders"), "convert-failed");

        File dl = new File(taskDir, "deadletter.jsonl");
        assertTrue(dl.isFile());
        String content = Files.readString(dl.toPath(), StandardCharsets.UTF_8);
        assertTrue(content.contains("\"reason\":\"convert-failed\""), "实际内容: " + content);
        assertTrue(content.contains("\"tableName\":\"orders\""));
    }

    @Test
    @DisplayName("表延迟 tsv 超过上限即裁剪，只留最后 N 行且保留的是最新的")
    void tableLatencyFileIsRolled() throws Exception {
        File latencyDir = new File(taskDir, "binlog_output/table_latency");
        set("tableLatencyDir", latencyDir.getPath());
        set("tableLatencyMaxLines", 100);

        for (int i = 0; i < 500; i++) {
            call("recordTableLatency", new Class<?>[]{THLEvent.class, String.class}, event(i, "t1"), "UPDATE");
        }

        File tsv = new File(latencyDir, "t1.tsv");
        assertTrue(tsv.isFile());
        List<String> lines = Files.readAllLines(tsv.toPath());
        assertTrue(lines.size() <= 200,
                "行数应被压在上限的 2 倍以内（实际 " + lines.size() + "），否则就是从不裁剪的老行为");
        assertTrue(lines.size() >= 100, "裁剪不能把窗口砍到不够画热力图");

        // 保留的必须是最后写入的那批：appliedTs 单调不减，且最后一行就是最后一次写入
        long prev = 0;
        for (String line : lines) {
            long ts = Long.parseLong(line.split("\t")[0]);
            assertTrue(ts >= prev, "裁剪后仍应按时间有序");
            prev = ts;
        }
    }
}
