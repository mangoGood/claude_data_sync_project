package com.migration.traffic;

import com.migration.traffic.capture.CaptureFilter;
import com.migration.traffic.model.StatementClass;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("捕获过滤")
class CaptureFilterTest {

    @Test
    @DisplayName("会话状态类语句永远不被过滤 —— 少一条 SET/USE，整个录制就不可回放了")
    void sessionStateNeverFiltered() {
        CaptureFilter f = new CaptureFilter(Set.of("only_this_db"), Set.of("only_this_user"),
                Set.of(StatementClass.SELECT), 1.0);
        // 库、账号、类别三条都不匹配，但会话状态类照样保留
        assertTrue(f.accept(StatementClass.SET, "other_db", "someone@host"));
        assertTrue(f.accept(StatementClass.USE, "other_db", "someone@host"));
        assertTrue(f.accept(StatementClass.TCL, null, null));
    }

    @Test
    @DisplayName("负载类按库/账号/类别筛")
    void payloadFiltered() {
        CaptureFilter f = new CaptureFilter(Set.of("app_db"), Set.of("app"),
                Set.of(StatementClass.SELECT, StatementClass.DML), 1.0);
        assertTrue(f.accept(StatementClass.SELECT, "app_db", "app@10.0.0.1"));
        assertFalse(f.accept(StatementClass.SELECT, "other_db", "app@10.0.0.1"), "库不匹配");
        assertFalse(f.accept(StatementClass.SELECT, "app_db", "guest@10.0.0.1"), "账号不匹配");
        assertFalse(f.accept(StatementClass.DDL, "app_db", "app@10.0.0.1"), "类别不匹配");
        assertFalse(f.accept(StatementClass.SELECT, null, "app@10.0.0.1"), "库未知即不匹配白名单");
    }

    @Test
    @DisplayName("空白名单 = 不限制")
    void emptyMeansNoRestriction() {
        CaptureFilter f = new CaptureFilter(Set.of(), Set.of(), Set.of(), 1.0);
        assertTrue(f.accept(StatementClass.DDL, null, null));
    }

    @Test
    @DisplayName("采样按会话恒定：同一 thread_id 每次结论必须一致，否则事务会被切碎")
    void samplingIsStablePerSession() {
        CaptureFilter f = new CaptureFilter(Set.of(), Set.of(), Set.of(), 0.5);
        for (long tid = 1; tid <= 200; tid++) {
            boolean first = f.sessionSampled(tid);
            for (int i = 0; i < 5; i++) {
                assertEquals(first, f.sessionSampled(tid), "同一会话的采样结论必须恒定");
            }
        }
    }

    @Test
    @DisplayName("采样率 1.0 全收；采样率生效时大致落在目标比例附近")
    void samplingRate() {
        CaptureFilter all = new CaptureFilter(Set.of(), Set.of(), Set.of(), 1.0);
        for (long tid = 1; tid <= 100; tid++) {
            assertTrue(all.sessionSampled(tid));
        }
        CaptureFilter half = new CaptureFilter(Set.of(), Set.of(), Set.of(), 0.5);
        int kept = 0;
        for (long tid = 1; tid <= 10_000; tid++) {
            if (half.sessionSampled(tid)) kept++;
        }
        assertTrue(kept > 4000 && kept < 6000, "10000 个会话按 50% 采样应落在 4000~6000，实际 " + kept);
    }

    @Test
    @DisplayName("从 properties 装配")
    void fromProperties() {
        Properties p = new Properties();
        p.setProperty("traffic.capture.databases", "DbA, dbb");
        p.setProperty("traffic.capture.classes", "SELECT,DDL");
        p.setProperty("traffic.capture.sample.rate", "0.25");
        CaptureFilter f = CaptureFilter.from(p);
        assertTrue(f.databases().contains("dba"), "库名比较应大小写不敏感");
        assertTrue(f.classes().contains(StatementClass.DDL));
        assertEquals(0.25, f.sampleRate(), 1e-9);
    }
}
