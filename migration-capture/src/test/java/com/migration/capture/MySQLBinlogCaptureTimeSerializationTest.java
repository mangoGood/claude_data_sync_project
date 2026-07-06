package com.migration.capture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link MySQLBinlogCapture} TIME 值序列化回归测试（#1）。
 *
 * <p>MySQL TIME 是时长语义，范围 ±838:59:59。历史 bug：用
 * {@code SimpleDateFormat("HH:mm:ss")} 格式化 {@code java.sql.Time}，
 * 小时对 24 取模（800:00:01 变成 08:00:01）且丢失符号。
 * 修复后直接从毫秒数重建 HH:MM:SS。通过反射调用私有 serializeValue（仓库既有测试模式）。
 */
@DisplayName("MySQLBinlogCapture TIME 序列化回归")
class MySQLBinlogCaptureTimeSerializationTest {

    private MySQLBinlogCapture capture;
    private Method serializeValue;

    @BeforeEach
    void setUp() throws Exception {
        capture = new MySQLBinlogCapture();
        serializeValue = MySQLBinlogCapture.class.getDeclaredMethod("serializeValue", Object.class);
        serializeValue.setAccessible(true);
    }

    private String serialize(Object v) throws Exception {
        return (String) serializeValue.invoke(capture, v);
    }

    @Test
    @DisplayName("#1 回归：超 24 小时的 TIME 不得取模（800:00:01 保持 800:00:01）")
    void extendedHoursPreserved() throws Exception {
        long ms = (800L * 3600 + 1) * 1000; // 800:00:01
        assertEquals("800:00:01", serialize(new java.sql.Time(ms)));
        long max = (838L * 3600 + 59 * 60 + 59) * 1000; // MySQL TIME 上限
        assertEquals("838:59:59", serialize(new java.sql.Time(max)));
    }

    @Test
    @DisplayName("负 TIME 保留符号（-100:00:00）")
    void negativeTimeKeepsSign() throws Exception {
        long ms = -(100L * 3600) * 1000;
        assertEquals("-100:00:00", serialize(new java.sql.Time(ms)));
    }

    @Test
    @DisplayName("普通范围 TIME 正常（12:30:45）")
    void normalRange() throws Exception {
        long ms = (12L * 3600 + 30 * 60 + 45) * 1000;
        assertEquals("12:30:45", serialize(new java.sql.Time(ms)));
    }

    @Test
    @DisplayName("NULL 与数字透传不受影响")
    void otherTypesUntouched() throws Exception {
        assertEquals("null", serialize(null));
        assertEquals("42", serialize(42));
    }
}
