package com.migration.common.clock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 源库时钟偏移的折算。
 *
 * <p>增量延迟是 {@code 本机 now − 事件的源端时间戳}，两台机器的时钟差会整体加到这个差值上：
 * 时钟差一分钟，延迟就凭空多一分钟；差成负的那一侧还会被下游当成非法值丢掉，
 * 指标从此停在旧值不动。
 */
@DisplayName("源库时钟偏移折算")
class SourceClockOffsetTest {

    @Test
    @DisplayName("没测到偏移时原样返回（等价于假设两端同步）")
    void passthroughBeforeMeasurement() {
        SourceClockOffset c = new SourceClockOffset();
        assertFalse(c.isMeasured());
        assertEquals(1000L, c.toLocal(1000L));
        assertEquals(0L, c.offsetMs());
    }

    @Test
    @DisplayName("源库时钟快 10 秒：源端时间戳折算后要减掉 10 秒")
    void sourceClockAhead() {
        SourceClockOffset c = new SourceClockOffset();
        // 本机 1000..1000（零 RTT），源库报 11000 ⇒ 偏移 +10000
        c.observe(11_000L, 1_000L, 1_000L);
        assertEquals(10_000L, c.offsetMs());
        // 源库在它自己的 11000 时刻记录了一个事件，对应本机时刻 1000
        assertEquals(1_000L, c.toLocal(11_000L));
    }

    @Test
    @DisplayName("源库时钟慢 10 秒：折算后要加回 10 秒")
    void sourceClockBehind() {
        SourceClockOffset c = new SourceClockOffset();
        c.observe(1_000L, 11_000L, 11_000L);
        assertEquals(-10_000L, c.offsetMs());
        assertEquals(11_000L, c.toLocal(1_000L));
    }

    @Test
    @DisplayName("取往返中点，抵掉一半 RTT")
    void usesRoundTripMidpoint() {
        SourceClockOffset c = new SourceClockOffset();
        // 查询在本机 1000 发出、1100 返回；源库回答"我这里是 1050"⇒ 两端其实是同步的
        c.observe(1_050L, 1_000L, 1_100L);
        assertEquals(0L, c.offsetMs(),
                "只取返回时刻会把整个 RTT 算成偏移（这里会误判成 -50）");
    }

    @Test
    @DisplayName("偏移过大要能被识别出来提醒运维")
    void suspiciousWhenSkewIsLarge() {
        SourceClockOffset c = new SourceClockOffset();
        c.observe(60_000L, 0L, 0L);
        assertTrue(c.isSuspicious(5_000L));
        assertFalse(c.isSuspicious(120_000L));
    }

    @Test
    @DisplayName("没测量过就不该报可疑（避免启动瞬间刷告警）")
    void notSuspiciousBeforeMeasurement() {
        assertFalse(new SourceClockOffset().isSuspicious(1L));
    }

    @Test
    @DisplayName("重新测量会覆盖旧偏移（时钟会被 NTP 调整）")
    void remeasurementOverrides() {
        SourceClockOffset c = new SourceClockOffset();
        c.observe(11_000L, 1_000L, 1_000L);
        assertEquals(10_000L, c.offsetMs());
        c.observe(1_000L, 1_000L, 1_000L);
        assertEquals(0L, c.offsetMs());
    }
}
