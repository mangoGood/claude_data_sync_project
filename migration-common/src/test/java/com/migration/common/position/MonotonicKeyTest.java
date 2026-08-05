package com.migration.common.position;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 位点单调标量的折算。
 *
 * <p>这个标量只用于"位点不许回退"这条守卫，所以真正要锁死的是<b>序关系</b>：
 * 同一条链路上后发生的位点必须折算出更大的数。折算不出来的形态必须老实返回
 * {@link MonotonicKey#UNKNOWN}，让守卫降级为不校验——拿一个瞎算的数去比，
 * 会把正常推进的位点永久拦死，比不校验危险得多。
 */
@DisplayName("位点单调标量折算")
class MonotonicKeyTest {

    @Test
    @DisplayName("binlog：跨文件与同文件内都保序")
    void binlogOrdering() {
        long a = MonotonicKey.ofBinlog("mysql-bin.000123", 4);
        long b = MonotonicKey.ofBinlog("mysql-bin.000123", 45678);
        long c = MonotonicKey.ofBinlog("mysql-bin.000124", 4);
        assertTrue(a < b, "同一文件内位点推进必须递增");
        assertTrue(b < c, "换下一个 binlog 文件必须比上一个文件的任何位点都大");
    }

    @Test
    @DisplayName("binlog：位点接近 4GB 也不会串到文件号那一段")
    void binlogPositionDoesNotOverflowIntoFileSeq() {
        long big = MonotonicKey.ofBinlog("mysql-bin.000001", 4_000_000_000L);
        long nextFile = MonotonicKey.ofBinlog("mysql-bin.000002", 4);
        assertTrue(big < nextFile, "位点再大也必须小于下一个文件的起始位点");
    }

    @Test
    @DisplayName("binlog：文件名认不出就返回 UNKNOWN，不瞎猜")
    void binlogUnparseableFile() {
        assertEquals(MonotonicKey.UNKNOWN, MonotonicKey.ofBinlog("binlog-without-suffix", 100));
        assertEquals(MonotonicKey.UNKNOWN, MonotonicKey.ofBinlog(null, 100));
        assertEquals(-1, MonotonicKey.binlogFileSeq("mysql-bin.abc"));
    }

    @Test
    @DisplayName("PostgreSQL LSN：十六进制两段式保序，段进位也保序")
    void lsnOrdering() {
        long a = MonotonicKey.ofLsn("0/16B3748");
        long b = MonotonicKey.ofLsn("0/16B3800");
        long c = MonotonicKey.ofLsn("1/0");
        assertTrue(a < b);
        assertTrue(b < c, "段号进位后必须比上一段的任何 offset 都大");
        assertEquals(MonotonicKey.UNKNOWN, MonotonicKey.ofLsn("not-an-lsn"));
        assertEquals(MonotonicKey.UNKNOWN, MonotonicKey.ofLsn(""));
    }

    @Test
    @DisplayName("Mongo clusterTime：秒相同时按 inc 排序")
    void clusterTimeOrdering() {
        long a = MonotonicKey.ofClusterTime(1754000000L, 1);
        long b = MonotonicKey.ofClusterTime(1754000000L, 2);
        long c = MonotonicKey.ofClusterTime(1754000001L, 1);
        assertTrue(a < b);
        assertTrue(b < c);
    }

    @Test
    @DisplayName("Kafka：同分区内按 offset 保序")
    void kafkaOrdering() {
        assertTrue(MonotonicKey.ofKafka(3, 100) < MonotonicKey.ofKafka(3, 101));
        assertEquals(MonotonicKey.UNKNOWN, MonotonicKey.ofKafka(-1, 1));
    }

    @Test
    @DisplayName("UNKNOWN 落库要变成 0：中心库那一列不存负数，0 的语义就是不校验单调")
    void unknownMapsToZeroColumn() {
        assertEquals(0L, MonotonicKey.toColumn(MonotonicKey.UNKNOWN));
        assertEquals(0L, MonotonicKey.toColumn(-999));
        assertEquals(42L, MonotonicKey.toColumn(42));
    }
}
