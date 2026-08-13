package com.migration.extract;

import com.migration.thl.THLEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * TiCDC 链路用不了的记录必须停机，而不是记一行日志然后丢。
 *
 * <p>改造前：canal-json 解析失败是 {@code warn} 后 return null，未知事件类型是 {@code debug}
 * 后 return null —— 黑名单式放行。位点照常前进，这条记录承载的变更就此永久消失，任务全绿。
 * TiCDC 的消息类型随版本增加，漏一个就是一次静默丢数据。
 */
@DisplayName("TiCDC：用不了的记录停机而不是静默丢")
class TiCDCUnknownEventTest {

    private static final char FS = '\001';

    private TiCDCExtractor extractor;

    /**
     * 只装配置、不连库。{@code initialize()} 会去连源端 information_schema，
     * 单测一律不碰外部库（见 TiCDCExtractorTest 的同类做法）。
     */
    private static TiCDCExtractor withProps(Properties config) {
        return new TiCDCExtractor() {
            {
                // 参数不能也叫 props：匿名类里继承来的成员会遮蔽外层局部变量，
                // 写成 this.props = props 是拿字段给自己赋值（null），静悄悄什么也没发生
                this.props = config;
            }

            @Override
            protected java.util.List<String> getTableColumns(String database, String table) {
                return java.util.Arrays.asList("id", "name");
            }

            @Override
            protected java.util.List<String> getTableColumnTypes(String database, String table) {
                return java.util.Arrays.asList("int", "varchar");
            }

            @Override
            protected java.util.List<String> getTablePrimaryKeys(String database, String table) {
                return java.util.Collections.singletonList("id");
            }
        };
    }

    @BeforeEach
    void setUp() {
        extractor = withProps(new Properties());
    }

    private THLEvent extract(String line) throws Exception {
        return extractor.doExtract(line.getBytes(StandardCharsets.UTF_8));
    }

    private static String record(String eventType, String payload) {
        return eventType + FS + "tidb-binlog" + FS + "457146305087406081" + FS
                + "1700000000000" + FS + "65535" + FS + "42" + FS + payload;
    }

    @Test
    @DisplayName("未知事件类型：停机")
    void unknownEventTypeFailsStop() {
        assertThrows(MySQLBinlogExtractor.UnsupportedBinlogEventException.class,
                () -> extract(record("TICDC_SOMETHING_NEW", "{\"database\":\"d\",\"table\":\"t\"}")));
    }

    @Test
    @DisplayName("canal-json 解析失败：停机")
    void brokenJsonFailsStop() {
        assertThrows(MySQLBinlogExtractor.UnsupportedBinlogEventException.class,
                () -> extract(record("TICDC_INSERT", "{not-json")));
    }

    @Test
    @DisplayName("字段数不足的记录：停机")
    void truncatedRecordFailsStop() {
        assertThrows(MySQLBinlogExtractor.UnsupportedBinlogEventException.class,
                () -> extract("TICDC_INSERT" + FS + "tidb-binlog" + FS + "1"));
    }

    @Test
    @DisplayName("心跳仍然照常放行（不能把正常记录一起打停）")
    void heartbeatStillPasses() throws Exception {
        THLEvent e = extract(record("SYNC_HEARTBEAT", ""));
        assertNotNull(e);
        assertEquals("SYNC_HEARTBEAT", e.getMetadata().get("event_type"));
    }

    @Test
    @DisplayName("空行照常忽略")
    void blankLineIsIgnored() throws Exception {
        assertNull(extract("   "));
    }

    @Test
    @DisplayName("extract.unknown.event.policy=SKIP 时退回只告警")
    void skipPolicyDegradesToWarn() throws Exception {
        Properties config = new Properties();
        config.setProperty("extract.unknown.event.policy", "SKIP");
        TiCDCExtractor skipping = withProps(config);

        assertNull(skipping.doExtract(
                record("TICDC_SOMETHING_NEW", "{\"database\":\"d\"}").getBytes(StandardCharsets.UTF_8)));
    }
}
