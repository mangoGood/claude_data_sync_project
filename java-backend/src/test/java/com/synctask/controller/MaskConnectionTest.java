package com.synctask.controller;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 元数据接口打日志前的连接串遮蔽。
 *
 * <p>旧写法 {@code replaceAll(":[^:@]+@", ":****@")} 只吃到<b>第一个</b> {@code @}：
 * 口令 {@code ^XVCGjd=Ax@1u#+X7fmLQuNM} 会被打成
 * {@code mysql://root:****@1u#+X7fmLQuNM@127.0.0.1:33306/db}——遮了个寂寞，
 * 口令后半段照样明文落进 backend.log。含 {@code @} 的口令并不罕见，
 * 所以这是实打实的凭据泄露，不是理论问题。
 *
 * <p>与 {@code SpecialCharPasswordConnectionTest} 是同一次修复的两半：
 * 那边守解析（口令要解得出来），这边守日志（口令不能漏出去）。
 */
@DisplayName("连接串日志遮蔽")
class MaskConnectionTest {

    private static final String REPORTED_PW = "^XVCGjd=Ax@1u#+X7fmLQuNM";

    @Test
    @DisplayName("含 @ 的口令必须整段被遮蔽，一个字符都不留")
    void masksWholePasswordContainingAtSign() {
        String masked = MetadataController.maskConnection(
                "mysql://root:" + REPORTED_PW + "@127.0.0.1:33306/db");
        assertEquals("mysql://root:****@127.0.0.1:33306/db", masked);
        assertFalse(masked.contains("XVCGjd"), "口令前半段漏进日志了: " + masked);
        assertFalse(masked.contains("1u#+X7fmLQuNM"), "口令后半段漏进日志了: " + masked);
    }

    @Test
    @DisplayName("普通口令与其余引擎前缀的遮蔽行为不变")
    void masksPlainPasswordsForAllSchemes() {
        assertEquals("mysql://root:****@h:3306/db",
                MetadataController.maskConnection("mysql://root:plain@h:3306/db"));
        assertEquals("postgresql://u:****@h:5432/db",
                MetadataController.maskConnection("postgresql://u:p@ss@h:5432/db"));
        assertEquals("oracle://sys:****@h:1521/ORCL",
                MetadataController.maskConnection("oracle://sys:pw@h:1521/ORCL"));
        assertEquals("mongodb://u:****@h:27017",
                MetadataController.maskConnection("mongodb://u:p@w@h:27017"));
        assertEquals("elastic://u:****@h:9200",
                MetadataController.maskConnection("elastic://u:p@h:9200"));
        assertEquals("redis://default:****@h:6379",
                MetadataController.maskConnection("redis://default:p@h:6379"));
    }

    @Test
    @DisplayName("空口令与 null 不炸")
    void handlesEmptyAndNull() {
        assertEquals("mysql://root:****@h:3306/db",
                MetadataController.maskConnection("mysql://root:@h:3306/db"));
        assertEquals("null", MetadataController.maskConnection(null));
    }

    @Test
    @DisplayName("连 @ 都没有的残缺串也要盖住口令段——这类输入最容易漏")
    void masksMalformedStringWithoutAtSign() {
        assertEquals("mysql://root:****", MetadataController.maskConnection("mysql://root:secret"));
        assertEquals("mysql://root:****", MetadataController.maskConnection("mysql://root:" + REPORTED_PW.replace("@", "")));
        // 压根没有口令段的串原样返回，不该被误伤
        assertEquals("mysql://root@h:3306", MetadataController.maskConnection("mysql://root@h:3306"));
    }
}
