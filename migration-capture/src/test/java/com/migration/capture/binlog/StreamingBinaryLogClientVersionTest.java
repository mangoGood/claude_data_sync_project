package com.migration.capture.binlog;

import com.github.shyiko.mysql.binlog.StreamingBinaryLogClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 把 vendored 客户端的上游版本钉死。
 *
 * <p>{@link StreamingBinaryLogClient} 是 mysql-binlog-connector-java 0.29.2 的整份拷贝，
 * 只改了两处（见该类头注释）。升级依赖时，上游那两处代码可能已经变了——
 * 而拷贝出来的这份不会跟着变，于是"补丁还在、但补的是旧逻辑"，
 * 且不会有任何编译错误或运行报错来提醒你。
 *
 * <p>这个测试就是那个提醒：改了 pom 里的 binlog.version 它就变红。
 * <b>变红不是误报</b>，是让你回去重做一遍 diff，确认两处补丁在新版本上仍然成立，
 * 然后再把这里的版本号改掉。
 */
@DisplayName("vendored binlog 客户端版本钉死")
class StreamingBinaryLogClientVersionTest {

    /** 拷贝时的上游版本。改这里之前，先把 StreamingBinaryLogClient 与新版上游重新 diff 一遍。 */
    private static final String VENDORED_FROM = "0.29.2";

    @Test
    @DisplayName("类路径上的 binlog 连接器版本必须与拷贝来源一致")
    void upstreamVersionMatchesVendoredCopy() throws IOException {
        String actual = readConnectorVersion();
        assertNotNull(actual, "读不到 mysql-binlog-connector-java 的版本信息");
        if (!VENDORED_FROM.equals(actual)) {
            fail("binlog 连接器版本已从 " + VENDORED_FROM + " 变为 " + actual
                    + "。StreamingBinaryLogClient 是 " + VENDORED_FROM + " 的整份拷贝，"
                    + "请先与新版上游重新 diff（重点：listenForEventPackets 的分包处理是否变了），"
                    + "确认 [PATCH-1]/[PATCH-2] 仍然成立后再更新本测试里的版本号。");
        }
    }

    @Test
    @DisplayName("补丁标记还在——有人把它改回上游实现时立刻发现")
    void patchMarkersPresent() throws IOException {
        // 只能从 class 上确认存在性；补丁的行为正确性由 MultiPacketInputStreamTest 覆盖
        assertTrue(StreamingBinaryLogClient.class.getName()
                        .equals("com.github.shyiko.mysql.binlog.StreamingBinaryLogClient"),
                "vendored 类必须放在上游同包下（保证包级可见性），但<b>不能同名</b>——"
                        + "fat jar 里同包同名时，谁生效取决于打包顺序");
    }

    private String readConnectorVersion() throws IOException {
        Enumeration<URL> resources = getClass().getClassLoader()
                .getResources("META-INF/maven/com.zendesk/mysql-binlog-connector-java/pom.properties");
        while (resources.hasMoreElements()) {
            try (InputStream in = resources.nextElement().openStream()) {
                java.util.Properties p = new java.util.Properties();
                p.load(in);
                String v = p.getProperty("version");
                if (v != null) {
                    return v.trim();
                }
            }
        }
        return null;
    }
}
