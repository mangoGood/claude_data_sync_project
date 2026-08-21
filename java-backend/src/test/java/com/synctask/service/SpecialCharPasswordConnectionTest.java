package com.synctask.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 口令含特殊字符时连接串仍要解得开。
 *
 * <p>起因：口令 {@code ^XVCGjd=Ax@1u#+X7fmLQuNM} 的 MySQL 实例测连必失败，报的却是
 * "连接串格式不正确"。真正的原因是几份连接串正则都把口令写成 {@code [^@]*}——
 * 它在<b>第一个</b> {@code @} 就停下，剩下的 {@code 1u#+X7fmLQuNM@127.0.0.1} 落到主机位，
 * 撞上主机字符集后整条正则匹配失败。口令里出现 {@code @} 是完全合法的，
 * 随机生成的口令里更是常见。
 *
 * <p>本仓库有<b>四份</b>连接串解析（后端三份 + agent 一份，见
 * {@code ConnectionStringParser}）。这里守住后端那三份 + 两处遮蔽函数；
 * 少改一处的表现是"测连绿了、任务起不来"，所以四份必须同步。
 *
 * <p>口径统一为：口令组贪婪 {@code (.*)} + 主机字符集不含 {@code @}
 * ⇒ 按<b>最后一个</b> {@code @} 切 userinfo，即 URI 的通行做法。
 */
@DisplayName("口令含特殊字符的连接串解析")
class SpecialCharPasswordConnectionTest {

    /** 用户报的那个口令，原样进测试：^ = @ # + 一次占全。 */
    private static final String REPORTED_PW = "^XVCGjd=Ax@1u#+X7fmLQuNM";

    private final MetadataService metadata = new MetadataService();
    private final DataValidationService validation = new DataValidationService();

    // ============================================================ MetadataService（测连走的那份）

    @Test
    @DisplayName("MySQL：口令 ^XVCGjd=Ax@1u#+X7fmLQuNM 能解出 host/port/db 与完整口令")
    void mysqlWithReportedPassword() {
        MetadataService.ParsedConnection c =
                metadata.parseConnection("mysql://root:" + REPORTED_PW + "@127.0.0.1:33306/sync_task_db");
        assertEquals("root", c.username);
        assertEquals(REPORTED_PW, c.password, "口令必须原样解出，不能在第一个 @ 处被截断");
        assertEquals("127.0.0.1", c.host);
        assertEquals(33306, c.port);
        assertEquals("sync_task_db", c.database);
    }

    @Test
    @DisplayName("其余五种引擎前缀同样要能解出含 @ 的口令")
    void allSchemesWithReportedPassword() {
        record Case(String scheme, String hostPort, String host, int port) {
        }
        Case[] cases = {
                new Case("postgresql", "10.0.0.9:5432", "10.0.0.9", 5432),
                new Case("oracle", "127.0.0.1:1521", "127.0.0.1", 1521),
                new Case("mongodb", "127.0.0.1:27117", "127.0.0.1", 27117),
                new Case("elastic", "127.0.0.1:9200", "127.0.0.1", 9200),
                new Case("redis", "127.0.0.1:6390", "127.0.0.1", 6390),
        };
        for (Case k : cases) {
            MetadataService.ParsedConnection c = metadata.parseConnection(
                    k.scheme() + "://admin:" + REPORTED_PW + "@" + k.hostPort());
            assertEquals(REPORTED_PW, c.password, k.scheme() + " 的口令被截断了");
            assertEquals(k.host(), c.host, k.scheme() + " 的主机解错了");
            assertEquals(k.port(), c.port, k.scheme() + " 的端口解错了");
        }
    }

    @Test
    @DisplayName("口令里多个 @：按最后一个 @ 切，前面的都归口令")
    void multipleAtSigns() {
        MetadataService.ParsedConnection c =
                metadata.parseConnection("mysql://u:a@b@c@d@127.0.0.1:3306/db");
        assertEquals("a@b@c@d", c.password);
        assertEquals("127.0.0.1", c.host);
        assertEquals(3306, c.port);
    }

    @Test
    @DisplayName("其它特殊字符（: / ? & # % 空格 引号 反斜杠）都不该影响解析")
    void otherSpecialCharacters() {
        for (String pw : new String[]{
                "p:a:s:s", "a/b/c", "q?x=1&y=2", "with space", "back\\slash",
                "quo\"te", "sin'gle", "per%cent", "hash#tag", "tilde~caret^",
                "semi;colon", "pipe|and&amp", "<angle>", "{brace}[bracket]",
                "$dollar", "!bang*star(paren)", "eq=als+plus", "全角中文口令"}) {
            MetadataService.ParsedConnection c =
                    metadata.parseConnection("mysql://u:" + pw + "@db.example.com:3306/appdb");
            assertEquals(pw, c.password, "口令 [" + pw + "] 解错了");
            assertEquals("db.example.com", c.host, "口令 [" + pw + "] 把主机带偏了");
            assertEquals("appdb", c.database, "口令 [" + pw + "] 把库名带偏了");
        }
    }

    @Test
    @DisplayName("空口令仍然可用（默认安装的 TiDB root 就是空口令）")
    void emptyPasswordStillWorks() {
        MetadataService.ParsedConnection c = metadata.parseConnection("mysql://root:@127.0.0.1:14000/test");
        assertEquals("", c.password);
        assertEquals("127.0.0.1", c.host);
    }

    @Test
    @DisplayName("放宽口令组没有放宽注入面：driver 参数注入依然被拒")
    void injectionStillRejected() {
        // 口令组放宽到 (.*) 之后，host/port/database 三组的闸门必须还在。
        // 这几条 payload 与 ConnectionStringInjectionTest 同源，在这里再守一遍，
        // 是因为"为了修口令而顺手把某一组也放开"是最容易发生的回归。
        assertThrows(IllegalArgumentException.class, () -> metadata.parseConnection(
                "mysql://u:p@10.0.0.9:3306/db?allowLoadLocalInfile=true"));
        assertThrows(IllegalArgumentException.class, () -> metadata.parseConnection(
                "postgresql://u:p@10.0.0.9:5432/db?socketFactory=org.springframework.context"
                        + ".support.ClassPathXmlApplicationContext"));
        assertThrows(IllegalArgumentException.class, () -> metadata.parseConnection(
                "mysql://u:p@10.0.0.9:3306/db?autoDeserialize=true"));
        // 口令里塞查询串也不行——它没法越过 @ 影响 host/db
        assertThrows(IllegalArgumentException.class, () -> metadata.parseConnection(
                "mysql://u:p?allowLoadLocalInfile=true@10.0.0.9:3306/db?autoDeserialize=true"));
        // 端口仍限 5 位
        assertThrows(IllegalArgumentException.class, () -> metadata.parseConnection(
                "mysql://u:" + REPORTED_PW + "@10.0.0.9:999999/db"));
    }

    @Test
    @DisplayName("口令含 @ 时不能被 host 位吃掉：解出的 host 必须是真实主机")
    void hostIsNotSwallowedByPassword() {
        MetadataService.ParsedConnection c =
                metadata.parseConnection("mysql://u:" + REPORTED_PW + "@db-01.internal.example.com:3306");
        assertEquals("db-01.internal.example.com", c.host);
        assertNull(c.database, "没写库名时 database 应为 null");
    }

    // ============================================================ DataValidationService（行数校验那份）

    @Test
    @DisplayName("行数校验的连接串解析同样吃得下含 @ 的口令")
    void dataValidationParsesSpecialCharPassword() {
        String[] mysql = validation.parseConnectionUrl(
                "mysql://root:" + REPORTED_PW + "@127.0.0.1:33306/appdb");
        assertEquals("root", mysql[1]);
        assertEquals(REPORTED_PW, mysql[2]);
        assertEquals("mysql", mysql[3]);
        assertTrue(mysql[0].startsWith("jdbc:mysql://127.0.0.1:33306/appdb"), "实际: " + mysql[0]);

        String[] pg = validation.parseConnectionUrl(
                "postgresql://pg:" + REPORTED_PW + "@10.0.0.9:5432/appdb");
        assertEquals(REPORTED_PW, pg[2]);
        assertEquals("postgresql", pg[3]);

        String[] ora = validation.parseConnectionUrl(
                "oracle://sys:" + REPORTED_PW + "@127.0.0.1:1521/ORCLPDB1");
        assertEquals(REPORTED_PW, ora[2]);
        assertEquals("oracle", ora[3]);
        assertEquals("jdbc:oracle:thin:@127.0.0.1:1521/ORCLPDB1", ora[0]);
    }

    @Test
    @DisplayName("行数校验：空口令不再被判成格式错")
    void dataValidationAcceptsEmptyPassword() {
        String[] parsed = validation.parseConnectionUrl("mysql://root:@127.0.0.1:14000/test");
        assertEquals("", parsed[2]);
    }

    // ============================================================ TrafficPrecheckService（流量复制预检那份）

    @Test
    @DisplayName("流量复制预检的连接串解析同样吃得下含 @ 的口令")
    void trafficPrecheckParsesSpecialCharPassword() throws Exception {
        TrafficPrecheckService.Parsed p = TrafficPrecheckService.parse(
                "mysql://root:" + REPORTED_PW + "@127.0.0.1:33306/appdb");
        assertEquals("mysql", p.scheme());
        assertEquals("root", p.username());
        assertEquals(REPORTED_PW, p.password());
        assertEquals("127.0.0.1", p.host());
        assertEquals("33306", p.port());
        assertEquals("appdb", p.database());

        TrafficPrecheckService.Parsed pg = TrafficPrecheckService.parse(
                "postgresql://pg:" + REPORTED_PW + "@10.0.0.9:5432/appdb");
        assertEquals(REPORTED_PW, pg.password());
        assertEquals("10.0.0.9", pg.host());
    }

    // ============================================================ 遮蔽：口令一个字符都不能漏进日志

    @Test
    @DisplayName("预检报错里的遮蔽必须盖住整个口令——含 @ 的口令旧写法会漏后半段")
    void maskingCoversWholePassword() {
        String conn = "mysql://root:" + REPORTED_PW + "@127.0.0.1:33306/db";
        String masked = TrafficPrecheckService.maskPassword(conn);
        assertEquals("mysql://root:***@127.0.0.1:33306/db", masked);
        assertFalse(masked.contains("XVCGjd"), "口令前半段漏进报错了: " + masked);
        assertFalse(masked.contains("1u#+X7fmLQuNM"), "口令后半段漏进报错了: " + masked);

        assertEquals("postgresql://u:***@h:5432/db",
                TrafficPrecheckService.maskPassword("postgresql://u:p@ss@h:5432/db"));
        assertEquals("mysql://root:***@h:3306/db",
                TrafficPrecheckService.maskPassword("mysql://root:plain@h:3306/db"));
        assertNull(TrafficPrecheckService.maskPassword(null));
        // 解析失败的残缺串（没有 @）也要盖住口令——这个遮蔽函数正是给报错路径用的
        assertEquals("mysql://root:***", TrafficPrecheckService.maskPassword("mysql://root:secret"));
        assertEquals("mysql://root@h:3306", TrafficPrecheckService.maskPassword("mysql://root@h:3306"));
    }

    @Test
    @DisplayName("流量预检：解析失败时抛出的报错里不能带口令")
    void trafficPrecheckErrorMessageCarriesNoPassword() {
        java.sql.SQLException e = assertThrows(java.sql.SQLException.class,
                () -> TrafficPrecheckService.parse("mysql://root:" + REPORTED_PW + "@127.0.0.1"));
        assertFalse(e.getMessage().contains("XVCGjd"), "报错里带了口令: " + e.getMessage());
        assertFalse(e.getMessage().contains("1u#+X7fmLQuNM"), "报错里带了口令: " + e.getMessage());
    }
}
