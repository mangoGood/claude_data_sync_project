package com.migration.agent.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * agent 侧连接串解析：口令含特殊字符时必须解得开。
 *
 * <p>这是后端 {@code SpecialCharPasswordConnectionTest} 的对侧。两边各有一份正则
 * （后端刻意不依赖 migration-common），口径必须一致：
 * <b>口令组贪婪 {@code (.*)} + 主机字符集不含 {@code @}</b>，即按最后一个 {@code @}
 * 切 userinfo。只改一边的后果是"测连绿了、任务派下来起不来"——
 * 而 agent 这侧报的是 {@code Invalid connection string format}，看着像格式错、实为口令。
 */
@DisplayName("agent 连接串解析：口令含特殊字符")
class ConnectionStringSpecialCharPasswordTest {

    /** 用户报的那个口令，原样进测试。 */
    private static final String REPORTED_PW = "^XVCGjd=Ax@1u#+X7fmLQuNM";

    @Test
    @DisplayName("MySQL：口令 ^XVCGjd=Ax@1u#+X7fmLQuNM 完整解出")
    void mysqlWithReportedPassword() {
        ConnectionStringParser.ConnectionInfo info = ConnectionStringParser.parse(
                "mysql://root:" + REPORTED_PW + "@127.0.0.1:33306/myapp_db");
        assertEquals("root", info.getUsername());
        assertEquals(REPORTED_PW, info.getPassword(), "口令不能在第一个 @ 处被截断");
        assertEquals("127.0.0.1", info.getHost());
        assertEquals(33306, info.getPort());
        assertEquals("myapp_db", info.getDatabase());
        assertEquals("mysql", info.getType());
    }

    @Test
    @DisplayName("六种引擎前缀全部要能解出含 @ 的口令")
    void allSchemesWithReportedPassword() {
        record Case(String scheme, String host, int port, String type) {
        }
        Case[] cases = {
                new Case("mysql", "127.0.0.1", 3306, "mysql"),
                new Case("postgresql", "10.0.0.9", 5432, "postgresql"),
                new Case("oracle", "127.0.0.1", 1521, "oracle"),
                new Case("mongodb", "127.0.0.1", 27117, "mongodb"),
                new Case("elastic", "127.0.0.1", 9200, "elasticsearch"),
                new Case("redis", "127.0.0.1", 6390, "redis"),
        };
        for (Case k : cases) {
            ConnectionStringParser.ConnectionInfo info = ConnectionStringParser.parse(
                    k.scheme() + "://admin:" + REPORTED_PW + "@" + k.host() + ":" + k.port());
            assertEquals(REPORTED_PW, info.getPassword(), k.scheme() + " 的口令被截断了");
            assertEquals(k.host(), info.getHost(), k.scheme() + " 的主机解错了");
            assertEquals(k.port(), info.getPort(), k.scheme() + " 的端口解错了");
            assertEquals(k.type(), info.getType(), k.scheme() + " 的类型判错了");
        }
    }

    @Test
    @DisplayName("口令里多个 @：按最后一个 @ 切")
    void multipleAtSigns() {
        ConnectionStringParser.ConnectionInfo info =
                ConnectionStringParser.parse("mysql://u:a@b@c@d@127.0.0.1:3306/db");
        assertEquals("a@b@c@d", info.getPassword());
        assertEquals("127.0.0.1", info.getHost());
        assertEquals("db", info.getDatabase());
    }

    @Test
    @DisplayName("库名自身含 @ 时，主机与端口仍要落在正确的位置")
    void databaseContainingAtSign() {
        ConnectionStringParser.ConnectionInfo info =
                ConnectionStringParser.parse("mysql://u:p@ss@127.0.0.1:3306/db@x");
        assertEquals("p@ss", info.getPassword());
        assertEquals("127.0.0.1", info.getHost());
        assertEquals(3306, info.getPort());
        assertEquals("db@x", info.getDatabase());
    }

    @Test
    @DisplayName("其它特殊字符（: / ? & # % 空格 引号 反斜杠）都不该影响解析")
    void otherSpecialCharacters() {
        for (String pw : new String[]{
                "p:a:s:s", "a/b/c", "q?x=1&y=2", "with space", "back\\slash",
                "quo\"te", "sin'gle", "per%cent", "hash#tag", "tilde~caret^",
                "semi;colon", "pipe|and&amp", "<angle>", "{brace}[bracket]",
                "$dollar", "!bang*star(paren)", "eq=als+plus", "全角中文口令"}) {
            ConnectionStringParser.ConnectionInfo info = ConnectionStringParser.parse(
                    "mysql://u:" + pw + "@db.example.com:3306/appdb");
            assertEquals(pw, info.getPassword(), "口令 [" + pw + "] 解错了");
            assertEquals("db.example.com", info.getHost(), "口令 [" + pw + "] 把主机带偏了");
            assertEquals("appdb", info.getDatabase(), "口令 [" + pw + "] 把库名带偏了");
        }
    }

    @Test
    @DisplayName("空口令仍然可用（默认安装的 TiDB root），六种前缀都要放行")
    void emptyPasswordAllSchemes() {
        assertEquals("", ConnectionStringParser.parse("mysql://root:@127.0.0.1:14000/test").getPassword());
        assertEquals("", ConnectionStringParser.parse("postgresql://pg:@127.0.0.1:5432/db").getPassword());
        assertEquals("", ConnectionStringParser.parse("oracle://sys:@127.0.0.1:1521/ORCL").getPassword());
        assertEquals("", ConnectionStringParser.parse("mongodb://u:@127.0.0.1:27017").getPassword());
        assertEquals("", ConnectionStringParser.parse("elastic://u:@127.0.0.1:9200").getPassword());
        assertEquals("", ConnectionStringParser.parse("redis://default:@127.0.0.1:6379").getPassword());
    }

    @Test
    @DisplayName("toString 仍然遮蔽口令——含 @ 的口令一个字符都不能漏")
    void toStringMasksPassword() {
        String s = ConnectionStringParser.parse(
                "mysql://root:" + REPORTED_PW + "@127.0.0.1:3306/db").toString();
        assertFalse(s.contains("XVCGjd"), "口令前半段漏进日志了: " + s);
        assertFalse(s.contains("1u#+X7fmLQuNM"), "口令后半段漏进日志了: " + s);
        assertTrue(s.contains(":***@"), "实际: " + s);
    }

    @Test
    @DisplayName("确实非法的连接串仍然要抛——不能为了放行口令把校验放没了")
    void trulyMalformedStillRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringParser.parse("mysql://root@127.0.0.1:3306/db"));   // 缺 :pass
        assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringParser.parse("mysql://root:pw@127.0.0.1/db"));     // 缺端口
        assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringParser.parse("mysql://root:pw@127.0.0.1:abc/db")); // 端口非数字
        assertNull(ConnectionStringParser.parse("   "));
    }
}
