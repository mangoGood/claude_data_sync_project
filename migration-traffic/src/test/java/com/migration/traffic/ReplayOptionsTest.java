package com.migration.traffic;

import com.migration.traffic.model.StatementClass;
import com.migration.traffic.replay.ReplayOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("回放档位")
class ReplayOptionsTest {

    @Test
    @DisplayName("会话状态类永远回放 —— 漏一条 SET autocommit=0，其后所有 DML 的事务语义就变了")
    void sessionStateAlwaysReplayed() {
        Properties p = new Properties();
        p.setProperty("traffic.replay.classes", "DML");   // 用户只勾了 DML
        ReplayOptions o = ReplayOptions.from(p);

        assertTrue(o.shouldExecute(StatementClass.SET), "SET 是会话状态，必须回放");
        assertTrue(o.shouldExecute(StatementClass.USE), "USE 是会话状态，必须回放");
        assertTrue(o.shouldExecute(StatementClass.TCL), "事务控制必须回放");
        assertTrue(o.shouldExecute(StatementClass.OTHER));

        assertTrue(o.shouldExecute(StatementClass.DML));
        assertFalse(o.shouldExecute(StatementClass.SELECT), "没勾 SELECT 就不该执行");
        assertFalse(o.shouldExecute(StatementClass.DDL));
    }

    @Test
    @DisplayName("DCL 有独立开关：即使勾了 DCL，allow.dcl 关着也不执行")
    void dclNeedsExplicitAllow() {
        Properties p = new Properties();
        p.setProperty("traffic.replay.classes", "DML,DCL");
        ReplayOptions off = ReplayOptions.from(p);
        assertFalse(off.shouldExecute(StatementClass.DCL), "DCL 默认必须关");

        p.setProperty("traffic.replay.allow.dcl", "true");
        ReplayOptions on = ReplayOptions.from(p);
        assertTrue(on.shouldExecute(StatementClass.DCL));
    }

    @Test
    @DisplayName("空类别 = 不限制负载类")
    void emptyClassesMeansAllPayload() {
        Properties p = new Properties();
        p.setProperty("traffic.replay.classes", "");
        ReplayOptions o = ReplayOptions.from(p);
        assertTrue(o.shouldExecute(StatementClass.SELECT));
        assertTrue(o.shouldExecute(StatementClass.DDL));
        assertFalse(o.shouldExecute(StatementClass.DCL), "DCL 仍需 allow.dcl");
    }

    @Test
    @DisplayName("默认值：1 倍速、WAIT、PRESERVE、不比对、危险语句拦截、不许同实例")
    void defaults() {
        ReplayOptions o = ReplayOptions.from(new Properties());
        assertEquals(1.0, o.speed, 1e-9);
        assertEquals(ReplayOptions.LagPolicy.WAIT, o.lagPolicy);
        assertEquals(ReplayOptions.GapPolicy.PRESERVE, o.gapPolicy);
        assertEquals(ReplayOptions.CompareMode.NONE, o.compare);
        assertFalse(o.allowDcl);
        assertFalse(o.allowDangerous);
        assertFalse(o.allowSameInstance);
        assertEquals(200, o.maxSessions);
    }

    @Test
    @DisplayName("非法/异常取值回落默认，不让任务因为一个笔误起不来")
    void badValuesFallBack() {
        Properties p = new Properties();
        p.setProperty("traffic.replay.speed", "0");
        p.setProperty("traffic.replay.lag.policy", "NONSENSE");
        p.setProperty("traffic.replay.max.sessions", "abc");
        ReplayOptions o = ReplayOptions.from(p);
        assertEquals(1.0, o.speed, 1e-9, "0 倍速无意义，回落 1.0");
        assertEquals(ReplayOptions.LagPolicy.WAIT, o.lagPolicy);
        assertEquals(200, o.maxSessions);
    }

    @Test
    @DisplayName("档位解析大小写不敏感")
    void enumParsingIsCaseInsensitive() {
        Properties p = new Properties();
        p.setProperty("traffic.replay.lag.policy", "skip");
        p.setProperty("traffic.replay.gap.policy", "compress");
        p.setProperty("traffic.replay.compare", "RowCount");
        ReplayOptions o = ReplayOptions.from(p);
        assertEquals(ReplayOptions.LagPolicy.SKIP, o.lagPolicy);
        assertEquals(ReplayOptions.GapPolicy.COMPRESS, o.gapPolicy);
        assertEquals(ReplayOptions.CompareMode.ROWCOUNT, o.compare);
    }
}
