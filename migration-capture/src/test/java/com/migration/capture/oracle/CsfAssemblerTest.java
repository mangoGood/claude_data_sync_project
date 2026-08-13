package com.migration.capture.oracle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LogMiner 续行拼接。
 *
 * <p>{@code V$LOGMNR_CONTENTS.SQL_REDO} 单行最多 4000 字节，超了 Oracle 把一条语句拆成多行、
 * 除末行外 {@code CSF=1}。不拼接的后果是静默的：截断处之后的列在解析时根本不存在，
 * 于是按残缺列集写目标端；续行本身又会被当成独立事件落到"原样透传"的兜底分支上。
 */
@DisplayName("Oracle CSF 续行拼接")
class CsfAssemblerTest {

    @Test
    @DisplayName("CSF=0：一行就是一条完整语句，原样返回")
    void singleRowStatement() {
        CsfAssembler a = new CsfAssembler();
        assertEquals("insert into \"S\".\"T\"(\"A\") values ('x');",
                a.accept("insert into \"S\".\"T\"(\"A\") values ('x');", 0));
        assertFalse(a.hasPending());
    }

    @Test
    @DisplayName("CSF=1 的行先攒着，直到 CSF=0 才吐出拼好的整条")
    void continuationRowsAreJoined() {
        CsfAssembler a = new CsfAssembler();
        assertNull(a.accept("insert into \"S\".\"T\"(\"A\") values ('", 1));
        assertTrue(a.hasPending());
        assertNull(a.accept("aaaa", 1));
        assertEquals(2, a.pendingRows());
        assertEquals("insert into \"S\".\"T\"(\"A\") values ('aaaa');",
                a.accept("');", 0));
        assertFalse(a.hasPending());
    }

    @Test
    @DisplayName("拼完之后状态归零，下一条不受影响")
    void stateResetsBetweenStatements() {
        CsfAssembler a = new CsfAssembler();
        a.accept("part1", 1);
        assertEquals("part1part2", a.accept("part2", 0));
        assertEquals("next", a.accept("next", 0));
    }

    @Test
    @DisplayName("4000 字节边界：三段拼回原文，长度不丢")
    void reassemblesAcrossFourThousandByteChunks() {
        String head = "update \"S\".\"T\" set \"BODY\" = '";
        String body1 = repeat('x', 4000 - head.length());
        String body2 = repeat('y', 4000);
        String tail = "' where \"ID\" = 1;";

        CsfAssembler a = new CsfAssembler();
        assertNull(a.accept(head + body1, 1));
        assertNull(a.accept(body2, 1));
        String full = a.accept(tail, 0);

        assertEquals(head.length() + body1.length() + body2.length() + tail.length(), full.length());
        assertTrue(full.startsWith("update \"S\".\"T\" set \"BODY\" = 'xxx"));
        assertTrue(full.endsWith("' where \"ID\" = 1;"));
    }

    @Test
    @DisplayName("null 的 SQL_REDO 当空串处理，不产生 NPE 也不丢已攒的部分")
    void nullPieceIsTreatedAsEmpty() {
        CsfAssembler a = new CsfAssembler();
        assertNull(a.accept("head", 1));
        assertEquals("head", a.accept(null, 0));
    }

    @Test
    @DisplayName("reset 丢弃半条语句：批边界上的残句不能与下一批拼串")
    void resetDropsPartialStatement() {
        CsfAssembler a = new CsfAssembler();
        a.accept("half", 1);
        assertTrue(a.hasPending());
        a.reset();
        assertFalse(a.hasPending());
        assertEquals("brand-new", a.accept("brand-new", 0));
    }

    @Test
    @DisplayName("超过上限时停止累积，但仍在 CSF=0 处正常收口（不会一直挂着）")
    void oversizedStatementStillTerminates() {
        CsfAssembler a = new CsfAssembler(10);
        assertNull(a.accept("0123456789ABCDEF", 1));
        String out = a.accept("!", 0);
        assertEquals("!", out, "超限的部分被丢弃，但语句必须收口，否则后面所有行都会被吞进这条残句");
        assertFalse(a.hasPending());
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
