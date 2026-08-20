package com.synctask.service;

import com.synctask.entity.DataClassification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Schema 审批的风险标注判据。
 *
 * <p>审批人真正要看的不是一句 SQL，而是"这条 DDL 会波及哪些列、里面有没有高敏的"。
 * 那两样分别来自血缘与分级——这也是设计文档把审批排在它们之后的原因。
 */
class SchemaApprovalRiskTest {

    @Test
    @DisplayName("从 ALTER TABLE 里认出被动到的列")
    void extractsColumnsFromAlter() {
        assertEquals(Set.of("phone"),
                SchemaApprovalService.columnsInDdl("ALTER TABLE users ADD COLUMN phone VARCHAR(32)"));
        assertEquals(Set.of("email"),
                SchemaApprovalService.columnsInDdl("ALTER TABLE `users` DROP COLUMN `email`"));
        assertEquals(Set.of("name"),
                SchemaApprovalService.columnsInDdl("ALTER TABLE users MODIFY name VARCHAR(128)"));
    }

    @Test
    @DisplayName("一条 DDL 动多列时全部认出来")
    void extractsMultipleColumns() {
        Set<String> cols = SchemaApprovalService.columnsInDdl(
                "ALTER TABLE users ADD COLUMN a INT, ADD COLUMN b VARCHAR(10), DROP COLUMN c");
        assertTrue(cols.containsAll(Set.of("a", "b", "c")), "实际认出: " + cols);
    }

    @Test
    @DisplayName("TABLE / COLUMN 关键字不能被当成列名")
    void doesNotMistakeKeywordsForColumns() {
        Set<String> cols = SchemaApprovalService.columnsInDdl("ALTER TABLE users ADD COLUMN phone INT");
        assertFalse(cols.contains("TABLE"));
        assertFalse(cols.contains("COLUMN"));
        assertFalse(cols.contains("users"), "表名不是列名");
    }

    @Test
    @DisplayName("认不出列时返回空集而不是抛——标注失败不该阻断建单")
    void toleratesUnparseable() {
        assertTrue(SchemaApprovalService.columnsInDdl(null).isEmpty());
        assertTrue(SchemaApprovalService.columnsInDdl("").isEmpty());
        assertTrue(SchemaApprovalService.columnsInDdl("CREATE INDEX idx ON t(a)").isEmpty());
    }

    @Test
    @DisplayName("级别可比：atLeast 的序决定\"不得降级\"这条策略")
    void levelOrdering() {
        assertTrue(DataClassification.Level.RESTRICTED.atLeast(DataClassification.Level.SENSITIVE));
        assertTrue(DataClassification.Level.SENSITIVE.atLeast(DataClassification.Level.INTERNAL));
        assertTrue(DataClassification.Level.INTERNAL.atLeast(DataClassification.Level.PUBLIC));
        assertTrue(DataClassification.Level.PUBLIC.atLeast(DataClassification.Level.PUBLIC));

        assertFalse(DataClassification.Level.PUBLIC.atLeast(DataClassification.Level.INTERNAL));
        assertFalse(DataClassification.Level.SENSITIVE.atLeast(DataClassification.Level.RESTRICTED));
    }

    @Test
    @DisplayName("级别的声明顺序不能乱——策略校验靠 ordinal，改顺序会静默反转结论")
    void levelDeclarationOrderIsLoadBearing() {
        DataClassification.Level[] v = DataClassification.Level.values();
        assertEquals(DataClassification.Level.PUBLIC, v[0]);
        assertEquals(DataClassification.Level.INTERNAL, v[1]);
        assertEquals(DataClassification.Level.SENSITIVE, v[2]);
        assertEquals(DataClassification.Level.RESTRICTED, v[3]);
    }
}
