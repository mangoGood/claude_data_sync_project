package com.synctask.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 脱敏配置校验判据。
 *
 * <p>这里的校验比其它列处理严——其它列处理"非法项跳过并告警"，而跳过一条脱敏规则
 * 等于把那一列的原值原样搬到目标端，用户看不见。所以必须建任务时就拦下。
 */
class ColumnMaskValidatorTest {

    private static String json(String rules) {
        return "{\"db1\":{\"tables\":[\"t1\"],\"columnMask\":{\"t1\":[" + rules + "]}}}";
    }

    @Test
    @DisplayName("合法配置通过，并列出被脱敏的列")
    void acceptsValid() {
        var r = ColumnMaskValidator.validate(json(
                "{\"column\":\"phone\",\"rule\":\"MASK_PARTIAL\",\"keepPrefix\":3,\"keepSuffix\":4},"
                        + "{\"column\":\"email\",\"rule\":\"HASH\"},"
                        + "{\"column\":\"name\",\"rule\":\"FAKE\",\"arg\":\"NAME\"}"), null);
        assertTrue(r.ok(), () -> String.join("；", r.errors));
        assertEquals(3, r.maskedColumns.size());
    }

    @Test
    @DisplayName("提示语必须说清\"脱敏列无法内容对比\"——这是产品语义的一部分")
    void warningStatesCompareLimitation() {
        var r = ColumnMaskValidator.validate(
                json("{\"column\":\"phone\",\"rule\":\"MASK_ALL\"}"), null);
        String w = r.warning();
        assertNotNull(w);
        assertTrue(w.contains("无法完成脱敏列内容对比"), "提示语实际为: " + w);
        assertTrue(w.contains("db1.t1.phone"), "要指明是哪些列: " + w);
        assertTrue(w.contains("行数对比"), "要说清哪些对比仍然可用: " + w);
    }

    @Test
    @DisplayName("没配脱敏时无提示")
    void noWarningWithoutMask() {
        assertNull(ColumnMaskValidator.validate("{\"db1\":{\"tables\":[\"t1\"]}}", null).warning());
        assertNull(ColumnMaskValidator.validate(null, null).warning());
    }

    @Test
    @DisplayName("非法规则名被拒")
    void rejectsUnknownRule() {
        var r = ColumnMaskValidator.validate(
                json("{\"column\":\"a\",\"rule\":\"ENCRYPT\"}"), null);
        assertFalse(r.ok());
        assertTrue(r.errors.get(0).contains("脱敏规则非法"));
    }

    @Test
    @DisplayName("非法列名被拒")
    void rejectsBadColumnName() {
        assertFalse(ColumnMaskValidator.validate(
                json("{\"column\":\"a; DROP TABLE x\",\"rule\":\"HASH\"}"), null).ok());
        assertFalse(ColumnMaskValidator.validate(
                json("{\"column\":\"\",\"rule\":\"HASH\"}"), null).ok());
    }

    @Test
    @DisplayName("同一列配多条规则被拒——后一条会覆盖前一条，可能从强规则退化成弱规则")
    void rejectsDuplicateRules() {
        var r = ColumnMaskValidator.validate(json(
                "{\"column\":\"a\",\"rule\":\"MASK_ALL\"},{\"column\":\"a\",\"rule\":\"HASH\"}"), null);
        assertFalse(r.ok());
        assertTrue(r.errors.get(0).contains("多条脱敏规则"));
    }

    @Test
    @DisplayName("主键不得脱敏")
    void rejectsMaskingPrimaryKey() {
        var pks = Map.of("db1", Map.of("t1", "id"));
        var r = ColumnMaskValidator.validate(json("{\"column\":\"id\",\"rule\":\"HASH\"}"), pks);
        assertFalse(r.ok());
        assertTrue(r.errors.get(0).contains("主键"), r.errors.get(0));
    }

    @Test
    @DisplayName("MASK_PARTIAL 首尾都不保留应改用 MASK_ALL")
    void rejectsPointlessPartial() {
        var r = ColumnMaskValidator.validate(json(
                "{\"column\":\"a\",\"rule\":\"MASK_PARTIAL\",\"keepPrefix\":0,\"keepSuffix\":0}"), null);
        assertFalse(r.ok());
        assertTrue(r.errors.get(0).contains("MASK_ALL"));
    }

    @Test
    @DisplayName("FAKE 必须指定合法类型")
    void rejectsFakeWithoutType() {
        assertFalse(ColumnMaskValidator.validate(
                json("{\"column\":\"a\",\"rule\":\"FAKE\"}"), null).ok());
        assertFalse(ColumnMaskValidator.validate(
                json("{\"column\":\"a\",\"rule\":\"FAKE\",\"arg\":\"WHATEVER\"}"), null).ok());
        assertTrue(ColumnMaskValidator.validate(
                json("{\"column\":\"a\",\"rule\":\"FAKE\",\"arg\":\"EMAIL\"}"), null).ok());
    }

    @Test
    @DisplayName("非法 JSON 与空配置不炸")
    void toleratesMalformed() {
        assertFalse(ColumnMaskValidator.validate("{not json", null).ok());
        assertTrue(ColumnMaskValidator.validate("", null).ok());
        assertTrue(ColumnMaskValidator.validate(null, null).ok());
    }
}
