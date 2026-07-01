package com.migration.subscribe.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * 数据脱敏服务
 *
 * <p>对订阅到 Kafka 的 CDC 事件进行字段级脱敏，支持：
 * <ul>
 *   <li>手机号脱敏：保留前3后4，中间用 **** 替换（138****5678）</li>
 *   <li>身份证脱敏：保留前6后4，中间用 ******** 替换（110101********1234）</li>
 *   <li>邮箱脱敏：用户名保留首字符，其余用 *** 替换（a***@example.com）</li>
 *   <li>银行卡脱敏：保留前4后4，中间用 **** 替换</li>
 *   <li>自定义字段脱敏：通过配置指定字段名和脱敏规则</li>
 * </ul>
 *
 * <p>配置示例（properties 文件）：
 * <pre>
 * # 启用脱敏
 * data.masking.enabled=true
 * # 默认规则（按字段名自动匹配）
 * data.masking.auto.detect=true
 * # 自定义字段规则：field.{fieldName}={rule}
 * data.masking.field.phone=PHONE
 * data.masking.field.mobile=PHONE
 * data.masking.field.id_card=ID_CARD
 * data.masking.field.idcard=ID_CARD
 * data.masking.field.email=EMAIL
 * data.masking.field.bank_card=BANK_CARD
 * data.masking.field.password=FULL_MASK
 * </pre>
 */
public class DataMaskingService {
    private static final Logger logger = LoggerFactory.getLogger(DataMaskingService.class);

    public enum MaskRule {
        /** 手机号脱敏：138****5678 */
        PHONE,
        /** 身份证脱敏：110101********1234 */
        ID_CARD,
        /** 邮箱脱敏：a***@example.com */
        EMAIL,
        /** 银行卡脱敏：6222****1234 */
        BANK_CARD,
        /** 全部脱敏：****** */
        FULL_MASK,
        /** 保留前N后M字符 */
        PARTIAL_MASK,
        /** 哈希脱敏：SHA-256 */
        HASH
    }

    private final boolean enabled;
    private final boolean autoDetect;
    private final Map<String, MaskRule> fieldRules = new HashMap<>();
    private final Map<MaskRule, Pattern> autoDetectPatterns = new EnumMap<>(MaskRule.class);

    // 统计
    private long totalMasked = 0;
    private long totalFieldsScanned = 0;

    public DataMaskingService(Properties props) {
        this.enabled = Boolean.parseBoolean(props.getProperty("data.masking.enabled", "false"));
        this.autoDetect = Boolean.parseBoolean(props.getProperty("data.masking.auto.detect", "true"));

        // 加载自定义字段规则
        for (String propName : props.stringPropertyNames()) {
            if (propName.startsWith("data.masking.field.")) {
                String fieldName = propName.substring("data.masking.field.".length()).toLowerCase();
                String ruleName = props.getProperty(propName).toUpperCase();
                try {
                    MaskRule rule = MaskRule.valueOf(ruleName);
                    fieldRules.put(fieldName, rule);
                } catch (IllegalArgumentException e) {
                    logger.warn("未知脱敏规则: {} -> {}", fieldName, ruleName);
                }
            }
        }

        // 自动检测正则
        autoDetectPatterns.put(MaskRule.PHONE, Pattern.compile("^1[3-9]\\d{9}$"));
        autoDetectPatterns.put(MaskRule.ID_CARD, Pattern.compile("^\\d{17}[\\dXx]$"));
        autoDetectPatterns.put(MaskRule.EMAIL, Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"));
        autoDetectPatterns.put(MaskRule.BANK_CARD, Pattern.compile("^\\d{16,19}$"));

        logger.info("DataMaskingService 初始化 | enabled={} | autoDetect={} | fieldRules={}",
                enabled, autoDetect, fieldRules.size());
    }

    /**
     * 对 CDC 事件数据 Map 进行脱敏处理（原地修改）
     *
     * @param dataMap 数据 Map（before/after 字段值）
     * @return 脱敏后的 Map（同一引用）
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> mask(Map<String, Object> dataMap) {
        if (!enabled || dataMap == null || dataMap.isEmpty()) {
            return dataMap;
        }

        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            totalFieldsScanned++;
            String fieldName = entry.getKey().toLowerCase();
            Object value = entry.getValue();

            if (value == null) continue;
            if (!(value instanceof String)) continue;

            String strValue = (String) value;
            if (strValue.isEmpty()) continue;

            // 1. 按字段名规则脱敏
            MaskRule rule = fieldRules.get(fieldName);
            if (rule == null && autoDetect) {
                // 2. 自动检测规则
                rule = autoDetectRule(strValue);
            }

            if (rule != null) {
                String masked = applyMask(strValue, rule);
                entry.setValue(masked);
                totalMasked++;
            }
        }

        return dataMap;
    }

    /** 自动检测字符串应使用的脱敏规则 */
    private MaskRule autoDetectRule(String value) {
        for (Map.Entry<MaskRule, Pattern> e : autoDetectPatterns.entrySet()) {
            if (e.getValue().matcher(value).matches()) {
                return e.getKey();
            }
        }
        return null;
    }

    /** 应用脱敏规则 */
    private String applyMask(String value, MaskRule rule) {
        if (value == null || value.isEmpty()) return value;
        switch (rule) {
            case PHONE:
                return maskPhone(value);
            case ID_CARD:
                return maskIdCard(value);
            case EMAIL:
                return maskEmail(value);
            case BANK_CARD:
                return maskBankCard(value);
            case FULL_MASK:
                return repeat('*', Math.min(value.length(), 8));
            case HASH:
                return Integer.toHexString(value.hashCode());
            case PARTIAL_MASK:
            default:
                return maskPartial(value, 1, 1);
        }
    }

    /** 手机号脱敏：138****5678 */
    private String maskPhone(String phone) {
        if (phone == null || phone.length() < 7) return repeat('*', Math.min(phone.length(), 8));
        return phone.substring(0, 3) + "****" + phone.substring(phone.length() - 4);
    }

    /** 身份证脱敏：110101********1234 */
    private String maskIdCard(String idCard) {
        if (idCard == null || idCard.length() < 10) return repeat('*', Math.min(idCard.length(), 8));
        return idCard.substring(0, 6) + repeat('*', idCard.length() - 10) + idCard.substring(idCard.length() - 4);
    }

    /** 邮箱脱敏：a***@example.com */
    private String maskEmail(String email) {
        if (email == null) return null;
        int atIdx = email.indexOf('@');
        if (atIdx <= 0) return repeat('*', Math.min(email.length(), 8));
        String user = email.substring(0, atIdx);
        String domain = email.substring(atIdx);
        if (user.length() <= 1) return user + "***" + domain;
        return user.charAt(0) + "***" + domain;
    }

    /** 银行卡脱敏：6222****1234 */
    private String maskBankCard(String card) {
        if (card == null || card.length() < 8) return repeat('*', Math.min(card.length(), 8));
        return card.substring(0, 4) + repeat('*', card.length() - 8) + card.substring(card.length() - 4);
    }

    /** 部分脱敏：保留前 prefix 后 suffix 字符 */
    private String maskPartial(String value, int prefix, int suffix) {
        if (value == null) return null;
        if (value.length() <= prefix + suffix) return repeat('*', value.length());
        return value.substring(0, prefix) + repeat('*', value.length() - prefix - suffix) + value.substring(value.length() - suffix);
    }

    private String repeat(char c, int n) {
        if (n <= 0) return "";
        char[] arr = new char[n];
        Arrays.fill(arr, c);
        return new String(arr);
    }

    /** 获取统计信息 */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", enabled);
        stats.put("autoDetect", autoDetect);
        stats.put("fieldRulesCount", fieldRules.size());
        stats.put("totalMasked", totalMasked);
        stats.put("totalFieldsScanned", totalFieldsScanned);
        return stats;
    }

    public boolean isEnabled() { return enabled; }
}
