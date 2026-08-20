package com.synctask.service;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 脱敏配置校验（建任务/改配置时调用）。
 *
 * <p>为什么要在控制面强校验，而不是像其它列处理那样"非法项跳过并告警"：
 * 跳过一条列名映射，表现是列名没改，用户一眼能看见；而跳过一条<b>脱敏</b>规则，
 * 表现是<b>那一列的原值被原样搬到了目标端</b>——用户看不见，直到出事。
 * 脱敏是"漏一条就等于没做"的那类配置，必须建任务时就拦下。
 */
public final class ColumnMaskValidator {

    /** 支持的规则。与引擎侧 {@code ColumnProcessingConfig.MaskKind} 一一对应。 */
    private static final Set<String> KINDS =
            Set.of("MASK_ALL", "MASK_PARTIAL", "HASH", "NULLIFY", "FAKE");
    private static final Set<String> FAKE_TYPES =
            Set.of("NAME", "EMAIL", "PHONE", "ADDRESS", "ID_CARD");
    private static final java.util.regex.Pattern IDENT =
            java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");

    private ColumnMaskValidator() {
    }

    /** 校验结果：错误列表 + 需要提示给用户的说明。 */
    public static final class Result {
        public final List<String> errors = new ArrayList<>();
        /** 配了脱敏的 "库.表.列"，用于生成"这些列无法内容对比"的提示。 */
        public final List<String> maskedColumns = new ArrayList<>();

        public boolean ok() {
            return errors.isEmpty();
        }

        /**
         * 给用户看的提示。<b>这是产品语义的一部分</b>：选了脱敏就意味着
         * 那些列的内容对比不再可用——目标端存的本来就不是源端的值。
         * 不说清楚，用户会在对比结果里看到一堆"不一致"却查不出原因。
         */
        public String warning() {
            if (maskedColumns.isEmpty()) {
                return null;
            }
            return "已配置脱敏的列：" + String.join("、", maskedColumns)
                    + "。如果选择了脱敏，则无法完成脱敏列内容对比——"
                    + "目标端存的是脱敏后的值，这些列会被自动排除在内容对比之外"
                    + "（行数对比与其余列的内容对比不受影响）。";
        }
    }

    /**
     * 校验 syncObjects 里的 columnMask 配置。
     *
     * @param syncObjectsJson 任务的 syncObjects
     * @param primaryKeys     库 → 表 → 主键列名（小写）。用于拦"对主键脱敏"——
     *                        主键是两端配对行的依据，脱掉它内容对比就无从配对；
     *                        且 HASH 一个数值主键会产生十六进制串，目标端类型直接不匹配。
     *                        传 null 表示暂不校验这一项（元数据尚未探查时）。
     */
    @SuppressWarnings("unchecked")
    public static Result validate(String syncObjectsJson,
                                  Map<String, Map<String, String>> primaryKeys) {
        Result r = new Result();
        if (syncObjectsJson == null || syncObjectsJson.isEmpty()) {
            return r;
        }
        Map<String, Object> root;
        try {
            root = new Gson().fromJson(syncObjectsJson, LinkedHashMap.class);
        } catch (JsonSyntaxException e) {
            r.errors.add("syncObjects 不是合法 JSON");
            return r;
        }
        if (root == null) {
            return r;
        }

        for (Map.Entry<String, Object> dbEntry : root.entrySet()) {
            String db = dbEntry.getKey();
            if (!(dbEntry.getValue() instanceof Map)) continue;
            Object maskObj = ((Map<String, Object>) dbEntry.getValue()).get("columnMask");
            if (!(maskObj instanceof Map)) continue;

            for (Map.Entry<String, Object> tEntry : ((Map<String, Object>) maskObj).entrySet()) {
                String table = tEntry.getKey();
                if (!(tEntry.getValue() instanceof List)) {
                    r.errors.add(db + "." + table + " 的脱敏配置格式不正确（应为规则数组）");
                    continue;
                }
                Set<String> seen = new LinkedHashSet<>();
                for (Object item : (List<Object>) tEntry.getValue()) {
                    if (!(item instanceof Map)) {
                        r.errors.add(db + "." + table + " 含格式不正确的脱敏规则");
                        continue;
                    }
                    validateOne(db, table, (Map<String, Object>) item, primaryKeys, seen, r);
                }
            }
        }
        return r;
    }

    private static void validateOne(String db, String table, Map<String, Object> rule,
                                    Map<String, Map<String, String>> primaryKeys,
                                    Set<String> seen, Result r) {
        String where = db + "." + table;
        Object colObj = rule.get("column");
        String col = colObj == null ? "" : String.valueOf(colObj).trim();
        if (col.isEmpty() || !IDENT.matcher(col).matches()) {
            r.errors.add(where + " 脱敏列名非法: '" + col + "'");
            return;
        }
        if (!seen.add(col.toLowerCase())) {
            // 同一列配两条规则，后一条会覆盖前一条——那是"以为配了 A 其实生效 B"，
            // 在脱敏上尤其危险（可能从强规则退化成弱规则）
            r.errors.add(where + "." + col + " 配置了多条脱敏规则，请只保留一条");
            return;
        }

        String kind = rule.get("rule") == null ? "" : String.valueOf(rule.get("rule")).toUpperCase();
        if (!KINDS.contains(kind)) {
            r.errors.add(where + "." + col + " 脱敏规则非法: '" + kind
                    + "'，可选 " + String.join(" / ", KINDS));
            return;
        }

        // 主键不得脱敏
        if (primaryKeys != null) {
            Map<String, String> byTable = primaryKeys.get(db);
            String pk = byTable == null ? null : byTable.get(table);
            if (pk != null && pk.equalsIgnoreCase(col)) {
                r.errors.add(where + "." + col + " 是主键，不能配置脱敏："
                        + "主键是两端配对行的依据，脱敏后内容对比无从配对；"
                        + "且哈希类规则会改变值的类型，目标端写入会直接失败。");
                return;
            }
        }

        if ("MASK_PARTIAL".equals(kind)) {
            int kp = intOf(rule.get("keepPrefix"), -1);
            int ks = intOf(rule.get("keepSuffix"), -1);
            if (kp < 0 || ks < 0) {
                r.errors.add(where + "." + col + " 的 MASK_PARTIAL 需要非负的 keepPrefix 与 keepSuffix");
                return;
            }
            if (kp == 0 && ks == 0) {
                // 首尾都不保留 = 整值遮蔽，那应该用 MASK_ALL，语义更明确
                r.errors.add(where + "." + col + " 的 MASK_PARTIAL 首尾都不保留，请改用 MASK_ALL");
                return;
            }
        } else if ("FAKE".equals(kind)) {
            String arg = rule.get("arg") == null ? "" : String.valueOf(rule.get("arg")).toUpperCase();
            if (!FAKE_TYPES.contains(arg)) {
                r.errors.add(where + "." + col + " 的 FAKE 需要指定类型，可选 "
                        + String.join(" / ", FAKE_TYPES));
                return;
            }
        }

        r.maskedColumns.add(where + "." + col);
    }

    private static int intOf(Object o, int dflt) {
        if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(o));
        } catch (Exception e) {
            return dflt;
        }
    }
}
