package com.migration.extract.schema;

/**
 * {@code COLUMN_TYPE} 里整数类型显示宽度的口径，按源库版本选。
 *
 * <p>MySQL 8.0.19 起废弃了整数类型的显示宽度，{@code information_schema.COLUMNS.COLUMN_TYPE}
 * 从 {@code int(11)} 变成 {@code int}。时序库要和 {@code information_schema} 逐列对得上
 * （阶段 1 的启动自检、阶段 5 的长跑对拍都靠这个），就必须按源库实际版本渲染，
 * 而不能固定一种写法。
 */
public enum TypeRenderMode {

    /** 5.7 / 8.0.18 及更早：{@code int(11)}、{@code bigint(20) unsigned}。 */
    WITH_DISPLAY_WIDTH,

    /** 8.0.19+：整数类型不带宽度（{@code tinyint(1)} 与 ZEROFILL 列除外，见 ColumnSchema）。 */
    NO_DISPLAY_WIDTH;

    /**
     * 按源库版本号（{@code SELECT VERSION()} 的原文，如 {@code 8.0.44} / {@code 5.7.41-log}）判口径。
     * 解析不出来时按 {@link #NO_DISPLAY_WIDTH}——现在的新库绝大多数是 8.0.19 之后的。
     */
    public static TypeRenderMode forServerVersion(String version) {
        if (version == null || version.isEmpty()) {
            return NO_DISPLAY_WIDTH;
        }
        int major = 0;
        int minor = 0;
        int patch = 0;
        int idx = 0;
        int[] parts = new int[3];
        StringBuilder num = new StringBuilder();
        for (int i = 0; i <= version.length() && idx < 3; i++) {
            char c = i < version.length() ? version.charAt(i) : '.';
            if (c >= '0' && c <= '9') {
                num.append(c);
            } else {
                if (num.length() > 0) {
                    try {
                        parts[idx++] = Integer.parseInt(num.toString());
                    } catch (NumberFormatException e) {
                        return NO_DISPLAY_WIDTH;
                    }
                    num.setLength(0);
                }
                if (c != '.') {
                    break;   // 版本号主体结束（如 5.7.41-log 的 '-'）
                }
            }
        }
        major = parts[0];
        minor = parts[1];
        patch = parts[2];

        if (major < 8) {
            return WITH_DISPLAY_WIDTH;
        }
        if (major == 8 && minor == 0 && patch < 19) {
            return WITH_DISPLAY_WIDTH;
        }
        return NO_DISPLAY_WIDTH;
    }
}
