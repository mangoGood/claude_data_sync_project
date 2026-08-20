package com.migration.common.security;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * {@code .cap} 的写入器：外形与 {@link BufferedWriter} 完全一致，但会把每条记录逐行加密。
 *
 * <p><b>为什么做成 BufferedWriter 的子类</b>：四个 capture 实现
 * （MySQL / PostgreSQL / Oracle / TiCDC）都持有一个 {@code BufferedWriter writer} 字段，
 * 并在十几处调 {@code writer.write(sb.toString())} / {@code flush()} / {@code close()}。
 * 做成子类之后，接入只需要改<b>创建那一行</b>，十几处调用点一个字都不用动——
 * 改动面越小，越不容易在这条最脆弱的路径上碰坏什么。
 *
 * <p>约定：调用方传入的字符串是<b>一条完整记录 + 行尾换行</b>。
 * 本类把换行摘下来、加密记录体、再把换行接回去，因此
 * <b>输出仍然是一行一条记录</b>，extract 的行计数与半行检测都不受影响。
 */
public class CapFileWriter extends BufferedWriter {

    private final CapLineCipher cipher;

    public CapFileWriter(Writer out, CapLineCipher cipher) {
        super(out);
        this.cipher = cipher;
    }

    @Override
    public void write(String str) throws IOException {
        if (cipher == null || !cipher.isEnabled() || str == null || str.isEmpty()) {
            super.write(str);
            return;
        }
        // 调用方一次写一条完整记录（末尾带 \n）。极少数情况下可能一次写多条，
        // 按换行切开逐条处理即可——切分只看 \n，而记录体内的换行在写入前
        // 已经被 capture 压成空格（见各 capture 的 FIELD_SEP/RECORD_SEP 处理）。
        int from = 0;
        StringBuilder out = new StringBuilder(str.length() + 64);
        while (from < str.length()) {
            int nl = str.indexOf('\n', from);
            if (nl < 0) {
                // 没有行尾换行：说明这不是一条完整记录（正常路径不会出现）。
                // 原样透传而不是猜——加密一个半条记录会让读侧解出半条业务数据。
                out.append(str, from, str.length());
                break;
            }
            String record = str.substring(from, nl);
            if (record.isEmpty()) {
                out.append('\n');
            } else {
                out.append(cipher.encryptRecord(record)).append('\n');
            }
            from = nl + 1;
        }
        super.write(out.toString());
    }
}
