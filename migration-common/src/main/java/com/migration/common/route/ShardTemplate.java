package com.migration.common.route;

import java.util.ArrayList;
import java.util.List;

/**
 * 目标库/表名模板：字面量 + {@code ${shard}} / {@code ${shard/N}} / {@code ${shard%N}}。
 *
 * <p>例：{@code dw_${shard/2}} + {@code order_${shard}} 表示 "8 库 × 16 表" 里的
 * 库 = 分片号 / 2、表 = 分片号。
 *
 * <p><b>为什么不引表达式引擎</b>：模板取值直接决定数据落到哪张表，是数据面而不是控制面。
 * 只放行 {@code shard} 一个变量和整数除/模两个运算，用手写解析器覆盖，
 * 既不给任意表达式执行留面，也不让规则变成没人看得懂的脚本。
 */
public final class ShardTemplate {

    private enum Op { LITERAL, SHARD, DIV, MOD }

    private static final class Segment {
        final Op op;
        final String literal;
        final int operand;

        Segment(Op op, String literal, int operand) {
            this.op = op;
            this.literal = literal;
            this.operand = operand;
        }
    }

    private final String raw;
    private final List<Segment> segments;
    private final boolean numericRequired;

    private ShardTemplate(String raw, List<Segment> segments, boolean numericRequired) {
        this.raw = raw;
        this.segments = segments;
        this.numericRequired = numericRequired;
    }

    /**
     * 编译模板。
     *
     * @throws IllegalArgumentException 占位符语法非法、除数/模数 &le; 0、或有未闭合的 {@code ${}
     */
    public static ShardTemplate compile(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException("模板为空");
        }
        String t = raw.trim();
        List<Segment> segs = new ArrayList<>();
        boolean numeric = false;
        int i = 0;
        StringBuilder literal = new StringBuilder();
        while (i < t.length()) {
            int start = t.indexOf("${", i);
            if (start < 0) {
                literal.append(t, i, t.length());
                break;
            }
            literal.append(t, i, start);
            int end = t.indexOf('}', start);
            if (end < 0) {
                throw new IllegalArgumentException("模板占位符未闭合: " + raw);
            }
            if (literal.length() > 0) {
                segs.add(new Segment(Op.LITERAL, literal.toString(), 0));
                literal.setLength(0);
            }
            segs.add(parsePlaceholder(t.substring(start + 2, end).trim(), raw));
            if (segs.get(segs.size() - 1).op != Op.SHARD) {
                numeric = true;
            }
            i = end + 1;
        }
        if (literal.length() > 0) {
            segs.add(new Segment(Op.LITERAL, literal.toString(), 0));
        }
        if (segs.stream().noneMatch(s -> s.op != Op.LITERAL)) {
            // 纯字面量模板是合法的（分库不分表：表名固定、库名带分片号）
            return new ShardTemplate(t, segs, false);
        }
        return new ShardTemplate(t, segs, numeric);
    }

    private static Segment parsePlaceholder(String expr, String raw) {
        if (expr.equals("shard")) {
            return new Segment(Op.SHARD, null, 0);
        }
        if (!expr.startsWith("shard")) {
            throw new IllegalArgumentException("模板只支持 ${shard} 变量: " + raw);
        }
        String rest = expr.substring(5).trim();
        if (rest.length() < 2 || (rest.charAt(0) != '/' && rest.charAt(0) != '%')) {
            throw new IllegalArgumentException("模板只支持 ${shard}、${shard/N}、${shard%N}: " + raw);
        }
        int operand;
        try {
            operand = Integer.parseInt(rest.substring(1).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("模板运算数不是整数: " + raw);
        }
        if (operand <= 0) {
            throw new IllegalArgumentException("模板运算数必须为正整数: " + raw);
        }
        return new Segment(rest.charAt(0) == '/' ? Op.DIV : Op.MOD, null, operand);
    }

    /** 模板是否用到 {@code ${shard/N}} / {@code ${shard%N}}（要求分片号是数值）。 */
    public boolean requiresNumericShard() {
        return numericRequired;
    }

    /** 模板是否含任何 {@code ${shard}} 占位（纯字面量 = 所有分片同名）。 */
    public boolean hasPlaceholder() {
        return segments.stream().anyMatch(s -> s.op != Op.LITERAL);
    }

    /**
     * 渲染。
     *
     * @throws IllegalStateException 模板要求数值分片但传入的是非数值分片（加载期校验已拦截，这里兜底）
     */
    public String render(ShardKey key) {
        StringBuilder sb = new StringBuilder();
        for (Segment s : segments) {
            switch (s.op) {
                case LITERAL:
                    sb.append(s.literal);
                    break;
                case SHARD:
                    sb.append(key.token());
                    break;
                case DIV:
                case MOD:
                    if (!key.isNumeric()) {
                        throw new IllegalStateException("模板 " + raw + " 需要数值分片号，实际分片标识: " + key.token());
                    }
                    sb.append(s.op == Op.DIV ? key.index() / s.operand : key.index() % s.operand);
                    break;
                default:
                    break;
            }
        }
        return sb.toString();
    }

    public String getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return raw;
    }
}
