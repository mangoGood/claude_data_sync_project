package com.migration.extract.schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 表结构的 JSON 编解码——时序库落盘（{@code schema_history.jsonl}）与审计共用一份格式。
 *
 * <p>刻意<b>不</b>开 pretty print：历史文件是一行一条的 jsonl，恢复时按行读，
 * 一条跨多行会让"读坏最后半条"这种崩溃残留无法安全跳过。
 *
 * <p>也刻意<b>不</b>序列化 null 字段（Gson 默认行为）：一张几十列的表，
 * 每列十几个可空属性，写全了体积翻几倍，而历史文件是要跟着任务长期保留的。
 */
public final class SchemaJson {

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private SchemaJson() {
    }

    public static String toJson(TableSchema schema) {
        return GSON.toJson(schema);
    }

    /** 解析失败抛 {@link com.google.gson.JsonSyntaxException}，由调用方决定是跳过这一行还是停。 */
    public static TableSchema fromJson(String json) {
        return GSON.fromJson(json, TableSchema.class);
    }
}
