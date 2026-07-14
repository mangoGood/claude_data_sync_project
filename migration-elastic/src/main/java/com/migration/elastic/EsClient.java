package com.migration.elastic;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * 极简 Elasticsearch REST 客户端（java.net.http + Gson）。
 *
 * <p>刻意不引入官方 elasticsearch-java / rest-client：一是避免与 Spring BOM 的版本管理
 * 相互干扰（mongo 驱动曾因 BOM 降级混包 NoClassDefFoundError），二是本模块只需要
 * _bulk / _count / _refresh / 建索引几个端点，纯 HTTP 足够且零依赖冲突。
 */
final class EsClient {

    private static final Logger logger = LoggerFactory.getLogger(EsClient.class);
    private static final Gson gson = new Gson();

    private final String baseUrl;
    private final String authHeader;
    private final HttpClient http;

    EsClient(String host, int port, String username, String password) {
        this.baseUrl = "http://" + host + ":" + port;
        this.authHeader = (username != null && !username.isEmpty())
                ? "Basic " + Base64.getEncoder().encodeToString(
                        (username + ":" + password).getBytes(StandardCharsets.UTF_8))
                : null;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .build();
    }

    /** GET /：连通性 + 版本；失败抛异常。 */
    JsonObject info() throws Exception {
        return JsonParser.parseString(request("GET", "/", null)).getAsJsonObject();
    }

    boolean indexExists(String index) throws Exception {
        HttpRequest req = builder("/" + index).method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
        return resp.statusCode() == 200;
    }

    void createIndexIfAbsent(String index) throws Exception {
        if (indexExists(index)) {
            return;
        }
        // 动态 mapping：MySQL 行转 JSON 后由 ES 自行推断字段类型
        request("PUT", "/" + index, "{}");
        logger.info("已创建索引 {}", index);
    }

    /**
     * _bulk 批量写入。ops 中每项为 [actionLine, sourceLine]（delete 无 sourceLine，传 null）。
     * 返回条目级失败数（HTTP 层失败直接抛异常）。
     */
    int bulk(List<String[]> ops) throws Exception {
        if (ops.isEmpty()) {
            return 0;
        }
        StringBuilder body = new StringBuilder();
        for (String[] op : ops) {
            body.append(op[0]).append('\n');
            if (op[1] != null) {
                body.append(op[1]).append('\n');
            }
        }
        String resp = request("POST", "/_bulk", body.toString(), "application/x-ndjson");
        JsonObject json = JsonParser.parseString(resp).getAsJsonObject();
        if (!json.get("errors").getAsBoolean()) {
            return 0;
        }
        int failed = 0;
        JsonArray items = json.getAsJsonArray("items");
        for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.get(i).getAsJsonObject();
            JsonObject action = item.entrySet().iterator().next().getValue().getAsJsonObject();
            int status = action.get("status").getAsInt();
            // delete 目标不存在（404）视作幂等成功
            if (status >= 300 && status != 404) {
                failed++;
                if (failed <= 3) {
                    logger.warn("bulk 条目失败: {}", action);
                }
            }
        }
        return failed;
    }

    void refresh(String index) throws Exception {
        request("POST", "/" + index + "/_refresh", null);
    }

    long count(String index) throws Exception {
        String resp = request("GET", "/" + index + "/_count", null);
        return JsonParser.parseString(resp).getAsJsonObject().get("count").getAsLong();
    }

    /** 组装一条 index（upsert 语义：同 _id 覆盖）bulk 操作。id 为 null 时自动生成（无主键表）。 */
    static String[] indexOp(String index, String id, Map<String, Object> doc) {
        JsonObject meta = new JsonObject();
        JsonObject inner = new JsonObject();
        inner.addProperty("_index", index);
        if (id != null) {
            inner.addProperty("_id", id);
        }
        meta.add("index", inner);
        return new String[]{gson.toJson(meta), gson.toJson(doc)};
    }

    static String[] deleteOp(String index, String id) {
        JsonObject meta = new JsonObject();
        JsonObject inner = new JsonObject();
        inner.addProperty("_index", index);
        inner.addProperty("_id", id);
        meta.add("delete", inner);
        return new String[]{gson.toJson(meta), null};
    }

    private HttpRequest.Builder builder(String path) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(60));
        if (authHeader != null) {
            b.header("Authorization", authHeader);
        }
        return b;
    }

    private String request(String method, String path, String body) throws Exception {
        return request(method, path, body, "application/json");
    }

    private String request(String method, String path, String body, String contentType) throws Exception {
        HttpRequest.Builder b = builder(path).header("Content-Type", contentType);
        HttpRequest req = b.method(method, body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new RuntimeException("ES " + method + " " + path + " -> HTTP " + resp.statusCode()
                    + ": " + truncate(resp.body()));
        }
        return resp.body();
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 300 ? s.substring(0, 300) : s;
    }
}
