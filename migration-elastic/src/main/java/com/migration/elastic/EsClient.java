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
import java.util.ArrayList;
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
        return (int) bulkWithRetry(ops, 0)[1];
    }

    /**
     * 带退避重试的 _bulk。
     *
     * <p><b>为什么必须退避而不是直接失败</b>：ES 的写入线程池队列满时会返回 429
     * （{@code es_rejected_execution_exception}），批量装载把写入压满时这几乎是常态——
     * 它表达的是"现在太忙，稍后再来"，不是"这条数据有问题"。原实现把 429 与真失败同等对待，
     * 全量在目标端一忙就整任务失败。HTTP 层的 429/503 重投整批，条目级的 429/503 只重投
     * <b>被拒的那些条目</b>（已成功的条目不能重投，index 动作虽幂等但会白白放大写入）。
     *
     * @return {成功条数, 失败条数}
     */
    long[] bulkWithRetry(List<String[]> ops, int maxRetries) throws Exception {
        if (ops.isEmpty()) {
            return new long[]{0, 0};
        }
        List<String[]> pending = new ArrayList<>(ops);
        long ok = 0;
        long failed = 0;
        for (int attempt = 0; ; attempt++) {
            String resp;
            try {
                resp = request("POST", "/_bulk", buildNdjson(pending), "application/x-ndjson");
            } catch (RetryableEsException e) {
                if (attempt >= maxRetries) {
                    throw new RuntimeException("ES _bulk 持续繁忙（重试 " + maxRetries + " 次后仍 "
                            + e.getMessage() + "）", e);
                }
                backoff(attempt, pending.size(), "HTTP " + e.status);
                continue;
            }
            JsonObject json = JsonParser.parseString(resp).getAsJsonObject();
            if (!json.get("errors").getAsBoolean()) {
                return new long[]{ok + pending.size(), failed};
            }
            List<String[]> retryable = new ArrayList<>();
            JsonArray items = json.getAsJsonArray("items");
            for (int i = 0; i < items.size(); i++) {
                JsonObject item = items.get(i).getAsJsonObject();
                JsonObject action = item.entrySet().iterator().next().getValue().getAsJsonObject();
                int status = action.get("status").getAsInt();
                // delete 目标不存在（404）视作幂等成功
                if (status < 300 || status == 404) {
                    ok++;
                } else if ((status == 429 || status == 503) && attempt < maxRetries) {
                    retryable.add(pending.get(i));
                } else {
                    failed++;
                    if (failed <= 3) {
                        logger.warn("bulk 条目失败: {}", action);
                    }
                }
            }
            if (retryable.isEmpty()) {
                return new long[]{ok, failed};
            }
            backoff(attempt, retryable.size(), "条目级 429/503");
            pending = retryable;
        }
    }

    /** 指数退避（100ms 起、上限 5s），带一点随机抖动避免多任务同步重试。 */
    private void backoff(int attempt, int size, String reason) throws InterruptedException {
        long sleep = Math.min(5000L, 100L * (1L << Math.min(attempt, 6)));
        sleep += (long) (Math.random() * 100);
        logger.warn("ES 繁忙（{}），{} 条待重投，{}ms 后重试", reason, size, sleep);
        Thread.sleep(sleep);
    }

    private static String buildNdjson(List<String[]> ops) {
        StringBuilder body = new StringBuilder();
        for (String[] op : ops) {
            body.append(op[0]).append('\n');
            if (op[1] != null) {
                body.append(op[1]).append('\n');
            }
        }
        return body.toString();
    }

    /**
     * 全量装载窗口：关掉刷新与副本，装载结束再恢复。
     * 这是 ES 侧公认的批量导入手法——每次 refresh 都要生成段并 fsync，副本还要把同一份数据
     * 再写一遍，全量期间这两件事都是纯开销。返回原设置，供结束后恢复。
     *
     * @return {refresh_interval, number_of_replicas}，取不到时对应项为 null
     */
    String[] beginLoadWindow(String index) throws Exception {
        String[] original = readLoadSettings(index);
        putSettings(index, "{\"index\":{\"refresh_interval\":\"-1\",\"number_of_replicas\":0}}");
        logger.info("索引 {} 进入装载窗口（refresh_interval=-1, replicas=0；原值 {}/{}）",
                index, original[0], original[1]);
        return original;
    }

    /**
     * 恢复装载窗口前的设置并刷新可见。<b>失败路径也必须调用</b>——否则任务一异常退出，
     * 索引就永久停在"不刷新、无副本"状态，查不到数据还没有冗余。
     */
    void endLoadWindow(String index, String[] original) {
        try {
            String refresh = original != null && original[0] != null ? original[0] : "1s";
            String replicas = original != null && original[1] != null ? original[1] : "1";
            putSettings(index, "{\"index\":{\"refresh_interval\":\"" + refresh
                    + "\",\"number_of_replicas\":" + replicas + "}}");
            refresh(index);
            logger.info("索引 {} 退出装载窗口，已恢复 refresh_interval={}, replicas={}", index, refresh, replicas);
        } catch (Exception e) {
            logger.error("索引 {} 恢复装载窗口设置失败（请手工检查 refresh_interval/number_of_replicas）: {}",
                    index, e.getMessage());
        }
    }

    private String[] readLoadSettings(String index) {
        try {
            String resp = request("GET", "/" + index + "/_settings", null);
            JsonObject root = JsonParser.parseString(resp).getAsJsonObject();
            JsonObject idx = root.getAsJsonObject(root.keySet().iterator().next())
                    .getAsJsonObject("settings").getAsJsonObject("index");
            String refresh = idx.has("refresh_interval") ? idx.get("refresh_interval").getAsString() : null;
            String replicas = idx.has("number_of_replicas") ? idx.get("number_of_replicas").getAsString() : null;
            return new String[]{refresh, replicas};
        } catch (Exception e) {
            logger.warn("读取索引 {} 设置失败，装载窗口结束后按默认值恢复: {}", index, e.getMessage());
            return new String[]{null, null};
        }
    }

    private void putSettings(String index, String body) throws Exception {
        request("PUT", "/" + index + "/_settings", body);
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
            // 429（队列满）/503（暂不可用）是"稍后再来"，与真正的数据错误分开，供调用方退避重试
            if (resp.statusCode() == 429 || resp.statusCode() == 503) {
                throw new RetryableEsException(resp.statusCode(), truncate(resp.body()));
            }
            throw new RuntimeException("ES " + method + " " + path + " -> HTTP " + resp.statusCode()
                    + ": " + truncate(resp.body()));
        }
        return resp.body();
    }

    /** ES 侧的背压信号（429/503）：可重试，不是数据错误。 */
    static final class RetryableEsException extends RuntimeException {
        final int status;

        RetryableEsException(int status, String body) {
            super("HTTP " + status + ": " + body);
            this.status = status;
        }
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 300 ? s.substring(0, 300) : s;
    }
}
