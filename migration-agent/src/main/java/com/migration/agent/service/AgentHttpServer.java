package com.migration.agent.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.migration.agent.AgentMain;
import com.migration.agent.model.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class AgentHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(AgentHttpServer.class);

    private final AgentMain agentMain;
    private final AgentConfig config;
    private final Gson gson;
    private final String apiToken;
    private final String allowedOrigin;
    private final CheckpointVisualizationService checkpointVisualizationService;
    private final SwitchoverService switchoverService;
    private final TableLatencyService tableLatencyService;
    private final DiagnosticsBundleService diagnosticsBundleService;
    private final Map<String, FanoutDispatcherService> fanoutServices = new java.util.concurrent.ConcurrentHashMap<>();
    /** 逃生开关：未配 token 时是否仍对匿名开放只读端点。默认 false（原来是无条件放行）。 */
    private final boolean allowAnonymousReadonly =
            Boolean.parseBoolean(System.getenv().getOrDefault("AGENT_READONLY_ALLOW_ANONYMOUS", "false"));

    public AgentHttpServer(AgentMain agentMain) {
        this.agentMain = agentMain;
        this.config = new AgentConfig();
        this.apiToken = System.getenv("AGENT_API_TOKEN");
        // CORS 允许来源：环境变量优先，否则取 agent.properties/默认（已更新为 backend 的 38080）
        this.allowedOrigin = System.getenv().getOrDefault("AGENT_CORS_ORIGIN", config.getAgentCorsAllowedOrigin());
        this.checkpointVisualizationService = new CheckpointVisualizationService();
        this.switchoverService = new SwitchoverService();
        this.tableLatencyService = new TableLatencyService();
        this.diagnosticsBundleService = new DiagnosticsBundleService();
        this.gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, (JsonSerializer<LocalDateTime>) (src, typeOfSrc, context) ->
                context.serialize(src.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
            .registerTypeAdapter(LocalDateTime.class, (JsonDeserializer<LocalDateTime>) (json, typeOfT, context) -> {
                if (json.isJsonArray()) {
                    com.google.gson.JsonArray arr = json.getAsJsonArray();
                    return LocalDateTime.of(
                        arr.get(0).getAsInt(), arr.get(1).getAsInt(), arr.get(2).getAsInt(),
                        arr.size() > 3 ? arr.get(3).getAsInt() : 0,
                        arr.size() > 4 ? arr.get(4).getAsInt() : 0,
                        arr.size() > 5 ? arr.get(5).getAsInt() : 0);
                }
                return LocalDateTime.parse(json.getAsString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            })
            .create();
    }
    private HttpServer server;

    public void start() {
        start(config.getHttpServerPort());
    }

    public void start(int port) {
        try {
            server = createServer(port);
            server.setExecutor(Executors.newFixedThreadPool(4));

            server.createContext("/api/agent/failover", this::handleFailover);
            server.createContext("/api/agent/switchover-drain", this::handleSwitchoverDrain);
            server.createContext("/api/agent/start-increment", this::handleStartIncrement);
            server.createContext("/api/agent/status", this::handleStatus);
            server.createContext("/api/agent/health", this::handleHealth);
            server.createContext("/api/metrics", this::handleMetrics);
            server.createContext("/api/checkpoint", this::handleCheckpointVisualization);
            server.createContext("/api/table-latency", this::handleTableLatency);
            server.createContext("/api/route-metrics", this::handleRouteMetrics);
            server.createContext("/api/fanout", this::handleFanout);
            server.createContext("/api/diagnostics", this::handleDiagnostics);
            server.createContext("/api/agent/deadletter", this::handleDeadletter);
            server.createContext("/api/agent/conflicts", this::handleConflicts);
            // 流量复制/回放：录制清单、录制打包下载、回放错误明细、回放报告
            server.createContext("/api/traffic/recordings", this::handleTrafficRecording);
            server.createContext("/api/traffic/bundle", this::handleTrafficBundle);
            server.createContext("/api/traffic/replay-errors", this::handleTrafficReplayErrors);
            server.createContext("/api/traffic/replay-report", this::handleTrafficReplayReport);

            server.start();
            logger.info("Agent HTTP Server started on port {}", port);
            if (apiToken == null || apiToken.isEmpty()) {
                logger.warn("⚠ 未配置 AGENT_API_TOKEN：敏感接口（主备倒换 failover / 启动增量 start-increment / " +
                    "排障包下载 diagnostics）将返回 401 拒绝。" +
                    "如需启用这些操作，请设置 AGENT_API_TOKEN 环境变量并让调用方带上 Bearer token。");
                if (allowAnonymousReadonly) {
                    logger.warn("⚠⚠ AGENT_READONLY_ALLOW_ANONYMOUS=true：只读监控接口对**任何人**开放。" +
                        "这些接口会暴露同步位点、表级延迟、路由分片与双向冲突记录（含行数据）。" +
                        "仅限完全隔离的内网调试使用。");
                } else {
                    logger.warn("只读监控接口同样返回 401（默认不对匿名开放）。" +
                        "确需匿名只读请显式设置 AGENT_READONLY_ALLOW_ANONYMOUS=true。");
                }
            }
        } catch (IOException e) {
            logger.error("Failed to start Agent HTTP Server", e);
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            logger.info("Agent HTTP Server stopped");
        }
    }

    private boolean handleCorsPreflight(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return true;
        }
        return false;
    }

    private void handleFailover(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuth(exchange)) return;
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            String requestBody = readRequestBody(exchange);
            logger.info("Received failover request: {}", requestBody);

            TaskMessage taskMessage = gson.fromJson(requestBody, TaskMessage.class);
            if (taskMessage.getTaskId() == null || taskMessage.getTaskId().isEmpty()) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId is required"));
                return;
            }

            if (taskMessage.getMessageType() == null) {
                taskMessage.setMessageType("failover");
            }

            if (agentMain.isFailoverInProgress(taskMessage.getTaskId())) {
                logger.warn("Failover already in progress for task: {}, rejecting duplicate request", taskMessage.getTaskId());
                sendResponse(exchange, 409, Map.of("success", false, "message", "Failover already in progress for task: " + taskMessage.getTaskId()));
                return;
            }

            new Thread(() -> agentMain.handleFailoverDirect(taskMessage)).start();

            sendResponse(exchange, 200, Map.of(
                "success", true,
                "message", "Failover initiated for task: " + taskMessage.getTaskId(),
                "taskId", taskMessage.getTaskId()
            ));
        } catch (Exception e) {
            logger.error("Error handling failover request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /**
     * 计划内切换的第一步：停旧主的写 + 等链路追平。POST /api/agent/switchover-drain
     *
     * <p>后端拿到 {@code success=true} 之后才去交换连接串、走原来那套倒换动作；
     * 拿到 false 就<b>什么都不改</b>——这正是"计划内切换零丢失"与"计划外接管有损"的分界线。
     * 同步返回（不像 failover 那样起线程），因为调用方必须等到结论才能决定下一步。
     */
    private void handleSwitchoverDrain(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuth(exchange)) return;
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> req = gson.fromJson(readRequestBody(exchange), Map.class);
            String taskId = req == null ? null : String.valueOf(req.get("taskId"));
            if (taskId == null || taskId.isEmpty() || "null".equals(taskId)) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId is required"));
                return;
            }
            long timeoutMs = req.get("timeoutMs") instanceof Number
                    ? ((Number) req.get("timeoutMs")).longValue() : 300000L;
            boolean fence = !Boolean.FALSE.equals(req.get("fence"));

            SwitchoverService.DrainResult r = switchoverService.drainAndFence(taskId, timeoutMs, fence);
            Map<String, Object> body = new java.util.LinkedHashMap<>();
            body.put("success", r.ok);
            body.put("message", r.reason);
            body.put("data", r.details);
            sendResponse(exchange, r.ok ? 200 : 409, body);
        } catch (Exception e) {
            logger.error("Error handling switchover-drain request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", String.valueOf(e.getMessage())));
        }
    }

    private void handleStartIncrement(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuth(exchange)) return;
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            String requestBody = readRequestBody(exchange);
            logger.info("Received start-increment request: {}", requestBody);

            TaskMessage taskMessage = gson.fromJson(requestBody, TaskMessage.class);
            if (taskMessage.getTaskId() == null || taskMessage.getTaskId().isEmpty()) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId is required"));
                return;
            }

            agentMain.startIncrementDirect(taskMessage);

            sendResponse(exchange, 200, Map.of(
                "success", true,
                "message", "Increment sync started for task: " + taskMessage.getTaskId(),
                "taskId", taskMessage.getTaskId()
            ));
        } catch (Exception e) {
            logger.error("Error handling start-increment request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            Map<String, Object> status = agentMain.getAgentStatus();
            sendResponse(exchange, 200, status);
        } catch (Exception e) {
            logger.error("Error handling status request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        sendResponse(exchange, 200, Map.of("status", "UP"));
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            String path = exchange.getRequestURI().getPath();

            if (path.equals("/api/metrics/prometheus")) {
                handlePrometheusInternal(exchange);
                return;
            }

            String[] parts = path.split("/");
            if (parts.length >= 4 && !parts[3].isEmpty() && !parts[3].equals("prometheus")) {
                String taskId = parts[3];

                if (parts.length >= 5 && "history".equals(parts[4])) {
                    handleMetricsHistory(exchange, taskId);
                    return;
                }

                Map<String, Object> metrics = MetricsService.getInstance().getTaskMetricsSnapshot(taskId);
                if (metrics.isEmpty()) {
                    sendResponse(exchange, 404, Map.of("success", false, "message", "No metrics found for task: " + taskId));
                } else {
                    sendResponse(exchange, 200, metrics);
                }
            } else {
                java.util.List<Map<String, Object>> allMetrics = MetricsService.getInstance().getAllTaskProcessStatus();
                sendResponse(exchange, 200, Map.of("tasks", allMetrics));
            }
        } catch (Exception e) {
            logger.error("Error handling metrics request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /**
     * 死信记录查询：GET /api/agent/deadletter/{taskId}
     * 读取增量进程写入的 files/{taskId}/deadletter.jsonl（人工裁决跳过的事件），按行解析返回。
     */
    // ==================== 流量复制与回放 ====================

    /** 录制元数据：GET /api/traffic/recordings/{captureTaskId}，返回 manifest 摘要。 */
    private void handleTrafficRecording(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        String taskId = trafficTaskIdOf(exchange, 4);
        if (taskId == null) return;
        try {
            java.io.File mf = new java.io.File("./files/" + taskId + "/traffic/manifest.json");
            if (!mf.isFile()) {
                sendResponse(exchange, 404, Map.of("success", false,
                        "message", "该任务没有录制文件（尚未开始录制，或文件已被清理）"));
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> manifest = gson.fromJson(new String(
                    java.nio.file.Files.readAllBytes(mf.toPath()), StandardCharsets.UTF_8), Map.class);
            Map<String, Object> out = new java.util.HashMap<>();
            out.put("success", true);
            out.put("taskId", taskId);
            out.put("t0Wall", manifest.get("t0Wall"));
            out.put("endWall", manifest.get("endWall"));
            out.put("durationMs", numOf(manifest.get("durationMs")));
            out.put("sealed", Boolean.TRUE.equals(manifest.get("sealed")));
            out.put("sha256", manifest.get("sha256"));
            out.put("source", manifest.get("source"));
            out.put("stats", manifest.get("stats"));
            Object segs = manifest.get("segments");
            long records = 0;
            long bytes = 0;
            if (segs instanceof java.util.List<?> list) {
                for (Object o : list) {
                    if (o instanceof Map<?, ?> m) {
                        records += numOf(m.get("records"));
                        bytes += numOf(m.get("bytes"));
                    }
                }
                out.put("segments", list.size());
            }
            out.put("records", records);
            out.put("bytes", bytes);
            Object gaps = manifest.get("gaps");
            out.put("gaps", gaps instanceof java.util.List<?> g ? g.size() : 0);
            out.put("gapDetail", gaps);
            Object stats = manifest.get("stats");
            if (stats instanceof Map<?, ?> sm) {
                out.put("sessions", numOf(sm.get("maxConcurrentSessions")));
            }
            sendResponse(exchange, 200, out);
        } catch (Exception e) {
            logger.error("Error handling traffic recording request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /** 录制打包下载：GET /api/traffic/bundle/{captureTaskId} → zip（manifest + 全部分段）。 */
    private void handleTrafficBundle(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        String taskId = trafficTaskIdOf(exchange, 4);
        if (taskId == null) return;
        try {
            java.io.File dir = new java.io.File("./files/" + taskId + "/traffic");
            java.io.File[] files = dir.listFiles(f -> f.isFile()
                    && (f.getName().equals("manifest.json") || f.getName().endsWith(".trf.gz")));
            if (files == null || files.length == 0) {
                sendResponse(exchange, 404, Map.of("success", false, "message", "没有可下载的录制文件"));
                return;
            }
            java.util.Arrays.sort(files, java.util.Comparator.comparing(java.io.File::getName));
            java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
            try (java.util.zip.ZipOutputStream zos = new java.util.zip.ZipOutputStream(buf)) {
                for (java.io.File f : files) {
                    zos.putNextEntry(new java.util.zip.ZipEntry(f.getName()));
                    java.nio.file.Files.copy(f.toPath(), zos);
                    zos.closeEntry();
                }
            }
            byte[] zip = buf.toByteArray();
            exchange.getResponseHeaders().set("Content-Type", "application/zip");
            exchange.getResponseHeaders().set("Content-Disposition",
                    "attachment; filename=\"traffic-" + taskId + ".trfz\"");
            exchange.sendResponseHeaders(200, zip.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(zip);
            }
        } catch (Exception e) {
            logger.error("Error handling traffic bundle request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /** 回放错误明细：GET /api/traffic/replay-errors/{taskId}?page=&pageSize= */
    private void handleTrafficReplayErrors(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        String taskId = trafficTaskIdOf(exchange, 4);
        if (taskId == null) return;
        try {
            Map<String, String> q = parseQuery(exchange.getRequestURI().getRawQuery());
            int page = Math.max(1, intOf(q.get("page"), 1));
            int pageSize = Math.min(500, Math.max(1, intOf(q.get("pageSize"), 50)));
            java.io.File f = new java.io.File("./files/" + taskId + "/traffic/replay_errors.jsonl");
            java.util.List<Object> all = new java.util.ArrayList<>();
            if (f.isFile()) {
                try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(
                        new java.io.FileInputStream(f), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (line.isBlank()) continue;
                        try {
                            all.add(gson.fromJson(line, Map.class));
                        } catch (Exception ex) {
                            all.add(Map.of("raw", line));
                        }
                    }
                }
            }
            int from = Math.min(all.size(), (page - 1) * pageSize);
            int to = Math.min(all.size(), from + pageSize);
            sendResponse(exchange, 200, Map.of("success", true, "taskId", taskId,
                    "total", all.size(), "page", page, "pageSize", pageSize,
                    "records", all.subList(from, to)));
        } catch (Exception e) {
            logger.error("Error handling traffic replay errors request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /** 回放报告：GET /api/traffic/replay-report/{taskId} */
    private void handleTrafficReplayReport(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        String taskId = trafficTaskIdOf(exchange, 4);
        if (taskId == null) return;
        try {
            java.io.File f = new java.io.File("./files/" + taskId + "/traffic/replay_report.properties");
            if (!f.isFile()) {
                sendResponse(exchange, 404, Map.of("success", false, "message", "回放报告尚未生成（回放还没结束）"));
                return;
            }
            java.util.Properties p = new java.util.Properties();
            try (java.io.InputStream in = new java.io.FileInputStream(f)) {
                p.load(in);
            }
            Map<String, Object> out = new java.util.HashMap<>();
            out.put("success", true);
            out.put("taskId", taskId);
            for (String name : p.stringPropertyNames()) {
                out.put(name, p.getProperty(name));
            }
            sendResponse(exchange, 200, out);
        } catch (Exception e) {
            logger.error("Error handling traffic replay report request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /** 取出并校验 URL 里的 taskId；不合法时已回响应并返回 null。 */
    private String trafficTaskIdOf(HttpExchange exchange, int index) throws IOException {
        String[] parts = exchange.getRequestURI().getPath().split("/");
        if (parts.length <= index || parts[index].isEmpty()) {
            sendResponse(exchange, 400, Map.of("success", false, "message", "taskId required"));
            return null;
        }
        String taskId = parts[index];
        // taskId 来自 URL，拼路径前拦掉路径穿越
        if (taskId.contains("..") || taskId.contains("/") || taskId.contains("\\")) {
            sendResponse(exchange, 400, Map.of("success", false, "message", "invalid taskId"));
            return null;
        }
        return taskId;
    }

    private static long numOf(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }

    private static int intOf(String s, int def) {
        try {
            return s == null || s.isBlank() ? def : Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> out = new java.util.HashMap<>();
        if (raw == null || raw.isEmpty()) return out;
        for (String kv : raw.split("&")) {
            int i = kv.indexOf('=');
            if (i > 0) {
                out.put(kv.substring(0, i), java.net.URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    private void handleDeadletter(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        try {
            String[] parts = exchange.getRequestURI().getPath().split("/");
            if (parts.length < 5 || parts[4].isEmpty()) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId required: /api/agent/deadletter/{taskId}"));
                return;
            }
            String taskId = parts[4];
            // taskId 来自 URL，拼路径前拦掉路径穿越
            if (taskId.contains("..") || taskId.contains("/") || taskId.contains("\\")) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "invalid taskId"));
                return;
            }
            java.io.File dlFile = new java.io.File("./files/" + taskId + "/deadletter.jsonl");
            java.util.List<Object> records = new java.util.ArrayList<>();
            if (dlFile.exists()) {
                try (java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(new java.io.FileInputStream(dlFile), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;
                        try {
                            records.add(gson.fromJson(line, Map.class));
                        } catch (Exception parseEx) {
                            records.add(Map.of("raw", line));
                        }
                    }
                }
            }
            sendResponse(exchange, 200, Map.of("success", true, "taskId", taskId, "total", records.size(), "records", records));
        } catch (Exception e) {
            logger.error("Error handling deadletter request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /**
     * 双向写写冲突记录查询：GET /api/agent/conflicts/{taskId}
     * 读取增量进程写入的 files/{taskId}/conflict.jsonl（CDR 裁决过的冲突），与死信同格式，复用同一套前端展示。
     */
    private void handleConflicts(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        try {
            String[] parts = exchange.getRequestURI().getPath().split("/");
            if (parts.length < 5 || parts[4].isEmpty()) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId required: /api/agent/conflicts/{taskId}"));
                return;
            }
            String taskId = parts[4];
            if (taskId.contains("..") || taskId.contains("/") || taskId.contains("\\")) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "invalid taskId"));
                return;
            }
            java.io.File f = new java.io.File("./files/" + taskId + "/conflict.jsonl");
            java.util.List<Object> records = new java.util.ArrayList<>();
            if (f.exists()) {
                try (java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(new java.io.FileInputStream(f), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;
                        try {
                            records.add(gson.fromJson(line, Map.class));
                        } catch (Exception parseEx) {
                            records.add(Map.of("raw", line));
                        }
                    }
                }
            }
            sendResponse(exchange, 200, Map.of("success", true, "taskId", taskId, "total", records.size(), "records", records));
        } catch (Exception e) {
            logger.error("Error handling conflicts request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    private void handlePrometheusInternal(HttpExchange exchange) throws IOException {
        try {
            String prometheusData = MetricsService.getInstance().scrapePrometheus();
            byte[] responseBytes = prometheusData.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=UTF-8");
            addCorsHeaders(exchange);
            exchange.sendResponseHeaders(200, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        } catch (Exception e) {
            logger.error("Error handling Prometheus scrape", e);
            String errorBody = "# ERROR: " + e.getMessage();
            byte[] errorBytes = errorBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, errorBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(errorBytes);
            }
        }
    }

    private void handleMetricsHistory(HttpExchange exchange, String taskId) throws IOException {
        try {
            String query = exchange.getRequestURI().getQuery();
            long endTs = System.currentTimeMillis();
            long startTs = endTs - 3600000;
            long intervalMs = 30000;

            if (query != null) {
                for (String param : query.split("&")) {
                    String[] kv = param.split("=", 2);
                    if (kv.length == 2) {
                        switch (kv[0]) {
                            case "start" -> startTs = Long.parseLong(kv[1]);
                            case "end" -> endTs = Long.parseLong(kv[1]);
                            case "interval" -> intervalMs = Long.parseLong(kv[1]);
                            case "last" -> {
                                long durationMs = parseDuration(kv[1]);
                                startTs = endTs - durationMs;
                            }
                        }
                    }
                }
            }

            MetricsPersistenceService persistence = MetricsPersistenceService.getInstance();
            if (persistence == null) {
                sendResponse(exchange, 503, Map.of("success", false, "message", "Metrics persistence not available"));
                return;
            }

            java.util.List<Map<String, Object>> metricsHistory = persistence.queryMetricsHistory(taskId, startTs, endTs, intervalMs);
            java.util.List<Map<String, Object>> processHistory = persistence.queryProcessHistory(taskId, startTs, endTs);

            Map<String, Object> result = new java.util.LinkedHashMap<>();
            result.put("success", true);
            result.put("taskId", taskId);
            result.put("startTs", startTs);
            result.put("endTs", endTs);
            result.put("intervalMs", intervalMs);
            result.put("metrics", metricsHistory);
            result.put("processes", processHistory);
            sendResponse(exchange, 200, result);
        } catch (Exception e) {
            logger.error("Error handling metrics history request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    private long parseDuration(String duration) {
        if (duration.endsWith("h")) {
            return Long.parseLong(duration.substring(0, duration.length() - 1)) * 3600000L;
        } else if (duration.endsWith("d")) {
            return Long.parseLong(duration.substring(0, duration.length() - 1)) * 86400000L;
        } else if (duration.endsWith("m")) {
            return Long.parseLong(duration.substring(0, duration.length() - 1)) * 60000L;
        } else {
            return Long.parseLong(duration);
        }
    }

    private void handleCheckpointVisualization(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            // /api/checkpoint/{taskId}
            if (parts.length >= 4 && !parts[3].isEmpty()) {
                String taskId = parts[3];
                Map<String, Object> visualization = checkpointVisualizationService.getCheckpointVisualization(taskId);
                sendResponse(exchange, 200, visualization);
            } else {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId is required in path"));
            }
        } catch (Exception e) {
            logger.error("Error handling checkpoint visualization request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    private void handleTableLatency(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            // /api/table-latency/{taskId}
            if (parts.length >= 4 && !parts[3].isEmpty()) {
                String taskId = parts[3];
                Map<String, Object> heatmap = tableLatencyService.getTableLatencyHeatmap(taskId);
                sendResponse(exchange, 200, heatmap);
            } else {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId is required in path"));
            }
        } catch (Exception e) {
            logger.error("Error handling table latency request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /**
     * 分片路由指标：GET /api/route-metrics/{taskId}
     * —— 每个落点应用了多少行（热点/空片一眼可见）、多少行算不出分片、跨分片搬迁多少次。
     * 数据由增量进程写在 files/{taskId}/binlog_output/route_metric，这里只做读取转发。
     */
    private void handleRouteMetrics(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }
        try {
            String[] parts = exchange.getRequestURI().getPath().split("/");
            if (parts.length < 4 || parts[3].isEmpty()) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId is required in path"));
                return;
            }
            String taskId = parts[3];
            java.io.File f = new java.io.File("./files/" + taskId + "/binlog_output/route_metric");
            if (!f.exists()) {
                // 未配路由、或增量还没跑起来：不是错误，前端据此显示"暂无数据"
                sendResponse(exchange, 200, Map.of("success", true, "data",
                        Map.of("mode", "NONE", "hits", Map.of(), "unrouted", 0, "crossShardMoves", 0)));
                return;
            }
            String json = new String(java.nio.file.Files.readAllBytes(f.toPath()),
                    java.nio.charset.StandardCharsets.UTF_8).trim();
            @SuppressWarnings("unchecked")
            Map<String, Object> data = new com.google.gson.Gson().fromJson(json, Map.class);
            sendResponse(exchange, 200, Map.of("success", true, "data", data == null ? Map.of() : data));
        } catch (Exception e) {
            logger.error("Error handling route metrics request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /**
     * 排障压缩包下载：GET /api/diagnostics/{taskId} -> zip（日志尾部 + 脱敏 config + checkpoint + THL 尾部）。
     */
    private void handleDiagnostics(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuth(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            // /api/diagnostics/{taskId}
            if (parts.length < 4 || parts[3].isEmpty()) {
                sendResponse(exchange, 400, Map.of("success", false, "message", "taskId is required in path"));
                return;
            }
            String taskId = parts[3];
            byte[] zipBytes = diagnosticsBundleService.buildBundle(taskId);

            addCorsHeaders(exchange);
            exchange.getResponseHeaders().set("Content-Type", "application/zip");
            exchange.getResponseHeaders().set("Content-Disposition",
                    "attachment; filename=\"diagnostics-" + taskId + ".zip\"");
            exchange.sendResponseHeaders(200, zipBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(zipBytes);
            }
        } catch (IOException e) {
            logger.warn("Error building diagnostics bundle: {}", e.getMessage());
            sendResponse(exchange, 404, Map.of("success", false, "message", e.getMessage()));
        } catch (Exception e) {
            logger.error("Error handling diagnostics request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    private void handleFanout(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuthOptional(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, Map.of("success", false, "message", "Method not allowed"));
            return;
        }

        try {
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            // /api/fanout/{taskId}
            if (parts.length >= 4 && !parts[3].isEmpty()) {
                String taskId = parts[3];
                FanoutDispatcherService fanoutService = fanoutServices.get(taskId);
                if (fanoutService == null) {
                    sendResponse(exchange, 404, Map.of("success", false, "message", "No fan-out service for task: " + taskId));
                } else {
                    sendResponse(exchange, 200, fanoutService.getStats());
                }
            } else {
                // 返回所有 fan-out 任务
                List<Map<String, Object>> allStats = new java.util.ArrayList<>();
                for (Map.Entry<String, FanoutDispatcherService> entry : fanoutServices.entrySet()) {
                    Map<String, Object> stat = entry.getValue().getStats();
                    stat.put("taskId", entry.getKey());
                    allStats.add(stat);
                }
                sendResponse(exchange, 200, Map.of("tasks", allStats));
            }
        } catch (Exception e) {
            logger.error("Error handling fanout request", e);
            sendResponse(exchange, 500, Map.of("success", false, "message", e.getMessage()));
        }
    }

    /** 注册 fan-out 分发服务 */
    public void registerFanoutService(String taskId, FanoutDispatcherService service) {
        fanoutServices.put(taskId, service);
    }

    /** 注销 fan-out 分发服务 */
    public void unregisterFanoutService(String taskId) {
        FanoutDispatcherService removed = fanoutServices.remove(taskId);
        if (removed != null) {
            removed.shutdown();
        }
    }

    private void addCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", allowedOrigin);
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
        exchange.getResponseHeaders().set("Access-Control-Allow-Credentials", "true");
    }

    private boolean checkAuth(HttpExchange exchange) throws IOException {
        // 未配置 AGENT_API_TOKEN 时，敏感端点（此方法的所有调用方：failover/start-increment/
        // metrics/checkpoint/table-latency/fanout/diagnostics）一律拒绝，而不是放行。
        // 排障包下载、主备倒换等操作不能在零鉴权下裸奔。health/status 不走 checkAuth，仍可探活。
        if (apiToken == null || apiToken.isEmpty()) {
            sendResponse(exchange, 401, Map.of("success", false,
                "message", "Agent 未配置 AGENT_API_TOKEN，敏感接口已禁用。请设置该环境变量后重启 agent。"));
            return false;
        }
        String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7);
            // 恒定时间比较：String.equals 在首个不同字节处就返回，逐字节的耗时差
            // 足以让调用方把 token 一个字符一个字符地试出来。MessageDigest.isEqual
            // 对等长输入不做短路；长度本身不是秘密（token 长度固定）。
            if (java.security.MessageDigest.isEqual(
                    apiToken.getBytes(StandardCharsets.UTF_8),
                    token.getBytes(StandardCharsets.UTF_8))) {
                return true;
            }
        }
        sendResponse(exchange, 401, Map.of("success", false, "message", "Unauthorized"));
        return false;
    }

    /**
     * 只读监控端点（metrics/checkpoint/table-latency/route-metrics/fanout/conflicts 等）的鉴权。
     *
     * <p><b>默认已改为"缺 token 即拒"</b>。原来是 fail-open——未配置 {@code AGENT_API_TOKEN}
     * 就一律放行，理由是"保证监控页可用"。但这些端点暴露的是同步位点、表级延迟、
     * 路由分片、以及双向冲突记录（其中带行数据），对匿名开放并不合适；
     * 而且监控页现在走后端代理，并不直连 agent，那条理由已经不成立。
     *
     * <p>标准部署不受影响：{@code start.sh} 与 {@code create_env.sh} 一直会生成并注入
     * {@code AGENT_API_TOKEN}，也就是说这些端点本来就在强制校验。真正受影响的只有
     * "裸跑 agent 且不配 token"的用法——给它留一个显式逃生开关
     * {@code AGENT_READONLY_ALLOW_ANONYMOUS=true}，并在启动时大声告警，
     * 而不是让不安全成为默认。
     */
    private boolean checkAuthOptional(HttpExchange exchange) throws IOException {
        if (apiToken == null || apiToken.isEmpty()) {
            if (allowAnonymousReadonly) {
                return true;
            }
            sendResponse(exchange, 401, Map.of("success", false,
                    "message", "Agent 未配置 AGENT_API_TOKEN，只读监控接口默认也不对匿名开放。"
                            + "请设置该环境变量；确需匿名只读请显式设置 "
                            + "AGENT_READONLY_ALLOW_ANONYMOUS=true。"));
            return false;
        }
        return checkAuth(exchange);
    }

    private String readRequestBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, Object body) throws IOException {
        String response = gson.toJson(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        addCorsHeaders(exchange);
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    /**
     * 建 HTTP 或 HTTPS 服务。
     *
     * <p>agent 的这些接口带的是运维动作（failover / switchover-drain / start-increment）与
     * 位点、指标等诊断信息，且用 {@code AGENT_API_TOKEN} 做鉴权——明文 HTTP 下 token 可被嗅探，
     * 拿到即可对任意任务发起倒换。配了 {@code AGENT_TLS_KEYSTORE} 就走 HTTPS。
     *
     * <p>只认 PKCS12（{@code .p12}）：与平台其它地方的证书形态保持一致，
     * 由 {@code test_scripts/ssl/gen_certs.sh} 一并生成。
     */
    private HttpServer createServer(int port) throws java.io.IOException {
        String keystore = System.getenv("AGENT_TLS_KEYSTORE");
        if (keystore == null || keystore.trim().isEmpty()) {
            return HttpServer.create(new InetSocketAddress(port), 0);
        }
        String storePass = System.getenv().getOrDefault("AGENT_TLS_KEYSTORE_PASSWORD", "");
        javax.net.ssl.SSLContext ctx;
        try {
            java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
            try (java.io.InputStream in = java.nio.file.Files.newInputStream(
                    java.nio.file.Path.of(keystore.trim()))) {
                ks.load(in, storePass.toCharArray());
            }
            javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(
                    javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, storePass.toCharArray());
            ctx = javax.net.ssl.SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), null, null);
        } catch (java.security.GeneralSecurityException e) {
            // 配了 keystore 却装不起来，绝不能退回明文 HTTP —— 那正是"以为加密了其实没有"
            throw new java.io.IOException("Agent TLS keystore 装配失败: " + keystore + " —— " + e.getMessage(), e);
        }

        com.sun.net.httpserver.HttpsServer https =
                com.sun.net.httpserver.HttpsServer.create(new InetSocketAddress(port), 0);
        https.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(ctx));
        logger.info("Agent HTTP 服务已启用 TLS（keystore={}）", keystore);
        return https;
    }
}
