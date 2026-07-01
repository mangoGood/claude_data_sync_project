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
    private final TableLatencyService tableLatencyService;
    private final Map<String, FanoutDispatcherService> fanoutServices = new java.util.concurrent.ConcurrentHashMap<>();

    public AgentHttpServer(AgentMain agentMain) {
        this.agentMain = agentMain;
        this.config = new AgentConfig();
        this.apiToken = System.getenv("AGENT_API_TOKEN");
        this.allowedOrigin = System.getenv().getOrDefault("AGENT_CORS_ORIGIN", "http://localhost:8082");
        this.checkpointVisualizationService = new CheckpointVisualizationService();
        this.tableLatencyService = new TableLatencyService();
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
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newFixedThreadPool(4));

            server.createContext("/api/agent/failover", this::handleFailover);
            server.createContext("/api/agent/start-increment", this::handleStartIncrement);
            server.createContext("/api/agent/status", this::handleStatus);
            server.createContext("/api/agent/health", this::handleHealth);
            server.createContext("/api/metrics", this::handleMetrics);
            server.createContext("/api/checkpoint", this::handleCheckpointVisualization);
            server.createContext("/api/table-latency", this::handleTableLatency);
            server.createContext("/api/fanout", this::handleFanout);

            server.start();
            logger.info("Agent HTTP Server started on port {}", port);
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
        if (!checkAuth(exchange)) return;
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
        if (!checkAuth(exchange)) return;
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

            if (path.equals("/api/metrics/demo")) {
                injectDemoMetrics();
                sendResponse(exchange, 200, Map.of("success", true, "message", "Demo metrics injected"));
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
        if (!checkAuth(exchange)) return;
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
        if (!checkAuth(exchange)) return;
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

    private void handleFanout(HttpExchange exchange) throws IOException {
        if (handleCorsPreflight(exchange)) return;
        if (!checkAuth(exchange)) return;
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
        if (apiToken == null || apiToken.isEmpty()) {
            return true;
        }
        String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7);
            if (apiToken.equals(token)) {
                return true;
            }
        }
        sendResponse(exchange, 401, Map.of("success", false, "message", "Unauthorized"));
        return false;
    }

    private void injectDemoMetrics() {
        String demoTaskId = "demo-task-001";
        MetricsService.TaskMetrics metrics = MetricsService.getInstance().getOrCreateTaskMetrics(demoTaskId);
        metrics.recordCaptureRate(1200 + (long)(Math.random() * 300));
        metrics.recordE2eLatency(50 + (long)(Math.random() * 100));
        metrics.recordQueueDepth("capture", 200 + (long)(Math.random() * 100));
        metrics.recordQueueDepth("extract", 150 + (long)(Math.random() * 80));
        metrics.recordQueueDepth("apply", 100 + (long)(Math.random() * 50));
        metrics.recordCheckpointLag(2 + (long)(Math.random() * 5));
        metrics.incrementEventsCaptured(1000 + (long)(Math.random() * 500));
        metrics.incrementEventsApplied(950 + (long)(Math.random() * 400));
        metrics.updateProcessStatus("capture", "RUNNING", 12345, "2h 30m", 0, "CLOSED");
        metrics.updateProcessStatus("extract", "RUNNING", 12346, "2h 30m", 0, "CLOSED");
        metrics.updateProcessStatus("apply", "RUNNING", 12347, "2h 28m", 1, "CLOSED");
        logger.info("Demo metrics injected for task: {}", demoTaskId);
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
}
