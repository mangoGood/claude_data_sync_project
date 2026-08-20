package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * 把回放要用的录制文件备到本机 {@code files/<taskId>/traffic_in/}。
 *
 * <p>三种来源，按顺序尝试：
 * <ol>
 *   <li><b>已经在位</b>——用户上传的录制由后端直接铺好，或上一次回放已经拉过；</li>
 *   <li><b>同机</b>——录制由本机的某个捕获任务产出，直接从
 *       {@code files/<captureTaskId>/traffic/} 拷过来；</li>
 *   <li><b>跨机</b>——从产出它的那台 agent 的 {@code /api/traffic/bundle/<captureTaskId>}
 *       拉 zip 回来解开。集群里回放任务被指派到哪台机器是调度决定的，
 *       不能假设它就是产出录制的那台。</li>
 * </ol>
 */
public class TrafficRecordingFetcher {

    private static final Logger logger = LoggerFactory.getLogger(TrafficRecordingFetcher.class);

    /** zip 解压的总量上限，防解压炸弹。 */
    private static final long MAX_UNPACKED_BYTES = 64L * 1024 * 1024 * 1024;

    private final AgentConfig config;

    public TrafficRecordingFetcher(AgentConfig config) {
        this.config = config;
    }

    public void ensureAvailable(String taskId, Properties cfg) throws Exception {
        File target = new File("./files/" + taskId + "/traffic_in");
        if (hasManifest(target)) {
            logger.info("[{}] 录制文件已在位: {}", taskId, target.getAbsolutePath());
            return;
        }
        String sourceTask = cfg.getProperty("traffic.replay.recording.source.task", "");
        if (sourceTask.isEmpty()) {
            throw new IllegalStateException("任务配置里没有录制来源，且本机 " + target.getPath() + " 下也没有录制文件");
        }

        File local = new File("./files/" + sourceTask + "/traffic");
        if (hasManifest(local)) {
            copyDir(local.toPath(), target.toPath());
            logger.info("[{}] 录制文件已从本机捕获任务 {} 复制", taskId, sourceTask);
            return;
        }

        String agentId = cfg.getProperty("traffic.replay.recording.source.agent", "");
        fetchFromPeer(taskId, sourceTask, agentId, target);
    }

    private void fetchFromPeer(String taskId, String sourceTask, String agentId, File target) throws Exception {
        String base = peerBaseUrl(agentId);
        String url = base + "/api/traffic/bundle/" + sourceTask;
        logger.info("[{}] 从 {} 拉取录制文件", taskId, url);
        String token = System.getenv("AGENT_API_TOKEN");
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(url).openConnection();
        conn.setRequestMethod("GET");
        if (token != null && !token.isEmpty()) {
            conn.setRequestProperty("Authorization", "Bearer " + token);
        }
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(600_000);
        if (conn.getResponseCode() != 200) {
            throw new IllegalStateException("拉取录制文件失败，对端返回 " + conn.getResponseCode()
                    + "（录制在 agent " + agentId + " 上，请确认它可达）");
        }
        if (!target.exists() && !target.mkdirs()) {
            throw new IllegalStateException("无法创建目录: " + target.getAbsolutePath());
        }
        long unpacked = 0;
        try (InputStream in = conn.getInputStream();
             ZipInputStream zin = new ZipInputStream(in)) {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null) {
                if (e.isDirectory()) continue;
                String name = new File(e.getName()).getName();   // 拍平，杜绝 ../ 穿越
                File out = new File(target, name);
                try (OutputStream os = new FileOutputStream(out)) {
                    byte[] buf = new byte[64 * 1024];
                    int n;
                    while ((n = zin.read(buf)) > 0) {
                        unpacked += n;
                        if (unpacked > MAX_UNPACKED_BYTES) {
                            throw new IllegalStateException("录制文件解压超过上限，已中止");
                        }
                        os.write(buf, 0, n);
                    }
                }
            }
        }
        if (!hasManifest(target)) {
            throw new IllegalStateException("拉回的录制文件里没有 manifest.json，文件不完整");
        }
        logger.info("[{}] 录制文件已拉取完成，共 {} 字节", taskId, unpacked);
    }

    private String peerBaseUrl(String agentId) {
        String explicit = System.getenv("AGENT_PEER_BASE_" + (agentId == null ? "" : agentId));
        if (explicit != null && !explicit.isEmpty()) {
            return explicit;
        }
        return System.getenv().getOrDefault("AGENT_BASE_URL", "http://localhost:8083");
    }

    private static boolean hasManifest(File dir) {
        return new File(dir, "manifest.json").isFile();
    }

    private static void copyDir(Path from, Path to) throws Exception {
        Files.createDirectories(to);
        try (java.util.stream.Stream<Path> s = Files.list(from)) {
            for (Path p : s.toList()) {
                if (Files.isRegularFile(p)) {
                    Files.copy(p, to.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }
}
