package com.migration.agent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

/**
 * TiDB 任务终结时清理 TiCDC changefeed。
 *
 * <p>changefeed 不是无代价的常驻对象：只要它还在，TiDB 就必须为它保留 GC safepoint
 * （旧版本数据不能回收），同时持续向 Kafka 投递变更。任务被"结束"或"删除"之后
 * 它已无人消费，留着只会让源集群的 GC 停滞、Kafka 无限堆积。
 *
 * <p>只在终态调用：<b>暂停</b>（stop 消息）必须保留 changefeed，否则恢复时这段时间的
 * 变更就永久丢了——恢复靠的正是 changefeed 自己的 checkpoint 继续投递。
 */
public class TicdcChangefeedService {
    private static final Logger logger = LoggerFactory.getLogger(TicdcChangefeedService.class);

    /**
     * 若该任务是 TiDB 源（capture.type=ticdc），删除其专属 changefeed。
     * 全过程 best-effort：配置缺失、changefeed 已不存在、TiCDC 不可达都只记日志，
     * 不影响任务终结流程本身。
     */
    public void removeChangefeedIfTidb(String taskId) {
        if (taskId == null || taskId.isEmpty()) {
            return;
        }
        File configFile = new File("files/" + taskId + "/config.properties");
        if (!configFile.exists()) {
            return;
        }
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(configFile)) {
            props.load(in);
        } catch (Exception e) {
            logger.debug("[{}] 读取任务配置失败，跳过 changefeed 清理: {}", taskId, e.getMessage());
            return;
        }
        if (!"ticdc".equalsIgnoreCase(props.getProperty("capture.type", ""))) {
            return;
        }

        String apiUrl = props.getProperty("capture.ticdc.api.url", "");
        String changefeedId = props.getProperty("capture.ticdc.changefeed.id", "");
        if (apiUrl.isEmpty() || changefeedId.isEmpty()) {
            logger.warn("[{}] TiDB 任务缺少 TiCDC 地址或 changefeed id，无法清理 changefeed", taskId);
            return;
        }
        while (apiUrl.endsWith("/")) {
            apiUrl = apiUrl.substring(0, apiUrl.length() - 1);
        }

        HttpURLConnection conn = null;
        try {
            URL url = new URL(apiUrl + "/api/v2/changefeeds/" + changefeedId);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(15000);
            int code = conn.getResponseCode();
            if (code == 200 || code == 202 || code == 404) {
                logger.info("[{}] TiCDC changefeed {} 已清理 (HTTP {})", taskId, changefeedId, code);
            } else {
                logger.warn("[{}] 删除 TiCDC changefeed {} 返回 HTTP {}，请手动确认（changefeed 会持有源库 GC safepoint）",
                        taskId, changefeedId, code);
            }
        } catch (Exception e) {
            logger.warn("[{}] 删除 TiCDC changefeed {} 失败: {}（changefeed 会持有源库 GC safepoint，请手动清理）",
                    taskId, changefeedId, e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
