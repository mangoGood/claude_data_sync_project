package com.migration.capture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CaptureMain {

    private static final Logger logger = LoggerFactory.getLogger(CaptureMain.class);

    public static void main(String[] args) {
        com.migration.common.OracleNetCompat.apply();
        String configPath = null;
        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
            }
        }

        Properties props = new Properties();

        if (configPath != null) {
            try (InputStream input = new FileInputStream(configPath)) {
                props.load(input);
                logger.info("Loaded config from: {}", configPath);
            } catch (IOException e) {
                logger.error("Failed to load config: {}", configPath, e);
                System.exit(1);
            }
        } else {
            String taskIdHint = System.getProperty("task.id", "unknown");
            String defaultConfig = "files/" + taskIdHint + "/config.properties";
            File configFile = new File(defaultConfig);
            if (configFile.exists()) {
                try (InputStream input = new FileInputStream(configFile)) {
                    props.load(input);
                    logger.info("Loaded default config from: {}", defaultConfig);
                } catch (IOException e) {
                    logger.error("Failed to load default config", e);
                    System.exit(1);
                }
            } else {
                logger.error("No config file found. Specify --config or ensure files/{}/config.properties exists", taskIdHint);
                System.exit(1);
            }
        }
        // 解密 config.properties 中的加密口令（ENC: 前缀）；历史明文配置无前缀，原样通过。
        com.migration.common.crypto.CredentialCipher.decryptProperties(props);

        String taskId = props.getProperty("task.id", System.getProperty("task.id", "unknown"));

        String outputDir = props.getProperty("capture.output.dir",
                "files/" + taskId + "/binlog_output");
        props.setProperty("capture.output.dir", outputDir);

        String captureType = props.getProperty("capture.type", "binlog").toLowerCase();
        logger.info("=== Migration Capture Starting (type={}) ===", captureType);

        com.migration.common.Capture<?> capture;

        if ("wal".equals(captureType) || "postgresql".equals(captureType)) {
            capture = new PostgresWalCapture();
        } else if ("redo".equals(captureType) || "oracle".equals(captureType)) {
            capture = new OracleRedoCapture();
        } else {
            capture = new MySQLBinlogCapture();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered, stopping capture...");
            try {
                capture.stop();
            } catch (Exception e) {
                logger.error("Error stopping capture during shutdown", e);
            }
        }));

        try {
            capture.initialize(props);
            capture.start();

            logger.info("Capture running (type={}). Press Ctrl+C to stop.", captureType);

            Thread.currentThread().join();
        } catch (Exception e) {
            logger.error("Capture failed", e);
            System.exit(1);
        }
    }
}
