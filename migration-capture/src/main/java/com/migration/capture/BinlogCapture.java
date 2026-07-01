package com.migration.capture;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogCapture {
    private static final Logger logger = LoggerFactory.getLogger(BinlogCapture.class);

    private String url;
    private String user;
    private String password;
    private String binlogDir;
    private String binlogFilePattern;
    private String outputDir;

    public BinlogCapture(Properties props) {
        this.url = props.getProperty("mysql.url");
        this.user = props.getProperty("mysql.user");
        this.password = props.getProperty("mysql.password");
        this.binlogDir = props.getProperty("binlog.dir", "/var/log/mysql");
        this.binlogFilePattern = props.getProperty("binlog.pattern", "mysql-bin");
        this.outputDir = props.getProperty("output.dir", "./output");
    }

    public void start() throws Exception {
        logger.info("Starting binlog capture...");
        
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        String[] binlogInfo = getCurrentBinlogPosition();
        String binlogFile = binlogInfo[0];
        long binlogPosition = Long.parseLong(binlogInfo[1]);

        logger.info("Starting from binlog position: {}", binlogFile + ":" + binlogPosition);

        readBinlog(binlogFile, binlogPosition);
    }

    private String[] getCurrentBinlogPosition() throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {
            if (rs.next()) {
                String binlogFile = rs.getString(1);
                String binlogPosition = rs.getString(2);
                return new String[] { binlogFile, binlogPosition };
            } else {
                throw new Exception("Could not get master status");
            }
        }
    }

    private void readBinlog(String binlogFile, long binlogPosition) throws Exception {
        logger.info("Reading binlog file {} from position {} via MySQL protocol", binlogFile, binlogPosition);

        byte[] dummyBinlogData = "Dummy binlog event data".getBytes();
        writeToOutput(dummyBinlogData, 0, dummyBinlogData.length, binlogFile);

        logger.info("Simulated binlog reading completed for {}", binlogFile);

        handleBinlogRotation(binlogFile);
    }

    private void writeToOutput(byte[] buffer, int offset, int length, String binlogFile) throws IOException {
        File outputFile = new File(outputDir, "binlog-" + binlogFile + ".bin");
        try (RandomAccessFile raf = new RandomAccessFile(outputFile, "rw")) {
            raf.seek(raf.length());
            raf.write(buffer, offset, length);
        }
    }

    private void handleBinlogRotation(String currentBinlogFile) throws Exception {
        String[] binlogInfo = getCurrentBinlogPosition();
        String nextBinlogFile = binlogInfo[0];
        
        if (!nextBinlogFile.equals(currentBinlogFile)) {
            logger.info("Binlog rotated to: {}", nextBinlogFile);
            readBinlog(nextBinlogFile, 4);
        }
    }
}
