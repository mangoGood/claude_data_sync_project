package com.migration.common.traffic;

import com.migration.common.crypto.CredentialCipher;
import com.migration.common.io.AtomicFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * 流量复制期间被改动的源库开关，以及把它<b>还原回去</b>的能力。
 *
 * <p>为什么要单独一个类、还放在 common 里：还原这件事必须在<b>捕获进程已经不存在</b>的情况下
 * 也做得到。捕获进程被 {@code kill -9}、agent 整个硬崩、宿主断电——这些情形下
 * 谁来还原？只能是"下一个活着的人"：agent 的看门狗，或 agent 重启后的扫尾流程。
 * 而 agent 不依赖任何引擎模块（它只负责起子进程），所以这段逻辑落在 common。
 *
 * <p>不还原的后果不是"配置有点脏"，而是<b>源库每条语句都继续往
 * {@code mysql.general_log} 写，直到把源库磁盘写满</b>。这是本功能最大的运维风险。
 *
 * <p>口令用与连接串同一套 AES-GCM（{@link CredentialCipher}）加密后落盘。
 */
public final class TrafficSourceState {

    private static final Logger logger = LoggerFactory.getLogger(TrafficSourceState.class);

    public static final String FILE_NAME = "source_state.properties";

    public String host;
    public String port;
    public String username;
    /** 明文口令（内存中）；落盘时加密。 */
    public String password;
    /** 源库上 {@code general_log} 的原值。 */
    public String generalLog;
    /** 源库上 {@code log_output} 的原值。 */
    public String logOutput;
    /** 附加的 JDBC 参数（SSL 等），形如 {@code useSSL=false&...}。 */
    public String urlParams;

    /** 录制目录下的状态文件位置。 */
    public static File fileIn(File recordingDir) {
        return new File(recordingDir, FILE_NAME);
    }

    /** 原子写出（写临时文件 → fsync → rename），避免留下半个文件让还原读到垃圾。 */
    public void save(File recordingDir) throws IOException {
        Properties p = new Properties();
        p.setProperty("source.host", nz(host));
        p.setProperty("source.port", nz(port));
        p.setProperty("source.username", nz(username));
        p.setProperty("source.password", CredentialCipher.encrypt(nz(password)));
        p.setProperty("source.url.params", nz(urlParams));
        p.setProperty("original.general_log", nz(generalLog));
        p.setProperty("original.log_output", nz(logOutput));
        AtomicFileWriter.writeProperties(fileIn(recordingDir),
                p, "流量复制：源库语句日志的原始开关值（用于兜底还原）");
    }

    /** 读回；文件不存在或不完整返回 null。 */
    public static TrafficSourceState load(File recordingDir) {
        File f = fileIn(recordingDir);
        if (!f.isFile()) return null;
        Properties p = new Properties();
        try (InputStream in = new FileInputStream(f)) {
            p.load(in);
        } catch (IOException e) {
            logger.warn("源库状态文件读取失败: {}", e.getMessage());
            return null;
        }
        TrafficSourceState s = new TrafficSourceState();
        s.host = p.getProperty("source.host", "");
        s.port = p.getProperty("source.port", "");
        s.username = p.getProperty("source.username", "");
        s.password = CredentialCipher.decrypt(p.getProperty("source.password", ""));
        s.urlParams = p.getProperty("source.url.params", "");
        s.generalLog = p.getProperty("original.general_log", "");
        s.logOutput = p.getProperty("original.log_output", "");
        if (s.host.isEmpty() || s.port.isEmpty()) {
            logger.warn("源库状态文件缺少连接信息，无法兜底还原");
            return null;
        }
        return s;
    }

    /** 还原成功后删除状态文件——留着会让下次扫尾重复去连一个不需要还原的库。 */
    public static void clear(File recordingDir) {
        File f = fileIn(recordingDir);
        if (f.exists() && !f.delete()) {
            logger.warn("源库状态文件删除失败: {}", f.getAbsolutePath());
        }
    }

    /**
     * 连上源库把两个开关还原回去，并清掉轮转残留表。
     *
     * <p>幂等：重复调用没有副作用（本来就是"设成某个确定值"）。
     *
     * @return true 表示确认已还原
     */
    public boolean restore() {
        String url = String.format("jdbc:mysql://%s:%s/?%s&connectTimeout=10000&socketTimeout=30000",
                host, port, urlParams == null || urlParams.isEmpty() ? "useSSL=false" : urlParams);
        try (Connection c = DriverManager.getConnection(url, username, password);
             Statement st = c.createStatement()) {
            // 还原动作本身没必要进语句日志
            st.execute("SET SESSION sql_log_off = 1");
            boolean wasOn = "1".equals(generalLog) || "ON".equalsIgnoreCase(generalLog);
            st.execute("SET GLOBAL general_log = " + (wasOn ? "'ON'" : "'OFF'"));
            if (logOutput != null && !logOutput.isEmpty()) {
                st.execute("SET GLOBAL log_output = '" + logOutput.replace("'", "''") + "'");
            }
            st.execute("DROP TABLE IF EXISTS mysql.general_log_trf_read");
            st.execute("DROP TABLE IF EXISTS mysql.general_log_trf_next");
            logger.info("源库语句日志已兜底还原: {}:{} general_log={}, log_output={}",
                    host, port, wasOn ? "ON" : "OFF", logOutput);
            return true;
        } catch (SQLException e) {
            logger.error("源库语句日志兜底还原失败（{}:{}）——源库可能仍在写语句日志，请人工确认",
                    host, port, e);
            return false;
        }
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
