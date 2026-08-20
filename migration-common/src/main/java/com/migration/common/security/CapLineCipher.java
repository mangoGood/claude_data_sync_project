package com.migration.common.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Properties;

/**
 * {@code .cap} 文件的<b>逐行</b>加密。
 *
 * <h3>为什么是逐行而不是整文件</h3>
 * <p>{@code .cap} 是行式文本：capture 每条事件写一行（{@code FIELD_SEP=''} 分字段、
 * {@code RECORD_SEP='\n'} 分记录），extract 用 {@code readLine()} 逐行读并按
 * <b>已读行数</b> 记断点。整文件或分块加密都会打断这个语义；逐行加密则保持
 * <b>一行一条记录</b>不变——行数、{@code readLine()}、以及"文件是否以换行结尾"
 * 那条半行检测（extract 唯一用到字节偏移的地方）全都原样成立。
 *
 * <p>这一点很要紧：仓库里"{@code .cap} 半行"是修过的静默丢数缺陷，
 * 任何动摇行边界的改法都在拿最脆弱的续传逻辑冒险。
 *
 * <h3>格式</h3>
 * <pre>
 *   明文行：  ROTATEbinlog.000035172573…
 *   密文行：  BASE64( iv[12] || ct+tag )
 * </pre>
 *
 * <p>行首标记用 {@code }（STX）：明文行的首字符恒为事件类型名的首字母
 * （ROTATE / QUERY / WRITE_ROWS…，均为大写 ASCII），不会与之歧义。
 * 用行首标记而不是文件头，是因为文件头会让 extract 的行计数整体偏移一位——
 * 升级瞬间所有存量任务的断点都会错。带标记则<b>同一个文件里明文行与密文行可以共存</b>，
 * 升级前写的行照读，升级后写的行解密，断点完全不受影响。
 *
 * <h3>密钥</h3>
 * <p>与 THL 加密同源：显式 {@code capture.encryption.password}，否则回落到
 * {@code SYNCTASK_MASTER_KEY}。capture 与 extract 是两个子进程，共享同一份
 * {@code config.properties} 与同一个主密钥环境变量，因此能各自派生出同一把密钥。
 */
public final class CapLineCipher {

    private static final Logger logger = LoggerFactory.getLogger(CapLineCipher.class);

    /** 密文行的行首标记。明文行首字符恒为大写 ASCII 事件类型名，不会撞。 */
    public static final char ENC_MARK = '';

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_BITS = 128;
    private static final int GCM_IV_LENGTH = 12;
    private static final int KEY_LENGTH = 32;
    static final int PBKDF2_ITERATIONS = 310_000;

    private static final SecureRandom RANDOM = new SecureRandom();

    private final boolean enabled;
    private final SecretKey key;

    /**
     * @param props 任务配置。{@code capture.encryption.enabled} 打开；未显式给出时
     *              继承 {@code thl.encryption.enabled}——"落盘加密"对使用者是一个概念，
     *              不该出现"THL 加密了、.cap 没加密"这种只有读代码才知道的差异。
     */
    public CapLineCipher(Properties props) {
        String explicit = props.getProperty("capture.encryption.enabled");
        this.enabled = Boolean.parseBoolean(
                explicit != null ? explicit : props.getProperty("thl.encryption.enabled", "false"));
        if (!enabled) {
            this.key = null;
            return;
        }
        String password = props.getProperty("capture.encryption.password", "");
        if (password.isEmpty()) {
            password = props.getProperty("thl.encryption.password", "");
        }
        if (password.isEmpty()) {
            password = System.getenv("SYNCTASK_MASTER_KEY");
            if (password == null || password.isEmpty()) {
                password = System.getProperty("synctask.master.key", "");
            }
            if (password == null || password.isEmpty()) {
                throw new IllegalStateException(
                        "capture 加密已启用，但既无 capture.encryption.password / thl.encryption.password，"
                                + "也没有 SYNCTASK_MASTER_KEY。拒绝用可预测的密钥加密。");
            }
        }
        this.key = derive(password);
        logger.info("CapLineCipher 初始化 | enabled=true | AES-256-GCM 逐行");
    }

    private static SecretKey derive(String password) {
        try {
            byte[] salt = MessageDigest.getInstance("SHA-256")
                    .digest("synctask-cap-encryption-v1".getBytes(StandardCharsets.UTF_8));
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, PBKDF2_ITERATIONS, KEY_LENGTH * 8);
            byte[] bytes = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(spec).getEncoded();
            spec.clearPassword();
            return new SecretKeySpec(bytes, "AES");
        } catch (Exception e) {
            throw new IllegalStateException("cap 密钥派生失败", e);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * 加密一条记录（<b>不含</b>行尾换行）。未启用时原样返回。
     */
    public String encryptRecord(String plainRecord) {
        if (!enabled || plainRecord == null) {
            return plainRecord;
        }
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            RANDOM.nextBytes(iv);
            Cipher c = Cipher.getInstance(TRANSFORMATION);
            c.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ct = c.doFinal(plainRecord.getBytes(StandardCharsets.UTF_8));
            ByteBuffer buf = ByteBuffer.allocate(iv.length + ct.length);
            buf.put(iv).put(ct);
            return ENC_MARK + Base64.getEncoder().encodeToString(buf.array());
        } catch (Exception e) {
            throw new IllegalStateException("cap 记录加密失败", e);
        }
    }

    /**
     * 解密一行。<b>没有行首标记的行原样返回</b>——同一个文件里明文行与密文行可以共存
     * （升级瞬间的那个文件就是这样），这也是不用文件头的原因。
     *
     * <p>是静态方法：读侧要能在"本任务没开加密、但文件里有历史密文行"这种情况下
     * 也正确工作，因此判定只看行本身，不看配置。
     */
    public String decryptLine(String line) {
        if (line == null || line.isEmpty() || line.charAt(0) != ENC_MARK) {
            return line;
        }
        if (key == null) {
            throw new IllegalStateException(
                    "读到加密的 .cap 行，但本进程未启用 capture 加密（或缺密钥）。"
                            + "请确认 capture.encryption.* 与 SYNCTASK_MASTER_KEY 与写入时一致。");
        }
        try {
            ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(line.substring(1)));
            byte[] iv = new byte[GCM_IV_LENGTH];
            buf.get(iv);
            byte[] ct = new byte[buf.remaining()];
            buf.get(ct);
            Cipher c = Cipher.getInstance(TRANSFORMATION);
            c.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(c.doFinal(ct), StandardCharsets.UTF_8);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            // 不能静默跳过：解不开就是密钥不对或文件损坏，继续读下去只会把错位的
            // 字节当成业务数据（本仓库 §silent-loss 反复吃过这个亏）
            throw new IllegalStateException("cap 行解密失败（密钥不匹配或文件损坏）", e);
        }
    }

    /** 该行是否是密文行。供读侧在无密钥时给出更准的报错。 */
    public static boolean isEncryptedLine(String line) {
        return line != null && !line.isEmpty() && line.charAt(0) == ENC_MARK;
    }
}
