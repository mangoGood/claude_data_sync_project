package com.synctask.controller;

import com.synctask.entity.AuditLog;
import com.synctask.audit.Audited;
import com.synctask.entity.DbCertificate;
import com.synctask.security.UserPrincipal;
import com.synctask.service.CertificateService;
import com.synctask.util.CertMaterial;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库连接的 TLS 证书库。
 *
 * <p>上传走 multipart（用户手上是 .pem 文件，让他打开文本编辑器复制粘贴是没必要的摩擦），
 * 但正文一律按 UTF-8 文本解析——PEM 本来就是文本格式，二进制 DER 在这里直接拒绝，
 * 与其让它在建连时报一句难懂的错，不如上传就说清楚。
 *
 * <p><b>响应中永远不含私钥</b>，连密文也不含（见 {@code CertificateService#toSafeMap}）。
 */
@RestController
@RequestMapping("/api/certificates")
public class CertificateController {

    private static final Logger logger = LoggerFactory.getLogger(CertificateController.class);

    @Autowired
    private CertificateService certificateService;

    @PostMapping
    @Audited(AuditLog.Action.UPLOAD_CERTIFICATE)
    public ResponseEntity<?> upload(
            @RequestParam("name") String name,
            @RequestParam(value = "caCert", required = false) MultipartFile caCert,
            @RequestParam(value = "clientCert", required = false) MultipartFile clientCert,
            @RequestParam(value = "clientKey", required = false) MultipartFile clientKey,
            Authentication authentication) {
        UserPrincipal principal = (UserPrincipal) authentication.getPrincipal();
        try {
            DbCertificate cert = certificateService.upload(
                    principal.getId(), name,
                    readPem(caCert, "CA 证书"),
                    readPem(clientCert, "客户端证书"),
                    readPem(clientKey, "客户端私钥"));
            return ResponseEntity.ok(ok("证书上传成功", certificateService.toSafeMap(cert)));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        } catch (Exception e) {
            logger.error("证书上传失败", e);
            return ResponseEntity.badRequest().body(fail("证书上传失败: " + e.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<?> list(Authentication authentication) {
        UserPrincipal principal = (UserPrincipal) authentication.getPrincipal();
        List<Map<String, Object>> certs = certificateService.list(principal.getId());
        return ResponseEntity.ok(ok(null, certs));
    }

    @GetMapping("/expiry")
    public ResponseEntity<?> expiry(Authentication authentication) {
        UserPrincipal principal = (UserPrincipal) authentication.getPrincipal();
        return ResponseEntity.ok(ok(null, certificateService.expiryReport(principal.getId())));
    }

    @DeleteMapping("/{id}")
    @Audited(value = AuditLog.Action.DELETE_CERTIFICATE, idArg = 0)
    public ResponseEntity<?> delete(@PathVariable String id, Authentication authentication) {
        UserPrincipal principal = (UserPrincipal) authentication.getPrincipal();
        try {
            certificateService.delete(id, principal.getId());
            return ResponseEntity.ok(ok("证书已删除", null));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(fail(e.getMessage()));
        }
    }

    /**
     * 读 multipart 内容为 PEM 文本。大小上限在这里也判一次——Spring 的 multipart 上限
     * 是整个请求的，单个文件超大时报的是一句框架异常，不如在这里说清楚是哪一份。
     */
    private static String readPem(MultipartFile file, String what) throws IOException {
        if (file == null || file.isEmpty()) {
            return null;
        }
        if (file.getSize() > CertMaterial.MAX_PEM_BYTES) {
            throw new IllegalArgumentException(
                    what + "过大（" + file.getSize() + " 字节，上限 " + CertMaterial.MAX_PEM_BYTES + "）");
        }
        String text = new String(file.getBytes(), StandardCharsets.UTF_8);
        if (!text.contains("-----BEGIN ")) {
            throw new IllegalArgumentException(
                    what + "不是 PEM 格式（未找到 -----BEGIN ... 标记）。若手上是 DER/二进制格式，请先转换：\n"
                            + "  openssl x509 -inform der -in cert.der -out cert.pem");
        }
        return text;
    }

    private static Map<String, Object> ok(String message, Object data) {
        Map<String, Object> m = new HashMap<>();
        m.put("success", true);
        if (message != null) {
            m.put("message", message);
        }
        m.put("data", data);
        return m;
    }

    private static Map<String, Object> fail(String message) {
        Map<String, Object> m = new HashMap<>();
        m.put("success", false);
        m.put("message", message);
        return m;
    }
}
