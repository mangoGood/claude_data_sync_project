package com.synctask.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

/**
 * 数据库连接的 TLS 证书。
 *
 * <p>一套证书通常要给多个任务用（同一个数据库集群），所以做成独立实体而不是任务的内嵌字段——
 * 复制粘贴 N 份的结果必然是"换了证书但只换了其中三个任务"。
 *
 * <p>私钥与连接串口令走同一套 AES-GCM 落库加密（{@code SYNCTASK_MASTER_KEY}）。
 * 公开证书（CA / 客户端证书）本身不是秘密，不加密，便于运维直接在库里核对。
 */
@Entity
@Table(name = "db_certificates")
public class DbCertificate {

    @Id
    @Column(length = 36)
    private String id;

    @Column(nullable = false, length = 128)
    private String name;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "ca_cert", columnDefinition = "TEXT")
    private String caCert;

    @Column(name = "client_cert", columnDefinition = "TEXT")
    private String clientCert;

    /** 私钥：落库前 AES-GCM 加密，读取自动解密。 */
    @Column(name = "client_key", columnDefinition = "TEXT")
    @Convert(converter = com.synctask.security.EncryptedStringConverter.class)
    private String clientKey;

    @Column(name = "subject_cn")
    private String subjectCn;

    @Column(length = 128)
    private String fingerprint;

    @Column(name = "not_after")
    private LocalDateTime notAfter;

    @Column(name = "has_client_cert")
    private Boolean hasClientCert = false;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getCaCert() { return caCert; }
    public void setCaCert(String caCert) { this.caCert = caCert; }
    public String getClientCert() { return clientCert; }
    public void setClientCert(String clientCert) { this.clientCert = clientCert; }
    public String getClientKey() { return clientKey; }
    public void setClientKey(String clientKey) { this.clientKey = clientKey; }
    public String getSubjectCn() { return subjectCn; }
    public void setSubjectCn(String subjectCn) { this.subjectCn = subjectCn; }
    public String getFingerprint() { return fingerprint; }
    public void setFingerprint(String fingerprint) { this.fingerprint = fingerprint; }
    public LocalDateTime getNotAfter() { return notAfter; }
    public void setNotAfter(LocalDateTime notAfter) { this.notAfter = notAfter; }
    public Boolean getHasClientCert() { return hasClientCert; }
    public void setHasClientCert(Boolean hasClientCert) { this.hasClientCert = hasClientCert; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
