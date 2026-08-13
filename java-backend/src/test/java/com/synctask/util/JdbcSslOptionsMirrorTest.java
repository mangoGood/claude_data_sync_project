package com.synctask.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link JdbcSslOptions} 是数据面 {@code com.migration.common.ssl.SslMaterial} 的镜像
 * （后端不依赖 migration-common，理由见 JdbcSslOptions 类注释）。镜像的风险是<b>漂移</b>：
 * 两侧对同一档位给出不同的 URL 参数，而这类差异不会有任何报错——只会表现为
 * "控制面加密了、数据面没有"或反过来，而两条路读的是同一批业务数据。
 *
 * <p>因此这里把期望值写成<b>字面量表格</b>而不是调用另一份实现：表格与
 * {@code SslMaterialTest} 里的那份是同一份口径，任何一侧改动都会在这里或那里断掉。
 */
class JdbcSslOptionsMirrorTest {

    /** {档位, MySQL 参数段, PG 参数段}（无证书）。 */
    private static final String[][] NO_CERT = {
            {"DISABLED",        "useSSL=false",             "sslmode=disable"},
            {"PREFERRED",       "sslMode=PREFERRED",        "sslmode=prefer"},
            {"REQUIRED",        "sslMode=REQUIRED",         "sslmode=require"},
            {"VERIFY_CA",       "sslMode=VERIFY_CA",        "sslmode=verify-ca"},
            {"VERIFY_IDENTITY", "sslMode=VERIFY_IDENTITY",  "sslmode=verify-full"},
    };

    private static final String CERT = "/etc/ssl/ca.pem";

    /** 同上，但带证书。DISABLED 下证书要被忽略——关掉就是关掉。 */
    private static final String[][] WITH_CERT = {
            {"DISABLED",  "useSSL=false", "sslmode=disable"},
            {"PREFERRED", "sslMode=PREFERRED&trustCertificateKeyStoreUrl=file:" + CERT,
                          "sslmode=prefer&sslrootcert=" + CERT},
            {"REQUIRED",  "sslMode=REQUIRED&trustCertificateKeyStoreUrl=file:" + CERT,
                          "sslmode=require&sslrootcert=" + CERT},
            {"VERIFY_CA", "sslMode=VERIFY_CA&trustCertificateKeyStoreUrl=file:" + CERT,
                          "sslmode=verify-ca&sslrootcert=" + CERT},
            {"VERIFY_IDENTITY", "sslMode=VERIFY_IDENTITY&trustCertificateKeyStoreUrl=file:" + CERT,
                          "sslmode=verify-full&sslrootcert=" + CERT},
    };

    @Test
    @DisplayName("全档位参数段与数据面口径一致（无证书）")
    void matchesDataPlaneWithoutCert() {
        for (String[] row : NO_CERT) {
            assertEquals(row[1], JdbcSslOptions.mysql(row[0], ""), "mysql/" + row[0]);
            assertEquals(row[2], JdbcSslOptions.postgres(row[0], ""), "pg/" + row[0]);
        }
    }

    @Test
    @DisplayName("全档位参数段与数据面口径一致（带证书）")
    void matchesDataPlaneWithCert() {
        for (String[] row : WITH_CERT) {
            assertEquals(row[1], JdbcSslOptions.mysql(row[0], CERT), "mysql/" + row[0]);
            assertEquals(row[2], JdbcSslOptions.postgres(row[0], CERT), "pg/" + row[0]);
        }
    }

    @Test
    @DisplayName("未配置 = DISABLED = 历史行为")
    void unsetIsDisabled() {
        assertEquals("DISABLED", JdbcSslOptions.normalizeMode(null));
        assertEquals("DISABLED", JdbcSslOptions.normalizeMode(""));
        assertEquals("DISABLED", JdbcSslOptions.normalizeMode("   "));
    }

    @Test
    @DisplayName("非法档位构造即抛，不静默退回明文")
    void invalidModeThrows() {
        for (String bad : new String[]{"REQUIRE", "true", "on", "DISABLE"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> JdbcSslOptions.normalizeMode(bad), "应拒绝: " + bad);
            assertTrue(e.getMessage().contains("VERIFY_IDENTITY"));
        }
    }

    @Test
    @DisplayName("大小写归一")
    void caseInsensitive() {
        assertEquals("REQUIRED", JdbcSslOptions.normalizeMode("required"));
        assertEquals("VERIFY_CA", JdbcSslOptions.normalizeMode(" Verify_Ca "));
    }

    @Test
    @DisplayName("p12 信任库声明类型，否则驱动按 JKS 读会失败")
    void pkcs12TypeDeclared() {
        assertTrue(JdbcSslOptions.mysql("VERIFY_CA", "/c/truststore.p12")
                           .contains("trustCertificateKeyStoreType=PKCS12"));
    }
}
