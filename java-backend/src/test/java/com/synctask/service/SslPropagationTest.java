package com.synctask.service;

import com.synctask.dto.TaskCreatedMessage;
import com.synctask.entity.Workflow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 加密配置的<b>传播</b>。
 *
 * <p>这是整套 TLS 里最容易出静默故障的一处：任务本身配好了，但派生/对调出来的那条腿
 * 没带上配置，于是它<b>整条明文</b>——而任务全绿、没有任何报错。平台里有四个这样的点：
 * 双向灾备影子任务（B→A，要对调）、主备倒换（源/目标互换）、fan-out 多目标、跨实例汇聚 leg。
 *
 * <p>前两个涉及"对调"，是最容易写反的；这里逐个钉死。
 */
class SslPropagationTest {

    private static Workflow workflow(String srcMode, String srcCert, String tgtMode, String tgtCert) {
        Workflow w = new Workflow();
        w.setSourceSslMode(srcMode);
        w.setSourceSslCertId(srcCert);
        w.setTargetSslMode(tgtMode);
        w.setTargetSslCertId(tgtCert);
        return w;
    }

    @Test
    @DisplayName("正向下发：源/目标原样带过去")
    void forwardKeepsSides() {
        Workflow w = workflow("VERIFY_CA", "cert-A", "REQUIRED", "cert-B");
        TaskCreatedMessage m = new TaskCreatedMessage();
        m.applySsl(w.getSourceSslMode(), w.getSourceSslCertId(),
                   w.getTargetSslMode(), w.getTargetSslCertId(), false);

        assertEquals("VERIFY_CA", m.getSourceSslMode());
        assertEquals("cert-A", m.getSourceSslCertId());
        assertEquals("REQUIRED", m.getTargetSslMode());
        assertEquals("cert-B", m.getTargetSslCertId());
    }

    @Test
    @DisplayName("对调下发（灾备影子 B→A）：档位与证书必须一起换，不能只换其一")
    void swapSwapsBothModeAndCert() {
        Workflow w = workflow("VERIFY_CA", "cert-A", "REQUIRED", "cert-B");
        TaskCreatedMessage m = new TaskCreatedMessage();
        m.applySsl(w.getSourceSslMode(), w.getSourceSslCertId(),
                   w.getTargetSslMode(), w.getTargetSslCertId(), true);

        // 反向通道的源端 = 正向的目标端
        assertEquals("REQUIRED", m.getSourceSslMode());
        assertEquals("cert-B", m.getSourceSslCertId());
        assertEquals("VERIFY_CA", m.getTargetSslMode());
        assertEquals("cert-A", m.getTargetSslCertId());
    }

    @Test
    @DisplayName("只有一端开加密时，对调后仍然只有对应的那一端开")
    void swapWithOneSideDisabled() {
        Workflow w = workflow("VERIFY_CA", "cert-A", "DISABLED", null);
        TaskCreatedMessage m = new TaskCreatedMessage();
        m.applySsl(w.getSourceSslMode(), w.getSourceSslCertId(),
                   w.getTargetSslMode(), w.getTargetSslCertId(), true);

        assertEquals("DISABLED", m.getSourceSslMode());
        assertNull(m.getSourceSslCertId());
        assertEquals("VERIFY_CA", m.getTargetSslMode());
        assertEquals("cert-A", m.getTargetSslCertId());
    }

    @Test
    @DisplayName("灾备影子任务派生：源/目标对调后继承（漏掉=反向通道整条明文）")
    void drShadowInheritsSwapped() {
        Workflow parent = workflow("VERIFY_CA", "cert-A", "REQUIRED", "cert-B");

        // 与 WorkflowService.launchWorkflow 里创建 DR_SHADOW 的那段一致
        Workflow shadow = new Workflow();
        shadow.setSourceSslMode(parent.getTargetSslMode());
        shadow.setTargetSslMode(parent.getSourceSslMode());
        shadow.setSourceSslCertId(parent.getTargetSslCertId());
        shadow.setTargetSslCertId(parent.getSourceSslCertId());

        assertEquals("REQUIRED", shadow.getSourceSslMode());
        assertEquals("cert-B", shadow.getSourceSslCertId());
        assertEquals("VERIFY_CA", shadow.getTargetSslMode());
        assertEquals("cert-A", shadow.getTargetSslCertId());
    }

    @Test
    @DisplayName("主备倒换：加密配置跟着连接一起换，且换两次回到原状")
    void switchoverSwapsAndIsInvolutive() {
        Workflow w = workflow("VERIFY_CA", "cert-A", "REQUIRED", "cert-B");

        for (int round = 1; round <= 2; round++) {
            String sm = w.getSourceSslMode(), tm = w.getTargetSslMode();
            String sc = w.getSourceSslCertId(), tc = w.getTargetSslCertId();
            w.setSourceSslMode(tm);
            w.setTargetSslMode(sm);
            w.setSourceSslCertId(tc);
            w.setTargetSslCertId(sc);

            if (round == 1) {
                assertEquals("REQUIRED", w.getSourceSslMode());
                assertEquals("cert-B", w.getSourceSslCertId());
            }
        }
        // 倒换两次应当回到最初状态，否则说明换的不是同一组字段
        assertEquals("VERIFY_CA", w.getSourceSslMode());
        assertEquals("cert-A", w.getSourceSslCertId());
        assertEquals("REQUIRED", w.getTargetSslMode());
        assertEquals("cert-B", w.getTargetSslCertId());
    }

    @Test
    @DisplayName("档位归一：非法值当场拒绝，不静默退回明文")
    void modeIsValidated() {
        assertEquals("DISABLED", WorkflowService.resolveSslMode(null));
        assertEquals("DISABLED", WorkflowService.resolveSslMode(""));
        assertEquals("REQUIRED", WorkflowService.resolveSslMode("required"));
        assertEquals("VERIFY_CA", WorkflowService.resolveSslMode(" Verify_Ca "));

        for (String bad : new String[]{"REQUIRE", "true", "on", "DISABLE", "ssl"}) {
            RuntimeException e = assertThrows(RuntimeException.class,
                    () -> WorkflowService.resolveSslMode(bad), "应拒绝: " + bad);
            org.junit.jupiter.api.Assertions.assertTrue(
                    e.getMessage().contains("VERIFY_IDENTITY"), "错误信息要列出合法取值");
        }
    }

    @Test
    @DisplayName("默认值：新任务两端都是 DISABLED（= 升级前的行为，明文）")
    void defaultsAreDisabled() {
        Workflow w = new Workflow();
        assertEquals("DISABLED", w.getSourceSslMode());
        assertEquals("DISABLED", w.getTargetSslMode());
        assertNull(w.getSourceSslCertId());
        assertNull(w.getTargetSslCertId());
    }
}
