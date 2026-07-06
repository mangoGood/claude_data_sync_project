package com.migration.common;

/**
 * Oracle thin 驱动网络兼容设置。
 *
 * <p>现代 ojdbc（NIO 网络栈）经部分容器端口转发（如 OrbStack）时，登录握手阶段
 * 连接被对端关闭，报 {@code ORA-17800 / Got minus one from a read call}。
 * 关闭 NIO 栈（{@code oracle.jdbc.javaNetNio=false}）后 ojdbc8 21.x 可正常工作
 * （已在本项目环境实测验证）。
 *
 * <p>在所有可能连接 Oracle 的进程入口调用 {@link #apply()}；若外部已显式设置该属性则不覆盖。
 */
public final class OracleNetCompat {

    private OracleNetCompat() {
    }

    public static void apply() {
        if (System.getProperty("oracle.jdbc.javaNetNio") == null) {
            System.setProperty("oracle.jdbc.javaNetNio", "false");
        }
    }
}
