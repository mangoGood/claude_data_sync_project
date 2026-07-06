package com.synctask;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableAsync
public class SyncTaskApplication {
    public static void main(String[] args) {
        // Oracle thin 驱动 NIO 栈经 OrbStack 端口转发握手即断(ORA-17800)，关闭 NIO（见 migration-common OracleNetCompat）
        if (System.getProperty("oracle.jdbc.javaNetNio") == null) {
            System.setProperty("oracle.jdbc.javaNetNio", "false");
        }
        SpringApplication.run(SyncTaskApplication.class, args);
    }
}
