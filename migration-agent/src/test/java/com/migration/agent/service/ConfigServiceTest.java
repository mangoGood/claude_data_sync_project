package com.migration.agent.service;

import com.migration.agent.model.TaskMessage;
import java.io.File;

public class ConfigServiceTest {
    public static void main(String[] args) {
        try {
            ConfigService configService = new ConfigService();
            
            TaskMessage taskMessage = new TaskMessage();
            taskMessage.setTaskId("test-task-001");
            taskMessage.setSourceConnection("mysql://root:rootpassword@192.168.107.6:3306/myapp_db");
            taskMessage.setTargetConnection("mysql://root:targetpass@192.168.107.7:3306/target_db");
            taskMessage.setMigrationMode("fullAndIncre");
            
            System.out.println("测试配置文件更新功能");
            System.out.println("源库连接串: " + taskMessage.getSourceConnection());
            System.out.println("目标库连接串: " + taskMessage.getTargetConnection());
            System.out.println();
            
            configService.updateConfig(taskMessage);
            
            System.out.println("\n配置文件更新成功！");
            
            File configFile = new File("../config.properties");
            System.out.println("配置文件路径: " + configFile.getAbsolutePath());
            System.out.println("配置文件存在: " + configFile.exists());
            
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
