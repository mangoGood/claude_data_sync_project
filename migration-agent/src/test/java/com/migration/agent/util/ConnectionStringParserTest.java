package com.migration.agent.util;

public class ConnectionStringParserTest {
    public static void main(String[] args) {
        String testConnectionString = "mysql://root:rootpassword@192.168.107.6:3306/myapp_db";
        
        System.out.println("测试连接串解析功能");
        System.out.println("输入: " + testConnectionString);
        System.out.println();
        
        try {
            ConnectionStringParser.ConnectionInfo info = ConnectionStringParser.parse(testConnectionString);
            
            System.out.println("解析结果:");
            System.out.println("  Host: " + info.getHost());
            System.out.println("  Port: " + info.getPort());
            System.out.println("  Database: " + info.getDatabase());
            System.out.println("  Username: " + info.getUsername());
            System.out.println("  Password: " + info.getPassword());
            System.out.println();
            System.out.println("安全输出: " + info);
            
        } catch (Exception e) {
            System.err.println("解析失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
