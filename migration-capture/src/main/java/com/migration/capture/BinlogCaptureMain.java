package com.migration.capture;

import java.io.FileInputStream;
import java.util.Properties;

public class BinlogCaptureMain {
    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            if (args.length > 0) {
                props.load(new FileInputStream(args[0]));
            } else {
                props.load(BinlogCaptureMain.class.getClassLoader().getResourceAsStream("capture.properties"));
            }

            MySQLBinlogCapture capture = new MySQLBinlogCapture();
            capture.initialize(props);
            capture.start();
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.out.println("Shutting down capture...");
                    capture.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
