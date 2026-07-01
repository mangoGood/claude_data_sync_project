package com.migration.increment;

import java.io.FileInputStream;
import java.util.Properties;

public class THLToSqlConverterMain {
    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            if (args.length > 0) {
                props.load(new FileInputStream(args[0]));
            } else {
                props.load(THLToSqlConverterMain.class.getClassLoader().getResourceAsStream("increment.properties"));
            }

            THLToSqlConverter converter = new THLToSqlConverter(props);
            converter.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
