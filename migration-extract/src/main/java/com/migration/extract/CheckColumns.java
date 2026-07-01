package com.migration.extract;

import com.migration.thl.*;
import java.util.*;

public class CheckColumns {
    public static void main(String[] args) throws Exception {
        String thlFile = args.length > 0 ? args[0] : "../output/thl/thl-1776492252660.thl";
        THLFileReader reader = new THLFileReader(thlFile);
        THLEvent event;
        int writeCount = 0;
        while ((event = reader.readEvent()) != null) {
            String eventType = (String) event.getMetadata("event_type");
            if ("EXT_WRITE_ROWS".equals(eventType) || "WRITE_ROWS".equals(eventType)) {
                String columns = (String) event.getMetadata("column_names");
                if (writeCount < 3) {
                    System.out.println("Event seqno=" + event.getSeqno() + " type=" + eventType);
                    System.out.println("column_names: " + columns);
                    if (columns != null) {
                        String[] cols = columns.split(",");
                        System.out.println("Column count: " + cols.length);
                        for (int i = 0; i < cols.length; i++) {
                            System.out.println("  col[" + i + "]: " + cols[i]);
                        }
                    } else {
                        System.out.println("column_names is NULL!");
                    }
                    Object rows = event.getMetadata("rows");
                    if (rows instanceof List) {
                        List<?> rowList = (List<?>) rows;
                        if (!rowList.isEmpty()) {
                            Object[] firstRow = (Object[]) rowList.get(0);
                            System.out.println("Row value count: " + firstRow.length);
                            for (int i = 0; i < firstRow.length; i++) {
                                String type = firstRow[i] == null ? "null" : firstRow[i].getClass().getSimpleName();
                                String val = firstRow[i] == null ? "NULL" : (firstRow[i] instanceof byte[] ? "byte[" + ((byte[])firstRow[i]).length + "]" : firstRow[i].toString());
                                if (val.length() > 60) val = val.substring(0, 60) + "...";
                                System.out.println("  val[" + i + "]: (" + type + ") " + val);
                            }
                        }
                    }
                    System.out.println();
                }
                writeCount++;
            }
        }
        System.out.println("Total WRITE_ROWS events found: " + writeCount);
        reader.close();
    }
}
