package com.migration.increment.filter;

import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.AbstractFilter;
import com.migration.thl.pipeline.PipelineContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class TableRenameFilter extends AbstractFilter {

    private String mapping;
    private String mappingFile;
    private final Map<String, String> renameMap = new HashMap<>();

    public void setMapping(String mapping) {
        this.mapping = mapping;
    }

    public void setMappingFile(String mappingFile) {
        this.mappingFile = mappingFile;
    }

    @Override
    public void configure(PipelineContext context) {
        super.configure(context);

        if (mapping != null && !mapping.trim().isEmpty()) {
            parseInlineMapping(mapping);
        }

        if (mappingFile != null && !mappingFile.trim().isEmpty()) {
            loadMappingFromFile(mappingFile);
        }

        if (!renameMap.isEmpty()) {
            logger.info("TableRenameFilter mapping loaded: {} entries", renameMap.size());
        }
    }

    private void parseInlineMapping(String mapping) {
        String[] pairs = mapping.split(",");
        for (String pair : pairs) {
            pair = pair.trim();
            if (pair.isEmpty()) continue;

            String[] kv = pair.split(":");
            if (kv.length == 2) {
                String from = kv[0].trim();
                String to = kv[1].trim();
                renameMap.put(from, to);
            }
        }
    }

    private void loadMappingFromFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.warn("Table rename mapping file not found: {}", filePath);
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                String[] kv = line.split("[:=]");
                if (kv.length == 2) {
                    String from = kv[0].trim();
                    String to = kv[1].trim();
                    renameMap.put(from, to);
                }
            }
            logger.info("Loaded {} table rename mappings from file: {}", renameMap.size(), filePath);
        } catch (Exception e) {
            logger.error("Failed to load table rename mapping file: {}", filePath, e);
        }
    }

    @Override
    public THLEvent filter(THLEvent event) {
        if (event == null || renameMap.isEmpty()) return event;

        Map<String, Object> metadata = event.getMetadata();
        String database = (String) metadata.getOrDefault("database_name", "");
        String table = (String) metadata.getOrDefault("table_name", "");

        if (table.isEmpty()) return event;

        String key = database.isEmpty() ? table : database + "." + table;
        String newName = renameMap.get(key);

        if (newName == null) {
            newName = renameMap.get(table);
        }

        if (newName != null) {
            String newTable;
            if (newName.contains(".")) {
                String[] parts = newName.split("\\.", 2);
                metadata.put("database_name", parts[0]);
                newTable = parts[1];
            } else {
                newTable = newName;
            }

            logger.debug("Renaming table: {} -> {} (seqno={})", key, newName, event.getSeqno());
            metadata.put("table_name", newTable);
        }

        return event;
    }
}
