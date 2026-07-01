package com.migration.increment.filter;

import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.AbstractFilter;
import com.migration.thl.pipeline.PipelineContext;

import java.util.HashMap;
import java.util.Map;

public class DatabaseRenameFilter extends AbstractFilter {

    private String mapping;
    private final Map<String, String> renameMap = new HashMap<>();

    public void setMapping(String mapping) {
        this.mapping = mapping;
    }

    @Override
    public void configure(PipelineContext context) {
        super.configure(context);

        if (mapping != null && !mapping.trim().isEmpty()) {
            String[] pairs = mapping.split(",");
            for (String pair : pairs) {
                pair = pair.trim();
                if (pair.isEmpty()) continue;

                String[] kv = pair.split(":");
                if (kv.length == 2) {
                    renameMap.put(kv[0].trim(), kv[1].trim());
                }
            }
        }

        if (!renameMap.isEmpty()) {
            logger.info("DatabaseRenameFilter mapping: {}", renameMap);
        }
    }

    @Override
    public THLEvent filter(THLEvent event) {
        if (event == null || renameMap.isEmpty()) return event;

        Map<String, Object> metadata = event.getMetadata();
        String database = (String) metadata.getOrDefault("database_name", "");

        if (database.isEmpty()) return event;

        String newDatabase = renameMap.get(database);
        if (newDatabase != null) {
            logger.debug("Renaming database: {} -> {} (seqno={})", database, newDatabase, event.getSeqno());
            metadata.put("database_name", newDatabase);
        }

        return event;
    }
}
