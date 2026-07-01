package com.migration.increment.filter;

import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.AbstractFilter;
import com.migration.thl.pipeline.PipelineContext;

import java.util.Map;

public class OptimizeUpdateFilter extends AbstractFilter {

    private boolean enabled = true;

    public void setEnabled(String enabled) {
        this.enabled = Boolean.parseBoolean(enabled);
    }

    @Override
    public void configure(PipelineContext context) {
        super.configure(context);
        logger.info("OptimizeUpdateFilter enabled: {}", enabled);
    }

    @Override
    public THLEvent filter(THLEvent event) {
        if (event == null || !enabled) return event;

        Map<String, Object> metadata = event.getMetadata();
        String eventType = (String) metadata.getOrDefault("event_type", "");

        if (!"UPDATE".equalsIgnoreCase(eventType)) {
            return event;
        }

        String columns = (String) metadata.get("columns");
        String oldValues = (String) metadata.get("old_values");
        String newValues = (String) metadata.get("new_values");

        if (columns == null || oldValues == null || newValues == null) {
            return event;
        }

        String[] colArr = columns.split(",");
        String[] oldArr = oldValues.split("\\|");
        String[] newArr = newValues.split("\\|");

        if (colArr.length != oldArr.length || colArr.length != newArr.length) {
            return event;
        }

        StringBuilder optColumns = new StringBuilder();
        StringBuilder optNewValues = new StringBuilder();

        int removedCount = 0;
        for (int i = 0; i < colArr.length; i++) {
            String oldVal = oldArr[i].trim();
            String newVal = newArr[i].trim();

            if (oldVal.equals(newVal)) {
                removedCount++;
                continue;
            }

            if (optColumns.length() > 0) {
                optColumns.append(",");
                optNewValues.append("|");
            }
            optColumns.append(colArr[i].trim());
            optNewValues.append(newVal);
        }

        if (removedCount > 0) {
            metadata.put("columns", optColumns.toString());
            metadata.put("new_values", optNewValues.toString());
            logger.debug("Optimized UPDATE: removed {} unchanged columns (seqno={})", removedCount, event.getSeqno());
        }

        return event;
    }
}
