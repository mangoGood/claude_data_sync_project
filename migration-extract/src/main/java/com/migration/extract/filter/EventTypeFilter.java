package com.migration.extract.filter;

import com.migration.thl.THLEvent;
import com.migration.thl.pipeline.AbstractFilter;
import com.migration.thl.pipeline.PipelineContext;

import java.util.*;

public class EventTypeFilter extends AbstractFilter {

    private String skipTypes;
    private String allowTypes;

    private final Set<String> skipTypeSet = new HashSet<>();
    private final Set<String> allowTypeSet = new HashSet<>();

    public void setSkipTypes(String skipTypes) {
        this.skipTypes = skipTypes;
    }

    public void setAllowTypes(String allowTypes) {
        this.allowTypes = allowTypes;
    }

    @Override
    public void configure(PipelineContext context) {
        super.configure(context);

        if (skipTypes != null && !skipTypes.trim().isEmpty()) {
            for (String type : skipTypes.split(",")) {
                skipTypeSet.add(type.trim().toUpperCase());
            }
            logger.info("EventTypeFilter skip types: {}", skipTypeSet);
        }

        if (allowTypes != null && !allowTypes.trim().isEmpty()) {
            for (String type : allowTypes.split(",")) {
                allowTypeSet.add(type.trim().toUpperCase());
            }
            logger.info("EventTypeFilter allow types: {}", allowTypeSet);
        }
    }

    @Override
    public THLEvent filter(THLEvent event) {
        if (event == null) return null;

        Map<String, Object> metadata = event.getMetadata();
        String eventType = (String) metadata.getOrDefault("event_type", "");

        if (eventType.isEmpty()) {
            return event;
        }

        String typeUpper = eventType.toUpperCase();

        if (!skipTypeSet.isEmpty() && matchesType(skipTypeSet, typeUpper)) {
            logger.debug("Event skipped by EventTypeFilter: type={} seqno={}", eventType, event.getSeqno());
            return null;
        }

        if (!allowTypeSet.isEmpty() && !matchesType(allowTypeSet, typeUpper)) {
            logger.debug("Event filtered by EventTypeFilter 'allow' rule: type={} seqno={}", eventType, event.getSeqno());
            return null;
        }

        return event;
    }

    private boolean matchesType(Set<String> typeSet, String eventType) {
        if (typeSet.contains(eventType)) {
            return true;
        }
        for (String type : typeSet) {
            if (eventType.contains(type)) {
                return true;
            }
        }
        return false;
    }
}
