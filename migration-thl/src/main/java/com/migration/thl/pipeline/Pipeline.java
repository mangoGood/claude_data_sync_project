package com.migration.thl.pipeline;

import com.migration.thl.THLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Pipeline {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    private final List<PipelineFilter> filters;
    private final PipelineContext context;
    private boolean prepared = false;

    private long totalProcessed = 0;
    private long totalFiltered = 0;

    public Pipeline(List<PipelineFilter> filters, PipelineContext context) {
        this.filters = filters;
        this.context = context;
    }

    public void prepare() {
        if (prepared) {
            logger.warn("Pipeline already prepared");
            return;
        }

        logger.info("Preparing pipeline with {} filters", filters.size());
        for (PipelineFilter filter : filters) {
            try {
                filter.prepare(context);
                logger.info("Filter prepared: {} (priority={})", filter.getName(), filter.getPriority());
            } catch (Exception e) {
                logger.error("Failed to prepare filter: {}", filter.getName(), e);
                throw new RuntimeException("Filter prepare failed: " + filter.getName(), e);
            }
        }
        prepared = true;
        logger.info("Pipeline prepared successfully");
    }

    public THLEvent process(THLEvent event) {
        if (!prepared) {
            logger.warn("Pipeline not prepared, skipping filter chain");
            return event;
        }

        if (event == null) {
            return null;
        }

        totalProcessed++;

        THLEvent current = event;
        for (PipelineFilter filter : filters) {
            if (current == null) {
                break;
            }

            try {
                long start = System.nanoTime();
                current = filter.filter(current);
                long elapsed = System.nanoTime() - start;

                if (current == null) {
                    totalFiltered++;
                    logger.debug("Event filtered out by filter: {} (seqno={})", filter.getName(), event.getSeqno());
                    context.incrementCounter("filter." + filter.getName() + ".filtered");
                } else {
                    context.incrementCounter("filter." + filter.getName() + ".passed");
                }

                if (elapsed > 1_000_000) {
                    logger.debug("Filter {} took {}ms for seqno={}", filter.getName(), elapsed / 1_000_000, event.getSeqno());
                }
            } catch (Exception e) {
                logger.error("Filter {} threw exception for seqno={}: {}", filter.getName(), event.getSeqno(), e.getMessage());
                context.incrementCounter("filter." + filter.getName() + ".errors");
            }
        }

        return current;
    }

    public void release() {
        logger.info("Releasing pipeline with {} filters", filters.size());
        for (PipelineFilter filter : filters) {
            try {
                filter.release(context);
                logger.info("Filter released: {}", filter.getName());
            } catch (Exception e) {
                logger.error("Failed to release filter: {}", filter.getName(), e);
            }
        }

        logger.info("Pipeline statistics - Total processed: {}, Filtered: {}, Passed: {}",
                totalProcessed, totalFiltered, totalProcessed - totalFiltered);
    }

    public List<PipelineFilter> getFilters() {
        return filters;
    }

    public PipelineContext getContext() {
        return context;
    }

    public long getTotalProcessed() {
        return totalProcessed;
    }

    public long getTotalFiltered() {
        return totalFiltered;
    }
}
