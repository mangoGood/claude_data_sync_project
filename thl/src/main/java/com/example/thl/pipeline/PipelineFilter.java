package com.example.thl.pipeline;

import com.example.thl.THLEvent;

public interface PipelineFilter {

    void configure(PipelineContext context);

    void prepare(PipelineContext context);

    void release(PipelineContext context);

    THLEvent filter(THLEvent event);

    String getName();

    int getPriority();
}
