/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.sinks.SinkAdapter;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.core.iterator.sinks.index.JvsLuceneSink;

import java.nio.file.Path;

/**
 * ServiceLoader glue for {@code kind: jvs-lucene}. The actual type-aware
 * Lucene sink lives in {@code hitorro-index} as {@link JvsLuceneSink}
 * so non-mesh callers (standalone indexers, Spring Boot batches) can
 * use it directly without adopting the mesh pipeline runtime.
 */
public final class JvsLuceneSinkAdapter implements SinkAdapter {

    @Override public boolean handles(SinkSpec spec) { return spec instanceof SinkSpec.JvsLucene; }

    @Override
    public Sink<JsonNode> create(SinkSpec spec, Path home) {
        SinkSpec.JvsLucene s = (SinkSpec.JvsLucene) spec;
        return new JvsLuceneSink(s.name(), s.typeJsonResource(), s.storeSource(), home);
    }
}
