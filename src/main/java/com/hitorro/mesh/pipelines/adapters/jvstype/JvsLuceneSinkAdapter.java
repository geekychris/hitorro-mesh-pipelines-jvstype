/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.sinks.Sink;
import com.hitorro.mesh.pipelines.sinks.SinkAdapter;

import java.nio.file.Path;

/** ServiceLoader-registered sink adapter for {@code kind: jvs-lucene}. */
public final class JvsLuceneSinkAdapter implements SinkAdapter {

    @Override public boolean handles(SinkSpec spec) { return spec instanceof SinkSpec.JvsLucene; }

    @Override
    public Sink create(SinkSpec spec, Path home) {
        SinkSpec.JvsLucene s = (SinkSpec.JvsLucene) spec;
        return new JvsLuceneSink(s.name(), s.typeJsonResource(), s.storeSource(), home);
    }
}
