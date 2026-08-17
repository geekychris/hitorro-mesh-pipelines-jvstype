/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.JsonTypeSystem;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jsontypesystem.executors.EnrichExecutionBuilderMapper;
import com.hitorro.jsontypesystem.executors.ExecutionBuilder;
import com.hitorro.jsontypesystem.executors.GroupTagPredicates;
import com.hitorro.jsontypesystem.executors.ProjectionContext;
import com.hitorro.jsontypesystem.resources.TypeJsonLoader;
import com.hitorro.mesh.pipelines.model.StepSpec;
import com.hitorro.mesh.pipelines.runtime.StepAdapter;
import com.hitorro.util.core.events.cache.HashCache;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

/**
 * Runs the JVS type-system enrichment projection over every row. Wraps
 * the row as a {@link JVS}, forces the loaded {@link Type} onto it (so
 * JsonTypeSystem needn't be pre-populated), then executes the enrich
 * projection filtered by the configured tag set — populating dynamic
 * sub-fields like {@code title.mls[].clean}, {@code .segmented},
 * {@code .pos}, {@code .segmented_ner} on the way through.
 *
 * <p>Depends on OpenNLP models under
 * {@code ${HT_BIN}/data/opennlpmodels1.5/} for the per-language
 * segmenter / pos-tagger / NER. If the model files aren't present the
 * mappers gracefully skip that field and log a warning — the row still
 * flows through, just without that enrichment.</p>
 */
public final class JvsEnrichStepAdapter implements StepAdapter {

    @Override
    public boolean handles(StepSpec spec) {
        return spec instanceof StepSpec.JvsEnrich;
    }

    @Override
    public Function<JsonNode, JsonNode> compile(StepSpec spec) {
        StepSpec.JvsEnrich s = (StepSpec.JvsEnrich) spec;

        // Load the type up front so every row uses the same instance.
        // First read the JSON so we know its "name", then try to route
        // through JsonTypeSystem — the shipped enrichment engine's dynamic
        // mappers resolve field paths against types looked up from the
        // singleton cache. A locally-init'd Type isn't discoverable by
        // sub-type references (e.g. title.mls has type mlselem, which the
        // engine looks up via JsonTypeSystem), so path resolution across
        // vector-shaped nested types silently no-ops. Falls back to
        // manual init when disk lookup misses (dev / test).
        Type type;
        try {
            JsonNode typeJson = TypeJsonLoader.load(s.typeJsonResource());
            String typeName = typeJson.has("name") ? typeJson.get("name").asText() : null;
            Type looked = typeName != null
                    ? JsonTypeSystem.getMe().getType(typeName)
                    : null;
            if (looked != null) {
                type = looked;
            } else {
                type = new Type();
                type.init(typeJson);
            }
        } catch (IOException | InterruptedException e) {
            throw new UnsupportedOperationException(
                "jvs-enrich: cannot load type from " + s.typeJsonResource(), e);
        }

        // Prime the ExecutionBuilder cache for this tag set. Reused per row.
        String[] tags = s.tags() == null ? new String[]{"basic"} : s.tags().toArray(new String[0]);
        HashCache<Type, ExecutionBuilder> cache = buildEnrichCache(tags);
        final Type resolvedType = type;

        return row -> {
            try {
                // Let JVS auto-resolve type from the row's "type" field via
                // JsonTypeSystem — mirrors JVS.read() in the Spring app, and
                // is what the dynamic mappers expect (they walk sub-type
                // references via JsonTypeSystem, so the root Type must come
                // from there too or path resolution silently misses).
                JVS jvs = new JVS(row);
                if (jvs.getType() == null) jvs.setType(resolvedType);
                ProjectionContext pc = new ProjectionContext();
                pc.source = jvs;
                pc.target = new JVS();
                ExecutionBuilder builder = cache.get(jvs.getType() != null ? jvs.getType() : resolvedType);
                if (builder != null && builder.getCurrentNode() != null) {
                    builder.getCurrentNode().project(pc);
                }
                return pc.source.getJsonNode();
            } catch (Exception e) {
                throw new RuntimeException("jvs-enrich failed on row: " + e.getMessage(), e);
            }
        };
    }

    private static HashCache<Type, ExecutionBuilder> buildEnrichCache(String[] tags) {
        EnrichExecutionBuilderMapper mapper = new EnrichExecutionBuilderMapper();
        Set<String> tagSet = tags == null ? Set.of() : Set.of(tags);
        mapper.setPredicate(mapper.getPredicate().and(GroupTagPredicates.anyOfOrUntagged(tagSet)));
        String key = "JvsEnrich:" + String.join(",", tags == null ? new String[0] : tags);
        return Type.getExecBuilderCache(key, mapper);
    }
}
