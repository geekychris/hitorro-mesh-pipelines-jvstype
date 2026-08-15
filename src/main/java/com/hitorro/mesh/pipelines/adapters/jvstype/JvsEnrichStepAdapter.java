/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.BaseT;
import com.hitorro.jsontypesystem.Group;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jsontypesystem.executors.EnrichExecutionBuilderMapper;
import com.hitorro.jsontypesystem.executors.ExecutionBuilder;
import com.hitorro.jsontypesystem.executors.ProjectionContext;
import com.hitorro.mesh.pipelines.model.StepSpec;
import com.hitorro.mesh.pipelines.runtime.StepAdapter;
import com.hitorro.util.core.events.cache.HashCache;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

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

    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public boolean handles(StepSpec spec) {
        return spec instanceof StepSpec.JvsEnrich;
    }

    @Override
    public Function<JsonNode, JsonNode> compile(StepSpec spec) {
        StepSpec.JvsEnrich s = (StepSpec.JvsEnrich) spec;

        // Load the type up front so every row uses the same instance.
        Type type;
        try {
            JsonNode typeJson = JvsLuceneSink.loadTypeJsonPublic(s.typeJsonResource());
            type = new Type();
            type.init(typeJson);
        } catch (IOException | InterruptedException e) {
            throw new UnsupportedOperationException(
                "jvs-enrich: cannot load type from " + s.typeJsonResource(), e);
        }

        // Prime the ExecutionBuilder cache for this tag set. Reused per row.
        String[] tags = s.tags() == null ? new String[]{"basic"} : s.tags().toArray(new String[0]);
        HashCache<Type, ExecutionBuilder> cache = buildEnrichCache(tags);

        return row -> {
            try {
                JVS jvs = new JVS(row);
                jvs.setType(type);
                // Matches the shipped JvsEnrichMapper pattern exactly:
                // source = the row we want to mutate; target = fresh JVS
                // that the projection can use as scratch. The dynamic
                // mappers write back into source at their .fields[] paths.
                ProjectionContext pc = new ProjectionContext();
                pc.source = jvs;
                pc.target = new JVS();
                ExecutionBuilder builder = cache.get(type);
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
        Predicate<BaseT> tagPredicate;
        if (tags == null || tags.length == 0) {
            // Run every enrich group regardless of tags.
            tagPredicate = new GroupTagAnyPredicate();
        } else {
            tagPredicate = new GroupTagMatchPredicate(Set.of(tags))
                    .or(new GroupTagNullPredicate());
        }
        mapper.setPredicate(mapper.getPredicate().and(tagPredicate));
        String key = "JvsEnrich:" + String.join(",", tags == null ? new String[0] : tags);
        return Type.getExecBuilderCache(key, mapper);
    }

    private static class GroupTagMatchPredicate implements Predicate<BaseT> {
        private final Set<String> tags;
        GroupTagMatchPredicate(Set<String> tags) { this.tags = tags; }
        @Override public boolean test(BaseT b) {
            if (!(b instanceof Group g)) return false;
            List<String> gt = g.getTags();
            if (gt == null) return false;
            for (String t : gt) if (tags.contains(t)) return true;
            return false;
        }
    }
    private static class GroupTagNullPredicate implements Predicate<BaseT> {
        @Override public boolean test(BaseT b) {
            if (!(b instanceof Group g)) return false;
            List<String> gt = g.getTags();
            return gt == null || gt.isEmpty();
        }
    }
    private static class GroupTagAnyPredicate implements Predicate<BaseT> {
        @Override public boolean test(BaseT b) { return b instanceof Group; }
    }
}
