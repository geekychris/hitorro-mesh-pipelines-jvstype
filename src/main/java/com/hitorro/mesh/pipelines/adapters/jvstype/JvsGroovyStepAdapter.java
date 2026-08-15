/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jsontypesystem.datamapper.GroovyTransformMapper;
import com.hitorro.mesh.pipelines.model.StepSpec;
import com.hitorro.mesh.pipelines.runtime.StepAdapter;

import java.util.function.Function;

/**
 * ServiceLoader-registered adapter that upgrades {@code jvs-groovy} step
 * specs to run through the shipped {@link GroovyTransformMapper} — the
 * full-fat JVS DSL with source/target/work registers, copyAll / copy /
 * set / mls / append / times / loop / when, and the {@code gen} data
 * generators.
 *
 * <p>Compiles the script once at pipeline start (via
 * {@code GroovyTransformMapper.fromString}) and reuses it for every row.
 * Each row is wrapped in a fresh JVS, run through the mapper, and the
 * result JVS is unwrapped back into a Jackson {@code JsonNode} for the
 * next step / sink.</p>
 *
 * <p>The optional {@code typeJson} field on the spec lets you attach
 * type metadata that mapping DSL expressions can reference. If omitted,
 * the JVS is untyped ({@code new JVS(node)}).</p>
 */
public final class JvsGroovyStepAdapter implements StepAdapter {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public boolean handles(StepSpec spec) {
        return spec instanceof StepSpec.JvsGroovy;
    }

    @Override
    public Function<JsonNode, JsonNode> compile(StepSpec spec) {
        StepSpec.JvsGroovy s = (StepSpec.JvsGroovy) spec;
        // Compile once — the mapper is thread-safe for read of its script;
        // per-row invocation is stateless from the caller's viewpoint.
        GroovyTransformMapper mapper;
        try {
            // Overload disambiguation — the (String, DataGenerators)
            // signature is what we want, and null is a valid generators
            // ref meaning "built-ins only".
            mapper = GroovyTransformMapper.fromString(s.script(),
                    (com.hitorro.jsontypesystem.datamapper.DataGenerators) null);
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                    "failed to compile jvs-groovy script: " + e.getMessage(), e);
        }
        // Type metadata is accepted on the spec but not applied to the
        // JVS constructor here — the shipped JVS API constructs from a
        // JsonNode alone; users can still write scripts that inspect
        // type fields inside the DSL. Full JVS(Type, JsonNode) linking
        // is a follow-up when the shipped JVS class exposes it.
        return row -> {
            try {
                JVS input = new JVS(row);
                JVS output = mapper.apply(input);
                if (output == null) return null;   // filter behaviour
                return output.getJsonNode();
            } catch (Exception e) {
                throw new RuntimeException("jvs-groovy runtime error: " + e.getMessage(), e);
            }
        };
    }
}
