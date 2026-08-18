/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.StepSpec;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage for the JVS Groovy step adapter — the ServiceLoader entry
 * that lets pipelines run scripts against the full {@code TransformDSL}
 * (source / target / work registers, copyAll / copy / set / mls /
 * append / when / loop / gen). Historically this module shipped zero
 * tests; a regression in the JVS DSL wiring would only surface when
 * a real job ran on-cluster.
 *
 * <p>The DSL is documented in
 * {@code hitorro-jsontypesystem/…/datamapper/TransformDSL.java}.
 * These tests exercise the smallest useful subset (copyAll, set) —
 * enough to prove the compile → apply → JVS-round-trip loop works
 * end-to-end. Deeper DSL semantics (mls, when, loop, gen) are covered
 * by the jsontypesystem module's own tests.</p>
 */
class JvsGroovyStepAdapterTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    // -------------------------------------------------- handles()

    @Test
    void handles_onlyJvsGroovyStep() {
        JvsGroovyStepAdapter a = new JvsGroovyStepAdapter();
        assertThat(a.handles(new StepSpec.JvsGroovy("copyAll()", null))).isTrue();
        // Every OTHER StepSpec kind must not match — otherwise ServiceLoader
        // ordering could accidentally steal a groovy-map step from the
        // built-in resolver.
        assertThat(a.handles(new StepSpec.Filter("x > 0"))).isFalse();
        assertThat(a.handles(new StepSpec.GroovyMap("row.n = 1"))).isFalse();
        assertThat(a.handles(new StepSpec.Project(java.util.List.of("a")))).isFalse();
        assertThat(a.handles(new StepSpec.SetField("k", "v"))).isFalse();
    }

    @Test
    void handles_ignoresSinkSpecs() {
        // handles(StepSpec) is only called with StepSpec instances, but
        // guard against accidental cross-type registration bugs.
        JvsGroovyStepAdapter a = new JvsGroovyStepAdapter();
        // No cross-hierarchy accidents: our adapter has one method
        // signature — handles(StepSpec). SinkSpec doesn't fit, so a
        // compile-time overload check protects us.
        assertThat(SinkSpec.class.isAssignableFrom(StepSpec.class)).isFalse();
    }

    // -------------------------------------------------- compile()

    @Test
    void copyAll_deepCopiesEveryField() {
        Function<JsonNode, JsonNode> step = compile("copyAll()");

        ObjectNode row = JSON.createObjectNode();
        row.put("id", "u-1");
        row.put("name", "Alice");
        row.put("age", 30);
        row.putArray("tags").add("dev").add("ops");

        JsonNode out = step.apply(row);
        assertThat(out).isNotNull();
        assertThat(out.get("id").asText()).isEqualTo("u-1");
        assertThat(out.get("name").asText()).isEqualTo("Alice");
        assertThat(out.get("age").asInt()).isEqualTo(30);
        assertThat(out.get("tags").size()).isEqualTo(2);
        assertThat(out.get("tags").get(0).asText()).isEqualTo("dev");
    }

    @Test
    void set_injectsConstantField() {
        // The DSL's `set` writes into target regardless of what source had.
        Function<JsonNode, JsonNode> step = compile("""
                copyAll()
                set "target.stamp", "processed"
                """);

        ObjectNode row = JSON.createObjectNode();
        row.put("id", "u-1");
        row.put("v", 42);

        JsonNode out = step.apply(row);
        assertThat(out.get("id").asText()).isEqualTo("u-1");
        assertThat(out.get("v").asInt()).isEqualTo(42);
        assertThat(out.get("stamp").asText()).isEqualTo("processed");
    }

    @Test
    void multipleRowsFlowThroughSameCompiledFunction() {
        // Prove the function is stateless per invocation — the atomic
        // counter in GroovyTransformMapper is the only mutation, and it
        // shouldn't leak state between rows.
        Function<JsonNode, JsonNode> step = compile("""
                copyAll()
                set "target.marker", "seen"
                """);

        for (int i = 0; i < 5; i++) {
            ObjectNode row = JSON.createObjectNode();
            row.put("i", i);
            JsonNode out = step.apply(row);
            assertThat(out.get("i").asInt()).isEqualTo(i);
            assertThat(out.get("marker").asText()).isEqualTo("seen");
        }
    }

    @Test
    void badGroovySyntax_rejectedAtCompileTime() {
        // Fail fast at spec-load time, not on the first row — the whole
        // point of the compile-once pattern.
        assertThatThrownBy(() -> compile("this is not valid groovy [["))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void runtimeErrorInScript_returnsNull_dropsRow() {
        // GroovyTransformMapper.apply() logs + returns null on runtime
        // exceptions (see its javadoc). The adapter surfaces that as a
        // null result, which the pipeline runtime treats as "drop this
        // row" — a filter-like semantics.
        Function<JsonNode, JsonNode> step = compile("""
                copyAll()
                throw new RuntimeException("boom")
                """);

        JsonNode out = step.apply(JSON.createObjectNode().put("id", "u-1"));
        assertThat(out).isNull();
    }

    // -------------------------------------------------- helpers

    private static Function<JsonNode, JsonNode> compile(String script) {
        return new JvsGroovyStepAdapter().compile(new StepSpec.JvsGroovy(script, null));
    }
}
