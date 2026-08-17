/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.jsontypesystem.datamapper.HttpOllamaClient;
import com.hitorro.mesh.pipelines.model.StepSpec;
import com.hitorro.mesh.pipelines.runtime.StepAdapter;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * For each MLS field named in {@link StepSpec.JvsTranslate#mlsFields},
 * look up the entry whose {@code lang} matches {@code sourceLang},
 * translate its {@code text} into every {@code targetLangs} via a
 * local Ollama endpoint, and append the result as a new element in the
 * {@code mls[]} array.
 *
 * <p>All HTTP-to-Ollama plumbing lives in core
 * {@link HttpOllamaClient}. This adapter is JVS-shape glue: iterate
 * the {@code mls[]} arrays, skip langs that already exist (idempotent),
 * and drop new elements in place. If Ollama is unreachable the client
 * returns {@code null} and this adapter silently skips — the pipeline
 * still succeeds so offline devs get a graceful degrade.</p>
 */
public final class JvsTranslateStepAdapter implements StepAdapter {

    @Override
    public boolean handles(StepSpec spec) {
        return spec instanceof StepSpec.JvsTranslate;
    }

    @Override
    public Function<JsonNode, JsonNode> compile(StepSpec spec) {
        StepSpec.JvsTranslate s = (StepSpec.JvsTranslate) spec;
        final String srcLang     = orDefault(s.sourceLang(), "en");
        final List<String> tgtL  = s.targetLangs() == null || s.targetLangs().isEmpty()
                                   ? List.of("es", "fr", "de") : s.targetLangs();
        final List<String> fields = s.mlsFields() == null || s.mlsFields().isEmpty()
                                    ? List.of("title", "body") : s.mlsFields();
        final HttpOllamaClient ollama =
                new HttpOllamaClient(s.ollamaUrl(), s.model());

        return row -> {
            if (!row.isObject()) return row;
            ObjectNode obj = (ObjectNode) row;
            for (String field : fields) {
                JsonNode mlsHost = obj.get(field);
                if (!(mlsHost instanceof ObjectNode host)) continue;
                JsonNode arr = host.get("mls");
                if (!(arr instanceof ArrayNode mls)) continue;
                JsonNode srcElem = findByLang(mls, srcLang);
                if (srcElem == null) continue;
                String srcText = srcElem.get("text") != null ? srcElem.get("text").asText() : null;
                if (srcText == null || srcText.isBlank()) continue;
                for (String tgt : tgtL) {
                    if (findByLang(mls, tgt) != null) continue;  // idempotent
                    String translated = ollama.translate(srcText, srcLang, tgt);
                    if (translated == null) continue;
                    ObjectNode newElem = JsonNodeFactory.instance.objectNode();
                    newElem.put("lang", tgt);
                    newElem.put("text", translated);
                    newElem.put("clean", translated);
                    mls.add(newElem);
                }
            }
            return obj;
        };
    }

    private static JsonNode findByLang(ArrayNode mls, String lang) {
        for (Iterator<JsonNode> it = mls.elements(); it.hasNext(); ) {
            JsonNode e = it.next();
            if (e != null && e.has("lang") && lang.equals(e.get("lang").asText())) return e;
        }
        return null;
    }

    private static String orDefault(String v, String d) { return v == null || v.isBlank() ? d : v; }
}
