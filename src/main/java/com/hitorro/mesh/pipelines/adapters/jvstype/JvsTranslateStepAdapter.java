/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.pipelines.model.StepSpec;
import com.hitorro.mesh.pipelines.runtime.StepAdapter;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
 * <p>Skips any target lang that already has an mls entry so re-runs
 * are idempotent. Skips translation entirely and returns the row
 * unchanged if the Ollama endpoint is unreachable — errors are logged
 * on stderr so the pipeline still succeeds. That means an offline dev
 * gets a graceful degrade rather than a whole pipeline failure.</p>
 */
public final class JvsTranslateStepAdapter implements StepAdapter {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

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
        final String url   = orDefault(s.ollamaUrl(), "http://localhost:11434") + "/api/generate";
        final String model = orDefault(s.model(), "llama3.2");

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
                    String translated = translate(url, model, srcText, srcLang, tgt);
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

    private static String translate(String url, String model, String text, String src, String tgt) {
        try {
            String prompt = "Translate the following text from " + langName(src) + " to "
                          + langName(tgt) + ". Return ONLY the translated text, no commentary.\n\n"
                          + "Text to translate:\n" + text;
            ObjectNode body = JsonNodeFactory.instance.objectNode();
            body.put("model", model);
            body.put("prompt", prompt);
            body.put("stream", false);
            HttpResponse<String> resp = HTTP.send(HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(60))
                    .header("content-type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8))
                    .build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                System.err.println("[jvs-translate] Ollama HTTP " + resp.statusCode() + " for " + tgt);
                return null;
            }
            JsonNode parsed = JSON.readTree(resp.body());
            if (!parsed.has("response")) return null;
            return parsed.get("response").asText().trim();
        } catch (Exception e) {
            System.err.println("[jvs-translate] " + tgt + " failed: " + e.getMessage());
            return null;
        }
    }

    private static String langName(String code) {
        return switch (code) {
            case "en" -> "English"; case "es" -> "Spanish"; case "fr" -> "French";
            case "de" -> "German"; case "it" -> "Italian"; case "pt" -> "Portuguese";
            case "ja" -> "Japanese"; case "zh" -> "Chinese"; case "ko" -> "Korean";
            case "ar" -> "Arabic"; case "ru" -> "Russian"; case "hi" -> "Hindi";
            default -> code;
        };
    }
    private static String orDefault(String v, String d) { return v == null || v.isBlank() ? d : v; }
}
