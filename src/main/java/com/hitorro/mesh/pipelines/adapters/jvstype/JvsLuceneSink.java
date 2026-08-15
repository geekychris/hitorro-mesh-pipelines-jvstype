/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.jvstype;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.index.IndexManager;
import com.hitorro.index.config.IndexConfig;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.mesh.pipelines.sinks.Sink;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Type-aware Lucene sink. Every row is wrapped as a {@link JVS} with the
 * loaded {@link Type}, then handed to
 * {@link com.hitorro.index.indexer.JVSLuceneIndexWriter} — which projects
 * each field according to the type's {@code groups[].method}
 * (identifier / text / long / mls / …). Physical index at
 * {@code &lt;home&gt;/lucene/&lt;name&gt;/}.
 *
 * <p>Type is loaded once at {@link #open()} from
 * {@code typeJsonResource} — a Spring-style URL: {@code file:...},
 * {@code classpath:...}, or {@code http(s)://...}. Each row's JVS gets
 * the loaded Type forcibly attached via {@link JVS#setType(Type)} so we
 * do not depend on JsonTypeSystem being pre-populated.</p>
 */
public final class JvsLuceneSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String name;
    private final String typeJsonResource;
    private final boolean storeSource;
    private final Path indexDir;
    private final AtomicLong n = new AtomicLong();

    private IndexManager indexManager;
    private Type type;

    public JvsLuceneSink(String name, String typeJsonResource, boolean storeSource, Path home) {
        this.name = name;
        this.typeJsonResource = typeJsonResource;
        this.storeSource = storeSource;
        this.indexDir = home.resolve("lucene").resolve(name);
    }

    @Override
    public void open() throws Exception {
        Files.createDirectories(indexDir);
        // Bootstrap LuceneFieldTypes: the shipped loader routes lucene_fields.json
        // through MapProperty → JsonNArrayToMapT which only handles JSON *arrays*,
        // but the config file ships as an *object* — silently yielding an empty
        // map, then LuceneIndexerAction's per-field LuceneFieldType lookup
        // returns null and projection skips every field. Populate the singleton
        // ourselves so this sink works regardless of the upstream loader.
        bootstrapLuceneFieldTypes();

        JsonNode typeJson = loadTypeJson(typeJsonResource);
        this.type = new Type();
        this.type.init(typeJson);

        IndexConfig cfg = IndexConfig.builder()
                .filesystem(indexDir)
                .storeSource(storeSource)
                .build();
        this.indexManager = new IndexManager("en");
        this.indexManager.createIndex(name, cfg, type);
    }

    @Override
    public void add(JsonNode row) throws Exception {
        if (indexManager == null) open();
        JVS jvs = new JVS(row);
        jvs.setType(type);
        indexManager.indexDocuments(name, List.of(jvs));
        n.incrementAndGet();
    }

    @Override public long count() { return n.get(); }

    @Override
    public void close() throws Exception {
        try { if (indexManager != null) indexManager.commit(name); } catch (Exception ignore) {}
        try { if (indexManager != null) indexManager.close(); }     catch (Exception ignore) {}
    }

    /**
     * Populate {@code LuceneFieldTypes.map} from
     * {@code ${HT_BIN}/config/jsonconfigs/lucene/lucene_fields.json}
     * by hand. Idempotent — no-op if {@code identifier} is already
     * registered.
     */
    private static void bootstrapLuceneFieldTypes() {
        try {
            var lfts = com.hitorro.index.config.LuceneFieldTypes.getInstance();
            if (lfts.get("identifier") != null) return;

            String bin = System.getProperty("HT_BIN",
                    System.getProperty("ht.bin", System.getenv("HT_BIN")));
            if (bin == null) bin = System.getProperty("user.dir");
            var path = java.nio.file.Paths.get(bin, "config", "jsonconfigs", "lucene", "lucene_fields.json");
            if (!java.nio.file.Files.exists(path)) return;

            JsonNode node = JSON.readTree(path.toFile());
            if (!node.has("fields")) return;

            java.lang.reflect.Field mf = com.hitorro.index.config.LuceneFieldTypes.class.getDeclaredField("map");
            mf.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<String, com.hitorro.index.config.LuceneFieldType> m =
                    (java.util.Map<String, com.hitorro.index.config.LuceneFieldType>) mf.get(lfts);
            m.clear();
            var it = node.get("fields").fields();
            while (it.hasNext()) {
                var entry = it.next();
                var lft = new com.hitorro.index.config.LuceneFieldType();
                lft.init(entry.getValue());
                m.put(entry.getKey(), lft);
            }
        } catch (Throwable t) {
            System.err.println("[JvsLuceneSink] bootstrapLuceneFieldTypes failed: " + t);
        }
    }

    /**
     * Public alias for {@link #loadTypeJson(String)} — reused by other
     * adapters in this package (e.g. {@link JvsEnrichStepAdapter}) that
     * need the same file:/classpath:/http: loader without duplicating
     * the URL-scheme dispatch.
     */
    public static JsonNode loadTypeJsonPublic(String url) throws IOException, InterruptedException {
        return loadTypeJson(url);
    }

    /** Load type JSON from a file:, classpath:, or http(s):// URL. */
    private static JsonNode loadTypeJson(String url) throws IOException, InterruptedException {
        if (url == null || url.isBlank()) throw new IllegalArgumentException("typeJsonResource is required");
        if (url.startsWith("classpath:")) {
            String path = url.substring("classpath:".length());
            if (path.startsWith("/")) path = path.substring(1);
            try (InputStream in = JvsLuceneSink.class.getClassLoader().getResourceAsStream(path)) {
                if (in == null) throw new IOException("classpath resource not found: " + path);
                return JSON.readTree(in);
            }
        }
        if (url.startsWith("file:")) {
            Path p = Paths.get(URI.create(url));
            try (InputStream in = Files.newInputStream(p)) { return JSON.readTree(in); }
        }
        if (url.startsWith("http://") || url.startsWith("https://")) {
            HttpResponse<InputStream> resp = HttpClient.newHttpClient()
                    .send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                          HttpResponse.BodyHandlers.ofInputStream());
            if (resp.statusCode() != 200) throw new IOException("HTTP " + resp.statusCode() + " from " + url);
            try (InputStream in = resp.body()) { return JSON.readTree(in); }
        }
        // Bare path — treat as file.
        try (InputStream in = Files.newInputStream(Paths.get(url))) { return JSON.readTree(in); }
    }
}
