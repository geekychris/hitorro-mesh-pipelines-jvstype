# hitorro-mesh-pipelines-jvstype

JVS type-system adapter for
[hitorro-mesh-pipelines](https://github.com/geekychris/hitorro-mesh-pipelines).
Bridges the pipelines' step + sink SPIs to
[hitorro-jsontypesystem](https://github.com/geekychris/hitorro-jsontypesystem)
so job authors get the full JVS DSL (source / target / work registers,
`copyAll` / `copy` / `set` / `mls` / `append` / `times` / `loop` /
`when`, plus the `gen.*` generators) and type-aware Lucene projection.
Drop this jar on the classpath and the pipelines' factories auto-load
its adapters via `ServiceLoader`.

## Adapters contributed

- **`kind: jvs-groovy` step** — full-fat JVS DSL script (see
  `TransformDSL` in `hitorro-jsontypesystem`). Compile-once,
  apply-many pattern; the script's source / target JVS registers get
  freshly-bound per row.
- **`kind: jvs-enrich` step** — runs the JVS enrichment projection
  over each row (populates dynamic sub-fields like
  `title.mls[].clean`, `.segmented`, `.pos`, `.segmented_ner` via
  the mappers registered in `config/implementations.json`). Needs
  OpenNLP models under `${HT_BIN}/data/opennlpmodels1.5/<lang>-*.bin`.
- **`kind: jvs-translate` step** — runs a local LLM (Ollama by
  default, `llama3.2`) to translate every `core_mls` field's text
  into the target languages, appending each translation as a new
  `mls[]` entry so downstream `JvsLuceneSink` gets per-language
  analyzer routing automatically.
- **`kind: jvs-lucene` sink** — type-aware Lucene projection via
  `JVSLuceneIndexWriter`. Each row is wrapped as a JVS with the
  loaded `Type`, then every field is projected per the type's
  `groups[].method` (identifier → StringField, text → TextField
  with language analyzer, mls → per-language TextField, long →
  LongPoint + NumericDocValuesField, etc.). Same `storeSource`
  default rules as the plain `lucene` sink (true by default).

## Wire shape

```yaml
sinks:
  - kind: jvs-lucene
    name: enriched-articles
    typeJsonResource: classpath:/types/demo_enriched_article.json
```

```yaml
steps:
  - {kind: jvs-groovy, script: |
      copyAll()
      set "target.stamp", "processed"
    }
```

## Dependency footprint

Pulls `hitorro-jsontypesystem` which transitively drags OpenNLP,
tokenizers, and the WordNet corpora — heavier than the lean
pipelines-core. Opt in only when you need full JVS semantics; the
core module's `groovy-map` step handles arbitrary Groovy transforms
without any of this weight.

## Tests

7 tests in `JvsGroovyStepAdapterTest` — contract (`handles()` only
matches `StepSpec.JvsGroovy`), DSL semantics via `copyAll` + `set`,
stateless-per-row invariant, and compile-time + runtime error paths.
Enrich / Translate / JvsLucene adapters not directly tested here —
they need OpenNLP models / Ollama / a real Lucene index respectively.
