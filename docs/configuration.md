# Configuration: models and parsing

Everything below is a field of `Settings` (`app/core/config.py`). Set it as an environment variable or in `.env`; the defaults are the
values in `.env.example`. A wrong value (an unknown `DOCLING_DEVICE`, a malformed `RERANK_COVERAGE_THRESHOLDS`) fails when the service
starts, not at the first query. `tests/unit/test_configurable.py` checks that `.env.example` lists only real settings and loads cleanly.

The embedding service has its own settings (`embedding-service/.env.example`). Where a choice spans both services, both halves are shown.

## What changing a setting requires

| You change | Where | You must |
|---|---|---|
| Reranker on/off, which reranker, its timeout, `RERANK_TOP_N` | retrieval-service (`RERANK_*`); the model also has to be enabled in embedding-service (`RERANKER_MODEL`, `RERANKER_ALLOWED_MODELS`) | restart. No re-index. |
| Coverage thresholds, `COVERAGE_ENABLED`, `EXPAND_NEIGHBOURS` | retrieval-service | restart. No re-index. |
| Docling: `ENABLED`, `DEVICE`, `NUM_THREADS`, `DOCUMENT_TIMEOUT_SECONDS`, `REUSE_CONVERTER` | retrieval-service | restart. Affects documents uploaded afterwards. |
| Docling: `TABLE_STRUCTURE`, `TABLE_MODE` | retrieval-service | restart, **and build a new index generation** to apply it to documents already indexed (the generation's `parser_version` records it, and `make reindex` refuses on a mismatch). |
| **Embedding model** (`MODEL_NAME`, prefixes, `MAX_SEQ_LENGTH`) | embedding-service | **build a new index generation** (below). Never switch it on a live index. |
| Chunk sizes (`CHUNK_*_V2`), contextual text | retrieval-service | new index generation. |

If you change a parse or chunk setting without a new generation, documents indexed from then on use the new value while older ones keep the
old one. The active generation is never rewritten in place.

## Reranker: use it or not, and which one

```
retrieval-service:   RERANK_ENABLED=true
                     RERANK_MODEL=cross-encoder/ms-marco-MiniLM-L-6-v2     # empty = embedding-service's default
embedding-service:   RERANKER_MODEL=BAAI/bge-reranker-v2-m3               # its default
                     RERANKER_ALLOWED_MODELS=cross-encoder/ms-marco-MiniLM-L-6-v2
                     RERANKER_MAX_LOADED=1                                  # models kept in memory; others are evicted least-recently-used
```

A request may only select a model that embedding-service lists (`RERANKER_MODEL` or `RERANKER_ALLOWED_MODELS`); anything else is refused with
INVALID_ARGUMENT, so no caller can make the service download or load an arbitrary model. If the call fails or is too slow the query falls
back to the non-reranked path and `retrieval_rerank_fallback_total` counts it.

**Measured cost on this machine (CPU, 12 logical cores), 30 passages per query:**

| Reranker | Time per query | Memory | Source |
|---|---|---|---|
| `cross-encoder/ms-marco-MiniLM-L-6-v2` (English) | 2.59 s median, 4.35 s p95 (passages of 200 tokens, shared machine). In the full pipeline with real chunks: 2.0 to 2.4 s median, 3.0 to 4.6 s p95 | 610 MB | embedding-service `docs/reranker-benchmark.md`; `p3-ms-real.json` |
| `BAAI/bge-reranker-v2-m3` (multilingual) | **34.6 s median, 43.4 s p95** (20 calls, idle machine, passages of 200 tokens) | 2.8 GB peak, 12 s to load | `scripts/bench_rerank.py` run on 2026-11-07 |

The defaults are `RERANK_TIMEOUT_SECONDS=0.8`, `MILVUS_QUERY_TIMEOUT_SECONDS=1.0` (raised from 0.5) and `QUERY_PIPELINE_DEADLINE_SECONDS=1.8`. **Neither reranker fits them on CPU**: with
`RERANK_ENABLED=true` and the defaults every query would time out and fall back. That is why it ships off. To use one you must raise those
timeouts (and llm-service's retrieval timeout) or reduce the work (`RERANK_TOP_N`, a GPU). bge at 35 s per query is not usable
interactively on this CPU. With real (shorter) chunks it would be faster than 35 s, but I did not measure that.

**Coverage thresholds.** Reranker scores are not comparable across models (`ms-marco` returns raw logits, about -11 to +4 here), so
`RERANK_COVERAGE_THRESHOLDS` is a JSON table per model: `{"<RERANK_MODEL>": {"weak_min": x, "strong_min": y}}`; the key `default` applies when
`RERANK_MODEL` is empty. A reranker with no entry still returns `top_score` but no coverage verdict. **The table ships empty**: thresholds for `ms-marco` chosen on
the main set (`weak_min` -9, `strong_min` -4.5) caught 7 of 8 unanswerable questions there but flagged 12 of 41 answerable held-out questions
as weak or none-found (7 as none), so they would make the model deny covered questions. `bge-reranker-v2-m3` was never measured on the benchmark.
Leave `COVERAGE_ENABLED=false` until a reranker's thresholds hold up on questions they were not chosen on.

## Embedding model: which one

The model lives in embedding-service:

```
embedding-service:   MODEL_NAME=intfloat/multilingual-e5-large
                     QUERY_PREFIX=query:                  # added to GenerateEmbedding (single texts)
                     PASSAGE_PREFIX=passage:              # added to GenerateEmbeddings (batches)
                     MAX_SEQ_LENGTH=0                     # 0 = the model's own limit; a smaller number caps it
                     TOKENIZER_ID=                        # empty = derived from MODEL_NAME
```

Two safeguards:

* `MODEL_PATH` (a local copy) is used only if its last folder name matches `MODEL_NAME`. Otherwise it is ignored with a warning, so changing
  `MODEL_NAME` can never silently keep loading the old weights under the new name.
* retrieval-service takes the model it expects from the **active index generation**, not from a setting. If the embedding service answers with a
  different model or vector size, ingestion and queries fail with a mismatch error instead of mixing vectors from two models in one index.

**To switch the embedding model** (vectors are not comparable, so this is a new generation, never an in-place change):

1. Start a second embedding-service with the new `MODEL_NAME` and prefixes. Keep production on the old one.
2. `make generation-create GENERATION=3 ARGS="--embedding-model intfloat/multilingual-e5-large --embedding-dimension 1024"` creates the Milvus
   collection (sized for that model) and the OpenSearch index and registers the generation as `building`.
3. Point `EMBEDDING_SERVICE_GRPC_TARGET` at the second service, then `make reindex GENERATION=3`. It refuses unless that service serves the
   generation's model, and it does not touch the active generation.
4. `make activate GENERATION=3 ARGS="--candidate-query-prefix 'query: ' --candidate-passage-prefix 'passage: '"` benchmarks both generations,
   each with its own model, and switches only if the new one is not worse on both question sets. Add `--dry-run` to check without switching.
   Then repoint production at the new embedding service.

The benchmark can also compare models without touching any index: `python -m evaluation.run --real --embedding-model <id> --query-prefix ...`.

## Docling (PDF, Word, PowerPoint, Excel, HTML)

| Setting | Default | Effect |
|---|---|---|
| `DOCLING_ENABLED` | `true` | `false` refuses those file types with a clear message and never imports Docling. Text, Markdown, CSV and code files still work. A text-only deployment then starts without loading torch (importing the parser took 77 s cold when Docling was loaded). |
| `DOCLING_TABLE_STRUCTURE` | `true` | Table detection. See the timings below: it did not change the parse time on the test PDF. Changes what is extracted: new generation. |
| `DOCLING_TABLE_MODE` | empty | Docling's default, or `fast` / `accurate`. Changes what is extracted: new generation. |
| `DOCLING_DEVICE` | `auto` | `auto`, `cpu`, `cuda`, `mps`, `xpu`. A GPU was not available here and was not measured. |
| `DOCLING_NUM_THREADS` | `0` | Docling's own default. |
| `DOCLING_DOCUMENT_TIMEOUT_SECONDS` | `0` | Stops a document that takes longer than this. `0` = no limit. |
| `DOCLING_REUSE_CONVERTER` | `false` | One shared converter instead of a new one per file; parses are then one at a time. |

**Measured on this machine** (6-page text-and-table PDF, idle CPU, same process; each line is consecutive parses):

| Setting | Parse time |
|---|---|
| defaults: new converter per file | 18.6 s, 28.8 s (20.1 s in an earlier run) |
| `DOCLING_REUSE_CONVERTER=true` | 20.9 s (builds it), then 16.2 s, 17.7 s |
| `DOCLING_TABLE_STRUCTURE=false` | 18.2 s, 18.6 s |
| `DOCLING_TABLE_MODE=fast` | 19.6 s, 20.2 s |
| `DOCLING_NUM_THREADS=2` | 32.3 s, 24.1 s |
| `DOCLING_NUM_THREADS=4 / 6 / 8 / 12` | 18.3 s / 15.0 s / 15.9 s / 23.2 s (second parse of each) |

So a 5 to 6 page PDF takes **about 15 to 20 s** once the worker is warm, and none of these settings changes that much on this CPU. The first
file after a worker starts is far slower (52 s, 57 s and 140 s in three measurements: model loading). Too few threads (2) is clearly slower,
and using all 12 logical cores is not better than 6 to 8. The remaining cost is Docling's layout model running on the CPU, about 3 s per page;
a GPU (`DOCLING_DEVICE=cuda`) is the lever I would try, but it is untested here. Reusing the converter saves the repeated model setup but
the per-page work dominates.

Not verified: whether the table text is as good with `DOCLING_TABLE_STRUCTURE=false` (one table block was found either way; the text was not
compared), so do not turn it off expecting a speed-up or assuming no loss.

## Optional features (all off by default)

| Setting | Notes |
|---|---|
| `OCR_ENABLED` | OCR only on PDF pages with fewer than `OCR_MIN_CHARS_PER_PAGE` characters, at most `OCR_MAX_PAGES_PER_DOCUMENT` per file. Needs an OCR model for Docling that is not installed here, so the code path is tested with a fake engine only. Without a model it records `engine_unavailable` and indexes the file as before. |
| `CONTEXTUAL_CHUNKS_ENABLED` | Master switch. When on, and only for a generation created with `--contextual`, a model named by `CONTEXT_LLM_URL` / `CONTEXT_LLM_MODEL` / `CONTEXT_LLM_API_KEY` receives each chunk's text and writes one or two sentences placing it in its document. **This sends document text to that model.** Tested with a mock endpoint only; never run against a live model. |
| `EXPAND_NEIGHBOURS` | Adds the previous and next chunk of the same section while `max_tokens` allows. |
| `LOG_QUERY_TEXT` | Adds the query text to the `retrieval.feedback` log line. A hash is always logged; document content never is. |
