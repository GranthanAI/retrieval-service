# Retrieval quality: findings, phase plan and status

Symptom: users attach documents and ask questions, but the right passages are often not retrieved. Example: PHASES.md, hld.md and
lld.md attached, question "what is phase 6 about?". The six sources returned were Phase 1, Open items, Phase 8, Phase 4, Phase 12
and an intro section, never Phase 6. Follow-ups ("Check again") and Hinglish ("Phase 6 mein kya karna hai?") did worse.

Working rules: one phase per branch (`fix/retrieval-phase-N-<topic>`), test first, contracts additive only, index changes go into a
new index generation, behaviour changes behind a flag with a documented default, nothing pushed without approval.

## Status

| Phase | Topic | Branch | Status |
|---|---|---|---|
| 0 | Offline benchmark (`evaluation/`, `make eval`) | main (`65bb38f`, pushed) | **Done** |
| 1 | Query-time fixes (MMR, request fields, HNSW `ef`, embed fallback, phrase boosts, dedupe, per-file balance) | main (pushed) | **Done** |
| 2 | Index generations, tokenizer-aware chunking, heading paths, provenance, tables, analyzers | main (pushed) | **Done; generation 2 built and measured, NOT activated** |
| 3 | Reranker, neighbour expansion, weak-retrieval signal | main (pushed) | **Built and measured; reranker, expansion and coverage all OFF by default; coverage target not met** |
| 4 | embedding-service performance under load | main (pushed, embedding-service) | **Done; priority lanes ship off; 0.4 s target still missed under load** |
| 5 | llm-service / memory-service: query rewrite, previous chunks, full-text for small docs, anchors, feedback logging | main (pushed, three repos) | **Done with fakes; rewrite cases not in the benchmark yet; end-to-end not run** |
| 6 | Selective OCR, contextual chunk text, embedding model evaluation | main | **OCR and contextual text built and tested with fakes; embedding-model comparison done (recommendation only); generation 3 not built** |

## Findings

Each was checked against `main` at commit `0ab1a22`. "Confirmed" means the code does what the finding says.

| Id | Finding | Where | Check |
|---|---|---|---|
| P1-1 | MMR relevance is the raw RRF score (about 0.01-0.03) while similarity is 0-1, so after the first pick MMR chooses whatever is least similar | `app/query/mmr.py` (`relevance = c.rrf_score`) | Confirmed in code; reproduced by strict-xfail test |
| P1-2 | `top_k`, `max_tokens`, `min_relevance_score` are never read; `MMR_FINAL_TOP_K` is used and `total_tokens` is a words/0.75 estimate | `app/query/pipeline.py`, `response_mapper.py` | Confirmed (grep finds no read of the three fields outside generated code) |
| P1-3 | `{"nprobe": ...}` is sent to an HNSW index; `ef` is never set | `app/stores/milvus_repository.py:105`, `scripts/provision_milvus_collection.py:77` | Confirmed. Milvus v2.4.15 accepts the call without error (it ignores `nprobe`), so the failure is silent |
| P1-4 | An `embed_query` failure returns UNAVAILABLE although BM25 needs no vector | `app/query/pipeline.py` step 3b | Confirmed |
| P1-5 | No phrase/heading boost in the keyword query | `app/stores/opensearch_repository.py` | Confirmed (`multi_match` only) |
| P1-6 | No near-duplicate removal before MMR; no per-file balancing | `app/query/pipeline.py` | Confirmed |
| P2-1 | `_tokens_to_words` divides by 0.75 instead of multiplying, so 450 tokens becomes 600 words; all-MiniLM-L6-v2 reads 256 wordpieces | `app/ingestion/chunker.py` | Confirmed (`int(tokens / 0.75)`) |
| P2-2 | Only the latest heading is kept, not the hierarchy | `text_parsers.parse_markdown`, `parser._parse_docling` | Confirmed. Baseline benchmark: every phase's "Exit criteria" chunk has the identical section label "Exit criteria" |
| P2-3 | A section spanning pages keeps its first page | `parser._parse_docling` (`if current_page is None`) | Confirmed with the benchmark PDF: a section on pages 3-5 is reported as page 3 |
| P2-4 | Tables are not kept whole; CSV row groups have no repeated header in split chunks | `chunker.py`, `text_parsers.parse_csv` | To confirm in Phase 2 |
| P2-5 | Keyword index has only the `standard` analyzer (no stemming, no identifier handling) | `scripts/provision_opensearch_index.py` | Confirmed |
| P3 | No cross-encoder reranker, no neighbour expansion, no weak-retrieval signal | | Not yet verified; Phase 3 |
| P4 | `ModelManager` serialises all requests behind one `RLock` | embedding-service | Not yet verified; Phase 4 |
| P5 | Retrieval query is the raw user message; assistant text is pinned as "Pinned Facts" | llm-service `collector.py`, memory-service `anchors.py` | Not yet verified; Phase 5 |

Notes found while building the benchmark (relevant to later phases):

* In OpenSearch, `minimum_should_match: 1` is satisfied by *any* `should` clause. The current query uses `should` for the
  per-document scope. If Phase 1 adds phrase boosts as further `should` clauses on the same `bool`, a phrase match in an
  out-of-scope document would satisfy the minimum and leak it into the results. Scope must move to `filter` when the boosts are added.
  The benchmark's OpenSearch fake follows the real semantics and has a unit test for this.
* Requests are capped at `MMR_FINAL_TOP_K` = 6 results today, so Recall@10 equals Recall@6 in the baseline.

## Benchmark (`evaluation/`)

```
make eval                                   # fake mode: deterministic hash embedder + in-memory stores (CI, ~30 s)
make eval MODE=real                         # all-MiniLM-L6-v2 (sentence-transformers) + live Milvus/OpenSearch if reachable
make eval MODE=real EVAL_ARGS="--label p1 --compare evaluation/results/baseline-real.json -v"
```

`MODE=real` needs `uv sync --group eval`. It creates a throwaway Milvus collection and OpenSearch index (`eval_chunks_<id>`), and
removes them afterwards; it never touches `retrieval_document_chunks`. If Milvus or OpenSearch are not reachable it falls back to
the in-memory stores and says so (`--stores real|fake` forces one).

What is real: Docling/Markdown/CSV parsing, the chunker, chunk ids, `ChunkRecord` construction, `MilvusRepository`,
`OpenSearchRepository`, and `pipeline.run` (fusion, MMR, response mapping). What is faked: Postgres lookups (every file is READY at
version 1), File Service permissions (all authorised), the embedding *service* (embedder runs in-process), and, in fake mode, the
Milvus/OpenSearch *clients* (the repositories still run on top of them, so the query bodies the service builds are what is scored).
`harness.ingest` mirrors `orchestrator._process` minus Postgres/MinIO/Kafka; Phase 2 must keep the two in step.

Corpus (`evaluation/fixtures/`, generated by `_generate.py`, all invented): `PHASES.md` (twelve similar `### Phase N` sections with
`#### Tasks` / `#### Exit criteria`, plus Open items), `hld.md`, `lld.md` (both mention phases), `runbook.md` (identifiers
`MAX_RETRY_ATTEMPTS`, `TXN-102394`, `/api/v1/subscriptions`), `invoices.csv`, and `billing-handbook.pdf` (6 pages; "Settlement
reconciliation" spans pages 3-5; pricing table on page 6).

Questions (`evaluation/questions.py`, 27): phase (3), phase-title (2), phase-exit (2), Hinglish (2), multi-file (3), identifier (4),
table (2), CSV (1), page (3), unanswerable (5). Gold is defined as evidence *text* that a returned chunk must contain, so the labels
survive chunker changes. The harness refuses to run if any gold string is missing from the indexed chunks.

Metrics: Recall@5, Recall@10, MRR@10, nDCG@10 (answerable questions); identifier exact-match recall (Recall@5 on identifier
questions); page-attribution accuracy (the first relevant chunk in the top 5 reports a page range containing the evidence's page);
multi-file coverage (share of required files with a relevant chunk in the top 10); no-answer precision (of all abstentions, the share
on unanswerable questions) and no-answer recall (share of unanswerable questions where the system abstained). The service has no
abstention signal before Phase 3, so an empty result is the only abstention and both no-answer numbers are expected to be 0 / n/a.

Fake-mode numbers are a regression signal for the pipeline only (the hash embedder is lexical). Quote real-mode numbers in reports.

### Phase 0 baseline (commit `0ab1a22` + benchmark)

| Metric | Fake | Real |
|---|---|---|
| Recall@5 | 0.591 | 0.432 |
| Recall@10 | 0.591 | 0.432 |
| MRR@10 | 0.636 | 0.500 |
| nDCG@10 | 0.601 | 0.447 |
| Identifier exact recall@5 | 1.000 | 1.000 |
| Page attribution accuracy | 0.333 | 0.000 |
| Multi-file coverage | 0.333 | 0.500 |
| No-answer precision | n/a | n/a |
| No-answer recall | 0.000 | 0.000 |

Real mode: all-MiniLM-L6-v2, live Milvus 2.4.15 + OpenSearch 2.11.0, 56 chunks. Raw results: `evaluation/results/baseline-*.json`.
In real mode "what is phase 6 about?" returned hld "Delivery phases", PHASES "Exit criteria" (three times), lld "Idempotency table"
and hld "Security"; the Phase 6 section was not among them. The bug is reproduced.

Strict-xfail tests (fail today, flip when fixed): `tests/unit/test_retrieval_quality_xfail.py` (P1-1, P1-2 x2, P1-3, P1-4) and
`tests/eval/test_benchmark_smoke.py::test_phase_6_section_is_in_the_top_3`.

## Phase 1 (query-time fixes, no re-index)

Changes (all in retrieval-service; flags and defaults in `app/core/config.py`):

| Item | Change | Flag / default |
|---|---|---|
| P1-1 | `mmr.normalise_relevance` min-max scales RRF to 0-1 before MMR; only candidates at or above the floor are diversified, the rest only fill `k` | `MMR_RELEVANCE_FLOOR=0.5` |
| P1-2 | `top_k` honoured (cap, 0 = `MMR_FINAL_TOP_K`); `max_tokens` drops the lowest-ranked chunks until the total fits; `total_tokens` is a real count. `min_relevance_score` is **ignored** until the reranker exists (Phase 3): RRF scores are rank-derived | `RETRIEVAL_MAX_TOP_K=20`, `TOKEN_COUNT_TOKENIZER_ID=sentence-transformers/all-MiniLM-L6-v2` |
| P1-3 | Milvus search sends `{"ef": max(MILVUS_HNSW_EF, limit)}`; `MILVUS_SEARCH_NPROBE` removed | `MILVUS_HNSW_EF=64` |
| P1-4 | Embedding failure falls back to the BM25-only path (`search_degraded_total{engine="bm25_only"}`), status OK / NO_MATCH | `EMBEDDING_FAILURE_FALLBACK_BM25=true` |
| phrases | `match_phrase` should-clauses on `section` (x5) and `content` (x2); `multi_match` stays `must`; document scope moved from `should` to `filter` so the boosts cannot widen it | `OPENSEARCH_PHRASE_BOOST_ENABLED=true` |
| near-duplicates | word 3-shingle Jaccard above the threshold drops the lower-ranked candidate before MMR | `DEDUPE_JACCARD_THRESHOLD=0.9` (1.0 = off) |
| per-file balance | with 2+ files, each file's best candidate (normalised relevance at or above the floor) is seeded into the selection | `BALANCE_ACROSS_FILES=true` |
| heading match | **not in the spec.** A candidate whose section contains a word pair from the query ("phase 6") gets one extra rank-1 RRF vote | `HEADING_MATCH_BOOST=1.0` (0 = off) |

Why the heading-match boost was added: with only the items above, the Phase 6 section was BM25 rank 1 but dense rank 29 (twelve
phase sections read alike), so RRF placed it 11th and it never reached the final set. Phrase boosts only help when the whole
query is a phrase; they cannot change an already-rank-1 BM25 position. Decision needed: keep, or drop and rely on the Phase 3 reranker.

Token counting: the tokenizer.json shipped with all-MiniLM-L6-v2 truncates to 128 and pads to 128, which silently caps every count;
`app/query/tokens.py` disables both (unit test `test_token_count_is_not_capped_or_padded`).

Existing tests changed (design change, not weakened):
* `tests/unit/test_pipeline_status.py::test_embedding_outage_is_unavailable` asserted UNAVAILABLE on an embedding failure, which P1-4
  reverses. It now runs with `EMBEDDING_FAILURE_FALLBACK_BM25=false` (original behaviour preserved under the flag), and a new test
  covers the default (NO_MATCH when the keyword leg finds nothing).
* The Phase 0 strict-xfail tests were renamed `test_retrieval_quality.py` and their markers removed as each bug was fixed. Their
  fixture chunks were made textually distinct (identical chunks are now correctly collapsed as near-duplicates), and the
  `max_tokens` budget in that test was raised to 1000 to match real wordpiece counts.

### Phase 1 results (before = Phase 0 baseline)

| Metric | Fake before | Fake after | Real before | Real after |
|---|---|---|---|---|
| Recall@5 | 0.591 | 0.773 | 0.432 | 0.909 |
| Recall@10 | 0.591 | 0.864 | 0.432 | 0.955 |
| MRR@10 | 0.636 | 0.789 | 0.500 | 0.827 |
| nDCG@10 | 0.601 | 0.795 | 0.447 | 0.857 |
| Identifier exact recall@5 | 1.000 | 1.000 | 1.000 | 1.000 |
| Page attribution accuracy | 0.333 | 0.333 | 0.000 | 0.333 |
| Multi-file coverage | 0.333 | 1.000 | 0.500 | 1.000 |
| No-answer precision / recall | n/a / 0 | n/a / 0 | n/a / 0 | n/a / 0 |

Acceptance: "what is phase 6 about?" has the Phase 6 section in the top 3 in both modes. Still failing: "exit criteria of phase 6"
and similar (Recall@5 0.0 for `phase_exit`): the "Exit criteria" chunk carries no phase number. That needs the heading path (Phase 2).
Page attribution only improves where the evidence happens to be on a section's first page (Phase 2). No-answer needs Phase 3.

## Phase 1 review follow-ups (in progress, paused)

Done:
* Phase 1 moved to branch `fix/retrieval-phase-1-query` (note: `origin/main` also already contains it, pushed on request before this review).
* Integration suite: 40 passed / 7 failed / 0 skipped on both 65bb38f and Phase 1, i.e. no new failures. All 7 pass against an
  OpenSearch index created by `scripts/provision_opensearch_index.py`: the shared dev index `retrieval_document_chunks` was
  auto-created by dynamic mapping (`document_id` is `text`, not `keyword`), so exact-match filters never match. Environment, not code.
  The Milvus roundtrip test is flaky under the 0.5 s timeout (passed on rerun).
* `tests/integration/test_opensearch_scope_live.py`: 14 live-OpenSearch tests (end-to-end query, out-of-scope document never returned
  on an exact heading/phrase, older `content_version` never returned, plus a sensitivity test). With scope moved back into `should`,
  8 of them fail.
* `tests/unit/test_retrieval_quality.py`: the `max_tokens` reproduction had been weakened (it passed on the old code); restored. All 5
  reproductions fail on 65bb38f and pass on Phase 1.
* Benchmark: main set 80 questions (72 answerable, 8 per category), held-out set 33 (29 answerable, `evaluation/heldout.py`, never tune
  against it). Metrics print with counts (e.g. 36/72). `page_attribution_accuracy` is now accompanied by `page_given_retrieved`:
  the earlier 0.0 to 0.333 change was retrieval (no PDF chunk was retrieved at baseline), not the parser.
* Harness: real-mode runs raise the Milvus/OpenSearch timeouts to 10 s and count questions that ran on one engine only
  (a 0.5 s Milvus timeout silently degraded some queries and invalidated a first sweep).

### Results on the 80-question main set and 33-question held-out set (real mode, MiniLM, live Milvus + OpenSearch)

Raw: `evaluation/results/p1-sweep-real.json` (Phase 1 code) and `old65bb38f-real.json` (code before Phase 1). Zero questions ran on a
single engine in either run. These supersede the 27-question numbers above. Counts are shown because one main-set question moves a
metric by 1.4 points and one held-out question by 3.4. Held-out gold with several evidences scores fractionally (hence 25.17/29).

Before (65bb38f) vs after (Phase 1), request top_k=10:

| Metric | Main before | Main after | Held-out before | Held-out after |
|---|---|---|---|---|
| Recall@5 | 0.500 (36/72) | 0.889 (64/72) | 0.741 (21.5/29) | 0.868 (25.17/29) |
| Recall@8 | 0.500 (36/72) | 0.917 (66/72) | 0.741 | 0.902 (26.17/29) |
| Recall@10 | 0.500 (36/72) | 0.931 (67/72) | 0.741 | 0.902 |
| MRR@10 | 0.539 | 0.789 | 0.793 | 0.842 |
| nDCG@10 | 0.500 | 0.815 | 0.753 | 0.843 |
| Identifier recall@5 | 8/8 | 8/8 | n/a | n/a |
| Page attribution (all page questions) | 10/16 | 12/16 | 2/5 | 3/5 |
| Multi-file coverage | 4/8 | 7/8 | 0.5/1 | 0.5/1 |
| No-answer recall | 0/8 | 0/8 | 0/4 | 0/4 |

Page attribution: the earlier 0.0 to 0.333 was retrieval (no PDF chunk was retrieved at baseline), not the parser; it is still limited
by the section-start page bug (Phase 2).

### HEADING_MATCH_BOOST: 0 vs 1.0 (Recall@5 / MRR@10 / nDCG@10)

Main set (tuning set; the boost was designed against it):

| Category (n) | Boost 0 | Boost 1.0 |
|---|---|---|
| phase (8) | 0.500 / 0.380 / 0.466 | 1.000 / 1.000 / 1.000 |
| phase_title (8) | 1.000 / 0.854 / 0.891 | 1.000 / 1.000 / 1.000 |
| phase_exit (8) | 0.250 / 0.104 / 0.170 | 0.375 / 0.184 / 0.287 |
| hinglish (8) | 0.625 / 0.478 / 0.596 | 0.875 / 0.802 / 0.848 |
| page (8) | 1.000 / 0.750 / 0.816 | 1.000 / 0.875 / 0.908 |
| csv, identifier, multi_file, table (8 each) | identical | identical |
| overall (72) | 0.792 (57/72) / 0.645 / 0.692 | 0.889 (64/72) / 0.789 / 0.815 |

Held-out set: every category is identical with the boost at 0 and at 1.0 (body 9, csv 2, hinglish 4, multi_file 1, page 1,
paraphrase 5, table 3, typo 4; overall Recall@5 25.17/29, MRR 0.842, nDCG 0.843 either way). No category drops, so by the agreed rule the
default stays **1.0**. Caveat: the held-out questions deliberately avoid heading words, so the boost never fires there. That is evidence
of no harm on paraphrased and body-answer questions, not evidence of benefit. The benefit is measured only on the main set, which the
boost was designed against. **Re-evaluate after the Phase 3 reranker** (it may make the boost redundant) and when the held-out set has
questions that name a heading.

### Request top_k (llm-service currently sends top_k=5, max_tokens=4000): DECISION OPEN

Recall of the returned set when the request asks for k chunks (Phase 1 honours top_k; before it, 6 were always returned):

| Request top_k | Main Recall@k | Main MRR | Held-out Recall@k | Held-out MRR |
|---|---|---|---|---|
| 5 (llm-service today) | 0.847 (61/72) | 0.772 | 0.891 (25.83/29) | 0.845 |
| 6 | 0.847 (61/72) at @5 | 0.772 | 0.891 at @5 | 0.845 |
| 8 | 0.889 (64/72) | 0.784 | 0.891 (25.83/29) | 0.843 |
| 10 | 0.931 (67/72) | 0.789 | 0.902 (26.17/29) | 0.842 |

Reading: 5 to 8 chunks adds 3 questions on main (61 to 64) and none on held-out; 10 adds 3 more on main and 1/3 of a question on
held-out. Cost is more prompt tokens (about 100-250 each at today's chunk size; chunks shrink in Phase 2). Not decided: whether
llm-service should send 8 (or 10). `max_tokens=4000` is the binding limit either way. llm-service is unchanged.

Remaining known gaps: `phase_exit` (0.375 Recall@5; the Exit-criteria chunk has no phase number until Phase 2 adds heading paths) and
`csv` (MRR 0.31; the 40-row chunks are near-identical tables, Phase 2 row groups).

## Step 0 (before Phases 2-6): decisions and measurements

Working mode from here: everything is committed to `main` in each repo (no feature branches), pushed after each completed step.

* **top_k: DECIDED = 8.** llm-service will send `top_k=8` from a config value (Phase 5; llm-service is unchanged until then). On the
  80/33-question sets (Phase 1 code), Recall@k of the returned set: top_k=5 61/72 main, 25.83/29 held-out; top_k=8 64/72 and
  25.83/29; top_k=10 67/72 and 26.17/29. Re-check 5/8/10 after Phase 2, because chunk sizes change.
* **Dev index.** The 7 integration failures (store layer x3, orchestrator, reconciliation x2, kafka) were caused by the shared dev
  index `retrieval_document_chunks` having been created by a first write (dynamic mapping: `document_id` as `text`, so the `term` scope
  filter matched nothing). It was deleted and re-provisioned from the real mapping; the suite then passed. Recurrence is prevented by
  (1) `OpenSearchRepository.verify_mapping()` at API startup (the process refuses to start on a wrong mapping) and in `/health/ready`
  (503 with an `errors` field naming the wrong fields), (2) writes check that the index exists and raise `IndexMissingError` instead of
  letting OpenSearch auto-create it, (3) tests: `tests/unit/test_index_guard.py` and the live
  `tests/integration/test_opensearch_scope_live.py::test_an_index_created_by_a_first_write_is_detected`.
  Operational hardening for any OpenSearch cluster: set `action.auto_create_index` so that application indexes are not auto-created,
  e.g. `PUT _cluster/settings {"persistent": {"action.auto_create_index": "-retrieval_*,+*"}}`.
* **Milvus timeout (`MILVUS_QUERY_TIMEOUT_SECONDS`, default 0.5 s, unchanged).** Benchmark run with the default timeouts
  (`python -m evaluation.run --real --sets main,heldout --default-timeouts`): 119 dense searches, 0 failed (0.0%); latency p50 229 ms,
  p95 337 ms, p99 353 ms, max 418 ms. The slowest search used 84% of the budget. In an earlier sweep on a busier machine 3 of about 560
  searches timed out (about 0.5%), and each silently degraded that query to keyword-only. Recommendation: raise it to 1.0 s. The
  query pipeline deadline is 1.8 s; embedding (0.4 s) and the search legs overlap only partly, so 1.0 s still fits. Not changed.
  `get_by_chunk_ids` (candidate embeddings for MMR) has no timeout of its own and should get one when the default is revisited.
  The flaky Milvus roundtrip test was fixed: store tests now raise the timeouts (they check correctness, not latency) and carry a
  `live_milvus` marker (about 20 s per test; deselect with `-m "not live_milvus"`).
* **No-answer recall is an explicit Phase 3 target.** Today it is 0/8 on main and 0/4 on held-out. Target: well above 0 without lowering
  answerable Recall@5 by more than 2 points.
* **`phase_exit` acceptance (Phase 2).** After heading paths, `phase_exit` Recall@5 must reach >= 0.8 on generation 2 (main: 3/8 today),
  and the held-out questions of the same kind are reported separately.
* **Benchmark.** Held-out set now has 6 more multi-file questions (7 in total).
* **Contracts (additive).** `embedding.proto`: `GetModelInfo`, `Rerank`. `retrieval.proto`: request `dense_query` (optional) and
  `previous_chunk_ids`; response `top_score` (optional) and `coverage` (enum `COVERAGE_UNSPECIFIED/STRONG/WEAK/NONE`); new RPC
  `GetDocumentChunks`. Until each server implements them they answer UNIMPLEMENTED or ignore the fields. Deploy order:
  embedding-service, retrieval-service, memory-service, llm-service. `tests/unit/test_proto_parity.py` keeps the vendored copies identical.

## Phase 2 (index generations, chunker v2, heading paths, provenance, tables, analyzers): results

Nothing is activated. Generation 1 stays the active (legacy, un-suffixed) index everywhere; generation 2 exists only in the benchmark's
throwaway indexes and as code. Activation is `make activate GENERATION=2` and is the owner's decision.

### What changed
| Area | Change |
|---|---|
| Generations | `index_generations` + `generation_documents` tables (Alembic `a7c3e91f2b64`: parser/chunker/embedding model+dimension/analyzer versions, status building/active/retired, created_at). Names `<base>_g<N>` for the Milvus collection and the OpenSearch index. Reads go through an OpenSearch alias (`OPENSEARCH_ACTIVE_ALIAS`) and the DB pointer; writes go to the active generation. Until a row exists the service uses the legacy names (generation 1), so applying the migration changes nothing at runtime. |
| Explicit creation | `scripts/provision_*.py --generation N` and `scripts/generation.py create N` create the collection/index from the versioned schema in `app/stores/index_schema.py`. A write into a missing index raises; nothing under `app/` creates an index. |
| Tooling | `make generation-status / generation-bootstrap / generation-create GENERATION=N / reindex GENERATION=N / activate GENERATION=N` (`scripts/generation.py`). `reindex` re-processes every READY current version into N without touching the active generation and records coverage; `activate` refuses unless coverage is complete AND Recall@5, MRR@10 and nDCG@10 on both sets are not worse than the active generation (`--dry-run` checks only). `retire` keeps data; `drop --yes` deletes it. |
| Chunker v2 | Sized in the embedding model's own tokens (`GetModelInfo` gives name, dimension, max_seq_length and tokenizer id; the tokenizer is loaded locally; the model name is validated against `EMBEDDING_MODEL_EXPECTED`). Target 200, overlap 30, hard maximum `max_seq_length - 16` including the heading-path prefix. Split order: paragraph, line, sentence, token window. Every chunk is re-counted and ingestion fails (`CHUNKER_ERROR`) if one exceeds the maximum. |
| Heading path | Markdown and Docling parsers keep the whole hierarchy. Every chunk's embedded AND indexed text starts with `<filename> › h1 › h2 › h3`; `section` stays the deepest heading; `heading_path` is stored and returned in the response metadata. |
| Provenance | Page per block; a new chunk starts when the page changes and the chunk is over half the target, otherwise `page_start`/`page_end`. Pages only for PDFs; slides and line/row ranges are `location`; no page is ever invented. |
| Tables | Never split mid-row. A table under the maximum is one chunk; larger ones become row groups repeating prefix, caption and header, `block_type=table`. CSV follows the same rule with `Rows a-b` locations. |
| Keyword mapping | `content`/`section`: `standard` + `.english` + `.exact` (identifiers kept whole at query time, split into snake_case/camelCase/hyphen/slash parts at index time); `filename.keyword`. Query fields: `content, content.english, content.exact^2, section^1.5, section.english, filename^2`, with the Phase 1 phrase boosts and the scope `filter`. |
| One embedding path | `app/ingestion/build.py` is the only path from parsed document to records for the orchestrator, `make reindex` and the benchmark; the vector is computed from `chunk.content`, the exact string indexed. |

### Benchmark: generation 1 vs generation 2 (real: MiniLM, live Milvus + OpenSearch, throwaway indexes created from each generation's schema; 0 degraded queries)
Raw: `evaluation/results/p2-real.json`. Request top_k=10 unless stated. Held-out now has 45 questions (41 answerable), including six
"exit criteria"-like ones phrased without "phase" or "exit criteria" (written before they were run).

| Metric | Main g1 | Main g2 | Held-out g1 | Held-out g2 |
|---|---|---|---|---|
| Recall@5 | 0.889 (64/72) | 0.993 (71.5/72) | 0.809 (33.17/41) | 0.915 (37.5/41) |
| Recall@10 | 0.931 (67/72) | 0.993 (71.5/72) | 0.870 (35.67/41) | 0.951 (39/41) |
| MRR@10 | 0.789 | 0.875 | 0.726 | 0.738 |
| nDCG@10 | 0.815 | 0.900 | 0.744 | 0.779 |
| Identifier exact recall@5 | 8/8 | 8/8 | n/a | n/a |
| Page attribution (all page questions) | 12/16 | 16/16 | 3/5 | 4/5 |
| Multi-file coverage | 7/8 | 7.5/8 | 5/7 | 7/7 |
| No-answer recall | 0/8 | 0/8 | 0/4 | 0/4 |

Acceptance: `phase_exit` Recall@5 >= 0.8 on generation 2: **main 8/8 (was 3/8), held-out 6/6**. Met.

Per category, Recall@5 / MRR@10 / nDCG@10, generation 1 -> generation 2:

| Set / category (n) | Generation 1 | Generation 2 |
|---|---|---|
| main phase_exit (8) | 0.375 / 0.184 / 0.287 | 1.000 / 0.521 / 0.640 |
| main csv (8) | 0.875 / 0.306 / 0.447 | 1.000 / 0.498 / 0.621 |
| main hinglish (8) | 0.875 / 0.802 / 0.848 | 1.000 / 0.917 / 0.938 |
| main page (8) | 1.000 / 0.875 / 0.908 | 1.000 / 0.938 / 0.954 |
| main multi_file (8) | 0.875 / 1.000 / 0.888 | 0.938 / 1.000 / 0.952 |
| main phase, phase_title, identifier, table (8 each) | 1.000 / 1.000 / 1.000 (table 1.000/0.938/0.954) | 1.000 / 1.000 / 1.000 |
| held-out phase_exit (6) | 0.833 / 0.417 / 0.490 | 1.000 / 0.514 / 0.605 |
| held-out multi_file (7) | 0.500 / 0.549 / 0.533 | 0.786 / 0.726 / 0.732 |
| held-out csv (2) | 0.500 / 0.208 / 0.393 | 1.000 / 0.292 / 0.465 |
| held-out paraphrase (5) | 0.733 / 0.700 / 0.706 | 0.800 / 0.700 / 0.743 |
| held-out body (9) | 1.000 / 1.000 / 1.000 | 1.000 / 0.944 / 0.959 **(MRR, nDCG lower)** |
| held-out hinglish (4) | 1.000 / 1.000 / 0.969 | 1.000 / 0.750 / 0.821 **(MRR, nDCG lower)** |
| held-out typo (4), page (1), table (3) | unchanged | unchanged |

Two held-out categories rank slightly worse on generation 2 (Recall@5 is unchanged at 1.0): `hinglish` MRR 1.00 to 0.75 (one question's
answer moved from rank 1 to a lower rank) and `body` MRR 1.00 to 0.94. Nothing was tuned against the held-out set. No default was
changed (generation 2 is not active), so this is a finding for the activation decision, not a blocker I acted on.

### top_k 5 / 8 / 10 re-check on generation 2 (Recall@k of the returned set)
| | top_k=5 | top_k=8 | top_k=10 |
|---|---|---|---|
| Main g1 | 0.847 (61/72) | 0.889 (64/72) | 0.931 (67/72) |
| Main g2 | 0.979 (70.5/72) | 0.979 (70.5/72) | 0.993 (71.5/72) |
| Held-out g1 | 0.764 (31.33/41) | 0.825 (33.83/41) | 0.870 (35.67/41) |
| Held-out g2 | 0.772 (31.67/41) | 0.919 (37.67/41) | 0.951 (39/41) |

On generation 2, 8 versus 5 is worth 6 held-out questions (31.67 to 37.67 of 41) and nothing on main; 10 versus 8 adds 1.33 held-out and
1 main. This supports the decision top_k=8 (llm-service, Phase 5). Chunks are smaller in generation 2 (about 46 tokens median, at most
224 against a 240 limit), so 8 chunks cost fewer prompt tokens than 8 did before.

### Not verified / open
* Generation 2 has not been built in the shared dev environment from real documents (`make generation-bootstrap/create/reindex`): the
  running dev embedding-service does not yet serve `GetModelInfo`; to be done at final integration after it is restarted.
* The Docling heading-level logic is covered by the benchmark PDF only (all its headings are the same size, so it exercises one level).
* The Docling table-caption and slide-location paths have no test document in the fixtures.

## Phases 3 to 6 and configurability: what was measured

Everything here is real mode (MiniLM, live Milvus and OpenSearch, throwaway indexes created from generation 2's schema, 0 degraded queries) on
the 80-question main set and the 45-question held-out set (41 answerable). Held-out results are reported, never tuned against.
Raw: `evaluation/results/p3-norerank-real.json`, `p3-ms-real.json`, `p3-ms-cal-real.json`. How to choose models and parsing: `docs/configuration.md`.

### Heading boost and neighbour expansion (no reranker), generation 2, top_k=10

| | Main R@5 | Main MRR | Main nDCG | Held-out R@5 | Held-out MRR | Held-out nDCG |
|---|---|---|---|---|---|---|
| `HEADING_MATCH_BOOST=1.0` (default) | 0.993 (71.5/72) | 0.875 | 0.900 | 0.915 (37.5/41) | 0.738 | 0.779 |
| `HEADING_MATCH_BOOST=0` | 0.979 (70.5/72) | 0.813 | 0.854 | 0.915 (37.5/41) | 0.738 | 0.779 |
| `EXPAND_NEIGHBOURS=true` | 0.993 (71.5/72) | 0.875 | 0.900 | 0.939 (38.5/41) | 0.743 | 0.788 |

* The boost still helps on main (`phase` MRR 0.76 to 1.00, `hinglish` MRR 0.78 to 0.92) and changes nothing on held-out in any category, so it stays on.
* Neighbour expansion gains one held-out question at no cost on main. The extra prompt tokens (up to two neighbours per chunk, median chunk
  46 tokens) were not measured. It stays off by default; this is the owner's call.

### Reranker (`cross-encoder/ms-marco-MiniLM-L-6-v2`, English), 30 passages reranked

| | Main R@5 | Main R@8 | Main MRR | Main nDCG | Held-out R@5 | Held-out R@8 | Held-out MRR | Held-out nDCG |
|---|---|---|---|---|---|---|---|---|
| no reranker | 0.993 (71.5/72) | 0.993 | 0.875 | 0.900 | 0.915 (37.5/41) | 0.951 (39/41) | 0.738 | 0.779 |
| with reranker | 1.000 (72/72) | 1.000 (72/72) | 0.975 | 0.979 | 0.890 (36.5/41) | 0.963 (39.5/41) | 0.811 | 0.838 |

* Ranking improves on both sets (MRR +0.10 main, +0.07 held-out). Held-out Recall@5 drops by one question (36.5 vs 37.5 of 41, minus 2.4 points,
  just over the 2-point budget the plan set), while Recall@8 rises. Held-out categories that dropped: `body` R@5 1.00 to 0.89, `multi_file`
  0.79 to 0.64, `phase_exit` MRR 0.51 to 0.35; gains: `paraphrase` R@5 0.80 to 1.00, `csv`, `hinglish`, `table` MRR up.
* With the reranker on, `HEADING_MATCH_BOOST` on and off give identical results in every category on both sets (the reranker makes it redundant).
  Re-evaluate again if the reranker changes.
* Cost: the whole query takes 2.0 to 2.4 s median (3.0 to 4.6 s p95) against 0.4 to 0.6 s without. That exceeds `RERANK_TIMEOUT_SECONDS` (0.8 s) and the
  query deadline (1.8 s), so with the defaults every reranked query falls back. It stays **off by default**.
* **`BAAI/bge-reranker-v2-m3` (multilingual): timing only, quality not measured.** 34.6 s median, 43.4 s p95 per 30 passages of 200 tokens (20 calls, idle
  machine), 12 s to load, 2.8 GB. A full quality run needs 252 queries at that speed, about 2.4 hours; the one attempt was killed by a session end
  without a result. It cannot serve interactive queries on this CPU. No recommendation can be made for a multilingual reranker from this data.

### Weak-retrieval signal (coverage): not safe to enable

* Rank-derived signal (no reranker): the best candidate is rank 1 in at least one engine whether or not the question is answerable
  (answerable top scores 0.90 to 1.00, unanswerable 0.87 to 1.00), so it labels everything STRONG. Off (`COVERAGE_RANK_SIGNAL_ENABLED=false`).
* Reranker signal: thresholds for ms-marco picked on the main set (`weak_min` -9, `strong_min` -4.5):

| Set | Answerable: STRONG / WEAK / NONE | Unanswerable: STRONG / WEAK / NONE |
|---|---|---|
| Main (tuned on) | 67 / 5 / 0 | 1 / 1 / 6 |
| Held-out (not tuned on) | 29 / 5 / **7** | 0 / 0 / 4 |

  On main, no-answer recall is 7/8 and precision 7/12. On held-out, 12 of 41 answerable questions would be told "the documents do not cover this"
  (7 as none-found), and only 4 of the 16 flagged questions are truly unanswerable. The main-set calibration did not generalise, so the thresholds
  are **not shipped** (`RERANK_COVERAGE_THRESHOLDS` is empty and `COVERAGE_ENABLED=false`). The earlier figure of 7/8 no-answer recall from the first
  reranker run used placeholder thresholds that did not fit that model's score scale, and should be ignored.
* **The Phase 3 target (no-answer recall well above 0 without lowering answerable Recall@5 by more than 2 points) is not met safely.** What would help:
  a stronger multilingual reranker (unmeasured, too slow on CPU) or many more unanswerable examples to calibrate on.

### Phase 4 (embedding-service under load): see `embedding-service/docs/performance.md`
Query p50 behind a 128-text ingestion batch: 8.9 s before (3 samples), 0.10 to 0.21 s with priority lanes (52 to 96 samples), p95 0.49 to 0.73 s.
The 0.4 s query target is still missed under load on this shared machine. Priority lanes ship off (`PRIORITY_LANES_ENABLED=false`). Redis cache untested against a real server.

### Phase 5, retrieval side
Delivered and tested with fakes: `dense_query` (embedded for the dense leg; the keyword leg keeps `query`), `previous_chunk_ids` (added as candidates only
after the authorisation and current-version check), `GetDocumentChunks`, one `retrieval.feedback` log line per query (hash, never the text unless
`LOG_QUERY_TEXT`). **Not done:** the follow-up and Hinglish rewrite cases from `llm-service/docs/retrieval-rewrite-cases.md` are not in the benchmark yet.

### Phase 6
* Selective OCR (`app/ingestion/ocr.py`): detection, page-ordered merge with real page numbers, quality and cost metrics, never fails ingestion. Tested with a
  fake engine and a real generated PDF whose middle page has no text. **No OCR model is installed, so recognition itself, its cost and its time per page are unmeasured.**
* Contextual chunk text (`app/ingestion/contextual.py`, generation 3 = `--contextual`): tested with mock HTTP only. **Generation 3 was not built or benchmarked**
  (it needs an LLM endpoint, and sends document text to it).
* Embedding-model comparison: run for the two cached multilingual models, see the next section. `bge-m3` and `multilingual-e5-base` are not cached and were not downloaded.

### Configuration (this change)
Reranker on/off and choice, embedding model (prefixes, sequence cap, path guard, model taken from the active generation), Docling (enable, table options,
device, threads, timeout, converter reuse) and the optional features are settings; `docs/configuration.md` lists them, what each needs (restart or new generation) and the
timings above. Changed tests: `test_reranker_scores_decide_the_order...` now supplies coverage thresholds for its fake model (a reranker without them gives a score but no verdict).
Docling parse time for a 6-page PDF was measured per setting: none of the settings changes it much (15 to 20 s warm).

### Embedding-model comparison (recommendation only; nothing was switched)

Generation 2 chunking, no reranker, live Milvus and OpenSearch, models run in-process on CPU from the local cache (no downloads), top_k=10, 0 degraded
queries. Each model gets the chunk sizes its own `max_seq_length` allows (MiniLM 256, e5-large 512, multilingual mpnet 128), so the comparison includes that
effect. Raw: `evaluation/results/emb-e5large-real.json`, `emb-mpnet-real.json`, `p3-norerank-real.json` (MiniLM).

| | MiniLM-L6-v2 (current) | multilingual-e5-large (`query:` / `passage:`) | paraphrase-multilingual-mpnet-base-v2 |
|---|---|---|---|
| Size on disk / vector size | 263 MB / 384 | 2.2 GB / 1024 | 1.1 GB / 768 |
| Chunks / max chunk tokens | 80 / 224 | 79 / 467 | 105 / 103 |
| Main R@5 / R@8 | 0.993 (71.5/72) / 0.993 | 0.993 (71.5/72) / 1.000 (72/72) | 0.979 (70.5/72) / 0.986 |
| Main MRR / nDCG | 0.875 / 0.900 | **0.925 / 0.939** | 0.896 / 0.916 |
| Held-out R@5 / R@8 | 0.915 (37.5/41) / 0.951 | **0.931 (38.2/41) / 0.963** | 0.833 (34.2/41) / 0.846 |
| Held-out MRR / nDCG | 0.738 / 0.779 | **0.787 / 0.817** | 0.739 / 0.749 |
| Query latency (whole pipeline, median) | 0.4 to 0.6 s | 1.3 s | 0.9 s |
| Ingest of the 7 fixtures | about 64 s | 80 s | 65 s |

Per category (R@5 / MRR@10):

| Category | MiniLM | e5-large | mpnet |
|---|---|---|---|
| main hinglish (8) | 1.00 / 0.92 | **1.00 / 1.00** | 1.00 / 1.00 |
| held-out hinglish (4) | 1.00 / 0.75 | **1.00 / 1.00** | 1.00 / 1.00 |
| main phase_exit (8) | 1.00 / 0.52 | **1.00 / 0.85** | 0.88 / 0.48 |
| held-out paraphrase (5) | 0.80 / 0.70 | **0.93 / 0.75** | 0.73 / 0.70 |
| held-out multi_file (7) | 0.79 / 0.73 | 0.79 / 0.72 | 0.64 / 0.64 |
| held-out body (9) | 1.00 / 0.94 | 1.00 / 0.89 | 0.89 / 0.83 |

* **multilingual-e5-large is the better candidate**: higher MRR and nDCG on both sets, better Recall@8, and Hinglish MRR 1.00 on both sets (MiniLM 0.92 and 0.75).
  Not better everywhere: held-out `body` MRR is lower (0.89 vs 0.94). No category's Recall@5 got worse.
* **paraphrase-multilingual-mpnet is not recommended**: held-out Recall@5 drops to 0.833 (34.2/41) from 0.915, and `multi_file` and `phase_exit` get worse.
* Costs of e5-large: 8 times the model size, vectors 2.7 times larger (more Milvus storage), roughly 3 times the query time in this in-process setup (0.9 s more
  per query; through embedding-service it was not measured), and slower ingestion. Switching needs a new index generation (`docs/configuration.md`).
* Limits of this evidence: 80 and 45 questions, and a single run each; the e5 chunks are larger (up to 467 tokens), so model and chunk size are mixed together.
  `bge-m3` and `multilingual-e5-base` were not evaluated (not in the local cache, and nothing was downloaded).
