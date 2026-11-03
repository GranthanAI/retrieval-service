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
| 1 | Query-time fixes (MMR, request fields, HNSW `ef`, embed fallback, phrase boosts, dedupe, per-file balance) | main (local commit, not pushed) | **Done, awaiting review** |
| 2 | Index generations, tokenizer-aware chunking, heading paths, provenance, tables, analyzers | | Not started |
| 3 | Reranker, neighbour expansion, weak-retrieval signal | | Not started |
| 4 | embedding-service performance under load | | Not started |
| 5 | llm-service / memory-service: query rewrite, previous chunks, full-text for small docs, anchors, feedback logging | | Not started |
| 6 | Selective OCR, contextual chunk text, embedding model evaluation | | Not started |

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
