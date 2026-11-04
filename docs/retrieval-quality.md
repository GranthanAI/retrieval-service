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
