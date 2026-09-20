# Task Update 8 — Phase 8: Query Pipeline Core

This document records the completion of Phase 8 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — every pure/near-pure query-pipeline component built and unit-tested with fakes standing in for every external client, before any gRPC wiring exists (Phase 9).

---

## 1. A Real Gap Found Before Writing `response_mapper.py`: Store Hits Had No Content

LLD §7.6's `response_mapper.to_proto()` reads `c.content`, `c.document_id`, `c.filename`, `c.page_number`, `c.section`, `c.chunk_index` straight off the fused candidate. But Phase 2's `DenseSearchHit`/`BM25SearchHit` (`app/stores/types.py`) only ever carried `chunk_id`/`score` — `milvus_repository.search()` requested `output_fields=["chunk_id"]` only, and `opensearch_repository.search()` never extracted `_source` beyond `chunk_id`/`score`. Building `fusion.py`/`response_mapper.py` against the real hit types (not a hypothetical richer one) would have failed the moment a real chunk needed to reach the wire.

Fixed at the source rather than bolting a second per-chunk lookup onto `pipeline.py`: both hit dataclasses gained the six provenance fields, `milvus_repository.search()`'s `output_fields` was expanded (and its sentinel-decoding logic — `-1`/`""` back to `None`, documented in Task Update 1 — now applies on the read path too, not just at write time), and `opensearch_repository.search()` now extracts the full `_source`. Re-ran Phase 2's own store-layer tests afterward to confirm nothing regressed (6/6 still passing).

## 2. `scope_guard.py`, `fusion.py`, `mmr.py` — Built Exactly to LLD §7.3–§7.5

No deviations worth noting here — these three are close to verbatim translations of the LLD's own pseudocode. `RetrievalScope` was added to `app/core/enums.py` as the mirror of the frozen proto's enum LLD §1 calls for, and `app/core/metrics.py` was created now (not deferred whole-hog to Phase 11) with the two counters Phase 8 actually calls (`unsupported_scope_counter`, `search_degraded_total`) plus the rest of LLD §10's metric list as defined-but-not-yet-wired-elsewhere instruments — matching the Implementation Plan's own framing that Phase 11 "wires call sites left as stubs in Phases 3/5/8/9," not that it invents the metrics module from scratch.

## 3. `pipeline.py` — One Real Spec Bug Found and Resolved

LLD §7.1/§7.2's own pseudocode references `request.request_id` throughout (`return empty_response(request.request_id)`, etc.). **The actual frozen proto has no such field** — `GetRelevantChunksRequest` has `trace_id`, not `request_id` (only the *response* has `request_id`). This is a real inconsistency in the LLD text against the proto it's supposed to be implementing exactly (HLD §1.1 #2's whole point). Resolved by using `request.trace_id` to populate the response's `request_id` field throughout `pipeline.py` — the closest existing correlation field on the request, and consistent with `trace_id`'s already-stated purpose (structlog contextvar correlation, LLD §10) extending naturally to gRPC response correlation too.

The degradation matrix (LLD §7.2's table) is implemented as one shared code path for "both succeed" and "OpenSearch-only-failed" (per the LLD's own closing note that these are "the same code path with the other list simply empty"), and a distinct path for "Milvus-only-failed" (MMR genuinely skipped, `search_degraded_total{engine="bm25_only"}` fires, Top-K taken directly since `fusion.py`'s output is already `rrf_score`-descending).

## 4. Exit Criteria Verified

- `scope_guard`: all three non-`FILES` enum values and empty `file_ids` rejected; `FILES` + non-empty accepted.
- `fusion`: hand-computed RRF values verified against a small fixed input (`k=60`, two candidates each appearing in both lists at swapped ranks, confirmed to land at the same score); a dense-only candidate still gets a correct `rrf_score`.
- `mmr`: `λ=0.999` reproduces pure relevance order; `λ=0.01` with a synthetic near-duplicate-vector fixture picks the orthogonal, lower-relevance candidate over the near-duplicate higher-relevance one — diversity visibly winning.
- `response_mapper`: `relevance_score` equals `rrf_score` (never `mmr_score`) across both a candidate with an `mmr_score` set and one without (the MMR-skipped shape).
- **Degradation-matrix, one test per row**: both-succeed (full RRF+MMR path, both chunk_ids present); OpenSearch-only-failed (same, using Milvus's own embeddings for MMR); Milvus-only-failed (`search_degraded_total{engine="bm25_only"}` counter genuinely increments, final order is `rrf_score`-descending straight from BM25's list); both-fail (`empty_response`).
- `pipeline.run()` given a real `current_versions` map (seeded via `DocumentCurrentVersionRepository` against a real, migrated ephemeral Postgres) — the fake Milvus/OpenSearch repositories' recorded `search()` calls carry exactly that map, proving HLD §13.3 is wired through the pipeline, not just documented.

18 pure-unit tests (`scope_guard`/`fusion`/`mmr`/`response_mapper`) plus 8 pipeline tests (real Postgres for version resolution, fakes for every other client) — 66/66 across the whole suite.
