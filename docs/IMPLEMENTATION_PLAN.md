# Retrieval Service — Implementation Plan (Coding Phases)

This document breaks [`LLD.md`](./LLD.md) into an ordered sequence of coding phases for the **GraphGPT Retrieval Service**, the same way `file-service/docs/IMPLEMENTATION_PLAN.md` breaks down File Service. Each phase is scoped so it can be built, tested, and merged independently — later phases depend only on earlier ones, never the reverse. File/module paths reference the directory tree in [`LLD.md §1`](./LLD.md#1-directory-to-module-mapping).

Conventions: **FastAPI** (health/metrics only), **grpcio** (the real API surface, LLD §7), **SQLAlchemy (async) + PostgreSQL**, **Alembic**, **aiokafka**, **pymilvus**, **opensearch-py**, **Docling**, **uv** for dependency management — matching `file-service`/`conversation-service`.

**Two external dependencies this plan cannot close on its own** — flagged here up front because they gate specific phases below, not because this repo can resolve them:
1. File Service's batch permission-check endpoint (HLD §16.2) — does not exist yet. Phase 7 builds Retrieval's client against the documented contract and a local fake; end-to-end integration with the real endpoint is blocked until File Service ships it.
2. `file.processing_retry` carrying `content_version` (HLD §1.3 #1, §8.5) — File Service's current payload doesn't have it. Phase 5's retry consumer is built to require the field and reject events missing it, rather than guess.

---

## Phase Overview

| Phase | Name | Depends on | Can be tested in isolation? |
|---|---|---|---|
| 0 | Project bootstrap & local infra | — | Service boots in both modes, `/health/live` returns 200 |
| 1 | PostgreSQL layer (jobs, inbox, outbox, version projection) | 0 | Repository unit tests against a test DB |
| 2 | Milvus + OpenSearch store layer | 0 | Store unit/integration tests, collections/index provisioned |
| 3 | embedding-service client | 0 | Contract test against a running embedding-service |
| 4 | Ingestion pipeline core (no Kafka yet) | 1, 2, 3 | `orchestrator.run(job)` invoked directly in a test |
| 5 | Kafka consumers + bounded ingestion + outbox producer | 1, 4 | `file.uploaded` in → chunks queryable, end to end |
| 6 | Partial-index recovery + reconciliation | 1, 2, 5 | Forced single-store failure recovers correctly |
| 7 | File Service permission-check client | 0 | Contract test against a local fake; fail-closed proven |
| 8 | Query pipeline core (fusion, MMR, scoring, version filter) | 1, 2, 3 | `pipeline.run()` unit-tested with fakes for every client |
| 9 | gRPC server + servicer wiring | 7, 8 | `GetRelevantChunks` callable over real gRPC |
| 10 | Deployment topology (API/worker split) | 5, 9 | Both `DEPLOYMENT_MODE`s boot correctly, independently |
| 11 | Observability | all above | Dashboards/alerts wired |
| 12 | Testing completion | all above | CI green, coverage target met |
| 13 | Deployment & CI/CD | all above | Deploys to staging via pipeline |

Each phase below lists: **Goal**, **Tasks**, **Files touched**, **Exit criteria (Definition of Done)**, and **HLD/LLD references**.

---

## Phase 0 — Project Bootstrap & Local Infrastructure

**Goal:** A running, empty service with all infra dependencies available locally, booting correctly in both `DEPLOYMENT_MODE`s from day one — so the API/worker split (LLD §9) is never retrofitted later.

**Tasks:**
1. Scaffold the directory tree from LLD §1 (empty `__init__.py` files, no logic yet).
2. `pyproject.toml`: `fastapi`, `uvicorn`, `grpcio`, `grpcio-tools`, `pydantic-settings`, `sqlalchemy[asyncio]`, `asyncpg`, `alembic`, `aiokafka`, `pymilvus`, `opensearch-py`, `docling`, `structlog`, `prometheus-client`.
3. `protos/retrieval.proto` — copied verbatim from `llm-service/proto/retrieval.proto` (HLD §1.1 #2). Generate `app/grpc/retrieval_pb2.py`/`retrieval_pb2_grpc.py` via `grpcio-tools`, wired into the build (`Makefile` target, not committed generated code, matching however the monorepo already handles generated proto code elsewhere — confirm the convention against `llm-service` before deciding).
4. `app/core/config.py` — full `Settings` table from LLD §2, including `DEPLOYMENT_MODE`.
5. `app/core/logging.py` — structlog setup, contextvars (`trace_id`, `document_id`, `event_id`).
6. `app/main.py` + `app/lifespan.py` — branch on `DEPLOYMENT_MODE` per LLD §9; both branches are stubs at this phase (construct clients, start nothing real yet) except the FastAPI health app, which runs in every mode.
7. `app/api/routers/health.py` — `GET /health/live` only (no dependency checks yet — added in Phase 11 once there's something real to check).
8. `docker-compose.yml` — Postgres, MinIO (reuse file-service's bucket if running locally against the same stack), Milvus (standalone), OpenSearch, Kafka (reuse from `conversation-service`/`file-service` if already running locally).
9. `scripts/provision_milvus_collection.py`, `scripts/provision_opensearch_index.py` — stubs at this phase (schemas land in Phase 2); their existence *outside* `app/` from day one is what makes LLD §13.2's "pods never create the collection" rule structurally true from the start, not bolted on later.
10. `Dockerfile`, `Makefile`, `.env.example`, `README.md`.
11. Alembic initialized (`alembic.ini`, `app/db/migrations/env.py`) — no migrations yet.

**Exit criteria:**
- `docker compose up` brings up Postgres + MinIO + Milvus + OpenSearch + Kafka + app cleanly.
- `DEPLOYMENT_MODE=api` and `DEPLOYMENT_MODE=worker` both boot without error (worker mode starts nothing yet beyond the health app — that's expected at this phase).
- `curl localhost:$HTTP_PORT/health/live` returns `200` in both modes.
- `uv run pytest` runs (even with zero tests) without import errors.
- Generated proto stubs importable; a trivial unit test constructs a `GetRelevantChunksRequest` and reads its fields back.

**HLD/LLD refs:** LLD §1 (directory tree), §2 (config), §9 (deployment topology), HLD §1.1 #2 (frozen proto).

---

## Phase 1 — PostgreSQL Layer

**Goal:** All four Retrieval-owned tables exist and are reachable through repositories, independent of Milvus/OpenSearch/Kafka/gRPC.

**Tasks:**
1. `app/models/ingestion_job.py`, `processed_event.py`, `outbox_event.py`, `document_current_version.py` — SQLAlchemy models matching LLD §3.1–§3.4 exactly, including `milvus_status`/`opensearch_status`/`embedding_status` as independent columns (not a single derived enum stored redundantly).
2. `app/core/enums.py` — `IngestionStatus` (HLD §19's seven values), stage-status enum (`PENDING`/`COMPLETE`/`FAILED`).
3. `app/db/base.py` — Declarative base importing all models.
4. Alembic migration: all four tables, all indexes from LLD §3 (`ix_ingestion_jobs_status`, `ix_ingestion_jobs_document_id`, `ix_outbox_events_unpublished`, the `uq_ingestion_jobs_doc_version` constraint, `document_current_versions`'s primary key).
5. `app/repositories/ingestion_job_repository.py` — `upsert_job`, `get_by_document_and_version`, `recompute_status` (the derivation rule from LLD §3.1's closing note), `list_partially_indexed`, `list_stuck_processing` (for Phase 6's reconciliation sweep).
6. `app/repositories/processed_event_repository.py` — implements the **corrected** inbox semantics from LLD §3.2: the same-transaction insert alongside the job upsert, and the status-dispatch lookup on redelivery (not a blind skip).
7. `app/repositories/document_current_version_repository.py` — `advance_if_newer` (the monotonic `WHERE current_version < :new` guard, LLD §3.4), `get_many(document_ids) -> dict[str, int]`.
8. `app/repositories/outbox_repository.py` — `insert`, `fetch_unprocessed_for_update` (`FOR UPDATE SKIP LOCKED`, same pattern as file-service), `mark_processed`.

**Exit criteria:**
- `alembic upgrade head` runs clean against a fresh Postgres.
- Unit test: two "job creation" calls in a single transaction (job upsert + inbox insert + version-projection advance) either all commit or all roll back together (transaction-boundary test, directly targeting the crash-window bug LLD §3.2 fixes).
- Unit test: `advance_if_newer` called with a lower version after a higher one is a no-op — proves the reorder-safety guard (HLD §13.3) before any Kafka code exists to exercise it for real.
- Unit test: `recompute_status` produces `PARTIALLY_INDEXED` when exactly one of `milvus_status`/`opensearch_status` is `FAILED`, and never produces `READY` unless both are `COMPLETE`.

**HLD/LLD refs:** HLD §11, §13.3, §15, §19, §20; LLD §3 (all subsections).

---

## Phase 2 — Milvus + OpenSearch Store Layer

**Goal:** Both search engines are provisioned once (never by application code) and reachable through narrow, testable repository wrappers.

**Tasks:**
1. `scripts/provision_milvus_collection.py` — creates `retrieval_document_chunks` per the schema in LLD §4 (HNSW index on `embedding`, scalar indexes on `document_id` **and** `content_version`, per HLD §13.3's two-clause filter). Run manually against local infra in this phase — never imported by `app/`.
2. `scripts/provision_opensearch_index.py` — creates the index per LLD §5's mapping (`filename`/`section` boosted fields).
3. `app/stores/milvus_repository.py` — `upsert_chunks`, `search(query_vector, top_n, current_versions)` (the per-document `document_id AND content_version` filter, LLD §4), `get_by_chunk_ids` (HLD §18.1), `delete_by_document_id` (HLD §21, unconditional across all versions).
4. `app/stores/opensearch_repository.py` — same four-operation shape, `search` building the `should`-of-`bool` per-document filter from LLD §5.
5. `app/models/chunk_record.py` (or a shared dataclass in `app/query`/`app/ingestion` — decide based on where it's imported from both ingestion and query code without a circular import) — the `ChunkRecord` shape shared by both stores' `upsert_chunks`.

**Exit criteria:**
- Integration test (real, already-running Milvus/OpenSearch, not fresh containers): provision once, `upsert_chunks` a handful of records, `search` returns them ranked, `get_by_chunk_ids` returns exact vectors for a known set, `delete_by_document_id` removes all versions.
- Idempotency test: `upsert_chunks` the same records twice — no duplicates in either store (chunk_id as primary key / `_id`, per HLD §11/§13/§14).
- Version-filter test: two chunks with the same `document_id` but different `content_version` — `search(..., current_versions={doc: v2})` returns only the `v2` chunk, proving HLD §13.3's correctness fix before any query pipeline code exists to exercise it for real.
- Static check: no `create_collection`/`create_index`/`PUT <index>` call exists anywhere under `app/` (grep-based test, mirroring file-service's reconciliation-worker `list_objects` check) — enforces LLD §13.2's "infra provisions, pods never create" rule as a CI gate, not just a convention.

**HLD/LLD refs:** HLD §13, §13.1–§13.3, §14, §21; LLD §4, §5.

---

## Phase 3 — embedding-service Client

**Goal:** Both embedding call shapes (batch for ingestion, single for query) work against the real embedding-service, with the model/dimension guard enforced from day one.

**Tasks:**
1. `app/clients/embedding_client.py` — `embed_query` (single, `EMBEDDING_QUERY_TIMEOUT_SECONDS`) and `embed_chunks` (batched per `EMBEDDING_BATCH_SIZE`, `EMBEDDING_INGESTION_TIMEOUT_SECONDS`), per LLD §8.1.
2. Model/dimension assertion on every response (`EMBEDDING_MODEL_EXPECTED`/`EMBEDDING_DIMENSION_EXPECTED`) — raises `EmbeddingModelMismatchError`, wired to `retrieval_embedding_model_mismatch_total` (metric added properly in Phase 11, but the raise/log path exists now).
3. Retry policy: `EMBEDDING_RETRY_ATTEMPTS`, exponential backoff.

**Exit criteria:**
- Integration test against a real, already-running embedding-service: `embed_query("test")` returns a 384-dim vector; `embed_chunks([...])` returns one vector per input, same order.
- Unit test (mocked gRPC response): a response claiming a different `model`/`dimension` raises `EmbeddingModelMismatchError` and never returns a vector to the caller.
- Timeout test: a deliberately slow mock server triggers the configured timeout, not a hang.

**HLD/LLD refs:** HLD §12; LLD §2, §8.1.

---

## Phase 4 — Ingestion Pipeline Core (No Kafka Yet)

**Goal:** `orchestrator.run(job)` works correctly end to end when invoked directly (from a test, not from a Kafka consumer) — proving the download→parse→chunk→embed→index pipeline and its `READY`/`PARTIALLY_INDEXED`/`FAILED` branching before any Kafka wiring is layered on.

**Tasks:**
1. `app/ingestion/downloader.py` — MinIO S3 GET (read-only client, `MINIO_DOWNLOAD_TIMEOUT_SECONDS`).
2. `app/ingestion/parser.py` — Docling wrapper, extracting page/heading/paragraph/table/section structure (HLD §9).
3. `app/ingestion/chunker.py` — structure-aware recursive chunking (`CHUNK_TARGET_TOKENS`/`CHUNK_MAX_TOKENS`/`CHUNK_OVERLAP_TOKENS`, HLD §10).
4. `app/ingestion/chunk_identity.py` — `parse_content_version(storage_key)` (HLD §8.3 Phase 1, strict regex, raises on mismatch) and `derive_chunk_id(document_id, content_version, chunk_index)` (deterministic `uuid5`, HLD §11).
5. `app/ingestion/embedder.py` — thin wrapper calling `embedding_client.embed_chunks` over the chunked output.
6. `app/ingestion/indexer.py` — `write(chunks_with_embeddings, target={MILVUS, OPENSEARCH, BOTH})`, calling both stores **concurrently** via `asyncio.gather` when `target=BOTH`.
7. `app/ingestion/orchestrator.py` — the full state machine from LLD §6.2: `PROCESSING` → download → parse → chunk → embed → index, branching to `READY`/`PARTIALLY_INDEXED`/`FAILED` per which stage(s) fail, publishing to `outbox_events` (not yet to real Kafka — the outbox row is the observable artifact this phase's tests assert against; Phase 5 makes it actually reach a broker) at each of `file.processing.started`/`.completed`/`.failed`.

**Exit criteria:**
- Integration test (real MinIO/Milvus/OpenSearch, a small real PDF fixture): construct an `ingestion_jobs` row directly, call `orchestrator.run(job)`, assert `status == READY`, `chunk_count` matches the actual chunk count, chunks are queryable in both stores, and exactly one `file.processing.completed` outbox row exists with the right `chunk_count`.
- Forced-failure test: point `job.storage_key` at a nonexistent object — assert `status == FAILED`, `error_code == 'DOWNLOAD_ERROR'`, a `file.processing.failed` outbox row exists, and **no** `file.processing.started`-triggered side effect is left half-done.
- Forced-partial-failure test (mock one store's `upsert_chunks` to raise): assert `status == PARTIALLY_INDEXED`, the failed store's status column is `FAILED`, the succeeded one is `COMPLETE`, and **no** `file.processing.completed` outbox row was written (directly testing LLD §3.3's same-transaction guarantee).
- Idempotency test: derive `chunk_id`s for the same object twice (simulating a redelivered event) — identical ids both times.

**HLD/LLD refs:** HLD §8.3, §9, §10, §11, §12, §19, §20; LLD §6.2, §6.3 (the recompute rule is exercised for real in Phase 6, but the underlying `orchestrator` primitives it reuses are built here).

---

## Phase 5 — Kafka Consumers, Bounded Ingestion, Outbox Producer

**Goal:** The pipeline built in Phase 4 is actually driven by real Kafka events, with the corrected inbox semantics (LLD §3.2), bounded concurrency (LLD §6.1.1), and a real outbox worker publishing the three produced events.

**Tasks:**
1. `app/events/payloads.py` — Pydantic models for all six event shapes, byte-matched against HLD §8: consumed `file.uploaded`, `file.deleted`, `file.processing_retry` (**with the required `content_version` field** — HLD §1.3 #1; a payload missing it is rejected, not defaulted), produced `file.processing.started/completed/failed`.
2. `app/events/topics.py` — topic name constants.
3. `app/events/consumers/file_uploaded_consumer.py` — the corrected flow from LLD §6.1: parse `content_version` → single transaction (job upsert + version-projection advance + inbox insert) → commit → ack → enqueue onto the bounded queue.
4. `app/events/consumers/file_processing_retry_consumer.py` — looks up the job by `(document_id, event.content_version)`; rejects (dead-letter/log, per LLD §6.1) any event missing `content_version`.
5. `app/events/consumers/file_deleted_consumer.py` — per LLD §6.4 (blind-skip dedup is safe here, unlike the ingestion path — both stores' deletes are naturally idempotent).
6. §6.1.1's bounded ingestion pool: `asyncio.Queue(maxsize=INGESTION_MAX_CONCURRENCY * 2)` + `INGESTION_MAX_CONCURRENCY` worker coroutines calling `orchestrator.run()`.
7. `app/delete/delete_service.py` — `purge(document_id)`, concurrent Milvus+OpenSearch delete, `ingestion_job_repository.mark_all_versions_deleted`.
8. `app/events/producers/processing_status_producer.py` + `app/workers/outbox_worker.py` — real poll-and-publish loop (same shape as file-service's), started only in `DEPLOYMENT_MODE=worker`.

**Exit criteria:**
- Integration test (real Kafka, real stores): publish a real `file.uploaded` message → assert a chunk becomes queryable end to end, and a real `file.processing.completed` message is observed on the output topic.
- Redelivery test: publish the same `event_id` twice — first delivery processes normally; second delivery, with the job already `READY`, is a no-op (proving LLD §3.2's status-dispatch table, not a blind skip).
- Crash-window regression test: simulate a crash between "transaction commits" and "job enqueued" (kill the process there) — restart, confirm the reconciliation sweep (Phase 6) picks up the stuck `PROCESSING` job rather than it being lost silently.
- Backpressure test: publish more events than `INGESTION_MAX_CONCURRENCY` can immediately handle — assert consumer-group lag grows (observable) rather than unbounded concurrent `orchestrator.run()` calls.
- Malformed-retry test: publish a `file.processing_retry` with no `content_version` — assert it's rejected/dead-lettered, never silently processed against a guessed version.

**HLD/LLD refs:** HLD §1.3 #1, §8.2, §8.5, §8.6, §21; LLD §6.1, §6.1.1, §6.4.

---

## Phase 6 — Partial-Index Recovery + Reconciliation

**Goal:** A `PARTIALLY_INDEXED` job actually recovers correctly (LLD §6.3's recompute-from-MinIO fix), and stuck jobs get swept without relying on redelivery.

**Tasks:**
1. §6.3's recovery path: re-download from `job.storage_bucket`/`storage_key`, re-parse, re-chunk, and — per the corrected rule — re-embed whenever `milvus_status == FAILED` (never skip embedding just because OpenSearch is the only failed target, per LLD §6.3's stated reasoning), then `indexer.write(target=<failed store only>)`.
2. `app/workers/reconciliation_worker.py` — periodic sweep: `list_partially_indexed(older_than=X)` → run §6.3's recovery; `list_stuck_processing(older_than=Y)` → re-enqueue (the safety net for the crash-after-ack case Phase 5's redelivery-based recovery can't cover on its own, LLD §3.2's closing paragraph).
3. Wire into `lifespan.py`, `DEPLOYMENT_MODE=worker` only, `RECONCILIATION_INTERVAL_SECONDS`.

**Exit criteria:**
- Forced-failure-then-recovery test: force OpenSearch to fail during ingestion, confirm `PARTIALLY_INDEXED`, run the recovery path directly, confirm `READY` and a `file.processing.completed` event, and confirm the OpenSearch document now exists.
- Same test with Milvus as the failed target — confirm re-embedding actually happened (e.g. via a call-count assertion on a mocked `embedding_client` in this specific test), not silently skipped.
- Stuck-job sweep test: create a job in `PROCESSING` with `updated_at` older than the threshold and no corresponding in-flight work — run one sweep, assert it gets re-enqueued.
- Chunk-id stability test: recovery's re-parse/re-chunk of the same object reproduces identical `chunk_id`s to the original attempt (proving the upsert-not-duplicate claim in LLD §6.3's closing note).

**HLD/LLD refs:** HLD §19, §20; LLD §3.2 (reconciliation as the crash-after-ack safety net), §6.3.

---

## Phase 7 — File Service Permission-Check Client

**Goal:** `file_service_client.py` is fully built and tested against the contract HLD §16.2 specifies, with fail-closed behavior proven under every failure mode — even though the real File Service endpoint doesn't exist yet.

**Tasks:**
1. `app/clients/file_service_client.py` — `batch_check_permissions(user_id, document_ids) -> list[str]`, authenticating via `FILE_SERVICE_SERVICE_CREDENTIAL` (service-to-service, HLD §16.2 — never a bare `user_id` claim).
2. `FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS` enforced (0.3s default, LLD §7.7's budget).
3. A local fake/stub server implementing the documented contract (`POST /internal/files/permissions/check` or the gRPC equivalent — whichever HLD §16.2 ultimately specifies once File Service's shape is confirmed), used for this repo's own tests until the real endpoint exists.
4. **Explicit fail-closed enforcement**: every failure mode (timeout, connection refused, 5xx, malformed response) returns `[]`, never the input `document_ids` unfiltered and never "everything authorized."

**Exit criteria:**
- Unit test against the local fake: authorized subset returned correctly for a mixed authorized/unauthorized request.
- Fail-closed test suite — one test per failure mode (timeout, connection error, malformed response, 5xx) — every single one asserts `batch_check_permissions` returns `[]`, not a partial/best-effort result. This is the one client in the codebase where "return less" is not good enough — it must be "return nothing" (LLD §8.2's explicit callout).
- Latency test: the fake server delayed beyond `FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS` triggers the client's own timeout, not the caller's outer deadline.

**HLD/LLD refs:** HLD §16.1, §16.2; LLD §8.2 — flagged in this plan's header as blocked on a real File Service endpoint for full end-to-end integration.

---

## Phase 8 — Query Pipeline Core

**Goal:** Every pure/near-pure query-pipeline component — scope guard, fusion, MMR, scoring, version resolution, response mapping — is built and unit-tested with fakes standing in for every external client, before any gRPC wiring exists.

**Tasks:**
1. `app/query/scope_guard.py` — the two-line HLD §16.3/§16.4 decision (not `FILES`, or empty `file_ids` → stop), plus the `unsupported_scope` metric hook.
2. `app/query/fusion.py` — Reciprocal Rank Fusion (LLD §7.4).
3. `app/query/candidate_embeddings.py` — Milvus primary-key fetch by `chunk_id` (HLD §18.1).
4. `app/query/mmr.py` — MMR selection (LLD §7.5), producing `mmr_score` for internal tracking only.
5. `app/query/scoring.py` — the `dense_score`/`bm25_score`/`dense_rank`/`bm25_rank`/`rrf_score`/`mmr_score` bookkeeping dataclass (HLD §18.2).
6. `app/query/dense_search.py`, `bm25_search.py` — thin wrappers over `milvus_repository.search`/`opensearch_repository.search`, taking `current_versions` (HLD §13.3).
7. `app/query/response_mapper.py` — internal chunk → proto `DocumentChunk`, `relevance_score = rrf_score` (never `mmr_score`, HLD §18.2), final sort by `rrf_score` descending.
8. `app/query/pipeline.py` — the full orchestration from LLD §7.2, **including the explicit degradation matrix** (both succeed / OpenSearch-only-failed / Milvus-only-failed, MMR skipped in the last case / both fail) and the version-resolution step (§13.3) — every external call (permission check, embedding, Milvus, OpenSearch) goes through an injected client interface so this phase's tests use fakes, not real services.

**Exit criteria:**
- Unit test: `scope_guard` rejected for all three non-`FILES` enum values and for empty `file_ids`, accepted only for `FILES` + non-empty.
- Unit test: `fusion` — a candidate present in only one input list still gets a correct `rrf_score`; the standard RRF formula verified against hand-computed values for a small fixed input.
- Unit test: `mmr` — with `λ` near 1.0, selection matches pure relevance order; with `λ` near 0.0, selection visibly diversifies away from near-duplicate embeddings (a synthetic near-duplicate-vector fixture makes this assertable).
- Unit test: `response_mapper` — `relevance_score` on every returned `DocumentChunk` equals the candidate's `rrf_score`, never `mmr_score`, across both the both-succeeded and degraded-path candidate shapes.
- **Degradation-matrix test, one case per row of LLD §7.2's table**: both stores succeed (full path exercised); OpenSearch fails (MMR still runs, uses Milvus embeddings); Milvus fails (MMR is skipped, `search_degraded_total{engine="bm25_only", mmr="skipped"}` fires, final order is `rrf_score`-descending direct from BM25's single list); both fail (`empty_response()`).
- Unit test: `pipeline.run()` given a `current_versions` map — search calls received by the fake stores carry the correct per-document filter, proving HLD §13.3 is actually wired through, not just documented.

**HLD/LLD refs:** HLD §13.3, §16.3, §16.4, §17, §18, §18.1, §18.2; LLD §7.2–§7.6.

---

## Phase 9 — gRPC Server + Servicer Wiring

**Goal:** `GetRelevantChunks` is callable over a real gRPC connection, with the deadline/timeout hierarchy from LLD §7.7 enforced end to end and every client from Phases 3/7/2 wired in for real (no more fakes at this layer).

**Tasks:**
1. `app/grpc/server.py` — `grpc.aio` server bootstrap, `GRPC_PORT`/`GRPC_MAX_WORKERS`/`GRPC_MAX_MESSAGE_LENGTH_BYTES`, graceful shutdown.
2. `app/grpc/interceptors/logging_interceptor.py` — binds `request.trace_id` into structlog contextvars for the duration of the call.
3. `app/grpc/interceptors/metrics_interceptor.py` — per-RPC latency/status histogram (metric definitions land properly in Phase 11; the interceptor and its hook point exist now).
4. `app/grpc/retrieval_servicer.py` — implements `GetRelevantChunks`, wrapping `pipeline.run(request)` in `asyncio.wait_for(..., timeout=QUERY_PIPELINE_DEADLINE_SECONDS)` per LLD §7.1, returning `empty_response()` on `asyncio.TimeoutError` rather than raising.
5. Wire real `embedding_client`, `file_service_client`, `milvus_repository`, `opensearch_repository`, `document_current_version_repository` into `pipeline.run()`'s dependencies (constructed in `lifespan.py`, `DEPLOYMENT_MODE=api`).

**Exit criteria:**
- Integration test: a real gRPC client (generated stub) calls `GetRelevantChunks` against a running server backed by real Postgres/Milvus/OpenSearch/embedding-service and the Phase 7 fake File Service — full round trip, correct `DocumentChunk`s back.
- Deadline test: artificially slow one downstream client beyond its configured sub-budget (LLD §7.7) — assert the *overall* RPC still returns within `QUERY_PIPELINE_DEADLINE_SECONDS` plus a small margin, empty rather than hanging or erroring.
- Load test at low concurrency: confirm `GRPC_MAX_WORKERS` doesn't bottleneck a modest burst of concurrent `GetRelevantChunks` calls against fast fakes for the downstream clients (a sanity check for this phase, not the full perf pass — that's Phase 12).

**HLD/LLD refs:** HLD §6, §17; LLD §7.1, §7.7.

---

## Phase 10 — Deployment Topology (API/Worker Split)

**Goal:** The `DEPLOYMENT_MODE=api`/`worker` split from LLD §9 is verified as a real, independently-scalable deployment shape, not just a config flag that happens to exist.

**Tasks:**
1. `lifespan.py`'s final branch: `api` starts gRPC server (Phase 9) + health app, nothing Kafka-related; `worker` starts Kafka consumers + bounded ingestion pool + outbox worker + reconciliation sweep (Phases 5–6) + health app, no gRPC.
2. Kubernetes manifests (or Helm chart, matching whatever file-service/conversation-service already use): `retrieval-api` Deployment (HPA on gRPC request rate/CPU) and `retrieval-worker` Deployment (HPA on Kafka consumer-group lag), both from the same image, differing only in `DEPLOYMENT_MODE`.
3. `docker-compose.yml` updated to run both modes as separate services locally (`retrieval-api`, `retrieval-worker`), matching the production shape rather than the earlier bootstrap phase's single combined container.

**Exit criteria:**
- `DEPLOYMENT_MODE=api` container: gRPC port open, Kafka consumer group shows zero active consumers from this process.
- `DEPLOYMENT_MODE=worker` container: gRPC port not listening, Kafka consumer group shows active consumption.
- Static check: `retrieval-api`'s process never opens a DB write path to `ingestion_jobs`/`outbox_events` (read-only where it touches Postgres at all — only `document_current_versions` reads), `retrieval-worker`'s process never binds the gRPC port (grep/process-inspection test, same style as file-service's `list_objects` static check).
- Scale-independently smoke test: scale `retrieval-worker` replicas up under a simulated upload burst, confirm `retrieval-api`'s query latency (measured via a concurrent load generator) is unaffected.

**HLD/LLD refs:** HLD §22; LLD §9.

---

## Phase 11 — Observability

**Goal:** Metrics, logs, and health checks match LLD §9 (observability metrics list) and HLD §24.

**Tasks:**
1. `app/api/routers/health.py` — `GET /health/ready`: Postgres ping always; Milvus/OpenSearch ping only in `api` mode (query path's real dependencies); MinIO/Kafka ping only in `worker` mode — mode-aware readiness, not a single check list shared blindly across both deployments.
2. `app/core/metrics.py` — all metrics from LLD §9's list: `retrieval_ingestion_jobs_total{status}`, `retrieval_ingestion_stage_duration_seconds{stage}`, `retrieval_query_latency_seconds`, `retrieval_query_short_circuit_total{reason}`, `retrieval_unsupported_scope_total{scope}`, `retrieval_search_degraded_total{engine}`, `retrieval_permission_check_latency_seconds`, `retrieval_embedding_model_mismatch_total`.
3. Wire the metric-emitting call sites left as stubs in Phases 3/5/8/9 into real Prometheus instruments.
4. `structlog` contextvars confirmed end to end: one `GetRelevantChunks` call's logs all carry the same `trace_id`; one ingestion job's logs all carry the same `document_id`/`event_id`.

**Exit criteria:**
- `/metrics` scrapeable and shows non-zero values for every metric above after running the Phase 5/6/8 integration tests once.
- `/health/ready` in `api` mode returns `503` when Milvus or OpenSearch is down and `200` when Kafka/MinIO are down (they're irrelevant to that mode); `/health/ready` in `worker` mode returns `503` when Kafka or MinIO is down.
- Log-correlation test: grep a test run's structured log output for a known `trace_id`, confirm every line from that one request carries it.

**HLD/LLD refs:** HLD §24; LLD §9 (metrics list), §10.

---

## Phase 12 — Testing Completion

**Goal:** Fill in whatever the per-phase exit-criteria tests didn't already cover, per LLD §11.

**Tasks:**
1. Unit coverage sweep for anything not already at a high bar from earlier phases: `chunk_identity`, `fusion`, `mmr`, `scope_guard`, `scoring`.
2. Full integration suite runnable via `docker compose -f docker-compose.test.yml up` + `pytest tests/integration` — one command, matching file-service's convention.
3. Contract tests: `protos/retrieval.proto` diffed against `llm-service/proto/retrieval.proto` on every CI run (fails loudly on drift — this contract must never silently diverge from its source of truth); event payload snapshot tests for all six shapes in `app/events/payloads.py` against HLD §8's documented JSON.
4. Idempotency test suite consolidated: Kafka redelivery (inbox), Milvus/OpenSearch upsert-not-duplicate, outbox replay — built per-phase, grouped here into one "replay safety" CI job.
5. Load/perf smoke test: concurrent `GetRelevantChunks` calls at a realistic rate, confirming p95 latency stays within the LLD §7.7 budget under load, not just in isolation.
6. Degradation-drill test: deliberately kill Milvus (or OpenSearch, or embedding-service, or the File Service fake) mid-load-test, confirm the service degrades per HLD §17/LLD §7.2's matrix rather than error-cascading or hanging.

**Exit criteria:** CI pipeline green across unit/integration/contract suites; coverage threshold met (align with the repo-wide standard, e.g. `auth-service`'s).

**HLD/LLD refs:** LLD §11.

---

## Phase 13 — Deployment & CI/CD

**Goal:** Ship it.

**Tasks:**
1. `Dockerfile` production build (multi-stage, non-root user, matching `auth-service`/`file-service` conventions) — a single image, `DEPLOYMENT_MODE` selecting behavior at runtime (Phase 10).
2. Kubernetes manifests/Helm chart for both `retrieval-api` and `retrieval-worker` Deployments + their respective HPAs (Phase 10), referencing the shared Milvus/OpenSearch/Postgres/Kafka/MinIO infrastructure.
3. CI pipeline: lint → unit tests → integration tests (docker-compose test stack) → contract tests (proto diff, event payload snapshots) → build image → push → deploy to staging.
4. Alembic migration step wired into the deploy pipeline (run-before-rollout), and the two `scripts/provision_*.py` scripts wired as a **separate, one-time, manually-triggered** pipeline step per environment (LLD §13.2/HLD §13.2 — never part of the regular rollout pipeline).
5. Runbook doc: what to check first if `/health/ready` fails in each mode, how to manually trigger the reconciliation sweep, how to inspect Kafka consumer lag and the outbox backlog, what "File Service permission-check endpoint is down" looks like from Retrieval's side (empty results, `retrieval_permission_check_latency_seconds` timing out) versus a real Retrieval bug.

**Exit criteria:** A merge to `main` deploys both `retrieval-api` and `retrieval-worker` to staging automatically; manual promotion to prod is a single approved step; the collection/index provisioning scripts are confirmed to never run as part of that automatic path.

**HLD/LLD refs:** HLD §13.2, §22; LLD §9, §13.2.

---

## Cross-Phase Notes

* **Query pipeline (Phase 8) and ingestion pipeline (Phases 4–6) can be built in parallel by different people** — they share only the Postgres layer (Phase 1) and the store layer (Phase 2) as real dependencies, and Phase 8 is deliberately built against fakes for every external client, per HLD §22's independent-scaling framing carried all the way down into how the code itself is developed and tested.
* **The two File Service dependencies flagged at the top of this document (permission-check endpoint, `content_version` on retry events) block *end-to-end* integration, not *this repo's* progress.** Phases 5 and 7 are fully buildable and testable against local fakes; track the actual File Service changes as separate tickets in that repo, and re-run this repo's integration suite against the real endpoints once they ship, rather than blocking any phase here on their delivery.
* **Every phase that introduces a new Kafka consumer or producer must add its own redelivery/idempotency test in that same phase** — don't defer all of it to Phase 12; Phase 12 only consolidates and closes gaps.
* **Phase 5 (Kafka wiring) is the highest-risk phase** — it's where the corrected inbox semantics (LLD §3.2), bounded concurrency (§6.1.1), and the version-projection's reorder-safety guard (HLD §13.3) all have to work together correctly under real, possibly-out-of-order delivery. Land Phase 4 (the pipeline's internals, driven directly rather than via Kafka) solidly first, so Phase 5 is additive wiring rather than a rewrite — the same reasoning file-service used for its own AV-scan phase.
* **Nothing in Phases 0–3 should require touching Phase 4+ code** except adding new config keys or new topic/model constants — if a later phase needs to change `chunk_identity.py`'s signature, for example, that's a signal an earlier phase under-specified something and should be revisited, not silently patched around.
