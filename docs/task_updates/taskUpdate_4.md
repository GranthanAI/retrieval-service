# Task Update 4 — Progress Snapshot, Local Environment Notes & Open Items

A cross-cutting checkpoint after Phases 0–4 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — not a new phase's worth of code, but the operational reality of running this service's dependencies locally, and what's still open before Phase 5.

---

## 1. Progress Snapshot

| Phase | Status | Verified against |
|---|---|---|
| 0 — Project bootstrap | ✅ Done | Both `DEPLOYMENT_MODE`s boot, `/health/live` 200 |
| 1 — PostgreSQL layer | ✅ Done | testcontainers Postgres + real Alembic chain, 8 tests |
| 2 — Milvus + OpenSearch store layer | ✅ Done | Real, already-running Milvus/OpenSearch, 7 tests |
| 3 — embedding-service client | ✅ Done | Real, already-running embedding-service |
| 4 — Ingestion pipeline core (no Kafka) | ✅ Done | Real MinIO/Milvus/OpenSearch/embedding-service + a real PDF fixture, 5 tests |
| 5 — Kafka consumers, bounded ingestion, outbox producer | Not started | — |
| 6 — Partial-index recovery + reconciliation | Not started | — |
| 7 — File Service permission-check client | Not started | — |
| 8 — Query pipeline core | Not started | — |
| 9+ | Not started | — |

**Total: 20/20 tests passing** across the whole suite as of this checkpoint.

## 2. Local Infra Is Not Persistent Across Sessions — A Recurring, Expected Condition

Docker containers on this machine have gone idle (`Exited`, not removed) between sessions multiple times over the course of Phases 0–4 — this appears to be Docker Desktop restarting between work sessions, not anything this codebase does. Each time, the fix has been the same and takes seconds, because nothing destructive ever happened to the underlying volumes:

```bash
docker start graphgpt-postgres graphgpt-kafka milvus-etcd milvus-minio milvus-standalone graphgpt-file-minio graphgpt-opensearch
```

If a container is missing entirely (not just stopped) rather than just stopped, `docker compose up -d <service>` from the monorepo root recreates it against the same named volume, with no data loss — confirmed directly during the Milvus v2.3.0 → v2.4.15 upgrade in Task Update 1, where `milvus-etcd`/`milvus-minio`/`milvus-standalone` were fully recreated and correctly reattached to `granthan_milvus_etcd_data`/`_minio_data`/`_standalone_data`.

**embedding-service is not part of the root `docker-compose.yml`** — it's a separate local process (see Task Update 2 for why: its own `pyproject.toml`/`uv` mismatch). It survives independently of the Docker containers but does not survive a full machine restart; if `localhost:8040`/`localhost:50054` aren't responding, it needs to be started again from `embedding-service/`:

```bash
.venv/Scripts/uvicorn.exe app.main:app --host 0.0.0.0 --port 8040
```

## 3. Known Cross-Service Mismatches, Tracked but Not Yet Reconciled

Carried forward from Task Update 1, still open:

- **`GRPC_PORT` mismatch with llm-service.** This repo's `.env.example` defaults `GRPC_PORT=50060` (LLD §2's documented default), but `llm-service`'s own `.env.example` currently points `RETRIEVAL_SERVICE_PORT` at `50053`. Neither side has been changed — flagged in both `.env.example`'s comments — because reconciling it is really a decision about which port is authoritative across two repos, not something to silently pick one side of. Needs resolving before Phase 9's real gRPC wiring, not before.
- **File Service prerequisites** (HLD §16.2's batch permission-check endpoint; HLD §1.3 #1's `content_version` field on `file.processing_retry`) are still just documented dependencies — Phase 7 builds against a local fake regardless of whether File Service has shipped them yet, per the Implementation Plan's own framing of these as tickets in a different repo, not blockers for this one.

## 4. Nothing in Phases 0–4 Required Revisiting Earlier Work

Per the Implementation Plan's own closing note ("nothing in Phases 0–3 should require touching Phase 4+ code except adding new config keys or new topic/model constants") — this held. Phase 4 added zero changes to Phase 0–3 code beyond what was always going to be additive: new files under `app/ingestion/`, one new exception class (`EmbeddingModelMismatchError`), and the proto-fixup script fix (a latent bug fix, not a design change). Phase 1's `IngestionJobRepository.recompute_status` — built with fakes in Phase 1, before any real ingestion code existed — was reused by the orchestrator in Phase 4 completely unmodified, which is the sharpest evidence the phase boundaries in the plan are doing their job.

## 5. Next Up

Phase 5 (Kafka consumers, bounded ingestion, outbox producer) is flagged in the Implementation Plan as the highest-risk phase — the corrected inbox semantics (LLD §3.2), bounded concurrency (§6.1.1), and the version-projection's reorder-safety guard (HLD §13.3) all have to work together correctly under real, possibly-out-of-order delivery. Phase 4 landing solidly first (orchestrator driven directly, not via Kafka) is what the plan expects to make Phase 5 additive wiring rather than a rewrite.
