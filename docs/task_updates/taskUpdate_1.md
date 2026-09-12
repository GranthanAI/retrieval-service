# Task Update 1 — Project Bootstrap, PostgreSQL Layer & Store Layer

This document records the completion of Phases 0–2 of the Retrieval Service development lifecycle (see [`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)), complete with code snippets, configuration details, and the real infra issues hit and resolved along the way.

---

## 1. Phase 0 — Project Bootstrap & Local Infrastructure

### 1.1 Directory Structure

Scaffolded the full `app/` tree from LLD §1 (`grpc/`, `api/`, `query/`, `ingestion/`, `events/`, `clients/`, `delete/`, `repositories/`, `stores/`, `models/`, `db/`, `core/`, `workers/`), matching file-service's/conversation-service's established package shape — `core/config.py` + `core/logging.py`, `db/base.py`/`postgres.py`/`session.py`, Alembic under `app/db/migrations/`.

### 1.2 Proto Codegen Convention

Surveyed the monorepo before deciding: `llm-service` and `memory-service` commit generated `_pb2.py`/`_pb2_grpc.py` files; `embedding-service` gitignores them and generates at build time. Followed the majority convention (commit generated stubs) since `retrieval.proto` is copied verbatim from `llm-service` (HLD §1.1 #2) and is diffed against it in CI — keeping the generated pair in git makes drift visible in the same PR that would introduce it:

```makefile
proto:
	uv run python -m grpc_tools.protoc -Iprotos --python_out=app/grpc --grpc_python_out=app/grpc protos/retrieval.proto
	uv run python -c "import glob, re; [open(p, 'w', encoding='utf-8').write(re.sub(r'import (\w+_pb2) as (\w+__pb2)', r'from app.grpc import \1 as \2', open(p, 'r', encoding='utf-8').read())) for p in glob.glob('app/grpc/*_pb2_grpc.py')]"
```

### 1.3 Deployment-Mode Branching (`app/lifespan.py`)

`DEPLOYMENT_MODE` (`api` | `worker` | `all`) branches lifespan startup per LLD §9 — both branches are stubs at this phase, only the FastAPI health app is real:

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    if settings.DEPLOYMENT_MODE in ("api", "all"):
        await _startup_api()
    if settings.DEPLOYMENT_MODE in ("worker", "all"):
        await _startup_worker()
    yield
```

### 1.4 Shared Infra, Not a New Compose Stack

Per explicit instruction, retrieval-service does **not** run its own docker-compose stack — it reuses the monorepo's root `docker-compose.yml` (Postgres, Kafka, Milvus, file-service's MinIO), adding only what genuinely didn't exist yet:

- **OpenSearch** — no service in the monorepo covered BM25 before this; added as a new `opensearch` service (single-node, security plugin disabled for dev).
- A `retrieval_service_db` Postgres init script (`docker/postgres/init-retrieval-service-db.sql`), mirroring file-service's own `init-file-service-db.sql` pattern.
- retrieval-service's own `docker-compose.yml` stays a commented-out deployment template, matching file-service's/auth-service's convention — the app runs locally via `uv run uvicorn`, not containerized.

### 1.5 Native-Postgres Port Conflict — Resolved Without Touching the Native Service

The dev machine has a native Windows `postgresql-x64-18` service already listening on `0.0.0.0:5432`, winning over Docker's port forward. Rather than stopping or reconfiguring that native install, the root `postgres` service now publishes **two** host ports onto the same container:

```yaml
postgres:
  ports:
    - "5432:5432"
    - "5433:5432"   # second host port onto the SAME container — for hosts
                     # where a native Postgres already owns 5432
```

`retrieval-service/.env.example`'s `DATABASE_URL` points at `localhost:5433` — same container, same `retrieval_service_db`, just a different host-side port. Every other service's `localhost:5432` config is untouched.

### 1.6 Exit Criteria Verified

- `DEPLOYMENT_MODE=api` and `=worker` both boot; `GET /health/live` → `200` in both.
- `uv run pytest` passes (proto roundtrip test — constructs a `GetRelevantChunksRequest`, reads fields back).
- `docker compose config` on the root file validates.

---

## 2. Phase 1 — PostgreSQL Layer

### 2.1 Schema (LLD §3, Verified Column-for-Column Against the Live DB)

Four tables — `ingestion_jobs`, `processed_events`, `outbox_events`, `document_current_versions` — generated via `alembic revision --autogenerate` against the real dev DB, then checked with `psql \d` to confirm every column, index, and constraint matches LLD §3.1–§3.4 exactly (`uq_ingestion_jobs_doc_version`, `ix_ingestion_jobs_status`, `ix_ingestion_jobs_document_id`, `ix_outbox_events_unpublished`'s partial-index `WHERE published = false`).

### 2.2 Corrected Inbox Semantics (LLD §3.2)

`ProcessedEventRepository.mark_processed` is a single `INSERT ... ON CONFLICT DO NOTHING`, documented as **must be called in the same DB transaction as the ingestion_jobs upsert it accompanies** — never committed alone. This is what closes the crash window where an event could be marked "processed" while the job row it was supposed to create never lands:

```python
async def mark_processed(db: AsyncSession, event_id: UUID, consumer: str) -> bool:
    stmt = (
        pg_insert(ProcessedEvent)
        .values(event_id=event_id, consumer=consumer)
        .on_conflict_do_nothing(index_elements=["event_id", "consumer"])
        .returning(ProcessedEvent.event_id)
    )
    result = await db.execute(stmt)
    await db.flush()
    return result.first() is not None
```

### 2.3 Monotonic Version Projection (LLD §3.4 / HLD §13.3)

`DocumentCurrentVersionRepository.advance_if_newer` uses a conditional `ON CONFLICT DO UPDATE ... WHERE current_version < EXCLUDED.current_version` — proven with a test that advances to v3 then "arrives" at v2 and confirms v3 still wins (File Service's Kafka producer doesn't key by `document_id`, so reordering is real, not hypothetical).

### 2.4 Status Derivation (LLD §3.1's Closing Note)

`IngestionJobRepository.recompute_status` derives the coarse `status` column purely from the three per-stage columns (`embedding_status`/`milvus_status`/`opensearch_status`) — never set independently, so it can't drift out of sync:

```python
if embedding == StageStatus.FAILED.value:
    job.status = IngestionStatus.FAILED.value
elif milvus == StageStatus.COMPLETE.value and opensearch == StageStatus.COMPLETE.value:
    job.status = IngestionStatus.READY.value
elif milvus == StageStatus.FAILED.value and opensearch == StageStatus.FAILED.value:
    job.status = IngestionStatus.FAILED.value
elif milvus == StageStatus.FAILED.value or opensearch == StageStatus.FAILED.value:
    job.status = IngestionStatus.PARTIALLY_INDEXED.value
else:
    job.status = IngestionStatus.PROCESSING.value
```

### 2.5 Exit Criteria Verified (testcontainers, Real Alembic Chain)

Following file-service's own pattern — an ephemeral Postgres via `testcontainers`, migrated with the real Alembic chain (not `create_all()`):

- Transaction-boundary test: job upsert + inbox insert + version-projection advance commit together, and roll back together.
- `advance_if_newer` reorder-safety test (lower version after higher is a no-op).
- `recompute_status` derivation tests (`PARTIALLY_INDEXED` on exactly one failed target; `READY` only when both `COMPLETE`; `FAILED` on embedding failure).

8/8 passing.

---

## 3. Phase 2 — Milvus + OpenSearch Store Layer

### 3.1 Provisioning Scripts, Never Imported by `app/`

`scripts/provision_milvus_collection.py` / `provision_opensearch_index.py` create the collection/index once, idempotently, entirely outside `app/` — enforced not just by convention but by a CI-gate test (§3.4 below).

### 3.2 A Real LLD Deviation, Found and Resolved Empirically

LLD §4 declares `conversation_id`/`page_number`/`section` as `nullable=True`. In practice:

- **Milvus v2.3.0** (the version originally pinned in root `docker-compose.yml`) silently drops the `nullable` kwarg client-side — no such concept exists in that server version.
- **Milvus v2.4.15** (upgraded to, on the assumption 2.4+ supports nullable fields) accepts `nullable=True` at collection-creation time without error, but the **server rejects a real `NULL` insert**:
  ```
  MilvusException: (code=1100, message=varchar field 'conversation_id' is illegal,
  array type mismatch: invalid parameter[expected=need string array][actual=got nil])
  ```
  confirmed by directly inserting a row with `conversation_id=None` against the live container, not assumed from documentation.

**Resolution** (decided with the user rather than picked unilaterally): keep Milvus on v2.4.15, declare the three fields as plain non-nullable, and encode absent values as sentinels **only at the Milvus repository boundary** — `""` for the two VARCHAR fields, `-1` for `page_number`. `ChunkRecord` itself, and OpenSearch (which handles real `None` fine), are unaffected:

```python
def _to_row(record: ChunkRecord) -> dict:
    return {
        ...,
        "conversation_id": record.conversation_id if record.conversation_id is not None else _ABSENT_VARCHAR,
        "page_number": record.page_number if record.page_number is not None else _ABSENT_PAGE_NUMBER,
        "section": record.section if record.section is not None else _ABSENT_VARCHAR,
        ...
    }
```

Documented in both `scripts/provision_milvus_collection.py`'s module docstring and `milvus_repository.py`.

### 3.3 Write/Read Consistency

`MilvusClient.upsert`/`.delete` don't make data immediately visible to a subsequent `search`/`query` (segment growth is async) — every store method now calls `client.flush(...)` after a write, and `search`/`query` pass `consistency_level="Strong"`. Without this, the idempotency and delete-visibility tests failed intermittently.

### 3.4 Store Layer Shape (LLD §4/§5)

Both `MilvusRepository` and `OpenSearchRepository` expose exactly four operations (`upsert_chunks`, `search`, `get_by_chunk_ids` / n/a, `delete_by_document_id`), synchronous SDKs wrapped in `asyncio.to_thread` — the same pattern file-service already uses for its own synchronous MinIO client. `ChunkRecord` (`app/models/chunk_record.py`) is the one shared record shape both stores' `upsert_chunks` consume.

### 3.5 Exit Criteria Verified (Real, Already-Running Milvus/OpenSearch)

- Upsert → search → get-by-chunk-ids → delete roundtrip, both stores.
- Idempotency: upserting the same record twice produces no duplicate (`chunk_id` as Milvus primary key / OpenSearch `_id`).
- Version-filter correctness (HLD §13.3): two chunks, same `document_id`, different `content_version` — `search(..., current_versions={doc: v2})` returns only the `v2` chunk, on both stores.
- Static CI-gate test: no `create_collection` / Milvus `.create_index(` / OpenSearch `indices.create` call exists anywhere under `app/` (scoped to exclude Alembic's own Postgres migrations, which legitimately manage Retrieval's own Postgres schema — a different concern from LLD §13.2).

15/15 tests passing across Phases 1–2 combined.

---

## 4. Infra Changes Summary (Root `docker-compose.yml`)

| Change | Reason |
|---|---|
| Added `opensearch` service | No existing service covered BM25 |
| `postgres` now publishes `5433:5432` in addition to `5432:5432` | Native Postgres on the dev host already owns 5432 |
| `milvus-standalone` bumped `v2.3.0` → `v2.4.15` | v2.3.0 has no nullable-field concept at all; needed for LLD §4's schema (nullable fields still don't fully work at insert time even on 2.4.15 — see §3.2 above, worked around at the repository layer instead of chasing further version bumps) |
| Mounted `retrieval-service/docker/postgres/init-retrieval-service-db.sql` | Creates `retrieval_service_db` on first init, mirroring file-service's own init script |

No other service's configuration was changed; all additions are backward compatible with what file-service/memory-service already depend on in the same containers.
