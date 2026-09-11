# Low-Level Design (LLD) — GraphGPT Retrieval Service

This document provides a detailed, production-grade Low-Level Design (LLD) for the **Retrieval Service**, mapping directly to the architecture frozen in [`HLD.md`](./HLD.md). It follows the same structure and depth used by [`file-service/docs/LLD.md`](../../file-service/docs/LLD.md) so the two services stay consistent for anyone reading across the monorepo. Every design decision below traces back to a specific HLD section — this document does not introduce new architecture, it specifies how the frozen HLD gets built.

Stack: **FastAPI** (health/metrics only — the service's real API surface is gRPC, per HLD §6), **grpcio** + **grpcio-tools**, **SQLAlchemy (async) + PostgreSQL** (job/inbox state only, HLD §15), **aiokafka**, **Alembic**, **pymilvus**, **opensearch-py**, **Docling**, **uv** for dependency management — matching `file-service`/`conversation-service`.

---

## 1. Directory to Module Mapping

```text
retrieval-service/
├── protos/
│   └── retrieval.proto                # copied verbatim from llm-service (HLD §1.1 #2) — never edited here
├── app/
│   ├── grpc/
│   │   ├── retrieval_servicer.py      # implements RetrievalServiceServicer.GetRelevantChunks (HLD §18)
│   │   ├── server.py                  # grpc.aio server bootstrap, interceptors, graceful shutdown
│   │   └── interceptors/
│   │       ├── logging_interceptor.py # binds trace_id (request field, HLD §17) into structlog contextvars
│   │       └── metrics_interceptor.py # per-RPC latency/status histogram (HLD §24 observability)
│   ├── api/                            # FastAPI app — health/readiness/metrics ONLY, no business endpoints
│   │   ├── routers/
│   │   │   └── health.py               # GET /health/live, GET /health/ready (Postgres + Milvus + OpenSearch ping)
│   │   └── middleware.py               # RequestIDMiddleware (matches file-service's pattern)
│   ├── query/                          # Query-time pipeline (HLD §18)
│   │   ├── pipeline.py                 # Orchestrates the full GetRelevantChunks flow end to end
│   │   ├── scope_guard.py              # HLD §16.3/§16.4 — short-circuit on scope != FILES or empty file_ids
│   │   ├── dense_search.py             # Milvus ANN search, Top-N
│   │   ├── bm25_search.py              # OpenSearch BM25 search, Top-N
│   │   ├── fusion.py                   # Reciprocal Rank Fusion (HLD §18)
│   │   ├── candidate_embeddings.py     # Milvus fetch-by-chunk_id for MMR (HLD §18.1)
│   │   ├── mmr.py                      # Maximal Marginal Relevance selection (HLD §18.1)
│   │   ├── scoring.py                  # dense_score/bm25_score/rrf_score/mmr_score bookkeeping (HLD §18.2)
│   │   └── response_mapper.py          # internal chunk → proto DocumentChunk (HLD §18.2)
│   ├── ingestion/                      # Ingestion-time pipeline (HLD §2 Ingestion Pipeline)
│   │   ├── orchestrator.py             # Per-event ingestion workflow: download → parse → chunk → embed → index
│   │   ├── downloader.py               # MinIO S3 client (read-only, HLD §5)
│   │   ├── parser.py                   # Docling wrapper (HLD §9)
│   │   ├── chunker.py                  # Structure-aware recursive chunking (HLD §10)
│   │   ├── chunk_identity.py           # content_version parsing + uuid5 chunk_id derivation (HLD §8.3, §11)
│   │   ├── embedder.py                 # Batch embedding via embedding-service (HLD §12)
│   │   └── indexer.py                  # Writes Milvus + OpenSearch, idempotent upsert by chunk_id (HLD §13/§14)
│   ├── events/
│   │   ├── consumers/
│   │   │   ├── file_uploaded_consumer.py       # file.uploaded → ingestion_jobs row + orchestrator trigger
│   │   │   ├── file_deleted_consumer.py        # file.deleted → delete_service (HLD §21)
│   │   │   └── file_processing_retry_consumer.py  # file.processing_retry → re-run orchestrator (HLD §8.5)
│   │   ├── producers/
│   │   │   └── processing_status_producer.py   # file.processing.started/completed/failed (HLD §8.6, outbox pattern)
│   │   ├── topics.py                   # Topic name constants (consumed + produced)
│   │   └── payloads.py                 # Pydantic models for every consumed/produced event, byte-matching HLD §8
│   ├── clients/
│   │   ├── embedding_client.py         # gRPC client to embedding-service (GenerateEmbedding/GenerateEmbeddings)
│   │   └── file_service_client.py      # gRPC/HTTP client for the batch permission-check endpoint (HLD §16.2)
│   ├── delete/
│   │   └── delete_service.py           # Purges a document_id from Milvus + OpenSearch (HLD §21), idempotent
│   ├── repositories/
│   │   ├── ingestion_job_repository.py # ingestion_jobs table CRUD (HLD §19 states)
│   │   ├── processed_event_repository.py  # inbox pattern (HLD §8.6, §11, corrected semantics — §3.2), matches
│   │   │                               #  file-service's processed_event_ids
│   │   ├── document_current_version_repository.py  # document_current_versions (HLD §13.3, §3.4) — monotonic
│   │   │                               #  advance-if-newer projection, read at query time for version filtering
│   │   └── outbox_repository.py        # outbox_events table CRUD (for the three produced events)
│   ├── stores/
│   │   ├── milvus_repository.py        # pymilvus Collection wrapper — upsert/search/get-by-id/delete (§4)
│   │   └── opensearch_repository.py    # opensearch-py wrapper — index/search/delete, mirrors milvus_repository's shape
│   ├── models/
│   │   ├── ingestion_job.py            # SQLAlchemy model
│   │   ├── processed_event.py          # SQLAlchemy model
│   │   ├── document_current_version.py # SQLAlchemy model (§3.4)
│   │   └── outbox_event.py             # SQLAlchemy model
│   ├── db/
│   │   ├── postgres.py                 # Async SQLAlchemy engine
│   │   ├── session.py                  # AsyncSession factory
│   │   ├── base.py                     # Declarative Base
│   │   └── migrations/                 # Alembic
│   ├── core/
│   │   ├── config.py                   # Pydantic Settings — every tunable in .env, nothing hardcoded (see §2 below)
│   │   ├── logging.py                  # structlog setup, contextvars (trace_id, document_id)
│   │   ├── metrics.py                  # Prometheus metric definitions (HLD §24)
│   │   ├── exceptions.py               # Domain exceptions
│   │   └── enums.py                    # IngestionStatus (HLD §19), RetrievalScope mirror of proto enum
│   ├── workers/
│   │   └── outbox_worker.py            # Poll-and-publish loop for the three produced events (matches file-service pattern),
│   │                                    #  runs only in DEPLOYMENT_MODE=worker (§9)
│   ├── main.py                         # Entrypoint — branches once on settings.DEPLOYMENT_MODE (§9): api | worker | all
│   └── lifespan.py                     # Startup, branched by DEPLOYMENT_MODE (§9):
│                                        #    api    → DB pool, gRPC server, FastAPI health/metrics
│                                        #    worker → DB pool, Milvus/OpenSearch/embedding/File-Service clients,
│                                        #             Kafka consumers, bounded ingestion pool (§6.1.1), outbox_worker,
│                                        #             reconciliation sweep, FastAPI health/metrics
│                                        #  Never both gRPC server and Kafka consumers in the same real deployment
├── alembic.ini
├── docs/
│   ├── HLD.md
│   └── LLD.md
├── scripts/
│   └── provision_milvus_collection.py  # ONE-TIME infra script (HLD §13.2) — never run by app pods
├── tests/
│   ├── unit/
│   │   ├── test_chunk_identity.py
│   │   ├── test_fusion.py
│   │   ├── test_mmr.py
│   │   ├── test_scope_guard.py
│   │   └── test_scoring.py
│   ├── integration/
│   │   ├── test_ingestion_pipeline.py  # against real MinIO + Milvus + OpenSearch + Postgres
│   │   ├── test_query_pipeline.py
│   │   └── test_grpc_servicer.py
│   └── conftest.py
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── uv.lock
├── .env.example
└── README.md
```

### Why this shape

* **`grpc/` and `api/` are siblings, not nested** — the gRPC server (`GetRelevantChunks`, HLD §6) and the FastAPI app (health/metrics only) are genuinely different transport concerns serving different callers (LLM Service vs. Kubernetes probes/Prometheus). Keeping them siblings, both wired from `lifespan.py`, mirrors how `file-service` keeps `events/` (Kafka) separate from `api/` (HTTP) rather than forcing one transport's lifecycle onto the other.
* **`query/` and `ingestion/` are split at the top level**, not nested under a shared `pipeline/` — per HLD §22, these scale independently (query servers on request rate, ingestion workers on Kafka lag) and are deployed as logically separate workloads even if they ship in one image/repo. Separating the directories keeps that boundary visible in code, not just in the deploy manifest.
* **`stores/` (Milvus + OpenSearch) is distinct from `repositories/` (Postgres)** — `repositories/` follows the SQLAlchemy-session, transactional-boundary pattern already used across the monorepo; `stores/` wraps two external search engines with their own SDKs, retry semantics, and no relational transaction concept. Merging them under one name would blur a real operational difference (a `stores/` failure degrades gracefully per HLD §17; a `repositories/` failure is a hard error on the job's own bookkeeping).
* **`clients/` holds only outbound service calls** (embedding-service, File Service) — deliberately separate from `stores/`, since these are RPCs to sibling services with their own SLAs/circuit-breaking needs (HLD §17), not local infrastructure adapters.
* **`scripts/provision_milvus_collection.py` lives outside `app/`** — HLD §13.2 is explicit that collection/index creation must never be pod-startup code; keeping the script physically outside the application package makes it structurally impossible for `lifespan.py` to accidentally import and run it.

---

## 2. Configuration — Everything in `.env`, Nothing Hardcoded

Matching the file-service config audit convention. `app/core/config.py` (Pydantic `BaseSettings`):

```python
class Settings(BaseSettings):
    # --- Service identity ---
    SERVICE_NAME: str = "retrieval-service"
    ENVIRONMENT: str = "development"
    DEPLOYMENT_MODE: str = "api"   # "api" | "worker" | "all" — see §9, drives lifespan.py's branch

    # --- gRPC server (DEPLOYMENT_MODE in {api, all}) ---
    GRPC_PORT: int = 50060
    GRPC_MAX_WORKERS: int = 10
    GRPC_MAX_MESSAGE_LENGTH_BYTES: int = 4 * 1024 * 1024

    # --- FastAPI (health/metrics — runs in every mode, §9) ---
    HTTP_PORT: int = 8060

    # --- PostgreSQL (job + inbox + outbox state) ---
    DATABASE_URL: str
    DB_POOL_SIZE: int = 10
    DB_POOL_MAX_OVERFLOW: int = 5

    # --- Kafka (DEPLOYMENT_MODE in {worker, all}) ---
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_CONSUMER_GROUP: str = "retrieval-service"
    KAFKA_CONSUMER_POLL_TIMEOUT_MS: int = 1000
    KAFKA_TOPIC_FILE_UPLOADED: str = "file.uploaded"
    KAFKA_TOPIC_FILE_DELETED: str = "file.deleted"
    KAFKA_TOPIC_FILE_PROCESSING_RETRY: str = "file.processing_retry"
    KAFKA_TOPIC_PROCESSING_STARTED: str = "file.processing.started"
    KAFKA_TOPIC_PROCESSING_COMPLETED: str = "file.processing.completed"
    KAFKA_TOPIC_PROCESSING_FAILED: str = "file.processing.failed"
    OUTBOX_BATCH_SIZE: int = 50
    OUTBOX_POLL_INTERVAL_SECONDS: float = 1.0

    # --- Ingestion concurrency (worker only) ---
    INGESTION_MAX_CONCURRENCY: int = 4   # bounded worker pool consuming from the Kafka
                                          # handler's queue — protects Docling CPU,
                                          # embedding-service, Milvus, OpenSearch, and
                                          # Postgres from an upload burst (§6.1)

    # --- MinIO (read-only source) ---
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_SECURE: bool = True
    MINIO_BUCKET_FILES: str = "graphgpt-files"
    MINIO_DOWNLOAD_TIMEOUT_SECONDS: float = 30.0

    # --- embedding-service (HLD §12) ---
    EMBEDDING_SERVICE_GRPC_TARGET: str
    EMBEDDING_MODEL_EXPECTED: str = "all-MiniLM-L6-v2"
    EMBEDDING_DIMENSION_EXPECTED: int = 384
    EMBEDDING_BATCH_SIZE: int = 64
    EMBEDDING_INGESTION_TIMEOUT_SECONDS: float = 20.0   # batch call, off the query path — generous
    EMBEDDING_QUERY_TIMEOUT_SECONDS: float = 0.4        # single call, ON the query path — see §7.7 budget
    EMBEDDING_RETRY_ATTEMPTS: int = 2

    # --- File Service permission-check client (HLD §16.2) ---
    FILE_SERVICE_GRPC_TARGET: str
    FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS: float = 0.3   # see §7.7 budget — was 0.8, tightened
    FILE_SERVICE_SERVICE_CREDENTIAL: str  # service-to-service auth token, never logged

    # --- Milvus (HLD §13) ---
    MILVUS_HOST: str
    MILVUS_PORT: int = 19530
    MILVUS_COLLECTION_NAME: str = "retrieval_document_chunks"
    MILVUS_SEARCH_TOP_N: int = 40
    MILVUS_SEARCH_NPROBE: int = 16
    MILVUS_INDEX_METRIC_TYPE: str = "COSINE"
    MILVUS_CONNECT_TIMEOUT_SECONDS: float = 5.0            # connection setup, at startup — not per-query
    MILVUS_QUERY_TIMEOUT_SECONDS: float = 0.5               # per-search, ON the query path — see §7.7

    # --- OpenSearch (HLD §14) ---
    OPENSEARCH_HOSTS: str  # comma-separated
    OPENSEARCH_INDEX_NAME: str = "retrieval_document_chunks"
    OPENSEARCH_SEARCH_TOP_N: int = 40
    OPENSEARCH_QUERY_TIMEOUT_SECONDS: float = 0.5           # per-search, ON the query path — was a flat 5s, tightened

    # --- Query pipeline tuning (HLD §18) ---
    FUSION_RRF_K: int = 60                  # standard RRF constant
    MMR_CANDIDATE_POOL_SIZE: int = 40
    MMR_FINAL_TOP_K: int = 6
    MMR_LAMBDA: float = 0.7
    QUERY_PIPELINE_DEADLINE_SECONDS: float = 1.8   # HLD §17 — comfortably under the 2000ms caller deadline;
                                                    # every *_QUERY_TIMEOUT_SECONDS above is a sub-budget of
                                                    # this, never independently sized — see §7.7

    # --- Ingestion pipeline tuning (HLD §9/§10) ---
    CHUNK_TARGET_TOKENS: int = 450
    CHUNK_MAX_TOKENS: int = 600
    CHUNK_OVERLAP_TOKENS: int = 60
    RECONCILIATION_ENABLED: bool = True
    RECONCILIATION_INTERVAL_SECONDS: int = 300

    # --- Observability ---
    LOG_LEVEL: str = "INFO"
    METRICS_ENABLED: bool = True
    HEALTH_CHECK_TIMEOUT_SECONDS: float = 3.0

    class Config:
        env_file = ".env"
```

`.env.example` ships with every key above, with safe non-secret defaults where applicable and empty placeholders for `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `MINIO_*`, `EMBEDDING_SERVICE_GRPC_TARGET`, `FILE_SERVICE_GRPC_TARGET`, `FILE_SERVICE_SERVICE_CREDENTIAL`, `MILVUS_HOST`, `OPENSEARCH_HOSTS`. `FILE_SERVICE_SERVICE_CREDENTIAL` is documented as secret-only, sourced from a secrets manager in real deployments, never committed.

**No child operation may outlive the parent pipeline deadline** — the rule the previous revision's flat `EMBEDDING_GRPC_TIMEOUT_SECONDS=5.0`/`OPENSEARCH_REQUEST_TIMEOUT_SECONDS=5.0` violated by being larger than the entire `QUERY_PIPELINE_DEADLINE_SECONDS=1.8` budget they sit inside. Every `*_QUERY_TIMEOUT_SECONDS` above is now sized as an explicit slice of that 1.8s (§7.7 gives the full breakdown), not picked independently per client. The ingestion-side embedding call (`EMBEDDING_INGESTION_TIMEOUT_SECONDS`) is deliberately excluded from this rule — it runs on the ingestion pipeline, which has no caller-imposed deadline (HLD §17's SLA is query-path only), so a generous, independently-tuned timeout there is correct, not an oversight.

---

## 3. PostgreSQL Schema

Three tables, all Retrieval-owned (HLD §15) — nothing here duplicates File Service's own schema.

### 3.1 `ingestion_jobs`

Tracks HLD §19's explicit state machine, one row per `(document_id, content_version)`.

```sql
CREATE TABLE ingestion_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id         UUID NOT NULL,
    content_version     INTEGER NOT NULL,
    owner_id            UUID NOT NULL,
    conversation_id     UUID,
    filename            TEXT NOT NULL,
    storage_bucket      TEXT NOT NULL,
    storage_key         TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'NOT_STARTED',
        -- NOT_STARTED | PROCESSING | PARTIALLY_INDEXED | READY | FAILED | DELETING | DELETED  (HLD §19)
    embedding_status    TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | COMPLETE | FAILED
    milvus_status       TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | COMPLETE | FAILED  (HLD §20)
    opensearch_status   TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | COMPLETE | FAILED  (HLD §20)
    chunk_count         INTEGER,
    error_code          TEXT,
    error_message       TEXT,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_ingestion_jobs_doc_version UNIQUE (document_id, content_version)
);

CREATE INDEX ix_ingestion_jobs_status ON ingestion_jobs (status);
CREATE INDEX ix_ingestion_jobs_document_id ON ingestion_jobs (document_id);
```

`milvus_status`/`opensearch_status` are the per-stage tracking HLD §20 requires — `status` is the coarser bucket the rest of the system reads, derived from these two plus `embedding_status` (never stored redundantly out of sync — always recomputed by `ingestion_job_repository.recompute_status()` whenever a stage transitions, per §6.2/§6.3 below).

`storage_bucket`/`storage_key` are deliberately retained on the row for the *lifetime* of the job, not just at creation — §6.3's partial-index recovery path re-downloads the original object from these exact fields, since this table stores no chunk or embedding payload (see §3.2's correction and §6.3).

### 3.2 `processed_events` (inbox pattern, HLD §11) — what "processed" means, corrected

```sql
CREATE TABLE processed_events (
    event_id     UUID NOT NULL,
    consumer     TEXT NOT NULL,   -- 'file_uploaded_consumer' | 'file_deleted_consumer' | 'file_processing_retry_consumer'
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (event_id, consumer)
);
```

Identical shape to `file-service`'s `processed_event_ids` (HLD §11's explicit recommendation) — composite-keyed so the same `event_id` redelivered to a different consumer group is tracked independently.

**A row in this table means "this event was durably accepted into an `ingestion_jobs` row," not "all processing caused by this event has finished."** An earlier version of this LLD treated a `processed_events` hit as a bare no-op — ack and skip — which has a real crash window: if the process dies *after* the `processed_events` insert commits but *before* the orchestrator actually runs, Kafka redelivers the event, the inbox check reports "already processed," and the ingestion is silently lost — the job row never leaves `NOT_STARTED`.

**Fix: the insert into `processed_events` and the creation/update of the corresponding `ingestion_jobs` row happen in the same database transaction**, so "durably accepted" and "a job row exists for this event" are the same fact, never two facts that can disagree:

```text
Kafka message
        │
        ▼
BEGIN
    INSERT INTO ingestion_jobs (...) ON CONFLICT (document_id, content_version) DO UPDATE ...
    INSERT INTO processed_events (event_id, consumer) ON CONFLICT DO NOTHING
COMMIT
        │
        ▼
ack Kafka offset (only after COMMIT succeeds)
        │
        ▼
enqueue job for orchestrator.run() (bounded worker pool, §6.1)
```

**On redelivery, the dedup check is no longer a blind skip — it dispatches on the job's actual status**, which is what makes recovery correct instead of merely idempotent-looking:

```text
processed_events row exists for (event_id, consumer)
        │
        ▼
look up the ingestion_jobs row this event maps to
        │
        ├── status = READY                          → no-op, already done
        ├── status = PROCESSING                      → still in flight (another worker holds it,
        │                                                or the previous attempt crashed) — do NOT
        │                                                re-enqueue blindly; see §6.1's crash-recovery
        │                                                sweep, which is what actually resolves this
        │                                                case on a timeout, not the consumer itself
        ├── status = PARTIALLY_INDEXED or FAILED      → re-enqueue for (re)processing — this is the
        │                                                case the old "already processed → skip" logic
        │                                                got wrong
        └── status = NOT_STARTED (row created this    → enqueue for the first time
              transaction, or a prior attempt never
              got past the transaction above)
```

A periodic reconciliation sweep (`RECONCILIATION_ENABLED`, same convention as file-service's own reconciliation worker) additionally scans for jobs stuck in `PROCESSING` past a threshold (a crash between "job created" and "job reached a terminal state," with no redelivered event to trigger recovery, e.g. because the offset was already acked) and re-enqueues those too — this is the safety net for the one case the transactional fix above doesn't cover on its own: a crash *after* ack but *during* `orchestrator.run()`.

### 3.3 `outbox_events` (HLD §8.6)

```sql
CREATE TABLE outbox_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id      UUID NOT NULL UNIQUE,
    topic         TEXT NOT NULL,
    payload       JSONB NOT NULL,
    published     BOOLEAN NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX ix_outbox_events_unpublished ON outbox_events (created_at) WHERE published = false;
```

Same poll-and-publish shape already used by `file-service`/`conversation-service` — `outbox_worker.py` polls unpublished rows in `created_at` order, publishes to Kafka, marks `published = true`. Written in the **same DB transaction** as the `ingestion_jobs` status transition that causes the event (e.g. job → `READY` and the `file.processing.completed` outbox row are one atomic commit) — this is what makes the "never publish `.completed` for a `PARTIALLY_INDEXED` job" rule (HLD §20) enforceable at the transaction level, not just by convention.

### 3.4 `document_current_versions` (HLD §13.3 — current-version-only retrieval)

```sql
CREATE TABLE document_current_versions (
    document_id      UUID PRIMARY KEY,
    current_version   INTEGER NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Updated by `file_uploaded_consumer.py` in the **same transaction** as the `ingestion_jobs` upsert (§3.2's flow), with the monotonic guard HLD §13.3 requires:

```sql
INSERT INTO document_current_versions (document_id, current_version)
VALUES (:document_id, :content_version)
ON CONFLICT (document_id) DO UPDATE
    SET current_version = EXCLUDED.current_version, updated_at = now()
    WHERE document_current_versions.current_version < EXCLUDED.current_version;
```

The `WHERE` clause is the reorder-safety guard HLD §13.3 explains (File Service's Kafka producer does not key by `document_id`, so `v3` can arrive before `v2`) — a lower/equal `content_version` arriving later is a no-op, never a regression. `query/pipeline.py`'s version-resolution step (§7.2, step 3a below) reads this table directly — a single indexed primary-key lookup per `document_id` in `authorized_file_ids`, well inside the query budget (§7.7).

---

## 4. Milvus Collection Schema (HLD §13)

Defined once by `scripts/provision_milvus_collection.py` (infra-run only, HLD §13.2) — application code only ever connects and reads/writes, never calls `create_collection`/`create_index`.

```python
from pymilvus import CollectionSchema, FieldSchema, DataType

fields = [
    FieldSchema(name="chunk_id", dtype=DataType.VARCHAR, max_length=64, is_primary=True),
    FieldSchema(name="document_id", dtype=DataType.VARCHAR, max_length=36),
    FieldSchema(name="content_version", dtype=DataType.INT32),
    FieldSchema(name="chunk_index", dtype=DataType.INT32),
    FieldSchema(name="owner_id", dtype=DataType.VARCHAR, max_length=36),
    FieldSchema(name="conversation_id", dtype=DataType.VARCHAR, max_length=36, nullable=True),
    FieldSchema(name="filename", dtype=DataType.VARCHAR, max_length=512),
    FieldSchema(name="page_number", dtype=DataType.INT32, nullable=True),
    FieldSchema(name="section", dtype=DataType.VARCHAR, max_length=512, nullable=True),
    FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=8192),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384),
]
schema = CollectionSchema(fields, description="Retrieval Service dense chunk index")
# Collection name: settings.MILVUS_COLLECTION_NAME ("retrieval_document_chunks")
# Index: HNSW, metric_type=COSINE, on `embedding`
# Scalar indexes: on `document_id` and `content_version` (query-time pre-filter,
#                 HLD §13/§13.3 — both fields participate in the same filter clause)
```

`chunk_id` is `VARCHAR`, not Milvus's auto-increment int64 — it stores the string form of the deterministic `uuid5` from HLD §11, which is what makes both the Milvus write and the MMR candidate-embedding fetch (HLD §18.1, a primary-key `get` by this same id) work without a secondary lookup index. `milvus_repository.upsert_chunks()` owns the exact upsert semantics for the deployed Milvus client version (delete-then-insert, a native `upsert` call, or whatever that version actually provides) — this LLD deliberately does not assert a specific mechanism (e.g. "insert with an existing primary key replaces") in prose, since that's a version-specific implementation detail the repository method should encapsulate, not a contract the rest of the codebase should come to depend on.

`app/stores/milvus_repository.py` exposes exactly four operations, deliberately narrow:

```python
class MilvusRepository:
    async def upsert_chunks(self, records: list[ChunkRecord]) -> None: ...
    async def search(
        self, query_vector: list[float], top_n: int, current_versions: dict[str, int]
    ) -> list[DenseSearchHit]:
        # current_versions: {document_id: current_version} for every id in
        # authorized_file_ids (HLD §13.3) — the filter expression built from
        # this is `document_id IN (...) AND content_version == current_versions[document_id]`
        # per document_id, not a single flat top_n/document_ids pair (a document-level
        # filter alone would let old-version chunks back in, HLD §13.3)
        ...
    async def get_by_chunk_ids(self, chunk_ids: list[str]) -> dict[str, list[float]]:  # HLD §18.1
        ...
    async def delete_by_document_id(self, document_id: str) -> None:  # HLD §21 — deletes ALL
        # content_versions for the document, unconditionally (no version filter — matches
        # file.deleted's own scope, HLD §8.2/§21)
        ...
```

No method here creates or alters the collection — enforced by review convention, not by a runtime guard, matching HLD §13.2's framing of this as a process rule, not a code-level one.

---

## 5. OpenSearch Index Schema (HLD §14)

```json
PUT /retrieval_document_chunks
{
  "mappings": {
    "properties": {
      "chunk_id":         { "type": "keyword" },
      "document_id":       { "type": "keyword" },
      "content_version":   { "type": "integer" },
      "chunk_index":       { "type": "integer" },
      "owner_id":          { "type": "keyword" },
      "conversation_id":   { "type": "keyword" },
      "filename":          { "type": "text", "analyzer": "standard", "boost": 2.0 },
      "page_number":       { "type": "integer" },
      "section":           { "type": "text", "analyzer": "standard", "boost": 1.5 },
      "content":           { "type": "text", "analyzer": "standard" }
    }
  }
}
```

Index creation is also a one-time infra step (`scripts/provision_milvus_collection.py`'s sibling — kept in the same `scripts/` directory, e.g. `provision_opensearch_index.py`), same reasoning as HLD §13.2. `chunk_id` is used directly as the OpenSearch document `_id` (not a mapped field lookup) — `PUT /retrieval_document_chunks/_doc/{chunk_id}` is what gives the same overwrite-not-duplicate idempotency HLD §14 requires.

`app/stores/opensearch_repository.py`, same narrow shape as `milvus_repository.py`:

```python
class OpenSearchRepository:
    async def upsert_chunks(self, records: list[ChunkRecord]) -> None: ...
    async def search(
        self, query_text: str, top_n: int, current_versions: dict[str, int]
    ) -> list[BM25SearchHit]: ...
    async def delete_by_document_id(self, document_id: str) -> None: ...
```

`search` builds a `bool` query: `must: multi_match(query_text, fields=["content", "section^1.5", "filename^2.0"])`, plus a `should`-of-`bool` filter — one `{term: {document_id}, term: {content_version}}` clause per authorized document — equivalent to `OR`-ing per-document `(document_id AND content_version)` pairs, since OpenSearch's `terms` filter alone can't express "this version, for this document" as a single flat clause the way Milvus's per-record scalar filter can. Same authorized+current-version scope as Milvus (HLD §13/§13.3), just expressed differently because of the query DSL's shape.

---

## 6. Ingestion Pipeline (HLD §2 Ingestion Pipeline, §8, §9–§14)

### 6.1 Trigger: `file_uploaded_consumer.py`

Corrected per §3.2's inbox fix — accepting an event and finishing its processing are no longer conflated, and ingestion no longer runs synchronously inline with the Kafka poll loop (bounded concurrency, below):

```text
Kafka message (file.uploaded)
        │
        ▼
Deserialize into FileUploadedEvent (app/events/payloads.py — byte-matches HLD §8.1)
        │
        ▼
chunk_identity.parse_content_version(event.storage.key)  (HLD §8.3 Phase 1 — strict regex,
        │                                                  reject + dead-letter on mismatch,
        │                                                  BEFORE the transaction below —
        │                                                  a malformed key must not create a job)
        ▼
BEGIN (single DB transaction, §3.2)
    ingestion_job_repository.upsert_job(document_id, content_version, status=NOT_STARTED, ...)
    document_current_version_repository.advance_if_newer(document_id, content_version)  (§3.4)
    processed_event_repository.mark_processed(event_id, consumer="file_uploaded_consumer")
COMMIT
        │
        ▼
ack Kafka offset (now safe — the job's existence is durable, independent of what
        │           happens next; see §3.2 for what redelivery does after this point)
        ▼
enqueue job.id onto the bounded ingestion queue (§6.1.1) — orchestrator.run() executes
        there, not inline on this consumer's call stack
```

**If `try_mark_processed` (or the equivalent job lookup) finds this `(event_id, consumer)` already recorded**, the consumer does not blindly skip — it re-derives the job status and dispatches per §3.2's table (`READY` → no-op, `PARTIALLY_INDEXED`/`FAILED` → re-enqueue, `PROCESSING` → leave to the reconciliation sweep, `NOT_STARTED` → enqueue). This is the one behavioral change from the original design: "already in the inbox" is no longer synonymous with "nothing to do."

`file_processing_retry_consumer.py` follows a similar shape, but looks up the job by `(document_id, event.content_version)` — **`content_version` is now a required field on `file.processing_retry`** (HLD §1.3 #1, §8.5); the consumer treats a retry event missing this field as malformed input (reject + dead-letter, same posture as an unparseable `storage.key`) rather than guessing which version to retry. The looked-up job's own `storage_bucket`/`storage_key` are reused as-is (HLD §8.5 — the bytes are untouched), no File Service call needed.

#### 6.1.1 Bounded ingestion concurrency

Neither consumer calls `orchestrator.run()` directly from the Kafka poll loop — an upload burst would otherwise let ingestion concurrency grow unbounded with Kafka lag, hammering Docling's CPU, embedding-service, Milvus, OpenSearch, and Postgres all at once with no backpressure:

```text
Kafka consumer loop
        │  (fast: parse, transaction, ack — no parsing/embedding/indexing here)
        ▼
asyncio.Queue(maxsize=settings.INGESTION_MAX_CONCURRENCY * 2)
        │
        ▼
settings.INGESTION_MAX_CONCURRENCY worker coroutines (default 4), each looping:
    job_id = await queue.get()
    await orchestrator.run(job_id)
```

If the queue is full, the Kafka consumer's `queue.put()` blocks — which is the intended backpressure: consumer-group lag grows visibly (an observable, alertable signal) instead of the process silently accumulating unbounded in-flight work. This also means the "ack after the DB transaction, before `orchestrator.run()`" ordering above is deliberate: acking must not wait on a full ingestion queue, since the job is already durable at that point (§3.2) and Kafka lag is the correct place for backpressure to show up, not an unacked, retried-forever poll loop.

### 6.2 `orchestrator.run(job)`

```text
status → PROCESSING
        │
        ▼
downloader.fetch(job.storage_bucket, job.storage_key)  (MinIO S3 GET, HLD §5)
        │
        ├── failure → status=FAILED, error_code=DOWNLOAD_ERROR, publish file.processing.failed, return
        │
        ▼
parser.parse(raw_bytes, content_type)  (Docling, HLD §9)
        │
        ├── failure → status=FAILED, error_code=PARSER_ERROR, publish file.processing.failed, return
        │
        ▼
chunker.chunk(parsed_document)  (HLD §10 — structure-aware recursive)
        │
        ▼
for each chunk: chunk_identity.derive_chunk_id(document_id, content_version, chunk_index)  (HLD §11)
        │
        ▼
embedder.embed_batch(chunks)  (HLD §12 — gRPC to embedding-service, batched per
        │                       settings.EMBEDDING_BATCH_SIZE; asserts model/dimension
        │                       match settings.EMBEDDING_MODEL_EXPECTED/EMBEDDING_DIMENSION_EXPECTED)
        │
        ├── failure → embedding_status=FAILED, status=FAILED, publish file.processing.failed, return
        │             (no partial index writes possible without embeddings — this is the
        │              one stage that isn't independently retriable per-target like §20's
        │              milvus/opensearch split, since both indexes need the same vectors)
        ▼
embedding_status = COMPLETE
        │
        ▼
indexer.write(chunks_with_embeddings)  — writes Milvus and OpenSearch CONCURRENTLY
        │           (asyncio.gather, matching the query-side concurrency pattern, HLD §18)
        │
        ├── both succeed        → milvus_status=COMPLETE, opensearch_status=COMPLETE, status=READY
        │                          → publish file.processing.completed{chunk_count} (HLD §8.6, §20)
        │
        ├── one succeeds,       → status=PARTIALLY_INDEXED (HLD §19/§20)
        │   one fails             → do NOT publish file.processing.completed
        │                          → schedule/allow retry of ONLY the failed target
        │                            (indexer.write() accepts a target filter:
        │                             {MILVUS, OPENSEARCH, BOTH})
        │
        └── both fail           → status=FAILED, error_code=INDEXING_ERROR,
                                    publish file.processing.failed
```

`file.processing.started` is published immediately after `status → PROCESSING`, before the download begins — matching HLD §8.6's shape (`event_id`, `document_id`, `timestamp` only) and giving File Service the earliest possible signal that ingestion has begun, for its own `UPLOADED → PROCESSING` transition.

### 6.3 Retrying a `PARTIALLY_INDEXED` job

**Corrected from an earlier revision of this LLD**, which claimed chunks/embeddings survive between the original attempt and a later retry and are simply reused. They don't survive: `ingestion_jobs` (§3.1) stores only job *state* — `status`, `chunk_count`, per-stage flags — never the chunks or embedding vectors themselves, and no other table in §3 holds them either. By the time a retry runs (a later Kafka poll cycle, a later reconciliation sweep tick, potentially a different pod entirely), the in-memory `chunks_with_embeddings` list from the original `orchestrator.run()` call is long gone. A retry that tries to call `indexer.write(chunks, ...)` with no `chunks` in hand is not implementable as originally written.

**V1 fix — reconstruct the indexing payload from the original MinIO object**, via a lightweight path that reuses the front half of `orchestrator.run()` (§6.2: download → parse → chunk) but is deliberately smarter about the back half, since which stage needs redoing genuinely depends on *which* target failed:

```text
ingestion_job_repository.list_partially_indexed(older_than=X)
        │
        ▼
for each job:
    downloader.fetch(job.storage_bucket, job.storage_key)   (re-download — §3.1's stored
        │                                                     storage_bucket/storage_key
        │                                                     are exactly what make this
        │                                                     possible without a File
        │                                                     Service call)
    parser.parse(...) → chunker.chunk(...)                   (re-parse, re-chunk — cheap
        │                                                     relative to embedding; no
        │                                                     durable store for the parsed/
        │                                                     chunked intermediate exists,
        │                                                     so this step cannot be skipped
        │                                                     in V1 regardless of which
        │                                                     target failed)
        ▼
    if job.opensearch_status == 'FAILED' and job.milvus_status == 'COMPLETE':
        embedder.embed_batch(chunks)          # re-embed — see note below on why this
        indexer.write(chunks_with_embeddings, target=OPENSEARCH)   # isn't skippable either
    elif job.milvus_status == 'FAILED' and job.opensearch_status == 'COMPLETE':
        embedder.embed_batch(chunks)
        indexer.write(chunks_with_embeddings, target=MILVUS)
        ▼
on success → status recomputed (READY if now both COMPLETE) → publish file.processing.completed
```

**Why embeddings are recomputed even when only OpenSearch (which doesn't use them) failed:** OpenSearch's `content`/`section`/`filename` fields come from the *same* chunk objects the embedder consumes, and V1 has no durable store for chunks independent of their embeddings — recomputing the chunks without the embeddings would mean two slightly different code paths (chunk-only vs. chunk-and-embed) for what's supposed to be one deterministic pipeline. Re-embedding a chunk set that will be thrown away for the Milvus side (since `milvus_status` is already `COMPLETE`) is wasted work, but it's *cheap*, bounded, wasted work — not a correctness risk — so V1 accepts it rather than forking the pipeline. If this proves to be a real cost in practice (frequent OpenSearch-only failures at scale), the fix is durable storage for the parsed/chunked/embedded intermediate — deliberately **not** built in V1 (no new database for this, matching the review's own recommendation), and only worth adding once partial-index failure rate is actually measured, not speculated about.

`chunk_identity.derive_chunk_id()` (HLD §11) is deterministic on `(document_id, content_version, chunk_index)` — re-parsing and re-chunking the same object reliably reproduces the same `chunk_id`s, so the re-indexed write is still a safe upsert (§4), not a duplicate.

### 6.4 `file_deleted_consumer.py` (HLD §8.2, §21)

Unlike §6.1's ingestion path, a blind "already in `processed_events` → skip" check is safe here without §3.2's job-status dispatch — `delete_service.purge()` has no durable intermediate state to lose (both stores' deletes are naturally idempotent, HLD §21), so there's no crash window equivalent to "job created but never processed" to protect against. The dedup check still exists purely to avoid redundant Milvus/OpenSearch calls on a genuinely redelivered event, not for correctness.

```text
Kafka message (file.deleted)
        │
        ▼
processed_event_repository.try_mark_processed(...)  — dedup only, safe to skip blindly here
        │
        ▼
delete_service.purge(document_id)
        │           milvus_repository.delete_by_document_id(document_id)
        │           opensearch_repository.delete_by_document_id(document_id)
        │           (concurrently; both idempotent — a redelivered file.deleted for an
        │            already-purged document_id is a no-op on both sides, HLD §21)
        ▼
ingestion_job_repository.mark_all_versions_deleted(document_id)  (status → DELETED,
        all content_versions, since the event carries no version filter, HLD §8.2)
```

---

## 7. Query Pipeline (HLD §18) — `retrieval_servicer.py` → `query/pipeline.py`

### 7.1 gRPC entrypoint

```python
class RetrievalServicer(retrieval_pb2_grpc.RetrievalServiceServicer):
    async def GetRelevantChunks(self, request, context):
        with metrics.query_latency_histogram.time():
            try:
                response = await asyncio.wait_for(
                    pipeline.run(request),
                    timeout=settings.QUERY_PIPELINE_DEADLINE_SECONDS,
                )
                return response
            except asyncio.TimeoutError:
                metrics.query_timeout_counter.inc()
                return empty_response(request.request_id)  # HLD §17 — degrade, never raise
```

The servicer itself never raises a gRPC error back to LLM Service for a pipeline-internal failure — HLD §17's "never let an internal failure block the whole request" is enforced here as the single outermost guard, on top of the narrower fallbacks inside `pipeline.run()` (§7.2).

### 7.2 `pipeline.run(request)` — full orchestration, matching HLD §18/§26.2's frozen diagram exactly

```text
1. scope_guard.check(request.scope, request.file_ids)         (HLD §16.3/§16.4)
   → not FILES, or file_ids empty → return empty_response()   [SHORT-CIRCUIT #1]

2. file_service_client.batch_check_permissions(
       user_id=request.user_id, document_ids=request.file_ids
   )                                                            (HLD §16.2, service-credential auth,
                                                                  FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS)
   → authorized_file_ids = subset
   → authorized_file_ids empty → return empty_response()        [SHORT-CIRCUIT #2]

3a. document_current_version_repository.get_many(authorized_file_ids)   (HLD §13.3, §3.4 —
    local Postgres lookup, NOT a File Service call)
    → current_versions: dict[document_id, content_version]
    → a document_id with no row here (ingestion never completed / was deleted) is
      dropped from the search scope entirely — it simply contributes no chunks,
      same "silently narrow scope, never error" posture as an unauthorized id

3b. embedding_client.embed_query(request.query)                 (HLD §12, single call,
    EMBEDDING_QUERY_TIMEOUT_SECONDS)
   → on failure/timeout after EMBEDDING_RETRY_ATTEMPTS → return empty_response()
     (query embedding is a hard dependency for both search legs — no partial-degrade
     path exists once this fails, unlike the Milvus/OpenSearch split below)

4. asyncio.gather(
       dense_search.search(query_vector, MILVUS_QUERY_TIMEOUT_SECONDS, current_versions),   (Milvus, Top-N)
       bm25_search.search(request.query, OPENSEARCH_QUERY_TIMEOUT_SECONDS, current_versions), (OpenSearch, Top-N)
       return_exceptions=True,
   )
   → degradation is explicit per which side failed — see the matrix below,
     since MMR (step 7) has a hard dependency on Milvus that BM25-only
     degradation cannot satisfy

   ┌─────────────────────┬───────────────────────────────────────────────────┐
   │ Outcome              │ Behavior                                          │
   ├─────────────────────┼───────────────────────────────────────────────────┤
   │ Both succeed          │ RRF → candidate embeddings from Milvus → MMR →    │
   │                       │ Top-K (the full path, steps 5-8 below)            │
   ├─────────────────────┼───────────────────────────────────────────────────┤
   │ OpenSearch fails,     │ Dense-only candidates, ranked by dense_score →    │
   │ Milvus succeeds       │ candidate embeddings from Milvus (still           │
   │                       │ available) → MMR runs normally → Top-K            │
   ├─────────────────────┼───────────────────────────────────────────────────┤
   │ Milvus fails,         │ BM25-only candidates, ranked by rrf_score (RRF    │
   │ OpenSearch succeeds   │ degrades to a single-list rank-score, not true    │
   │                       │ fusion) → MMR is SKIPPED (no embeddings           │
   │                       │ available — Milvus is both the dense search       │
   │                       │ engine AND the only candidate-embedding store,    │
   │                       │ HLD §18.1) → Top-K taken directly by rank,        │
   │                       │ metrics.search_degraded_total{engine="bm25_only", │
   │                       │ mmr="skipped"}.inc()                              │
   ├─────────────────────┼───────────────────────────────────────────────────┤
   │ Both fail             │ return empty_response()                          │
   └─────────────────────┴───────────────────────────────────────────────────┘

5. fusion.reciprocal_rank_fusion(dense_hits, bm25_hits, k=settings.FUSION_RRF_K)
   → fused candidates, each carrying dense_rank/bm25_rank/rrf_score (HLD §18.2)
   → skipped entirely in the dense-only/BM25-only rows above; those already
     produce a single ranked list with no second list to fuse against

6. candidate_embeddings.fetch(milvus_repository, [c.chunk_id for c in fused[:POOL_SIZE]])
   → { chunk_id: embedding } via Milvus primary-key get (HLD §18.1 — NOT embedding-service)
   → only reached when Milvus is available (both-succeed or OpenSearch-only-failed rows)

7. mmr.select(fused_candidates, candidate_embeddings, k=MMR_FINAL_TOP_K, lambda_=MMR_LAMBDA)
   → selected set (diversity-aware membership only, HLD §18.2)
   → SKIPPED when Milvus is unavailable (see matrix) — selection instead is a
     straight take of the top MMR_FINAL_TOP_K candidates by rrf_score

8. sorted_selected = sorted(selected, key=lambda c: c.rrf_score, reverse=True)  (HLD §18.2 —
   response order is NOT MMR selection order; a no-op when step 7 was skipped,
   since the BM25-only candidates are already rank-ordered by rrf_score)

9. response_mapper.to_proto(sorted_selected, request.request_id)
   → GetRelevantChunksResponse{chunks, total_tokens, request_id}
```

Every short-circuit and degrade path returns via the **same** `empty_response(request_id)` helper — `chunks: [], total_tokens: 0, request_id` — so there is exactly one code path constructing an empty result, not one per failure branch, keeping HLD §17's "empty is not an error" posture structurally enforced rather than merely conventional. `relevance_score` on the wire is always `rrf_score` per HLD §18.2 regardless of which row of the matrix produced it — in the degraded rows, `rrf_score` for a dense-only or BM25-only candidate is just that engine's own rank-based score (§7.4's `setdefault` behavior already handles a candidate appearing in only one list; the degraded rows are the same code path with the other list simply empty, not a separate implementation).

### 7.3 `scope_guard.py` (HLD §16.3, §16.4)

```python
def check(scope: RetrievalScope, file_ids: list[str]) -> bool:
    """Returns True if the pipeline should proceed to authorization; False → empty response."""
    if scope != RetrievalScope.FILES:
        metrics.unsupported_scope_counter.labels(scope=scope.name).inc()  # HLD §16.3 product visibility
        return False
    if not file_ids:
        return False
    return True
```

Nothing else in `scope_guard.py` — deliberately the smallest possible module, since HLD §16.3 froze this as a two-line decision (not FILES, or empty → stop) and any complexity creeping in here is a sign scope semantics are being guessed at again, which the HLD explicitly deferred to a product decision.

### 7.4 `fusion.py` — Reciprocal Rank Fusion

```python
def reciprocal_rank_fusion(dense_hits: list[DenseHit], bm25_hits: list[BM25Hit], k: int) -> list[FusedCandidate]:
    scores: dict[str, FusedCandidate] = {}
    for rank, hit in enumerate(dense_hits, start=1):
        c = scores.setdefault(hit.chunk_id, FusedCandidate(chunk_id=hit.chunk_id, ...))
        c.dense_rank, c.dense_score = rank, hit.score
        c.rrf_score += 1.0 / (k + rank)
    for rank, hit in enumerate(bm25_hits, start=1):
        c = scores.setdefault(hit.chunk_id, FusedCandidate(chunk_id=hit.chunk_id, ...))
        c.bm25_rank, c.bm25_score = rank, hit.score
        c.rrf_score += 1.0 / (k + rank)
    return sorted(scores.values(), key=lambda c: c.rrf_score, reverse=True)
```

A candidate present in only one list still accumulates an `rrf_score` from that list alone (HLD §18.2) — no special-casing needed, the `setdefault` + independent accumulation naturally produces that.

### 7.5 `mmr.py`

```python
def select(candidates: list[FusedCandidate], embeddings: dict[str, list[float]], k: int, lambda_: float) -> list[FusedCandidate]:
    pool = candidates[: settings.MMR_CANDIDATE_POOL_SIZE]
    selected: list[FusedCandidate] = []
    remaining = list(pool)
    while remaining and len(selected) < k:
        def mmr_score(c):
            relevance = c.rrf_score  # normalized relevance input to MMR — NOT what ships on the wire
            if not selected:
                return relevance
            max_sim = max(cosine_similarity(embeddings[c.chunk_id], embeddings[s.chunk_id]) for s in selected)
            return lambda_ * relevance - (1 - lambda_) * max_sim
        best = max(remaining, key=mmr_score)
        best.mmr_score = mmr_score(best)  # kept for debugging/tuning only, HLD §18.2 — never mapped to the proto
        selected.append(best)
        remaining.remove(best)
    return selected
```

`mmr_score` is computed and stored on each selected candidate purely for the internal-tracking reason HLD §18.2 gives (debuggability, future `λ` tuning) — `response_mapper.py` never reads it.

### 7.6 `response_mapper.py`

```python
def to_proto(chunks: list[FusedCandidate], request_id: str) -> GetRelevantChunksResponse:
    return GetRelevantChunksResponse(
        chunks=[
            DocumentChunk(
                chunk_id=c.chunk_id,
                content=c.content,
                source_file_id=c.document_id,
                source_name=c.filename,
                relevance_score=c.rrf_score,          # HLD §18.2 — NOT mmr_score
                metadata={
                    "page_number": str(c.page_number) if c.page_number is not None else "",
                    "section": c.section or "",
                    "chunk_index": str(c.chunk_index),
                },
            )
            for c in chunks  # already sorted by rrf_score descending, per pipeline.py step 8
        ],
        total_tokens=sum(tokenizer.count(c.content) for c in chunks),
        request_id=request_id,
    )
```

### 7.7 Latency budget breakdown (fixes the timeout-hierarchy issue in an earlier revision)

`QUERY_PIPELINE_DEADLINE_SECONDS=1.8` is a **budget**, not just an outer ceiling — every downstream call's timeout is sized as an explicit slice of it, never picked independently:

```text
Permission check (File Service)   0.3s
Version resolution (local PG)     ~0.02s (not separately configurable — a single
                                    indexed lookup, not worth its own timeout knob)
Query embedding (embedding-svc)   0.4s
Dense + BM25 search (parallel)    0.5s each (concurrent, so this costs max(0.5, 0.5),
                                    not their sum)
RRF + candidate fetch + MMR       ~0.05s (in-process, Milvus primary-key get only)
─────────────────────────────────────────
Sum of sequential stages          ~1.27s
Network/scheduling margin         ~0.53s  (the remainder under 1.8s)
```

An earlier revision of this LLD set `EMBEDDING_GRPC_TIMEOUT_SECONDS=5.0` and `OPENSEARCH_REQUEST_TIMEOUT_SECONDS=5.0` — both larger than the entire 1.8s pipeline budget they sit inside, meaning a single slow downstream call could silently blow the whole request past LLM Service's own 2s caller deadline before the pipeline's own `asyncio.wait_for` in §7.1 ever got a chance to intervene. §2's config now gives the query path its own tight, budget-derived timeouts (`EMBEDDING_QUERY_TIMEOUT_SECONDS`, `MILVUS_QUERY_TIMEOUT_SECONDS`, `OPENSEARCH_QUERY_TIMEOUT_SECONDS`) distinct from the ingestion path's generous ones (`EMBEDDING_INGESTION_TIMEOUT_SECONDS`), which have no caller deadline to respect. The exact per-stage numbers above are starting points for benchmarking, not final — the rule that must hold regardless of the specific numbers is: **no child operation's timeout may exceed the remaining parent deadline at the point it's issued.**

---

## 8. Clients

### 8.1 `embedding_client.py` (HLD §12)

```python
class EmbeddingClient:
    async def embed_query(self, text: str) -> list[float]: ...
        # GenerateEmbedding, single — QUERY path, timeout=settings.EMBEDDING_QUERY_TIMEOUT_SECONDS (§7.7)
    async def embed_chunks(self, texts: list[str]) -> list[list[float]]: ...
        # GenerateEmbeddings, batched — INGESTION path, timeout=settings.EMBEDDING_INGESTION_TIMEOUT_SECONDS
        # (no caller deadline to respect here, unlike embed_query — §2, §7.7)

    # Every response validated:
    #   assert response.model == settings.EMBEDDING_MODEL_EXPECTED
    #   assert response.dimension == settings.EMBEDDING_DIMENSION_EXPECTED
    # A mismatch raises EmbeddingModelMismatchError — never silently indexed/queried on
    # (HLD §12's explicit runtime guard against a silent model upgrade).
```

Retries: `EMBEDDING_RETRY_ATTEMPTS` (default 2), exponential backoff, same shape as LLM Service's own policy toward Retrieval (HLD §17) — consistency here is deliberate, not incidental.

### 8.2 `file_service_client.py` (HLD §16.2)

```python
class FileServiceClient:
    async def batch_check_permissions(self, user_id: str, document_ids: list[str]) -> list[str]:
        """Calls File Service's internal batch permission-check endpoint.
        Authenticates via settings.FILE_SERVICE_SERVICE_CREDENTIAL (service-to-service,
        HLD §16.2 — proves Retrieval Service is the caller; user_id is data, not identity).
        Returns the authorized subset of document_ids. On timeout/failure, returns []
        (degrade to 'nothing authorized', never 'everything authorized' — fail closed)."""
```

**Fail-closed is the one non-negotiable property of this client** — every other client in this service (`embedding_client`, `milvus_repository`, `opensearch_repository`) degrades toward "return less/empty," but this one specifically must never degrade toward "treat as authorized" under any failure mode, since that would silently reopen the exact gap HLD §16.1 exists to close.

`FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS` (default 0.3s, §7.7's budget breakdown) is sized as an explicit slice of `QUERY_PIPELINE_DEADLINE_SECONDS` (1.8s), not picked independently, so a slow File Service degrades the request to empty rather than starving the rest of the pipeline's budget (HLD §17).

---

## 9. Deployment Topology — One Codebase, Two Independently-Scaled Workloads

HLD §22 requires query and ingestion to scale independently — a burst of uploads must never degrade query latency for unrelated in-flight chat turns, and vice versa. An earlier revision of this LLD undercut that by having a single `lifespan.py` start the gRPC server, the Kafka consumers, and the outbox worker all in the same process — which makes independent scaling impossible in practice: scaling for query load also scales idle Kafka consumers, and scaling for ingestion lag also scales idle gRPC capacity.

**Fix: one repository, one image, two deployment-time entrypoints**, selected by `settings.DEPLOYMENT_MODE` (§2) rather than two separate codebases:

```text
retrieval-service image
        │
        ├── DEPLOYMENT_MODE=api     → app/main.py starts:
        │                              - gRPC server (grpc/server.py) — GetRelevantChunks
        │                              - FastAPI health/metrics
        │                              NOT started: Kafka consumers, outbox_worker,
        │                              ingestion workers
        │
        └── DEPLOYMENT_MODE=worker  → app/main.py starts:
                                       - Kafka consumers (file_uploaded/file_deleted/
                                         file_processing_retry)
                                       - bounded ingestion worker pool (§6.1.1)
                                       - outbox_worker (publishes file.processing.*)
                                       - reconciliation sweep
                                       - FastAPI health/metrics (liveness only needs
                                         the process up, not gRPC)
                                       NOT started: gRPC server
```

`lifespan.py` branches once, at startup, on `DEPLOYMENT_MODE` — it is not two different files, just two different subsets of the same startup routine, keeping the branch small and auditable rather than duplicating wiring logic. `DEPLOYMENT_MODE=all` (both, in one process) exists purely for local development / docker-compose convenience — never used in a real deployment, where the two are always separate.

```text
Kubernetes (or equivalent):

retrieval-api            Deployment, replicas scale on gRPC request rate / CPU
  └── DEPLOYMENT_MODE=api

retrieval-worker         Deployment, replicas scale on Kafka consumer-group lag
  └── DEPLOYMENT_MODE=worker
      (each replica runs its own bounded ingestion queue, §6.1.1 — scaling
       replica count is the outer concurrency control, INGESTION_MAX_CONCURRENCY
       is the per-replica inner bound)
```

Both deployments share the same Postgres (`ingestion_jobs`/`processed_events`/`outbox_events`/`document_current_versions`, §3), Milvus, and OpenSearch — there is no per-workload data partitioning, only compute scales independently. `retrieval-api` never writes to `ingestion_jobs` or touches Kafka at all; `retrieval-worker` never opens the gRPC port. This matches HLD §22 exactly — the earlier "one process does everything" framing in §1's directory mapping was a code-organization description, not a deployment one, and is superseded by this section for deployment purposes.

---

## 10. Observability (HLD §24)

Prometheus metrics, mirroring file-service's `core/metrics.py` convention:

```text
retrieval_ingestion_jobs_total{status}            — counter, incremented on every terminal transition
retrieval_ingestion_stage_duration_seconds{stage} — histogram: download, parse, chunk, embed, index
retrieval_query_latency_seconds                   — histogram, the servicer's own end-to-end timer
retrieval_query_short_circuit_total{reason}        — counter: no_scope, no_file_ids, no_authorized_ids
retrieval_unsupported_scope_total{scope}           — counter (HLD §16.3 product-visibility signal)
retrieval_search_degraded_total{engine}            — counter: dense_only, bm25_only (HLD §17 fallback)
retrieval_permission_check_latency_seconds         — histogram, File Service client call only
retrieval_embedding_model_mismatch_total            — counter (HLD §12 guard — should always be 0)
```

`structlog` contextvars carry `trace_id` (from the gRPC request field, HLD §6) through every log line in a single `GetRelevantChunks` call, and `document_id`/`event_id` through every log line in a single ingestion job — same pattern as file-service's `RequestIDMiddleware`, adapted for gRPC via `logging_interceptor.py` instead of HTTP middleware.

---

## 11. Testing Strategy

* **Unit** (`tests/unit/`): `chunk_identity` (regex parse success/reject cases, HLD §8.3), `fusion` (RRF math, candidates present in only one list), `mmr` (diversity actually changes selection vs. pure rrf_score ranking), `scope_guard` (all four proto enum values × empty/non-empty `file_ids`), `scoring` (all five internal scores computed correctly, only `rrf_score` ever reaches a proto object in `response_mapper` tests).
* **Integration** (`tests/integration/`): run against real, already-running MinIO/Milvus/OpenSearch/Postgres/Kafka containers (matching the file-service/conversation-service convention of testing against existing infra, never spinning up fresh containers ad hoc) — full ingestion round trip (`file.uploaded` → chunks queryable in both stores), full query round trip (`GetRelevantChunks` → real fused/diversified response), and the `PARTIALLY_INDEXED` retry path (force one store to fail, confirm only that store is retried).
* No test asserts against `embedding-service`, `File Service`, or `LLM Service`'s real network endpoints in CI — those are mocked at the gRPC client boundary (`embedding_client`, `file_service_client`) per standard test isolation; the *contracts* themselves (proto shapes, event payloads) are verified once, by construction, since `protos/retrieval.proto` and `app/events/payloads.py` are copied/byte-matched from the source of truth rather than hand-typed independently (HLD §1.1 #2, §8).

---

## 12. What This LLD Deliberately Leaves Open

Matching HLD §25's own framing — these are not gaps in this LLD, they're the HLD's own tracked open items, carried forward unchanged since nothing at the LLD level resolves them:

1. File Service's batch permission-check endpoint (HLD §16.2) is a **dependency**, not something this repo builds — `file_service_client.py` is written against the contract HLD §16.2 specifies, but cannot be integration-tested end-to-end until File Service ships it.
2. `KNOWLEDGE_BASE`/`ALL` scope semantics (HLD §16.3) remain unimplemented by design — `scope_guard.py` treats both as "unsupported, return empty" per the frozen V1 behavior, with no further logic to build until Product defines them.
3. `content_version` Phase 2 (HLD §8.3) — `chunk_identity.py` implements Phase 1 (strict `storage.key` parsing) as the permanent path; a `content_version` field appearing on the wire later is an additive change to `app/events/payloads.py`'s `FileUploadedEvent`, not a redesign.
4. `file.processing_retry` gaining `content_version` (HLD §1.3 #1, §8.5) is also a **File Service dependency** — `file_processing_retry_consumer.py` is written to require it, but File Service must ship the one-line payload change before retry-of-a-versioned-document is safe end-to-end; until then, retrying a document that has ever had more than one content version should be treated as unsupported (reject + log, not a silent guess).
5. Durable storage for parsed/chunked/embedded intermediates, which would let §6.3's partial-index recovery skip re-parsing/re-embedding entirely, is explicitly **not** built in V1 (no new database for this) — recomputing from the original MinIO object is the deliberate V1 tradeoff; revisit only if measured `PARTIALLY_INDEXED` volume makes recomputation a real cost, not a hypothetical one.
