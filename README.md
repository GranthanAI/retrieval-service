# Retrieval Service

Granthan's Retrieval Service — ingests uploaded documents (via File Service's `file.uploaded` Kafka event) into Milvus (dense) and OpenSearch (BM25), and serves the top-K most relevant chunks over gRPC (`GetRelevantChunks`, frozen proto owned by `llm-service`).

See [`docs/HLD.md`](./docs/HLD.md) for the high-level design, [`docs/LLD.md`](./docs/LLD.md) for the low-level design, and [`docs/IMPLEMENTATION_PLAN.md`](./docs/IMPLEMENTATION_PLAN.md) for the phased build plan this codebase follows.

## Stack

FastAPI (health/metrics only) · gRPC (`grpcio`, the real API surface) · SQLAlchemy (async) + PostgreSQL · Alembic · `aiokafka` · `pymilvus` · `opensearch-py` · Docling · `uv`

## Local development

Shared infrastructure (Postgres, Kafka, Milvus, OpenSearch, and file-service's dedicated MinIO instance) lives in the **root** [`../docker-compose.yml`](../docker-compose.yml), alongside the rest of GraphGPT's shared containers — this service never runs its own duplicate containers for any of these.

```bash
# 1. One-time / whenever infra is down: start shared containers
make infra

# 2. One-time: create the retrieval_service_db database
#    (only needed if the shared postgres_data volume already existed before
#    this service was added — otherwise it's created automatically)
make db-create

# 3. Install dependencies
make sync
cp .env.example .env

# 4. Regenerate gRPC stubs from protos/retrieval.proto
make proto

# 5. Apply database migrations (none yet at Phase 0)
make db-migrate

# 6. Run the app locally (dev, auto-reload)
make run
# -> http://127.0.0.1:8060/health/live

# Worker mode (Kafka consumers, ingestion pipeline — stubs until Phase 5)
make run-worker
```

The `Dockerfile` is still built and used for actual deployment (see `docs/IMPLEMENTATION_PLAN.md` Phase 13) — it's just not part of the everyday local dev loop.

### Milvus collection / OpenSearch index provisioning

`scripts/provision_milvus_collection.py` and `scripts/provision_opensearch_index.py` are one-time infra steps, run manually per environment — **never** imported by `app/` or run as part of pod startup (LLD §13.2). Their schemas land in Phase 2.

### Ports (local dev)

| Service | Port(s) |
|---|---|
| retrieval-service gRPC (api mode) | `50060` |
| retrieval-service HTTP health/metrics | `8060` |
| PostgreSQL (shared) | `5432` |
| Kafka (shared) | `9092` |
| Milvus (shared) | `19530` |
| OpenSearch (shared) | `9200` |
| file-service MinIO API (shared) | `9010` |

## Tests

```bash
make test
```
