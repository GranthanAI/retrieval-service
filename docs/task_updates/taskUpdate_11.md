# Task Update 11 — Phase 11: Observability

This document records the completion of Phase 11 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — mode-aware readiness, a scrapeable `/metrics`, every metric from LLD §10 wired to a real call site, and log correlation. Phases 10, 12 and 13 (deployment topology, testing completion, CI/CD) were deliberately not started — deployment is being done later.

---

## 1. `GET /health/ready` — Mode-Aware (LLD §9)

`app/api/dependency_checks.py` holds one reachability probe per dependency, each bounded by `HEALTH_CHECK_TIMEOUT_SECONDS`, and a mode map deciding which ones a deployment is judged on:

| Mode | Checked |
|---|---|
| `api` | Postgres, Milvus, OpenSearch |
| `worker` | Postgres, MinIO, Kafka |
| `all` | union of both |

The point is that a Kafka outage must not take `retrieval-api` pods NotReady (they never touch Kafka), and a Milvus outage must not take workers NotReady (they never query it). Tests assert the irrelevant probes aren't merely ignored but **never called**. To support this without reaching into private helpers, `MilvusRepository.ping()`, `OpenSearchRepository.ping()` and `downloader.ping()` were added; the Kafka probe uses the already-started producer's metadata fetch. `/health/live` is unchanged: no dependency calls, so a slow dependency can't cause a restart loop.

## 2. `/metrics` and the Full Metric Set (LLD §10)

`GET /metrics` serves `prometheus_client.generate_latest()`. `core/metrics.py` gained the last two missing instruments (`retrieval_ingestion_jobs_total{status}`, `retrieval_ingestion_stage_duration_seconds{stage}`), and the ones that had been defined-but-unused since Phase 8 are now wired:

- `retrieval_embedding_model_mismatch_total` — incremented in `embedding_client._assert_model_matches` just before it raises.
- `retrieval_permission_check_latency_seconds` — wraps the File Service call in `FileServiceClient`.
- `retrieval_ingestion_stage_duration_seconds` — `download`, `parse`, `chunk`, `embed`, `index` in the orchestrator.
- `retrieval_ingestion_jobs_total` — incremented after each settled outcome (`READY`, `PARTIALLY_INDEXED`, `FAILED`) in the first-run path, the failure path, and the Phase 6 recovery path.

## 3. Log Correlation

- **Query path:** `trace_id` was already bound by the gRPC `LoggingInterceptor` (Phase 9); now covered by a test that the value is visible inside the handler and gone afterward.
- **Ingestion path:** `orchestrator.run()` and `recover_partially_indexed_job()` are now thin wrappers that bind `document_id` into structlog contextvars for the whole job and unbind it in a `finally` (the pool's worker tasks run many jobs, so leaking it would mislabel the next one). The three Kafka consumers bind `event_id` and `document_id` once the payload validates, and clear contextvars after every record.
- `api/middleware.py` (`RequestIDMiddleware`) was added per LLD §1 for the HTTP health/metrics surface, mirroring file-service's.

**Known limit:** `event_id` appears in the consumer's own log lines but not in the orchestrator's, because the ingestion queue carries only `(document_id, content_version)`. Threading `event_id` through `pool.enqueue()` would change that signature and every test spy on it; left as-is since `document_id` already ties the two together.

## 4. Verification

- 13 new unit tests (`tests/unit/test_health_and_metrics.py`): 503 when Milvus/OpenSearch is down in `api` mode and when Kafka/MinIO is down in `worker` mode; 200 when only irrelevant dependencies are down; Postgres down is 503 in every mode; `/metrics` exposes every LLD §10 metric name; the unsupported-scope, model-mismatch and permission-latency instruments each move when their code path runs; `trace_id` is bound for a call and cleared after.
- Orchestrator integration tests extended: a real run increments `retrieval_ingestion_jobs_total{status="READY"}` by exactly one and each of the five stage histograms by exactly one; a failed run increments `{status="FAILED"}`; and `document_id` is observed in the contextvars *during* the run and absent *after*.

One honest scoping note on the plan's exit criterion "`/metrics` shows non-zero values for every metric after running the Phase 5/6/8 integration tests once": the tests assert each wired instrument moves by the expected amount in the specific path that owns it, rather than scraping a running server after a combined run. `retrieval_query_latency_seconds` and `retrieval_query_timeout_total` are exercised by the existing gRPC servicer tests but have no dedicated assertion here.
