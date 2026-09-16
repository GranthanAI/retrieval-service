# Task Update 6 — Phase 6: Partial-Index Recovery + Reconciliation

This document records the completion of Phase 6 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — a `PARTIALLY_INDEXED` job now actually recovers, and stuck jobs get swept without relying on Kafka redelivery.

---

## 1. Recovery Path (LLD §6.3)

`orchestrator.recover_partially_indexed_job()` reconstructs the indexing payload from the original MinIO object — chunks/embeddings never survive between the original attempt and a later retry, since `ingestion_jobs` stores only job state. Determines which store to target from the job's own per-stage columns:

```python
if opensearch_status == StageStatus.FAILED.value and milvus_status == StageStatus.COMPLETE.value:
    target = IndexTarget.OPENSEARCH
elif milvus_status == StageStatus.FAILED.value and opensearch_status == StageStatus.COMPLETE.value:
    target = IndexTarget.MILVUS
```

**Always re-embeds, regardless of which store failed** — this is the one part of LLD §6.3 easy to misread as "only re-embed when Milvus needs it." Re-reading the LLD's own pseudocode confirms both branches call `embed_batch` unconditionally; the reasoning is that OpenSearch's content fields come from the same chunk objects the embedder consumes, and V1 has no durable store for chunks independent of their embeddings — forking the pipeline to skip embedding for an OpenSearch-only recovery would mean two different code paths for what's supposed to be one deterministic pipeline. Wasted-but-cheap re-embedding was accepted deliberately by the LLD, not an oversight.

Re-uses `IngestionJobRepository.recompute_status` unchanged (built in Phase 1, already reused unmodified by Phase 4's orchestrator) — the same derivation rule correctly turns a successful single-target recovery into `READY`, or leaves it `PARTIALLY_INDEXED` if the retry also fails.

## 2. Reconciliation Worker — Now Sweeping Three Cases, Not Two

`app/workers/reconciliation_worker.py`'s `run_sweep()` (separated from the periodic loop for direct testability, matching file-service's own convention):

- `PARTIALLY_INDEXED` jobs older than the threshold → recovery.
- `PROCESSING` jobs stuck with no in-flight work → re-enqueued (LLD §3.2's own example: a crash after ack but during `orchestrator.run()`).
- **`NOT_STARTED` jobs stuck with no in-flight work → re-enqueued.** This is a real addition beyond LLD §3.2's literal text, not a Phase 6 task-list item — it closes the exact gap Task Update 5 flagged and left open: a crash between the inbox transaction committing (job created as `NOT_STARTED`, Kafka offset already acked) and `pool.enqueue()` actually running never reaches `PROCESSING` at all, so `list_stuck_processing` alone can never find it. Added `IngestionJobRepository.list_stuck_not_started` (mirrors `list_stuck_processing` exactly, different status filter) and wired it into the same sweep.

Wired into `lifespan.py`'s `worker` branch, gated on `settings.RECONCILIATION_ENABLED` (already existed as a Phase 0 setting, unused until now).

## 3. Exit Criteria Verified — Real Infra Throughout

- **OpenSearch-failure recovery**: force OpenSearch to fail during ingestion (Phase 4's own forced-failure mechanism, reused), confirm `PARTIALLY_INDEXED`, run recovery, confirm `READY`, a `file.processing.completed` outbox row, and the chunk is genuinely findable in OpenSearch again.
- **Milvus-failure recovery with a re-embedding call-count assertion**: wrapped `embedding_client.embed_chunks` to count invocations — confirmed exactly one real call during recovery, proving re-embedding actually happened rather than being silently skipped.
- **Chunk-id stability**: `derive_chunk_id` on the same document before and after recovery produces the identical id, and OpenSearch's post-recovery result set contains exactly that one id — the re-indexed write is a safe upsert, not a duplicate.
- **Stuck-`PROCESSING` sweep**: a backdated `PROCESSING` job gets re-enqueued by one sweep call; a fresh one (inside the threshold) is correctly left alone.
- **Stuck-`NOT_STARTED` sweep** (the gap closed in §2 above): same mechanism, proven with its own dedicated test.

6/6 tests passing; 40/40 across the whole suite (Phases 1–7 — Phase 7's client, built alongside this phase, is included in that count).
