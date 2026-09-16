# Task Update 5 — Phase 5: Kafka Consumers, Bounded Ingestion, Outbox Producer

This document records the completion of Phase 5 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — the pipeline built in Phase 4 is now actually driven by real Kafka events.

---

## 1. Event Payloads and Topics (HLD §8)

`app/events/payloads.py` — all six event shapes, byte-matched to HLD §8. The one load-bearing detail: `FileProcessingRetryEvent.content_version` has no default, so a payload missing it fails Pydantic validation at the door (HLD §1.3 #1):

```python
class FileProcessingRetryEvent(BaseModel):
    event_id: UUID
    event_type: Literal["file.processing_retry"] = "file.processing_retry"
    version: int = 1
    document_id: UUID
    content_version: int  # required — no default, never guessed
    retry_count: int
    timestamp: datetime
```

`app/events/topics.py` derives its constants from `settings.KAFKA_TOPIC_*` (Phase 0's config) rather than duplicating literal strings — one source of truth for topic names.

## 2. The Corrected Inbox Flow (LLD §3.2)

`file_uploaded_consumer.py`'s `handle_message` always runs the same transaction (job upsert + version-projection advance + inbox insert), then branches on whether the inbox insert reports "new" or "already recorded":

```python
if is_new:
    await pool.enqueue(payload.document_id, content_version)
    return

# Redelivery — re-derive the job's actual status, don't blindly skip.
async with session_factory() as db:
    job = await IngestionJobRepository.get_by_document_and_version(db, payload.document_id, content_version)
if job is not None and job.status in _REENQUEUE_STATUSES:  # NOT_STARTED, PARTIALLY_INDEXED, FAILED
    await pool.enqueue(payload.document_id, content_version)
# READY -> no-op; PROCESSING -> leave to the reconciliation sweep (Phase 6).
```

`file_processing_retry_consumer.py` looks up the job by `(document_id, event.content_version)` and reuses its stored `storage_bucket`/`storage_key` as-is — no File Service call needed. `file_deleted_consumer.py` uses blind-skip dedup instead (LLD §6.4) since both stores' deletes are naturally idempotent — there's no crash window to protect against the way there is for ingestion.

## 3. The Bounded Ingestion Pool — Built Fresh (LLD §6.1.1)

No service in the monorepo had a bounded-queue-plus-worker-pool pattern to mirror (confirmed by surveying file-service's Kafka code first). `app/events/ingestion_pool.py`:

```python
class BoundedIngestionPool:
    def __init__(self, session_factory, embedding_client, max_concurrency=None):
        self._queue = asyncio.Queue(maxsize=self._max_concurrency * 2)
        self._workers = [asyncio.create_task(self._worker_loop()) for _ in range(self._max_concurrency)]

    async def enqueue(self, document_id, content_version) -> None:
        await self._queue.put((document_id, content_version))  # blocks when full — the intended backpressure
```

Neither Kafka consumer calls `orchestrator.run()` directly from its own poll loop — this is what keeps a Kafka lag spike from turning into unbounded concurrent ingestion.

## 4. Outbox Producer + Worker (HLD §8.6)

`app/events/producers/processing_status_producer.py` (idempotent `start_producer`/`stop_producer`, matching file-service's own singleton pattern) and `app/workers/outbox_worker.py` — `FOR UPDATE SKIP LOCKED` fetch, `producer.send_and_wait` per row, one commit at the end of the batch (at-least-once, not exactly-once — File Service's own consumer dedupes on `event_id`).

## 5. `app/lifespan.py` — `worker` Mode Is Real Now

Previously a stub since Phase 0. Now starts the DB engine, an `EmbeddingClient`, the `BoundedIngestionPool`, all three consumers, and the outbox worker as tracked background tasks, each with its own `asyncio.Event` for graceful shutdown — same lifecycle shape file-service uses.

## 6. Manual Smoke Test Before Writing Automated Tests

Booted `DEPLOYMENT_MODE=worker` for real, published one real `file.uploaded` message to the real dev Kafka, and watched it land:

```
document_id                          | content_version | status | chunk_count
41e0dc6b-a1aa-4c9c-b09d-a61a3fafd8b6 | 1               | READY  | 1
```

with both `file.processing.started` and `file.processing.completed` outbox rows marked `published`. This preceded and de-risked the automated suite below.

## 7. Exit Criteria Verified — Real Kafka (testcontainers), Real Everything Else

- **End-to-end**: publish `file.uploaded` → job reaches `READY`, chunk genuinely queryable in OpenSearch, and `file.processing.completed` is observed on the **real output topic** (a throwaway verify-consumer reads it back), not just present in the outbox table.
- **Redelivery**: publishing the identical `event_id` twice — the second delivery leaves the job's `updated_at` byte-for-byte unchanged, proving a genuine no-op rather than a silent, harmless re-run that happened to also land `READY`.
- **Backpressure**: tested at the pool level directly (no Kafka lag metric exists until Phase 11) — with `max_concurrency=2` and a slow fake orchestrator, peak concurrent execution never exceeds 2, and `enqueue()` genuinely blocks while full.
- **Malformed retry**: a `file.processing_retry` missing `content_version` is rejected — nothing enqueued, no job row created — and the consumer loop survives to correctly handle a subsequent well-formed message.
- **Crash window** — see the honest gap below.

5/5 new tests passing; 25/25 across the whole suite.

## 8. An Honest Gap, Not Glossed Over

The plan's crash-window exit criterion assumes recovery finds the job stuck in `PROCESSING`. In this implementation, a crash between the inbox transaction committing and `pool.enqueue()` actually being called leaves the job `NOT_STARTED` — `orchestrator.run()` (the only thing that sets `PROCESSING`) never got to run. Built a test proving the actual observable precondition (the job survives as a durable, un-enqueued `NOT_STARTED` row — not silently lost, since its Kafka offset is already committed) and left an explicit note for Phase 6: `list_stuck_processing` alone won't catch this specific case; the sweep needs to also consider aged `NOT_STARTED` jobs, not just `PROCESSING` ones.

This is exactly what Phase 6 (next) needed to account for — and does; see Task Update 6.
