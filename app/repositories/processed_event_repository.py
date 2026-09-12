from uuid import UUID

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.processed_event import ProcessedEvent


class ProcessedEventRepository:
    @staticmethod
    async def mark_processed(db: AsyncSession, event_id: UUID, consumer: str) -> bool:
        """LLD §3.2 — `INSERT ... ON CONFLICT DO NOTHING`, the single
        statement that makes the check-and-record atomic under concurrent
        delivery/retries. **Must be called in the same DB transaction as the
        ingestion_jobs upsert it accompanies** (never committed on its own)
        so "durably accepted" and "a job row exists for this event" are
        always the same fact — the crash-window bug an earlier revision of
        this design had (LLD §3.2).

        Returns True if this is the first time (event_id, consumer) has been
        seen. Returns False if it was already recorded — callers on this
        path must NOT treat that as a blind no-op; they must look up the
        job's actual status and dispatch per LLD §3.2's table (READY -> no-op,
        PARTIALLY_INDEXED/FAILED -> re-enqueue, PROCESSING -> leave to the
        reconciliation sweep, NOT_STARTED -> enqueue). That dispatch lives in
        the Kafka consumer (Phase 5), not here — this repository only
        answers "have we seen this before," never what to do about it.
        """
        stmt = (
            pg_insert(ProcessedEvent)
            .values(event_id=event_id, consumer=consumer)
            .on_conflict_do_nothing(index_elements=["event_id", "consumer"])
            .returning(ProcessedEvent.event_id)
        )
        result = await db.execute(stmt)
        await db.flush()
        return result.first() is not None
