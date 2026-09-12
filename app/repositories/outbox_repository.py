from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox_event import OutboxEvent


class OutboxRepository:
    @staticmethod
    async def insert(db: AsyncSession, *, event_id: UUID, topic: str, payload: dict[str, Any]) -> OutboxEvent:
        """LLD §3.3 — callers must `insert` in the same DB transaction as the
        ingestion_jobs status transition that causes this event, never as a
        separate commit (this is what makes "never publish .completed for a
        PARTIALLY_INDEXED job" enforceable at the transaction level, HLD §20)."""
        event = OutboxEvent(event_id=event_id, topic=topic, payload=payload, published=False)
        db.add(event)
        await db.flush()
        return event

    @staticmethod
    async def fetch_unprocessed_for_update(db: AsyncSession, limit: int = 100) -> list[OutboxEvent]:
        """`FOR UPDATE SKIP LOCKED` so multiple retrieval-worker pods can run
        outbox_worker concurrently without double-publishing the same row —
        same pattern as file-service's own outbox. This alone does not make
        publishing exactly-once; File Service's own consumer dedupes on
        event_id (HLD §8.6)."""
        result = await db.execute(
            select(OutboxEvent)
            .where(OutboxEvent.published.is_(False))
            .order_by(OutboxEvent.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars().all())

    @staticmethod
    async def mark_processed(db: AsyncSession, event: OutboxEvent) -> None:
        event.published = True
        event.published_at = datetime.now(timezone.utc)
        db.add(event)
        await db.flush()
