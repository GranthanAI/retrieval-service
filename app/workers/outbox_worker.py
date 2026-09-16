import asyncio
import json

from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.outbox_repository import OutboxRepository

logger = get_logger(__name__)


async def _publish_batch(db: AsyncSession, producer: AIOKafkaProducer) -> int:
    """`FOR UPDATE SKIP LOCKED` (OutboxRepository) lets multiple
    retrieval-worker pods run this loop concurrently without
    double-publishing the same row. One commit at the end of the whole
    batch, not per-row — this is at-least-once, not exactly-once: a crash
    between the Kafka ack and this commit republishes the row on the next
    poll. File Service's own consumer dedupes on `event_id` (HLD §8.6), so
    a fresh, unique `event_id` per outbox row (set at insert time by the
    orchestrator/delete_service, never mutated here) is what keeps this
    safe.
    """
    rows = await OutboxRepository.fetch_unprocessed_for_update(db, limit=settings.OUTBOX_BATCH_SIZE)
    for row in rows:
        message = json.dumps(row.payload).encode("utf-8")
        await producer.send_and_wait(row.topic, message)
        await OutboxRepository.mark_processed(db, row)
    await db.commit()
    return len(rows)


async def run_outbox_worker(
    session_factory: async_sessionmaker, producer: AIOKafkaProducer, stop_event: asyncio.Event
) -> None:
    while not stop_event.is_set():
        try:
            async with session_factory() as db:
                published = await _publish_batch(db, producer)
                if published:
                    logger.info("outbox_worker.published", count=published)
        except Exception:
            logger.exception("outbox_worker.batch_failed")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.OUTBOX_POLL_INTERVAL_SECONDS)
        except TimeoutError:
            pass
