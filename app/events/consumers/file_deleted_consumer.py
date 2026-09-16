import asyncio

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import settings
from app.core.logging import get_logger
from app.delete import delete_service
from app.events.payloads import FileDeletedEvent
from app.events.topics import FILE_DELETED
from app.repositories.processed_event_repository import ProcessedEventRepository

logger = get_logger(__name__)


async def handle_message(session_factory: async_sessionmaker, raw_value: bytes) -> None:
    """LLD §6.4 — unlike the ingestion path, a blind "already in
    processed_events -> skip" check is safe here without status-dispatch:
    `delete_service.purge()` has no durable intermediate state to lose
    (both stores' deletes are naturally idempotent, HLD §21), so there's no
    crash window equivalent to "job created but never processed" to protect
    against. The dedup check exists purely to avoid redundant Milvus/
    OpenSearch calls on a genuinely redelivered event, not for correctness.
    """
    try:
        payload = FileDeletedEvent.model_validate_json(raw_value)
    except ValidationError:
        logger.exception("file_deleted_consumer.malformed_payload")
        return

    async with session_factory() as db:
        is_new = await ProcessedEventRepository.mark_processed(db, payload.event_id, "file_deleted_consumer")
        await db.commit()

    if not is_new:
        return

    await delete_service.purge(session_factory, payload.document_id)


async def run_file_deleted_consumer(session_factory: async_sessionmaker, stop_event: asyncio.Event) -> None:
    consumer = AIOKafkaConsumer(
        FILE_DELETED,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    try:
        while not stop_event.is_set():
            result = await consumer.getmany(timeout_ms=settings.KAFKA_CONSUMER_POLL_TIMEOUT_MS)
            for _tp, records in result.items():
                for record in records:
                    try:
                        await handle_message(session_factory, record.value)
                    except Exception:
                        logger.exception("file_deleted_consumer.message_failed")
    finally:
        await consumer.stop()
