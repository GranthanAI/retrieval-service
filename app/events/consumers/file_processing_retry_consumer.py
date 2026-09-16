import asyncio

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import settings
from app.core.logging import get_logger
from app.events.ingestion_pool import BoundedIngestionPool
from app.events.payloads import FileProcessingRetryEvent
from app.events.topics import FILE_PROCESSING_RETRY
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.repositories.processed_event_repository import ProcessedEventRepository

logger = get_logger(__name__)


async def handle_message(session_factory: async_sessionmaker, pool: BoundedIngestionPool, raw_value: bytes) -> None:
    """LLD §6.1's closing paragraph — looks up the job by `(document_id,
    content_version)`; `content_version` is a required field on
    `FileProcessingRetryEvent` (HLD §1.3 #1/§8.5), so a payload missing it
    fails Pydantic validation here and is rejected/dead-lettered, never
    guessed. The looked-up job's own `storage_bucket`/`storage_key` are
    reused as-is (HLD §8.5 — the bytes are untouched), no File Service call
    needed — re-enqueuing is identical to a fresh `file.uploaded` dispatch.

    Dedup here is blind-skip (like `file_deleted_consumer`, LLD §6.4's
    reasoning) rather than status-dispatched (like `file_uploaded_consumer`,
    LLD §3.2): re-running `orchestrator.run()` for an already-`READY` job is
    itself idempotent (deterministic chunk ids, upsert-not-duplicate), so a
    redelivered retry event is safe to skip outright — this dedup exists to
    avoid redundant work, not to prevent a correctness bug.
    """
    try:
        payload = FileProcessingRetryEvent.model_validate_json(raw_value)
    except ValidationError:
        logger.error("file_processing_retry_consumer.missing_or_malformed_content_version")
        return

    async with session_factory() as db:
        is_new = await ProcessedEventRepository.mark_processed(db, payload.event_id, "file_processing_retry_consumer")
        job = await IngestionJobRepository.get_by_document_and_version(db, payload.document_id, payload.content_version)
        await db.commit()

    if not is_new:
        return

    if job is None:
        logger.error(
            "file_processing_retry_consumer.job_not_found",
            document_id=str(payload.document_id),
            content_version=payload.content_version,
        )
        return

    await pool.enqueue(payload.document_id, payload.content_version)


async def run_file_processing_retry_consumer(
    session_factory: async_sessionmaker, pool: BoundedIngestionPool, stop_event: asyncio.Event
) -> None:
    consumer = AIOKafkaConsumer(
        FILE_PROCESSING_RETRY,
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
                        await handle_message(session_factory, pool, record.value)
                    except Exception:
                        logger.exception("file_processing_retry_consumer.message_failed")
    finally:
        await consumer.stop()
