import asyncio

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import settings
from app.core.enums import IngestionStatus
from app.core.logging import get_logger
from app.events.ingestion_pool import BoundedIngestionPool
from app.events.payloads import FileUploadedEvent
from app.events.topics import FILE_UPLOADED
from app.ingestion.chunk_identity import ContentVersionParseError, parse_content_version
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.repositories.processed_event_repository import ProcessedEventRepository

logger = get_logger(__name__)

_REENQUEUE_STATUSES = {IngestionStatus.NOT_STARTED.value, IngestionStatus.PARTIALLY_INDEXED.value, IngestionStatus.FAILED.value}


async def handle_message(session_factory: async_sessionmaker, pool: BoundedIngestionPool, raw_value: bytes) -> None:
    """LLD §6.1's corrected flow: parse `content_version` from
    `storage.key` BEFORE the transaction (a malformed key must not create a
    job), then a single transaction (job upsert + version-projection
    advance + inbox insert), commit, then dispatch — on first delivery,
    always enqueue; on redelivery, re-derive the job's actual status and
    dispatch per LLD §3.2's table rather than blindly skip.
    """
    try:
        payload = FileUploadedEvent.model_validate_json(raw_value)
    except ValidationError:
        logger.exception("file_uploaded_consumer.malformed_payload")
        return

    try:
        content_version = parse_content_version(payload.storage.key)
    except ContentVersionParseError:
        logger.error("file_uploaded_consumer.unparseable_storage_key", storage_key=payload.storage.key)
        return

    async with session_factory() as db:
        await IngestionJobRepository.upsert_job(
            db,
            document_id=payload.document_id,
            content_version=content_version,
            owner_id=payload.owner_id,
            conversation_id=payload.conversation_id,
            filename=payload.filename,
            storage_bucket=payload.storage.bucket,
            storage_key=payload.storage.key,
        )
        await DocumentCurrentVersionRepository.advance_if_newer(db, payload.document_id, content_version)
        is_new = await ProcessedEventRepository.mark_processed(db, payload.event_id, "file_uploaded_consumer")
        await db.commit()

    if is_new:
        await pool.enqueue(payload.document_id, content_version)
        return

    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, payload.document_id, content_version)

    if job is not None and job.status in _REENQUEUE_STATUSES:
        await pool.enqueue(payload.document_id, content_version)
    # READY -> no-op; PROCESSING -> leave to the reconciliation sweep (Phase 6).


async def run_file_uploaded_consumer(
    session_factory: async_sessionmaker, pool: BoundedIngestionPool, stop_event: asyncio.Event
) -> None:
    consumer = AIOKafkaConsumer(
        FILE_UPLOADED,
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
                        logger.exception("file_uploaded_consumer.message_failed")
    finally:
        await consumer.stop()
