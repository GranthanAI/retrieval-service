import uuid
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.core.config import settings
from app.core.enums import IngestionStatus, StageStatus
from app.core.logging import get_logger
from app.ingestion import chunk_identity, chunker, downloader
from app.ingestion import parser as parser_module
from app.ingestion.embedder import embed_batch
from app.ingestion.indexer import IndexTarget
from app.ingestion.indexer import write as index_write
from app.models.chunk_record import ChunkRecord
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.repositories.outbox_repository import OutboxRepository

logger = get_logger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _mark_processing_and_publish_started(
    session_factory: async_sessionmaker, document_id: UUID, content_version: int
) -> None:
    """`file.processing.started` is published immediately after `status ->
    PROCESSING`, before the download begins (LLD §6.2) — a separate, early
    transaction from the final one, giving File Service the earliest
    possible signal that ingestion has begun.
    """
    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, content_version)
        job.status = IngestionStatus.PROCESSING.value
        db.add(job)
        await OutboxRepository.insert(
            db,
            event_id=uuid.uuid4(),
            topic=settings.KAFKA_TOPIC_PROCESSING_STARTED,
            payload={"event_id": str(uuid.uuid4()), "document_id": str(document_id), "timestamp": _now_iso()},
        )
        await db.commit()


async def _fail(
    session_factory: async_sessionmaker,
    document_id: UUID,
    content_version: int,
    *,
    error_code: str,
    error_message: str,
    embedding_status: str | None = None,
) -> None:
    """Terminal failure path shared by the download/parse/embedding stages
    (LLD §6.2) — `status=FAILED`, the given `error_code`/`error_message`,
    and a `file.processing.failed` outbox row, all in one transaction.
    """
    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, content_version)
        job.status = IngestionStatus.FAILED.value
        job.error_code = error_code
        job.error_message = error_message
        if embedding_status is not None:
            job.embedding_status = embedding_status
        db.add(job)
        await OutboxRepository.insert(
            db,
            event_id=uuid.uuid4(),
            topic=settings.KAFKA_TOPIC_PROCESSING_FAILED,
            payload={
                "event_id": str(uuid.uuid4()),
                "document_id": str(document_id),
                "error_code": error_code,
                "error_message": error_message,
                "timestamp": _now_iso(),
            },
        )
        await db.commit()


async def run(
    session_factory: async_sessionmaker,
    document_id: UUID,
    content_version: int,
    embedding_client: EmbeddingClient,
) -> None:
    """The full ingestion state machine (LLD §6.2): `PROCESSING` -> download
    -> parse -> chunk -> embed -> index, branching to `READY`/
    `PARTIALLY_INDEXED`/`FAILED` per which stage(s) fail. Publishes to
    `outbox_events` at each of `file.processing.started`/`.completed`/
    `.failed` — not yet to real Kafka (Phase 5 adds the worker that drains
    this table onto a real broker); the outbox row is the observable
    artifact this phase's tests assert against.

    Takes `document_id`/`content_version` (not an `IngestionJob` object)
    because every stage boundary below re-fetches its own copy of the job
    row in a fresh, short-lived transaction — the object a caller might
    pass in would be detached the moment its own session closes.
    """
    await _mark_processing_and_publish_started(session_factory, document_id, content_version)

    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, content_version)
        storage_bucket, storage_key, filename = job.storage_bucket, job.storage_key, job.filename
        owner_id, conversation_id = job.owner_id, job.conversation_id

    try:
        raw_bytes = await downloader.fetch(storage_bucket, storage_key)
    except downloader.DownloadError as exc:
        logger.warning("orchestrator.download_failed", document_id=str(document_id), error=str(exc))
        await _fail(session_factory, document_id, content_version, error_code="DOWNLOAD_ERROR", error_message=str(exc))
        return

    try:
        parsed = parser_module.parse(raw_bytes, filename)
    except parser_module.ParserError as exc:
        logger.warning("orchestrator.parse_failed", document_id=str(document_id), error=str(exc))
        await _fail(session_factory, document_id, content_version, error_code="PARSER_ERROR", error_message=str(exc))
        return

    chunks = chunker.chunk(parsed)
    chunk_ids = [
        chunk_identity.derive_chunk_id(str(document_id), content_version, c.chunk_index) for c in chunks
    ]

    try:
        vectors = await embed_batch(embedding_client, chunks)
    except Exception as exc:
        # No partial index writes are possible without embeddings — this is
        # the one stage that isn't independently retriable per-target like
        # the Milvus/OpenSearch split below, since both indexes need the
        # same vectors (LLD §6.2).
        logger.warning("orchestrator.embedding_failed", document_id=str(document_id), error=str(exc))
        await _fail(
            session_factory,
            document_id,
            content_version,
            error_code="EMBEDDING_ERROR",
            error_message=str(exc),
            embedding_status=StageStatus.FAILED.value,
        )
        return

    records = [
        ChunkRecord(
            chunk_id=chunk_id,
            document_id=str(document_id),
            content_version=content_version,
            chunk_index=c.chunk_index,
            owner_id=str(owner_id),
            conversation_id=str(conversation_id) if conversation_id else None,
            filename=filename,
            page_number=c.page_number,
            section=c.section,
            content=c.content,
            embedding=vector,
        )
        for c, chunk_id, vector in zip(chunks, chunk_ids, vectors)
    ]

    index_result = await index_write(records, IndexTarget.BOTH)

    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, content_version)
        job.embedding_status = StageStatus.COMPLETE.value
        job.milvus_status = index_result.milvus_status
        job.opensearch_status = index_result.opensearch_status
        job.chunk_count = len(records)
        db.add(job)
        await IngestionJobRepository.recompute_status(db, job)

        if job.status == IngestionStatus.READY.value:
            # Written in the SAME transaction as the status transition
            # (LLD §3.3) — this is what makes "never publish .completed for
            # a PARTIALLY_INDEXED job" (HLD §20) enforceable structurally.
            await OutboxRepository.insert(
                db,
                event_id=uuid.uuid4(),
                topic=settings.KAFKA_TOPIC_PROCESSING_COMPLETED,
                payload={
                    "event_id": str(uuid.uuid4()),
                    "document_id": str(document_id),
                    "chunk_count": len(records),
                    "timestamp": _now_iso(),
                },
            )
        elif job.status == IngestionStatus.FAILED.value:
            job.error_code = "INDEXING_ERROR"
            job.error_message = "both Milvus and OpenSearch writes failed"
            db.add(job)
            await OutboxRepository.insert(
                db,
                event_id=uuid.uuid4(),
                topic=settings.KAFKA_TOPIC_PROCESSING_FAILED,
                payload={
                    "event_id": str(uuid.uuid4()),
                    "document_id": str(document_id),
                    "error_code": "INDEXING_ERROR",
                    "error_message": "both Milvus and OpenSearch writes failed",
                    "timestamp": _now_iso(),
                },
            )
        # PARTIALLY_INDEXED: no outbox row — LLD §6.2/HLD §20, the document
        # must not be treated as fully searchable until both indexes agree.

        await db.commit()
