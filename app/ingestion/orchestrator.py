import uuid
from datetime import datetime, timezone
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.core import metrics
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
    metrics.ingestion_jobs_total.labels(status=IngestionStatus.FAILED.value).inc()


async def _run(
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
        with metrics.ingestion_stage_duration_seconds.labels(stage="download").time():
            raw_bytes = await downloader.fetch(storage_bucket, storage_key)
    except downloader.DownloadError as exc:
        logger.warning("orchestrator.download_failed", document_id=str(document_id), error=str(exc))
        await _fail(session_factory, document_id, content_version, error_code="DOWNLOAD_ERROR", error_message=str(exc))
        return

    try:
        with metrics.ingestion_stage_duration_seconds.labels(stage="parse").time():
            parsed = parser_module.parse(raw_bytes, filename)
    except parser_module.ParserError as exc:
        logger.warning("orchestrator.parse_failed", document_id=str(document_id), error=str(exc))
        await _fail(session_factory, document_id, content_version, error_code="PARSER_ERROR", error_message=str(exc))
        return

    with metrics.ingestion_stage_duration_seconds.labels(stage="chunk").time():
        chunks = chunker.chunk(parsed)
        chunk_ids = [
            chunk_identity.derive_chunk_id(str(document_id), content_version, c.chunk_index) for c in chunks
        ]

    if not chunks:
        # Parsing succeeded but produced no text (e.g. a scanned/image-only PDF, or an empty file).
        # Indexing zero chunks must NOT end in READY: the file would look usable while no query
        # can ever match it. Fail it visibly with an actionable message instead.
        logger.warning("orchestrator.no_extractable_text", document_id=str(document_id), content_version=content_version)
        await _fail(
            session_factory,
            document_id,
            content_version,
            error_code="NO_EXTRACTABLE_TEXT",
            error_message="No text could be extracted from this document (it may be a scanned or image-only file).",
        )
        return

    try:
        with metrics.ingestion_stage_duration_seconds.labels(stage="embed").time():
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

    with metrics.ingestion_stage_duration_seconds.labels(stage="index").time():
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
            logger.info(
                "orchestrator.indexed",
                document_id=str(document_id),
                content_version=content_version,
                chunk_count=len(records),
            )
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
        settled_status = job.status
    metrics.ingestion_jobs_total.labels(status=settled_status).inc()


async def _recover(
    session_factory: async_sessionmaker,
    document_id: UUID,
    content_version: int,
    embedding_client: EmbeddingClient,
) -> None:
    """LLD §6.3 — reconstructs the indexing payload from the original MinIO
    object, since chunks/embeddings do not survive between the original
    attempt and a later retry (no durable store for them in V1). Re-writes
    ONLY the store that previously failed — never re-touches the one that
    already succeeded.

    Always re-embeds, regardless of which store failed. OpenSearch's
    content/section/filename fields come from the same chunk objects the
    embedder consumes, and V1 has no durable store for chunks independent
    of their embeddings — recomputing the chunks without the embeddings
    would mean two slightly different code paths for what's supposed to be
    one deterministic pipeline. Re-embedding a chunk set that's discarded
    for the Milvus side (when only Milvus needs redoing) is wasted work,
    but cheap and bounded, not a correctness risk (LLD §6.3's own
    reasoning for why this isn't worth forking the pipeline over in V1).
    """
    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, content_version)
        if job is None or job.status != IngestionStatus.PARTIALLY_INDEXED.value:
            return
        storage_bucket, storage_key, filename = job.storage_bucket, job.storage_key, job.filename
        owner_id, conversation_id = job.owner_id, job.conversation_id
        milvus_status, opensearch_status = job.milvus_status, job.opensearch_status

    if opensearch_status == StageStatus.FAILED.value and milvus_status == StageStatus.COMPLETE.value:
        target = IndexTarget.OPENSEARCH
    elif milvus_status == StageStatus.FAILED.value and opensearch_status == StageStatus.COMPLETE.value:
        target = IndexTarget.MILVUS
    else:
        logger.warning("orchestrator.recovery_skipped_unexpected_state", document_id=str(document_id))
        return

    try:
        raw_bytes = await downloader.fetch(storage_bucket, storage_key)
        parsed = parser_module.parse(raw_bytes, filename)
    except (downloader.DownloadError, parser_module.ParserError) as exc:
        # Left PARTIALLY_INDEXED — the reconciliation sweep retries again
        # next interval; no durable failure is recorded for a transient
        # re-download/re-parse error on a job that already succeeded once.
        logger.warning("orchestrator.recovery_download_or_parse_failed", document_id=str(document_id), error=str(exc))
        return

    chunks = chunker.chunk(parsed)
    chunk_ids = [chunk_identity.derive_chunk_id(str(document_id), content_version, c.chunk_index) for c in chunks]

    try:
        vectors = await embed_batch(embedding_client, chunks)
    except Exception as exc:
        logger.warning("orchestrator.recovery_embedding_failed", document_id=str(document_id), error=str(exc))
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

    index_result = await index_write(records, target)

    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, content_version)
        if target == IndexTarget.OPENSEARCH:
            job.opensearch_status = index_result.opensearch_status
        else:
            job.milvus_status = index_result.milvus_status
        job.chunk_count = len(records)
        db.add(job)
        await IngestionJobRepository.recompute_status(db, job)

        if job.status == IngestionStatus.READY.value:
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
            job.error_message = "recovery attempt: both Milvus and OpenSearch writes failed"
            db.add(job)
            await OutboxRepository.insert(
                db,
                event_id=uuid.uuid4(),
                topic=settings.KAFKA_TOPIC_PROCESSING_FAILED,
                payload={
                    "event_id": str(uuid.uuid4()),
                    "document_id": str(document_id),
                    "error_code": "INDEXING_ERROR",
                    "error_message": job.error_message,
                    "timestamp": _now_iso(),
                },
            )
        # Still PARTIALLY_INDEXED (the target write failed again) — no
        # outbox row, the sweep retries again next interval.

        await db.commit()
        settled_status = job.status
    metrics.ingestion_jobs_total.labels(status=settled_status).inc()


async def run(
    session_factory: async_sessionmaker,
    document_id: UUID,
    content_version: int,
    embedding_client: EmbeddingClient,
) -> None:
    """See `_run`. Wrapper only binds `document_id` into structlog
    contextvars so every log line from one ingestion job carries it (LLD
    §10), and clears it afterward — the pool worker task runs many jobs."""
    structlog.contextvars.bind_contextvars(document_id=str(document_id))
    try:
        await _run(session_factory, document_id, content_version, embedding_client)
    finally:
        structlog.contextvars.unbind_contextvars("document_id")


async def recover_partially_indexed_job(
    session_factory: async_sessionmaker,
    document_id: UUID,
    content_version: int,
    embedding_client: EmbeddingClient,
) -> None:
    """See `_recover`; same contextvar binding as `run`."""
    structlog.contextvars.bind_contextvars(document_id=str(document_id))
    try:
        await _recover(session_factory, document_id, content_version, embedding_client)
    finally:
        structlog.contextvars.unbind_contextvars("document_id")
