import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.core.config import settings
from app.core.logging import get_logger
from app.events.ingestion_pool import BoundedIngestionPool
from app.ingestion.orchestrator import recover_partially_indexed_job
from app.repositories.ingestion_job_repository import IngestionJobRepository

logger = get_logger(__name__)


async def run_sweep(
    session_factory: async_sessionmaker,
    pool: BoundedIngestionPool,
    embedding_client: EmbeddingClient,
    *,
    older_than_seconds: float,
) -> dict[str, int]:
    """One sweep, separated from the periodic loop for testability (matches
    file-service's own `reconciliation_worker.run_sweep` convention) — a
    test calls this directly instead of waiting on a real interval.

    Three independent safety nets, all looking `older_than` a threshold so
    an in-flight job isn't disturbed mid-attempt:
    - `PARTIALLY_INDEXED` jobs -> LLD §6.3's recovery (re-download/parse/
      chunk/embed, re-write only the store that failed).
    - `PROCESSING` jobs stuck with no in-flight work (LLD §3.2's own
      example: a crash after ack but during `orchestrator.run()`, with no
      redelivered event to trigger recovery) -> re-enqueued.
    - `NOT_STARTED` jobs stuck with no in-flight work — a second crash
      window found during Phase 5 testing, not explicitly named in LLD
      §3.2's text: a crash between the inbox transaction committing and
      the job actually being handed to the bounded pool never reaches
      PROCESSING at all, so `list_stuck_processing` alone can't find it.
      Same recovery action: re-enqueue.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)

    async with session_factory() as db:
        partially_indexed_jobs = await IngestionJobRepository.list_partially_indexed(db, cutoff)
        stuck_processing_jobs = await IngestionJobRepository.list_stuck_processing(db, cutoff)
        stuck_not_started_jobs = await IngestionJobRepository.list_stuck_not_started(db, cutoff)

    for job in partially_indexed_jobs:
        await recover_partially_indexed_job(session_factory, job.document_id, job.content_version, embedding_client)

    stuck_jobs = stuck_processing_jobs + stuck_not_started_jobs
    for job in stuck_jobs:
        await pool.enqueue(job.document_id, job.content_version)

    return {"recovered": len(partially_indexed_jobs), "reenqueued": len(stuck_jobs)}


async def run_reconciliation_worker(
    session_factory: async_sessionmaker,
    pool: BoundedIngestionPool,
    embedding_client: EmbeddingClient,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            summary = await run_sweep(
                session_factory,
                pool,
                embedding_client,
                older_than_seconds=settings.RECONCILIATION_INTERVAL_SECONDS,
            )
            if summary["recovered"] or summary["reenqueued"]:
                logger.info("reconciliation_worker.swept", **summary)
        except Exception:
            logger.exception("reconciliation_worker.sweep_failed")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.RECONCILIATION_INTERVAL_SECONDS)
        except TimeoutError:
            pass
