import asyncio
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository


async def purge(session_factory: async_sessionmaker, document_id: UUID) -> None:
    """HLD §21 — purges `document_id` from Milvus + OpenSearch concurrently
    (both idempotent — a redelivered `file.deleted` for an already-purged
    `document_id` is a no-op on both sides) and marks every
    `ingestion_jobs` row for the document `DELETED` (all `content_version`s,
    since the event carries none, LLD §6.4).
    """
    await asyncio.gather(
        MilvusRepository().delete_by_document_id(str(document_id)),
        OpenSearchRepository().delete_by_document_id(str(document_id)),
    )
    async with session_factory() as db:
        await IngestionJobRepository.mark_all_versions_deleted(db, document_id)
        await db.commit()
