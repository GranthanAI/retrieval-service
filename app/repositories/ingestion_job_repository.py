from datetime import datetime
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import IngestionStatus, StageStatus
from app.models.ingestion_job import IngestionJob


class IngestionJobRepository:
    @staticmethod
    async def upsert_job(
        db: AsyncSession,
        *,
        document_id: UUID,
        content_version: int,
        owner_id: UUID,
        conversation_id: UUID | None,
        filename: str,
        storage_bucket: str,
        storage_key: str,
    ) -> IngestionJob:
        """LLD §6.1 — `ON CONFLICT (document_id, content_version) DO UPDATE`,
        called inside the same transaction as the inbox insert and the
        version-projection advance (LLD §3.2). A conflict (a redelivered or
        corrected file.uploaded for the same version) updates only the
        denormalized metadata fields — never status/retry bookkeeping, which
        belongs to the ingestion pipeline itself, not to re-delivery.
        """
        stmt = (
            pg_insert(IngestionJob)
            .values(
                document_id=document_id,
                content_version=content_version,
                owner_id=owner_id,
                conversation_id=conversation_id,
                filename=filename,
                storage_bucket=storage_bucket,
                storage_key=storage_key,
            )
            .on_conflict_do_update(
                index_elements=["document_id", "content_version"],
                set_={
                    "owner_id": owner_id,
                    "conversation_id": conversation_id,
                    "filename": filename,
                    "storage_bucket": storage_bucket,
                    "storage_key": storage_key,
                },
            )
            .returning(IngestionJob)
        )
        result = await db.execute(stmt)
        await db.flush()
        return result.scalar_one()

    @staticmethod
    async def get_by_document_and_version(
        db: AsyncSession, document_id: UUID, content_version: int
    ) -> IngestionJob | None:
        result = await db.execute(
            select(IngestionJob).where(
                IngestionJob.document_id == document_id,
                IngestionJob.content_version == content_version,
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def recompute_status(db: AsyncSession, job: IngestionJob) -> IngestionJob:
        """LLD §3.1's closing note — `status` is always derived from the
        three per-stage columns, never set independently, so it can never
        drift out of sync with them (HLD §19/§20):

        - embedding FAILED                              -> FAILED (no partial
          index writes are possible without embeddings, LLD §6.2)
        - milvus and opensearch both COMPLETE            -> READY
        - milvus and opensearch both FAILED              -> FAILED
        - exactly one of milvus/opensearch FAILED         -> PARTIALLY_INDEXED
        - otherwise (still PENDING on one or both)        -> PROCESSING
        """
        milvus = job.milvus_status
        opensearch = job.opensearch_status
        embedding = job.embedding_status

        if embedding == StageStatus.FAILED.value:
            job.status = IngestionStatus.FAILED.value
        elif milvus == StageStatus.COMPLETE.value and opensearch == StageStatus.COMPLETE.value:
            job.status = IngestionStatus.READY.value
        elif milvus == StageStatus.FAILED.value and opensearch == StageStatus.FAILED.value:
            job.status = IngestionStatus.FAILED.value
        elif milvus == StageStatus.FAILED.value or opensearch == StageStatus.FAILED.value:
            job.status = IngestionStatus.PARTIALLY_INDEXED.value
        else:
            job.status = IngestionStatus.PROCESSING.value

        db.add(job)
        await db.flush()
        return job

    @staticmethod
    async def list_partially_indexed(db: AsyncSession, older_than: datetime) -> list[IngestionJob]:
        """LLD §6.3 / Phase 6's reconciliation sweep input."""
        result = await db.execute(
            select(IngestionJob).where(
                IngestionJob.status == IngestionStatus.PARTIALLY_INDEXED.value,
                IngestionJob.updated_at < older_than,
            )
        )
        return list(result.scalars().all())

    @staticmethod
    async def mark_all_versions_deleted(db: AsyncSession, document_id: UUID) -> None:
        """LLD §6.4 — `status -> DELETED`, all `content_version`s, since
        `file.deleted` carries no version filter (HLD §8.2/§21)."""
        result = await db.execute(select(IngestionJob).where(IngestionJob.document_id == document_id))
        for job in result.scalars().all():
            job.status = IngestionStatus.DELETED.value
            db.add(job)
        await db.flush()

    @staticmethod
    async def list_stuck_processing(db: AsyncSession, older_than: datetime) -> list[IngestionJob]:
        """LLD §3.2's closing paragraph — the safety net for a crash *after*
        ack but *during* orchestrator.run(), with no redelivered event to
        trigger recovery (e.g. because the offset was already acked)."""
        result = await db.execute(
            select(IngestionJob).where(
                IngestionJob.status == IngestionStatus.PROCESSING.value,
                IngestionJob.updated_at < older_than,
            )
        )
        return list(result.scalars().all())

    @staticmethod
    async def list_stuck_not_started(db: AsyncSession, older_than: datetime) -> list[IngestionJob]:
        """A gap identified during Phase 5 testing, not explicitly named in
        LLD §3.2's own text: a crash between the inbox transaction
        committing (job created as NOT_STARTED, Kafka offset already
        acked) and the job actually being handed to the bounded ingestion
        pool leaves it NOT_STARTED forever — `orchestrator.run()` never ran,
        so it never reached PROCESSING, and `list_stuck_processing` alone
        cannot find it. This is the equivalent safety net for that window.
        """
        result = await db.execute(
            select(IngestionJob).where(
                IngestionJob.status == IngestionStatus.NOT_STARTED.value,
                IngestionJob.updated_at < older_than,
            )
        )
        return list(result.scalars().all())


    @staticmethod
    async def statuses_for(db: AsyncSession, versions: dict[str, int]) -> dict[str, tuple[str, int | None]]:
        """Indexing status of exactly the CURRENT version of each document: `{document_id:
        (status, chunk_count)}`. A document with no ingestion job for its current version is
        absent. This is what lets a query tell "indexed" from "registered but still indexing"
        (the current version is registered when file.uploaded is consumed, before indexing)."""
        if not versions:
            return {}
        pairs = [(UUID(d), v) for d, v in versions.items()]
        result = await db.execute(
            select(IngestionJob.document_id, IngestionJob.status, IngestionJob.chunk_count).where(
                tuple_(IngestionJob.document_id, IngestionJob.content_version).in_(pairs)
            )
        )
        return {str(doc): (status, chunks) for doc, status, chunks in result.all()}
