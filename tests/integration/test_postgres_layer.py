import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import StageStatus
from app.models.ingestion_job import IngestionJob
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.repositories.processed_event_repository import ProcessedEventRepository


async def _create_job(db: AsyncSession, document_id: uuid.UUID, content_version: int = 1) -> IngestionJob:
    return await IngestionJobRepository.upsert_job(
        db,
        document_id=document_id,
        content_version=content_version,
        owner_id=uuid.uuid4(),
        conversation_id=None,
        filename="report.pdf",
        storage_bucket="graphgpt-files",
        storage_key=f"owner/{document_id}/v{content_version}/original.pdf",
    )


class TestTransactionBoundary:
    """LLD §3.2 — job upsert + inbox insert + version-projection advance
    must commit or roll back together, directly targeting the crash window
    where a bare inbox insert could be durably committed while the job
    creation it's supposed to accompany never lands."""

    @pytest.mark.asyncio
    async def test_all_three_writes_commit_together(self, db_session: AsyncSession) -> None:
        document_id = uuid.uuid4()
        event_id = uuid.uuid4()

        await _create_job(db_session, document_id, content_version=1)
        await DocumentCurrentVersionRepository.advance_if_newer(db_session, document_id, 1)
        is_new = await ProcessedEventRepository.mark_processed(db_session, event_id, "file_uploaded_consumer")
        await db_session.commit()

        assert is_new is True
        job = await IngestionJobRepository.get_by_document_and_version(db_session, document_id, 1)
        assert job is not None
        versions = await DocumentCurrentVersionRepository.get_many(db_session, [document_id])
        assert versions[str(document_id)] == 1

    @pytest.mark.asyncio
    async def test_rollback_undoes_all_three_writes_together(self, db_session: AsyncSession) -> None:
        document_id = uuid.uuid4()
        event_id = uuid.uuid4()

        await _create_job(db_session, document_id, content_version=1)
        await DocumentCurrentVersionRepository.advance_if_newer(db_session, document_id, 1)
        await ProcessedEventRepository.mark_processed(db_session, event_id, "file_uploaded_consumer")
        await db_session.rollback()

        job = await IngestionJobRepository.get_by_document_and_version(db_session, document_id, 1)
        assert job is None
        versions = await DocumentCurrentVersionRepository.get_many(db_session, [document_id])
        assert str(document_id) not in versions
        # A rolled-back inbox insert must not be durably recorded either —
        # otherwise a retry would see "already processed" for a job that
        # was never actually created (the exact crash window LLD §3.2 fixes).
        is_new_again = await ProcessedEventRepository.mark_processed(db_session, event_id, "file_uploaded_consumer")
        await db_session.rollback()
        assert is_new_again is True


class TestAdvanceIfNewer:
    """HLD §13.3 — proves the reorder-safety guard before any Kafka code
    exists to exercise it for real (File Service's producer doesn't key by
    document_id, so v3 can arrive before v2)."""

    @pytest.mark.asyncio
    async def test_lower_version_after_higher_is_a_noop(self, db_session: AsyncSession) -> None:
        document_id = uuid.uuid4()

        await DocumentCurrentVersionRepository.advance_if_newer(db_session, document_id, 3)
        await DocumentCurrentVersionRepository.advance_if_newer(db_session, document_id, 2)
        await db_session.commit()

        versions = await DocumentCurrentVersionRepository.get_many(db_session, [document_id])
        assert versions[str(document_id)] == 3

    @pytest.mark.asyncio
    async def test_higher_version_advances(self, db_session: AsyncSession) -> None:
        document_id = uuid.uuid4()

        await DocumentCurrentVersionRepository.advance_if_newer(db_session, document_id, 1)
        await DocumentCurrentVersionRepository.advance_if_newer(db_session, document_id, 2)
        await db_session.commit()

        versions = await DocumentCurrentVersionRepository.get_many(db_session, [document_id])
        assert versions[str(document_id)] == 2


class TestRecomputeStatus:
    """LLD §3.1's closing note — status must be a pure derivation of the
    three per-stage columns, never independently settable into READY."""

    @pytest.mark.asyncio
    async def test_partially_indexed_when_exactly_one_target_failed(self, db_session: AsyncSession) -> None:
        job = await _create_job(db_session, uuid.uuid4())
        job.embedding_status = StageStatus.COMPLETE.value
        job.milvus_status = StageStatus.COMPLETE.value
        job.opensearch_status = StageStatus.FAILED.value

        await IngestionJobRepository.recompute_status(db_session, job)
        await db_session.commit()

        assert job.status == "PARTIALLY_INDEXED"

    @pytest.mark.asyncio
    async def test_ready_only_when_both_targets_complete(self, db_session: AsyncSession) -> None:
        job = await _create_job(db_session, uuid.uuid4())
        job.embedding_status = StageStatus.COMPLETE.value
        job.milvus_status = StageStatus.COMPLETE.value
        job.opensearch_status = StageStatus.PENDING.value

        await IngestionJobRepository.recompute_status(db_session, job)
        assert job.status != "READY"

        job.opensearch_status = StageStatus.COMPLETE.value
        await IngestionJobRepository.recompute_status(db_session, job)
        await db_session.commit()

        assert job.status == "READY"

    @pytest.mark.asyncio
    async def test_failed_when_embedding_failed(self, db_session: AsyncSession) -> None:
        job = await _create_job(db_session, uuid.uuid4())
        job.embedding_status = StageStatus.FAILED.value

        await IngestionJobRepository.recompute_status(db_session, job)
        await db_session.commit()

        assert job.status == "FAILED"
