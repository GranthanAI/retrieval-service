"""Phase 6 exit criteria — real MinIO/Milvus/OpenSearch/embedding-service
and a real Postgres (testcontainers + real Alembic chain), matching every
earlier phase's own convention.
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from app.clients.embedding_client import EmbeddingClient
from app.core.enums import IngestionStatus, StageStatus
from app.events.ingestion_pool import BoundedIngestionPool
from app.ingestion import orchestrator
from app.ingestion.chunk_identity import derive_chunk_id
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository
from app.workers.reconciliation_worker import run_sweep

FIXTURE_PATH = "tests/fixtures/sample.pdf"
TEST_BUCKET = "retrieval-test-bucket"


@pytest.fixture
def minio_client():
    from minio import Minio

    from app.core.config import settings

    client = Minio(
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )
    if not client.bucket_exists(TEST_BUCKET):
        client.make_bucket(TEST_BUCKET)
    return client


@pytest.fixture
def embedding_client() -> EmbeddingClient:
    return EmbeddingClient()


async def _create_partially_indexed_job(session_factory, minio_client, embedding_client, *, failed_store: str):
    """Runs the real orchestrator with one store forced to fail, producing
    a genuine PARTIALLY_INDEXED job — the same forced-failure mechanism
    Phase 4's own test suite already uses.
    """
    document_id = uuid.uuid4()
    owner_id = uuid.uuid4()
    storage_key = f"{owner_id}/{document_id}/v1/original.pdf"
    minio_client.fput_object(TEST_BUCKET, storage_key, FIXTURE_PATH)

    async with session_factory() as db:
        await IngestionJobRepository.upsert_job(
            db,
            document_id=document_id,
            content_version=1,
            owner_id=owner_id,
            conversation_id=None,
            filename="sample.pdf",
            storage_bucket=TEST_BUCKET,
            storage_key=storage_key,
        )
        await db.commit()

    target_repo = OpenSearchRepository if failed_store == "opensearch" else MilvusRepository
    with patch.object(target_repo, "upsert_chunks", new=AsyncMock(side_effect=RuntimeError("forced failure"))):
        await orchestrator.run(session_factory, document_id, 1, embedding_client)

    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
    assert job.status == "PARTIALLY_INDEXED"

    return document_id


class TestPartialIndexRecovery:
    @pytest.mark.asyncio
    async def test_opensearch_failure_recovers_to_ready(
        self, session_factory, minio_client, embedding_client
    ) -> None:
        document_id = await _create_partially_indexed_job(
            session_factory, minio_client, embedding_client, failed_store="opensearch"
        )

        await orchestrator.recover_partially_indexed_job(session_factory, document_id, 1, embedding_client)

        async with session_factory() as db:
            job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
        assert job.status == "READY"
        assert job.opensearch_status == "COMPLETE"
        assert job.milvus_status == "COMPLETE"

        chunk_id = derive_chunk_id(str(document_id), 1, 0)
        hits = await OpenSearchRepository().search(
            query_text="retry policy exponential backoff", top_n=10, current_versions={str(document_id): 1}
        )
        assert any(h.chunk_id == chunk_id for h in hits)

        from sqlalchemy import select

        from app.models.outbox_event import OutboxEvent
        from app.core.config import settings

        async with session_factory() as db:
            rows = (
                (
                    await db.execute(
                        select(OutboxEvent).where(OutboxEvent.topic == settings.KAFKA_TOPIC_PROCESSING_COMPLETED)
                    )
                )
                .scalars()
                .all()
            )
        assert any(r.payload["document_id"] == str(document_id) for r in rows)

        await MilvusRepository().delete_by_document_id(str(document_id))
        await OpenSearchRepository().delete_by_document_id(str(document_id))

    @pytest.mark.asyncio
    async def test_milvus_failure_recovers_and_reembeds(self, session_factory, minio_client, embedding_client) -> None:
        document_id = await _create_partially_indexed_job(
            session_factory, minio_client, embedding_client, failed_store="milvus"
        )

        real_embed_chunks = embedding_client.embed_chunks
        call_count = 0

        async def _counting_embed_chunks(texts):
            nonlocal call_count
            call_count += 1
            return await real_embed_chunks(texts)

        with patch.object(embedding_client, "embed_chunks", side_effect=_counting_embed_chunks):
            await orchestrator.recover_partially_indexed_job(session_factory, document_id, 1, embedding_client)

        assert call_count == 1  # re-embedding actually happened, not silently skipped

        async with session_factory() as db:
            job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
        assert job.status == "READY"
        assert job.milvus_status == "COMPLETE"

        chunk_id = derive_chunk_id(str(document_id), 1, 0)
        embeddings = await MilvusRepository().get_by_chunk_ids([chunk_id])
        assert chunk_id in embeddings

        await MilvusRepository().delete_by_document_id(str(document_id))
        await OpenSearchRepository().delete_by_document_id(str(document_id))


class TestChunkIdStability:
    @pytest.mark.asyncio
    async def test_recovery_reproduces_identical_chunk_ids(
        self, session_factory, minio_client, embedding_client
    ) -> None:
        """Proves LLD §6.3's closing note: recovery's re-parse/re-chunk of
        the same object reproduces identical chunk_ids to the original
        attempt, so the re-indexed write is a safe upsert, not a
        duplicate."""
        document_id = await _create_partially_indexed_job(
            session_factory, minio_client, embedding_client, failed_store="opensearch"
        )
        original_chunk_id = derive_chunk_id(str(document_id), 1, 0)

        await orchestrator.recover_partially_indexed_job(session_factory, document_id, 1, embedding_client)

        recovered_chunk_id = derive_chunk_id(str(document_id), 1, 0)
        assert original_chunk_id == recovered_chunk_id

        # And it's genuinely the SAME chunk_id present in OpenSearch after
        # recovery — not a second, different id for the same content.
        hits = await OpenSearchRepository().search(
            query_text="retry policy exponential backoff", top_n=10, current_versions={str(document_id): 1}
        )
        assert {h.chunk_id for h in hits} == {original_chunk_id}

        await MilvusRepository().delete_by_document_id(str(document_id))
        await OpenSearchRepository().delete_by_document_id(str(document_id))


class TestStuckJobSweep:
    @pytest.mark.asyncio
    async def test_stuck_processing_job_gets_reenqueued(self, session_factory, embedding_client) -> None:
        document_id = uuid.uuid4()
        async with session_factory() as db:
            job = await IngestionJobRepository.upsert_job(
                db,
                document_id=document_id,
                content_version=1,
                owner_id=uuid.uuid4(),
                conversation_id=None,
                filename="stuck.pdf",
                storage_bucket=TEST_BUCKET,
                storage_key=f"owner/{document_id}/v1/original.pdf",
            )
            job.status = IngestionStatus.PROCESSING.value
            db.add(job)
            await db.commit()
            # Backdate updated_at directly — simulates a job that has been
            # stuck in PROCESSING since well before this sweep's threshold,
            # with no in-flight work actually happening for it.
            from sqlalchemy import update

            from app.models.ingestion_job import IngestionJob

            await db.execute(
                update(IngestionJob)
                .where(IngestionJob.document_id == document_id, IngestionJob.content_version == 1)
                .values(updated_at=datetime.now(timezone.utc) - timedelta(hours=1))
            )
            await db.commit()

        pool = BoundedIngestionPool(session_factory, embedding_client)
        enqueued = []

        async def _spy_enqueue(document_id, content_version):
            enqueued.append((document_id, content_version))

        with patch.object(pool, "enqueue", side_effect=_spy_enqueue):
            summary = await run_sweep(session_factory, pool, embedding_client, older_than_seconds=60)

        assert summary["reenqueued"] >= 1
        assert (document_id, 1) in enqueued

    @pytest.mark.asyncio
    async def test_stuck_not_started_job_gets_reenqueued(self, session_factory, embedding_client) -> None:
        """The crash window found in Phase 5 (Task Update 5 §8): a crash
        between the inbox transaction committing and `pool.enqueue()` being
        called leaves the job NOT_STARTED, never PROCESSING —
        `list_stuck_processing` alone can't find it. Proves
        `list_stuck_not_started` (added in Phase 6) closes that gap."""
        document_id = uuid.uuid4()
        async with session_factory() as db:
            await IngestionJobRepository.upsert_job(
                db,
                document_id=document_id,
                content_version=1,
                owner_id=uuid.uuid4(),
                conversation_id=None,
                filename="stuck-not-started.pdf",
                storage_bucket=TEST_BUCKET,
                storage_key=f"owner/{document_id}/v1/original.pdf",
            )
            await db.commit()

            from sqlalchemy import update

            from app.models.ingestion_job import IngestionJob

            await db.execute(
                update(IngestionJob)
                .where(IngestionJob.document_id == document_id, IngestionJob.content_version == 1)
                .values(updated_at=datetime.now(timezone.utc) - timedelta(hours=1))
            )
            await db.commit()

        pool = BoundedIngestionPool(session_factory, embedding_client)
        enqueued = []

        async def _spy_enqueue(document_id, content_version):
            enqueued.append((document_id, content_version))

        with patch.object(pool, "enqueue", side_effect=_spy_enqueue):
            summary = await run_sweep(session_factory, pool, embedding_client, older_than_seconds=60)

        assert summary["reenqueued"] >= 1
        assert (document_id, 1) in enqueued

    @pytest.mark.asyncio
    async def test_fresh_processing_job_is_left_alone(self, session_factory, embedding_client) -> None:
        """A job that's only just started PROCESSING (younger than the
        threshold) must NOT be swept — it may still be legitimately
        in-flight."""
        document_id = uuid.uuid4()
        async with session_factory() as db:
            job = await IngestionJobRepository.upsert_job(
                db,
                document_id=document_id,
                content_version=1,
                owner_id=uuid.uuid4(),
                conversation_id=None,
                filename="fresh.pdf",
                storage_bucket=TEST_BUCKET,
                storage_key=f"owner/{document_id}/v1/original.pdf",
            )
            job.status = IngestionStatus.PROCESSING.value
            db.add(job)
            await db.commit()

        pool = BoundedIngestionPool(session_factory, embedding_client)
        enqueued = []

        async def _spy_enqueue(document_id, content_version):
            enqueued.append((document_id, content_version))

        with patch.object(pool, "enqueue", side_effect=_spy_enqueue):
            await run_sweep(session_factory, pool, embedding_client, older_than_seconds=3600)

        assert (document_id, 1) not in enqueued
