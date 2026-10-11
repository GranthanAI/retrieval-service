"""Phase 4 exit criteria — `orchestrator.run(...)` invoked directly (no
Kafka), against real MinIO/Milvus/OpenSearch/embedding-service and a real
Postgres (testcontainers, matching Phase 1/2's own convention), with a
small real PDF fixture (tests/fixtures/sample.pdf).
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
import structlog
from minio import Minio
from prometheus_client import REGISTRY
from sqlalchemy import select

from app.clients.embedding_client import EmbeddingClient
from app.core.config import settings
from app.ingestion import orchestrator
from app.ingestion.chunk_identity import derive_chunk_id, parse_content_version
from app.ingestion.indexer import IndexResult
from app.models.outbox_event import OutboxEvent
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository

def _sample(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


STAGES = ("download", "parse", "chunk", "embed", "index")

FIXTURE_PATH = "tests/fixtures/sample.pdf"
TEST_BUCKET = "retrieval-test-bucket"


@pytest.fixture(scope="module")
def minio_client() -> Minio:
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


async def _create_job(db, *, document_id, storage_key, content_version=1):
    return await IngestionJobRepository.upsert_job(
        db,
        document_id=document_id,
        content_version=content_version,
        owner_id=uuid.uuid4(),
        conversation_id=None,
        filename="sample.pdf",
        storage_bucket=TEST_BUCKET,
        storage_key=storage_key,
    )


class TestOrchestratorHappyPath:
    @pytest.mark.asyncio
    async def test_run_produces_ready_job_and_queryable_chunks(
        self, db_session, migrated_database_url, minio_client, embedding_client
    ) -> None:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        document_id = uuid.uuid4()
        owner_id = uuid.uuid4()
        storage_key = f"{owner_id}/{document_id}/v1/original.pdf"
        minio_client.fput_object(TEST_BUCKET, storage_key, FIXTURE_PATH)

        engine = create_async_engine(migrated_database_url)
        session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

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

        ready_before = _sample("retrieval_ingestion_jobs_total", {"status": "READY"})
        stage_before = {
            stage: _sample("retrieval_ingestion_stage_duration_seconds_count", {"stage": stage}) for stage in STAGES
        }

        await orchestrator.run(session_factory, document_id, 1, embedding_client)

        assert _sample("retrieval_ingestion_jobs_total", {"status": "READY"}) == ready_before + 1
        for stage in STAGES:
            after = _sample("retrieval_ingestion_stage_duration_seconds_count", {"stage": stage})
            assert after == stage_before[stage] + 1, stage

        async with session_factory() as db:
            job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
            assert job.status == "READY"
            assert job.chunk_count is not None and job.chunk_count > 0

            completed_rows = (
                (
                    await db.execute(
                        select(OutboxEvent).where(OutboxEvent.topic == settings.KAFKA_TOPIC_PROCESSING_COMPLETED)
                    )
                )
                .scalars()
                .all()
            )
            matching = [r for r in completed_rows if r.payload["document_id"] == str(document_id)]
            assert len(matching) == 1
            assert matching[0].payload["chunk_count"] == job.chunk_count

        chunk_id_0 = derive_chunk_id(str(document_id), 1, 0)
        milvus_hits = await MilvusRepository().get_by_chunk_ids([chunk_id_0])
        assert chunk_id_0 in milvus_hits

        opensearch_hits = await OpenSearchRepository().search(
            query_text="retry policy exponential backoff", top_n=10, current_versions={str(document_id): 1}
        )
        assert any(h.chunk_id == chunk_id_0 for h in opensearch_hits)

        await MilvusRepository().delete_by_document_id(str(document_id))
        await OpenSearchRepository().delete_by_document_id(str(document_id))
        await engine.dispose()


class TestOrchestratorForcedFailures:
    @pytest.mark.asyncio
    async def test_download_failure_marks_job_failed(self, migrated_database_url, embedding_client) -> None:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        document_id = uuid.uuid4()
        engine = create_async_engine(migrated_database_url)
        session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

        async with session_factory() as db:
            await IngestionJobRepository.upsert_job(
                db,
                document_id=document_id,
                content_version=1,
                owner_id=uuid.uuid4(),
                conversation_id=None,
                filename="missing.pdf",
                storage_bucket=TEST_BUCKET,
                storage_key="nonexistent/key/v1/original.pdf",
            )
            await db.commit()

        await orchestrator.run(session_factory, document_id, 1, embedding_client)

        async with session_factory() as db:
            job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
            assert job.status == "FAILED"
            assert job.error_code == "DOWNLOAD_ERROR"

            failed_rows = (
                (await db.execute(select(OutboxEvent).where(OutboxEvent.topic == settings.KAFKA_TOPIC_PROCESSING_FAILED)))
                .scalars()
                .all()
            )
            matching = [r for r in failed_rows if r.payload["document_id"] == str(document_id)]
            assert len(matching) == 1

            completed_rows = (
                (
                    await db.execute(
                        select(OutboxEvent).where(OutboxEvent.topic == settings.KAFKA_TOPIC_PROCESSING_COMPLETED)
                    )
                )
                .scalars()
                .all()
            )
            assert not any(r.payload["document_id"] == str(document_id) for r in completed_rows)

        await engine.dispose()

    @pytest.mark.asyncio
    async def test_one_store_failure_yields_partially_indexed_no_completed_event(
        self, migrated_database_url, minio_client, embedding_client
    ) -> None:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        document_id = uuid.uuid4()
        owner_id = uuid.uuid4()
        storage_key = f"{owner_id}/{document_id}/v1/original.pdf"
        minio_client.fput_object(TEST_BUCKET, storage_key, FIXTURE_PATH)

        engine = create_async_engine(migrated_database_url)
        session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

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

        with patch.object(OpenSearchRepository, "upsert_chunks", new=AsyncMock(side_effect=RuntimeError("boom"))):
            await orchestrator.run(session_factory, document_id, 1, embedding_client)

        async with session_factory() as db:
            job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
            assert job.status == "PARTIALLY_INDEXED"
            assert job.milvus_status == "COMPLETE"
            assert job.opensearch_status == "FAILED"

            completed_rows = (
                (
                    await db.execute(
                        select(OutboxEvent).where(OutboxEvent.topic == settings.KAFKA_TOPIC_PROCESSING_COMPLETED)
                    )
                )
                .scalars()
                .all()
            )
            assert not any(r.payload["document_id"] == str(document_id) for r in completed_rows)

        await MilvusRepository().delete_by_document_id(str(document_id))
        await engine.dispose()


class TestIngestionObservability:
    @pytest.mark.asyncio
    async def test_failed_job_is_counted_and_document_id_is_bound_during_the_run(
        self, migrated_database_url, embedding_client, monkeypatch
    ) -> None:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from app.ingestion import downloader

        document_id = uuid.uuid4()
        engine = create_async_engine(migrated_database_url)
        session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

        async with session_factory() as db:
            await IngestionJobRepository.upsert_job(
                db,
                document_id=document_id,
                content_version=1,
                owner_id=uuid.uuid4(),
                conversation_id=None,
                filename="x.pdf",
                storage_bucket=TEST_BUCKET,
                storage_key="o/d/v1/original.pdf",
            )
            await db.commit()

        seen: dict = {}

        async def _failing_fetch(bucket, key):
            seen.update(structlog.contextvars.get_contextvars())
            raise downloader.DownloadError("boom")

        monkeypatch.setattr(downloader, "fetch", _failing_fetch)
        failed_before = _sample("retrieval_ingestion_jobs_total", {"status": "FAILED"})

        await orchestrator.run(session_factory, document_id, 1, embedding_client)

        assert _sample("retrieval_ingestion_jobs_total", {"status": "FAILED"}) == failed_before + 1
        assert seen["document_id"] == str(document_id)  # bound for the whole job...
        assert "document_id" not in structlog.contextvars.get_contextvars()  # ...and cleaned up after
        await engine.dispose()


class TestChunkIdIdempotency:
    def test_derive_chunk_id_is_deterministic(self) -> None:
        document_id = str(uuid.uuid4())
        first = derive_chunk_id(document_id, 1, 3)
        second = derive_chunk_id(document_id, 1, 3)
        assert first == second

    def test_parse_content_version_strict(self) -> None:
        assert parse_content_version("owner/doc/v3/original.pdf") == 3
        with pytest.raises(Exception):
            parse_content_version("owner/doc/original.pdf")
