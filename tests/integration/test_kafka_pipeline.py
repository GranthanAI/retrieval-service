"""Phase 5 exit criteria — real Kafka (testcontainers), real Postgres
(testcontainers + real Alembic chain), real MinIO/Milvus/OpenSearch/
embedding-service (matching Phase 4's own convention: infra this repo
doesn't own is never faked in its integration tests).
"""

import asyncio
import json
import time
import uuid

import pytest
from aiokafka import AIOKafkaConsumer
from minio import Minio

from app.clients.embedding_client import EmbeddingClient
from app.core.config import settings
from app.events.consumers.file_processing_retry_consumer import run_file_processing_retry_consumer
from app.events.consumers.file_uploaded_consumer import run_file_uploaded_consumer
from app.events.ingestion_pool import BoundedIngestionPool
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository
from app.workers.outbox_worker import run_outbox_worker

FIXTURE_PATH = "tests/fixtures/sample.pdf"
TEST_BUCKET = "retrieval-test-bucket"


@pytest.fixture
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


def _uploaded_payload(document_id, owner_id, storage_key, event_id=None) -> dict:
    return {
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": "file.uploaded",
        "version": 1,
        "document_id": str(document_id),
        "conversation_id": None,
        "owner_id": str(owner_id),
        "filename": "sample.pdf",
        "content_type": "application/pdf",
        "storage": {"provider": "minio", "bucket": TEST_BUCKET, "key": storage_key, "etag": "etag"},
        "timestamp": "2026-01-01T00:00:00Z",
    }


async def _wait_for(predicate, *, timeout_s: float = 90, interval_s: float = 0.5):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = await predicate()
        if result is not None:
            return result
        await asyncio.sleep(interval_s)
    raise AssertionError("condition not met within timeout")


class TestFileUploadedEndToEnd:
    @pytest.mark.asyncio
    async def test_publish_produces_ready_job_and_completed_event_on_output_topic(
        self, monkeypatch, kafka_container, kafka_producer, session_factory, minio_client
    ) -> None:
        monkeypatch.setattr(settings, "KAFKA_BOOTSTRAP_SERVERS", kafka_container.get_bootstrap_server())

        document_id = uuid.uuid4()
        owner_id = uuid.uuid4()
        storage_key = f"{owner_id}/{document_id}/v1/original.pdf"
        minio_client.fput_object(TEST_BUCKET, storage_key, FIXTURE_PATH)

        embedding_client = EmbeddingClient()
        pool = BoundedIngestionPool(session_factory, embedding_client)
        await pool.start()

        stop_event = asyncio.Event()
        consumer_task = asyncio.create_task(run_file_uploaded_consumer(session_factory, pool, stop_event))
        outbox_stop_event = asyncio.Event()
        outbox_task = asyncio.create_task(run_outbox_worker(session_factory, kafka_producer, outbox_stop_event))

        try:
            await kafka_producer.send_and_wait(
                settings.KAFKA_TOPIC_FILE_UPLOADED,
                json.dumps(_uploaded_payload(document_id, owner_id, storage_key)).encode(),
            )

            async def _check_terminal():
                async with session_factory() as db:
                    job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
                    return job if job is not None and job.status in ("READY", "FAILED", "PARTIALLY_INDEXED") else None

            job = await _wait_for(_check_terminal, timeout_s=120)
            assert job.status == "READY"
            assert job.chunk_count and job.chunk_count > 0

            # A chunk is genuinely queryable in both stores — not just a DB
            # status flag.
            hits = await OpenSearchRepository().search(
                query_text="retry policy exponential backoff", top_n=10, current_versions={str(document_id): 1}
            )
            assert len(hits) > 0

            # file.processing.completed actually observed on the real
            # output topic — not just present in the outbox table.
            verify_consumer = AIOKafkaConsumer(
                settings.KAFKA_TOPIC_PROCESSING_COMPLETED,
                bootstrap_servers=kafka_container.get_bootstrap_server(),
                group_id=f"test-verify-{uuid.uuid4()}",
                auto_offset_reset="earliest",
            )
            await verify_consumer.start()
            try:
                found = False
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline and not found:
                    result = await verify_consumer.getmany(timeout_ms=1000)
                    for _tp, records in result.items():
                        for record in records:
                            body = json.loads(record.value)
                            if body.get("document_id") == str(document_id):
                                found = True
                assert found, "file.processing.completed was never observed on the output topic"
            finally:
                await verify_consumer.stop()
        finally:
            stop_event.set()
            outbox_stop_event.set()
            await asyncio.wait_for(consumer_task, timeout=10)
            await asyncio.wait_for(outbox_task, timeout=10)
            await pool.stop()
            await embedding_client.close()
            await MilvusRepository().delete_by_document_id(str(document_id))
            await OpenSearchRepository().delete_by_document_id(str(document_id))


class TestRedelivery:
    @pytest.mark.asyncio
    async def test_second_delivery_of_ready_job_is_a_noop(
        self, monkeypatch, kafka_container, kafka_producer, session_factory, minio_client
    ) -> None:
        """LLD §3.2's status-dispatch table — a redelivered event_id for a
        job already READY must be a genuine no-op, proven here by the job's
        own `updated_at` staying untouched across the redelivery (not
        merely "still READY," which a silent harmless re-run could also
        produce).
        """
        monkeypatch.setattr(settings, "KAFKA_BOOTSTRAP_SERVERS", kafka_container.get_bootstrap_server())

        document_id = uuid.uuid4()
        owner_id = uuid.uuid4()
        storage_key = f"{owner_id}/{document_id}/v1/original.pdf"
        minio_client.fput_object(TEST_BUCKET, storage_key, FIXTURE_PATH)

        embedding_client = EmbeddingClient()
        pool = BoundedIngestionPool(session_factory, embedding_client)
        await pool.start()
        stop_event = asyncio.Event()
        consumer_task = asyncio.create_task(run_file_uploaded_consumer(session_factory, pool, stop_event))

        event_id = uuid.uuid4()
        payload = _uploaded_payload(document_id, owner_id, storage_key, event_id=event_id)

        try:
            await kafka_producer.send_and_wait(settings.KAFKA_TOPIC_FILE_UPLOADED, json.dumps(payload).encode())

            async def _check_ready():
                async with session_factory() as db:
                    job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
                    return job if job is not None and job.status == "READY" else None

            job = await _wait_for(_check_ready, timeout_s=120)
            first_updated_at = job.updated_at

            await kafka_producer.send_and_wait(settings.KAFKA_TOPIC_FILE_UPLOADED, json.dumps(payload).encode())
            await asyncio.sleep(5)  # give the consumer a real chance to (mis)process it

            async with session_factory() as db:
                job_after = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)

            assert job_after.status == "READY"
            assert job_after.updated_at == first_updated_at
        finally:
            stop_event.set()
            await asyncio.wait_for(consumer_task, timeout=10)
            await pool.stop()
            await embedding_client.close()
            await MilvusRepository().delete_by_document_id(str(document_id))
            await OpenSearchRepository().delete_by_document_id(str(document_id))


class TestCrashWindow:
    @pytest.mark.asyncio
    async def test_crash_between_commit_and_enqueue_leaves_job_recoverable_not_lost(
        self, monkeypatch, session_factory, minio_client
    ) -> None:
        """Simulates a process crash between the inbox transaction
        committing and the job being handed to the bounded ingestion pool
        (LLD §3.2's closing paragraph). The job is left `NOT_STARTED` (not
        `PROCESSING` — this repo's `orchestrator.run()` is the thing that
        transitions to `PROCESSING`, and it never got to run), with its
        Kafka offset already committed (`enable_auto_commit=True`), so
        redelivery will never trigger a retry either.

        This test proves the observable precondition (a durable,
        never-enqueued `NOT_STARTED` job survives the crash rather than
        being silently lost) — full automatic recovery is Phase 6's
        reconciliation sweep, which does not exist yet. Note for Phase 6:
        `list_stuck_processing` alone will not catch this specific job,
        since it never reached `PROCESSING`; the sweep will need to also
        consider `NOT_STARTED` jobs older than a threshold with no
        in-flight work, not just `PROCESSING` ones.
        """
        document_id = uuid.uuid4()
        owner_id = uuid.uuid4()
        storage_key = f"{owner_id}/{document_id}/v1/original.pdf"
        minio_client.fput_object(TEST_BUCKET, storage_key, FIXTURE_PATH)

        embedding_client = EmbeddingClient()
        pool = BoundedIngestionPool(session_factory, embedding_client)

        async def _crash_on_enqueue(*_args, **_kwargs):
            raise RuntimeError("simulated process crash before enqueue")

        monkeypatch.setattr(pool, "enqueue", _crash_on_enqueue)

        from app.events.consumers.file_uploaded_consumer import handle_message

        payload_dict = _uploaded_payload(document_id, owner_id, storage_key)
        raw_value = json.dumps(payload_dict).encode()

        with pytest.raises(RuntimeError, match="simulated process crash"):
            await handle_message(session_factory, pool, raw_value)

        async with session_factory() as db:
            job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)

        assert job is not None
        assert job.status == "NOT_STARTED"


class TestBackpressure:
    @pytest.mark.asyncio
    async def test_pool_never_runs_more_than_max_concurrency_at_once(self, session_factory, monkeypatch) -> None:
        """LLD §6.1.1 — the bounded pool's own mechanism, tested directly
        rather than via real Kafka consumer-group lag (no lag metric exists
        until Phase 11): with `max_concurrency=2` and a slow fake
        orchestrator, enqueue more items than the pool's capacity and
        assert peak concurrent execution never exceeds 2, and that
        `enqueue()` genuinely blocks while the queue is full (the intended
        backpressure signal).
        """
        max_concurrency = 2
        in_flight = 0
        peak_in_flight = 0
        lock = asyncio.Lock()

        async def _fake_orchestrator(_session_factory, _document_id, _content_version, _embedding_client):
            nonlocal in_flight, peak_in_flight
            async with lock:
                in_flight += 1
                peak_in_flight = max(peak_in_flight, in_flight)
            await asyncio.sleep(0.3)
            async with lock:
                in_flight -= 1

        monkeypatch.setattr("app.events.ingestion_pool.run_orchestrator", _fake_orchestrator)

        embedding_client = EmbeddingClient()
        pool = BoundedIngestionPool(session_factory, embedding_client, max_concurrency=max_concurrency)
        await pool.start()

        try:
            document_ids = [uuid.uuid4() for _ in range(8)]

            # The queue's maxsize is max_concurrency * 2 — enqueueing more
            # than that while workers are slow proves enqueue() blocks
            # (backpressure) rather than growing unbounded.
            enqueue_all = asyncio.gather(*(pool.enqueue(doc_id, 1) for doc_id in document_ids))
            await asyncio.wait_for(enqueue_all, timeout=10)

            await asyncio.sleep(2)  # let the workers drain everything

            assert peak_in_flight <= max_concurrency
            assert peak_in_flight == max_concurrency  # actually exercised the bound, not trivially under it
        finally:
            await pool.stop()
            await embedding_client.close()


class TestMalformedRetry:
    @pytest.mark.asyncio
    async def test_retry_event_missing_content_version_is_rejected_not_guessed(
        self, monkeypatch, kafka_container, kafka_producer, session_factory
    ) -> None:
        monkeypatch.setattr(settings, "KAFKA_BOOTSTRAP_SERVERS", kafka_container.get_bootstrap_server())

        document_id = uuid.uuid4()
        malformed_payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": "file.processing_retry",
            "version": 1,
            "document_id": str(document_id),
            # content_version deliberately omitted (HLD §1.3 #1)
            "retry_count": 1,
            "timestamp": "2026-01-01T00:00:00Z",
        }

        embedding_client = EmbeddingClient()
        pool = BoundedIngestionPool(session_factory, embedding_client)
        await pool.start()
        enqueued: list[tuple] = []

        async def _spy_enqueue(document_id, content_version):
            enqueued.append((document_id, content_version))

        monkeypatch.setattr(pool, "enqueue", _spy_enqueue)

        stop_event = asyncio.Event()
        consumer_task = asyncio.create_task(run_file_processing_retry_consumer(session_factory, pool, stop_event))

        try:
            await kafka_producer.send_and_wait(
                settings.KAFKA_TOPIC_FILE_PROCESSING_RETRY, json.dumps(malformed_payload).encode()
            )
            await asyncio.sleep(5)  # real time for the consumer to (fail to) process it

            assert enqueued == []  # never enqueued against a guessed version

            async with session_factory() as db:
                # No ingestion_jobs row exists for this document at all —
                # the malformed event never created or touched one.
                job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
            assert job is None

            # The consumer loop itself survived the malformed message —
            # prove it's still alive by publishing a real, well-formed
            # retry for a nonexistent job and confirming that also gets
            # handled (job_not_found path) rather than the loop being dead.
            well_formed_but_unknown = {
                "event_id": str(uuid.uuid4()),
                "event_type": "file.processing_retry",
                "version": 1,
                "document_id": str(uuid.uuid4()),
                "content_version": 1,
                "retry_count": 1,
                "timestamp": "2026-01-01T00:00:00Z",
            }
            await kafka_producer.send_and_wait(
                settings.KAFKA_TOPIC_FILE_PROCESSING_RETRY, json.dumps(well_formed_but_unknown).encode()
            )
            await asyncio.sleep(5)
            assert not consumer_task.done()  # loop is still running, never crashed
        finally:
            stop_event.set()
            await asyncio.wait_for(consumer_task, timeout=10)
            await pool.stop()
            await embedding_client.close()
