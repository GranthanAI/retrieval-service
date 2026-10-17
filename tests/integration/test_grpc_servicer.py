"""Phase 9 exit criteria — a real gRPC client (generated stub) against a
real running `grpc.aio` server, backed by real Postgres/Milvus/OpenSearch/
embedding-service and a local fake File Service (Phase 7's own contract,
run as a real ASGI app on a real port here — the servicer's
`FileServiceClient` makes real HTTP calls, not an injected transport).
"""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from unittest.mock import patch

import grpc
import pytest
import uvicorn
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from app.clients.file_service_client import PermissionCheckResult
from app.clients.embedding_client import EmbeddingClient
from app.clients.file_service_client import FileServiceClient
from app.core.config import settings
from app.grpc import retrieval_pb2, retrieval_pb2_grpc
from app.grpc.retrieval_servicer import RetrievalServicer
from app.grpc.server import create_server
from app.ingestion import orchestrator
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository

FIXTURE_PATH = "tests/fixtures/sample.pdf"
TEST_BUCKET = "retrieval-test-bucket"


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@asynccontextmanager
async def _running_fake_file_service(authorized_document_ids: set[str]):
    async def check_permissions(request: Request) -> JSONResponse:
        body = await request.json()
        document_ids = body.get("document_ids", [])
        authorized = [d for d in document_ids if d in authorized_document_ids]
        return JSONResponse({"authorized_document_ids": authorized})

    app = Starlette(routes=[Route("/internal/files/permissions/check", check_permissions, methods=["POST"])])
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    try:
        while not server.started:
            await asyncio.sleep(0.05)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@asynccontextmanager
async def _running_grpc_server(session_factory, file_service_base_url: str):
    port = _free_port()
    with patch.object(settings, "GRPC_PORT", port):
        servicer = RetrievalServicer(
            session_factory=session_factory,
            file_service_client=FileServiceClient(base_url=file_service_base_url),
            embedding_client=EmbeddingClient(),
            milvus_repository=MilvusRepository(),
            opensearch_repository=OpenSearchRepository(),
        )
        server = create_server(servicer)
        await server.start()
        try:
            yield port
        finally:
            await server.stop(grace=1.0)


@pytest.fixture
def minio_client():
    from minio import Minio

    client = Minio(
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )
    if not client.bucket_exists(TEST_BUCKET):
        client.make_bucket(TEST_BUCKET)
    return client


async def _ingest_real_document(migrated_database_url: str, minio_client) -> str:
    """Reuses Phase 4's own pattern — real orchestrator, real everything —
    so there's real, queryable data for the gRPC round trip to retrieve.
    """
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
        # orchestrator.run() alone never touches document_current_versions —
        # only file_uploaded_consumer.handle_message does that (LLD §6.1).
        # Since this helper bypasses Kafka entirely, seed it directly here,
        # or the query pipeline's version-resolution step (HLD §13.3) would
        # find no current_version and short-circuit to empty regardless of
        # how successful the ingestion was.
        await DocumentCurrentVersionRepository.advance_if_newer(db, document_id, 1)
        await db.commit()

    embedding_client = EmbeddingClient()
    await orchestrator.run(session_factory, document_id, 1, embedding_client)
    await embedding_client.close()
    await engine.dispose()

    return str(document_id)


class TestGrpcRoundTrip:
    @pytest.mark.asyncio
    async def test_get_relevant_chunks_over_real_grpc_returns_correct_chunks(
        self, session_factory, migrated_database_url, minio_client
    ) -> None:
        document_id = await _ingest_real_document(migrated_database_url, minio_client)

        async with _running_fake_file_service({document_id}) as file_service_url:
            async with _running_grpc_server(session_factory, file_service_url) as port:
                async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                    stub = retrieval_pb2_grpc.RetrievalServiceStub(channel)
                    request = retrieval_pb2.GetRelevantChunksRequest(
                        user_id="user-1",
                        conversation_id="conv-1",
                        query="what is the retry policy?",
                        top_k=6,
                        scope=retrieval_pb2.RetrievalScope.FILES,
                        file_ids=[document_id],
                        min_relevance_score=0.0,
                        max_tokens=2000,
                        trace_id="trace-round-trip",
                    )
                    response = await stub.GetRelevantChunks(request, timeout=10)

        assert len(response.chunks) > 0
        assert response.chunks[0].source_file_id == document_id
        assert response.chunks[0].content

        await MilvusRepository().delete_by_document_id(document_id)
        await OpenSearchRepository().delete_by_document_id(document_id)

    @pytest.mark.asyncio
    async def test_unauthorized_document_returns_empty_over_real_grpc(self, session_factory) -> None:
        # Fake File Service authorizes nothing — proves the servicer's real
        # network path enforces HLD §16.2, not just pipeline.py in isolation.
        async with _running_fake_file_service(set()) as file_service_url:
            async with _running_grpc_server(session_factory, file_service_url) as port:
                async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                    stub = retrieval_pb2_grpc.RetrievalServiceStub(channel)
                    request = retrieval_pb2.GetRelevantChunksRequest(
                        user_id="user-1",
                        query="anything",
                        scope=retrieval_pb2.RetrievalScope.FILES,
                        file_ids=["some-doc"],
                        trace_id="trace-2",
                    )
                    response = await stub.GetRelevantChunks(request, timeout=10)

        assert list(response.chunks) == []


class TestDeadline:
    @pytest.mark.asyncio
    async def test_slow_downstream_call_still_returns_within_deadline_plus_margin(
        self, session_factory, migrated_database_url, minio_client
    ) -> None:
        document_id = await _ingest_real_document(migrated_database_url, minio_client)

        async def _slow_search(self, query_text, top_n, current_versions):
            await asyncio.sleep(settings.QUERY_PIPELINE_DEADLINE_SECONDS + 5)
            return []

        async with _running_fake_file_service({document_id}) as file_service_url:
            with patch.object(OpenSearchRepository, "search", _slow_search):
                async with _running_grpc_server(session_factory, file_service_url) as port:
                    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                        stub = retrieval_pb2_grpc.RetrievalServiceStub(channel)
                        request = retrieval_pb2.GetRelevantChunksRequest(
                            user_id="user-1",
                            query="what is the retry policy?",
                            scope=retrieval_pb2.RetrievalScope.FILES,
                            file_ids=[document_id],
                            trace_id="trace-slow",
                        )

                        start = time.monotonic()
                        response = await stub.GetRelevantChunks(
                            request, timeout=settings.QUERY_PIPELINE_DEADLINE_SECONDS + 3
                        )
                        elapsed = time.monotonic() - start

        # Returns EMPTY, not hanging or erroring (LLD §7.1) — well within
        # QUERY_PIPELINE_DEADLINE_SECONDS plus a small margin, never close
        # to the artificially slow call's full duration.
        assert list(response.chunks) == []
        assert elapsed < settings.QUERY_PIPELINE_DEADLINE_SECONDS + 2

        await MilvusRepository().delete_by_document_id(document_id)
        await OpenSearchRepository().delete_by_document_id(document_id)


class TestLoadAtLowConcurrency:
    @pytest.mark.asyncio
    async def test_modest_concurrent_burst_against_fast_fakes(self, session_factory) -> None:
        """Sanity check for this phase only (the full perf pass is Phase
        12) — GRPC_MAX_WORKERS/the aio event loop shouldn't bottleneck a
        modest concurrent burst when every downstream call is fast."""

        async def _fast_check_permissions(self, user_id, document_ids):
            return PermissionCheckResult(authorized=list(document_ids))

        async def _fast_embed_query(self, text):
            return [0.1] * 384

        async def _fast_search_empty_dense(self, query_vector, top_n, current_versions):
            return []

        async def _fast_search_empty_bm25(self, query_text, top_n, current_versions):
            return []

        with (
            patch.object(FileServiceClient, "check_permissions", _fast_check_permissions),
            patch.object(EmbeddingClient, "embed_query", _fast_embed_query),
            patch.object(MilvusRepository, "search", _fast_search_empty_dense),
            patch.object(OpenSearchRepository, "search", _fast_search_empty_bm25),
        ):
            servicer = RetrievalServicer(
                session_factory=session_factory,
                file_service_client=FileServiceClient(),
                embedding_client=EmbeddingClient(),
                milvus_repository=MilvusRepository(),
                opensearch_repository=OpenSearchRepository(),
            )
            port = _free_port()
            with patch.object(settings, "GRPC_PORT", port):
                server = create_server(servicer)
                await server.start()

            try:
                async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                    stub = retrieval_pb2_grpc.RetrievalServiceStub(channel)

                    async def _one_call(i: int):
                        request = retrieval_pb2.GetRelevantChunksRequest(
                            user_id="user-1",
                            query=f"query {i}",
                            scope=retrieval_pb2.RetrievalScope.FILES,
                            file_ids=[str(uuid.uuid4())],
                            trace_id=f"trace-{i}",
                        )
                        return await stub.GetRelevantChunks(request, timeout=5)

                    start = time.monotonic()
                    responses = await asyncio.gather(*(_one_call(i) for i in range(20)))
                    elapsed = time.monotonic() - start
            finally:
                await server.stop(grace=1.0)

        assert len(responses) == 20
        assert elapsed < 5.0
