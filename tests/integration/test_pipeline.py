"""Phase 8 exit criteria for `pipeline.run()`. Every external client
(embedding, File Service, Milvus, OpenSearch) is a fake — the only real
infra involved is Postgres (testcontainers + real Alembic chain, via the
`session_factory` fixture), for the version-resolution step (HLD §13.3),
which LLD §7.2's own task list does not name as one of the
injected-fake-standing-in-for-a-real-service clients.
"""

import uuid

import pytest

from app.core.enums import RetrievalScope
from app.grpc.retrieval_pb2 import GetRelevantChunksRequest
from app.query import pipeline
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.stores.types import BM25SearchHit, DenseSearchHit


class FakeEmbeddingClient:
    def __init__(self, vector: list[float] | None = None, should_fail: bool = False):
        self._vector = vector or [0.1] * 384
        self._should_fail = should_fail

    async def embed_query(self, text: str) -> list[float]:
        if self._should_fail:
            raise RuntimeError("embedding-service unavailable")
        return self._vector


class FakeFileServiceClient:
    def __init__(self, authorized: list[str]):
        self._authorized = authorized
        self.last_call: dict | None = None

    async def batch_check_permissions(self, user_id: str, document_ids: list[str]) -> list[str]:
        self.last_call = {"user_id": user_id, "document_ids": document_ids}
        return [d for d in document_ids if d in self._authorized]


class FakeMilvusRepository:
    def __init__(self, hits: list[DenseSearchHit] | Exception | None = None, embeddings: dict | None = None):
        self._hits = hits if hits is not None else []
        self._embeddings = embeddings or {}
        self.last_search_call: dict | None = None

    async def search(self, query_vector, top_n, current_versions):
        self.last_search_call = {"query_vector": query_vector, "top_n": top_n, "current_versions": current_versions}
        if isinstance(self._hits, Exception):
            raise self._hits
        return self._hits

    async def get_by_chunk_ids(self, chunk_ids: list[str]) -> dict[str, list[float]]:
        return {cid: self._embeddings[cid] for cid in chunk_ids if cid in self._embeddings}


class FakeOpenSearchRepository:
    def __init__(self, hits: list[BM25SearchHit] | Exception | None = None):
        self._hits = hits if hits is not None else []
        self.last_search_call: dict | None = None

    async def search(self, query_text, top_n, current_versions):
        self.last_search_call = {"query_text": query_text, "top_n": top_n, "current_versions": current_versions}
        if isinstance(self._hits, Exception):
            raise self._hits
        return self._hits


def _request(*, scope=RetrievalScope.FILES, file_ids=None, query="what is the retry policy?") -> GetRelevantChunksRequest:
    return GetRelevantChunksRequest(
        user_id="user-1",
        conversation_id="conv-1",
        query=query,
        top_k=6,
        scope=int(scope),
        file_ids=file_ids or ["doc-a"],
        min_relevance_score=0.0,
        max_tokens=2000,
        trace_id="trace-1",
    )


def _dense_hit(chunk_id: str, score: float) -> DenseSearchHit:
    return DenseSearchHit(chunk_id=chunk_id, score=score, document_id="doc-a", filename="a.pdf", content="hit " + chunk_id)


def _bm25_hit(chunk_id: str, score: float) -> BM25SearchHit:
    return BM25SearchHit(chunk_id=chunk_id, score=score, document_id="doc-a", filename="a.pdf", content="hit " + chunk_id)


async def _seed_current_version(session_factory, document_id: str, version: int = 1) -> None:
    async with session_factory() as db:
        await DocumentCurrentVersionRepository.advance_if_newer(db, document_id, version)
        await db.commit()


class TestDegradationMatrix:
    @pytest.mark.asyncio
    async def test_both_succeed_runs_full_rrf_and_mmr(self, session_factory) -> None:
        document_id = str(uuid.uuid4())
        await _seed_current_version(session_factory, document_id)

        dense_hits = [_dense_hit("c1", 0.9), _dense_hit("c2", 0.8)]
        bm25_hits = [_bm25_hit("c2", 10.0), _bm25_hit("c1", 5.0)]
        embeddings = {"c1": [1.0, 0.0], "c2": [0.0, 1.0]}

        response = await pipeline.run(
            _request(file_ids=[document_id]),
            session_factory=session_factory,
            file_service_client=FakeFileServiceClient(authorized=[document_id]),
            embedding_client=FakeEmbeddingClient(),
            milvus_repository=FakeMilvusRepository(hits=dense_hits, embeddings=embeddings),
            opensearch_repository=FakeOpenSearchRepository(hits=bm25_hits),
        )

        assert len(response.chunks) == 2
        chunk_ids = {c.chunk_id for c in response.chunks}
        assert chunk_ids == {"c1", "c2"}

    @pytest.mark.asyncio
    async def test_opensearch_fails_mmr_still_runs_using_milvus_embeddings(self, session_factory) -> None:
        document_id = str(uuid.uuid4())
        await _seed_current_version(session_factory, document_id)

        dense_hits = [_dense_hit("c1", 0.9), _dense_hit("c2", 0.8)]
        embeddings = {"c1": [1.0, 0.0], "c2": [0.0, 1.0]}

        response = await pipeline.run(
            _request(file_ids=[document_id]),
            session_factory=session_factory,
            file_service_client=FakeFileServiceClient(authorized=[document_id]),
            embedding_client=FakeEmbeddingClient(),
            milvus_repository=FakeMilvusRepository(hits=dense_hits, embeddings=embeddings),
            opensearch_repository=FakeOpenSearchRepository(hits=RuntimeError("opensearch down")),
        )

        assert len(response.chunks) == 2
        assert {c.chunk_id for c in response.chunks} == {"c1", "c2"}

    @pytest.mark.asyncio
    async def test_milvus_fails_mmr_is_skipped_and_bm25_only_metric_fires(self, session_factory) -> None:
        document_id = str(uuid.uuid4())
        await _seed_current_version(session_factory, document_id)

        bm25_hits = [_bm25_hit("c1", 10.0), _bm25_hit("c2", 5.0)]

        from app.core import metrics

        before = metrics.search_degraded_total.labels(engine="bm25_only")._value.get()

        response = await pipeline.run(
            _request(file_ids=[document_id]),
            session_factory=session_factory,
            file_service_client=FakeFileServiceClient(authorized=[document_id]),
            embedding_client=FakeEmbeddingClient(),
            milvus_repository=FakeMilvusRepository(hits=RuntimeError("milvus down")),
            opensearch_repository=FakeOpenSearchRepository(hits=bm25_hits),
        )

        after = metrics.search_degraded_total.labels(engine="bm25_only")._value.get()
        assert after == before + 1

        # Final order is rrf_score-descending direct from BM25's single list.
        assert [c.chunk_id for c in response.chunks] == ["c1", "c2"]

    @pytest.mark.asyncio
    async def test_both_fail_returns_empty_response(self, session_factory) -> None:
        document_id = str(uuid.uuid4())
        await _seed_current_version(session_factory, document_id)

        response = await pipeline.run(
            _request(file_ids=[document_id]),
            session_factory=session_factory,
            file_service_client=FakeFileServiceClient(authorized=[document_id]),
            embedding_client=FakeEmbeddingClient(),
            milvus_repository=FakeMilvusRepository(hits=RuntimeError("milvus down")),
            opensearch_repository=FakeOpenSearchRepository(hits=RuntimeError("opensearch down")),
        )

        assert list(response.chunks) == []
        assert response.total_tokens == 0


class TestShortCircuits:
    @pytest.mark.asyncio
    async def test_non_files_scope_returns_empty_without_any_client_call(self, session_factory) -> None:
        file_service_client = FakeFileServiceClient(authorized=["doc-a"])

        response = await pipeline.run(
            _request(scope=RetrievalScope.KNOWLEDGE_BASE),
            session_factory=session_factory,
            file_service_client=file_service_client,
            embedding_client=FakeEmbeddingClient(),
            milvus_repository=FakeMilvusRepository(),
            opensearch_repository=FakeOpenSearchRepository(),
        )

        assert list(response.chunks) == []
        assert file_service_client.last_call is None  # never even reached authorization

    @pytest.mark.asyncio
    async def test_zero_authorized_ids_returns_empty(self, session_factory) -> None:
        response = await pipeline.run(
            _request(file_ids=["doc-unauthorized"]),
            session_factory=session_factory,
            file_service_client=FakeFileServiceClient(authorized=[]),
            embedding_client=FakeEmbeddingClient(),
            milvus_repository=FakeMilvusRepository(),
            opensearch_repository=FakeOpenSearchRepository(),
        )
        assert list(response.chunks) == []

    @pytest.mark.asyncio
    async def test_no_current_version_row_returns_empty_without_embedding_call(self, session_factory) -> None:
        # Authorized, but never ingested — no document_current_versions row.
        document_id = str(uuid.uuid4())
        embedding_client = FakeEmbeddingClient()

        response = await pipeline.run(
            _request(file_ids=[document_id]),
            session_factory=session_factory,
            file_service_client=FakeFileServiceClient(authorized=[document_id]),
            embedding_client=embedding_client,
            milvus_repository=FakeMilvusRepository(),
            opensearch_repository=FakeOpenSearchRepository(),
        )
        assert list(response.chunks) == []


class TestVersionFilterWiring:
    @pytest.mark.asyncio
    async def test_search_calls_receive_the_correct_current_versions_map(self, session_factory) -> None:
        """Proves HLD §13.3 is actually wired through pipeline.py, not just
        documented — the fake stores' search() calls must carry the exact
        per-document current_version resolved from Postgres."""
        document_id = str(uuid.uuid4())
        await _seed_current_version(session_factory, document_id, version=3)

        milvus_repo = FakeMilvusRepository(hits=[_dense_hit("c1", 0.9)], embeddings={"c1": [1.0, 0.0]})
        opensearch_repo = FakeOpenSearchRepository(hits=[_bm25_hit("c1", 5.0)])

        await pipeline.run(
            _request(file_ids=[document_id]),
            session_factory=session_factory,
            file_service_client=FakeFileServiceClient(authorized=[document_id]),
            embedding_client=FakeEmbeddingClient(),
            milvus_repository=milvus_repo,
            opensearch_repository=opensearch_repo,
        )

        assert milvus_repo.last_search_call["current_versions"] == {document_id: 3}
        assert opensearch_repo.last_search_call["current_versions"] == {document_id: 3}
