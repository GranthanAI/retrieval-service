"""Phase 2 exit criteria — run against the real, already-running Milvus/
OpenSearch from the root docker-compose.yml (provisioned once via
scripts/provision_milvus_collection.py / provision_opensearch_index.py),
NOT fresh ephemeral containers — matching docs/IMPLEMENTATION_PLAN.md
Phase 2's explicit wording.
"""

import uuid

import pytest

from app.models.chunk_record import ChunkRecord
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository

EMBEDDING_DIM = 384


def _vector(seed: float) -> list[float]:
    return [seed] * EMBEDDING_DIM


def _make_record(
    *, document_id: str, content_version: int = 1, chunk_index: int = 0, seed: float = 0.1
) -> ChunkRecord:
    return ChunkRecord(
        chunk_id=str(uuid.uuid4()),
        document_id=document_id,
        content_version=content_version,
        chunk_index=chunk_index,
        owner_id=str(uuid.uuid4()),
        conversation_id=None,
        filename="report.pdf",
        page_number=1,
        section="Introduction",
        content="the retry policy uses exponential backoff with jitter",
        embedding=_vector(seed),
    )


@pytest.fixture
def milvus_repo() -> MilvusRepository:
    return MilvusRepository()


@pytest.fixture
def opensearch_repo() -> OpenSearchRepository:
    return OpenSearchRepository()


class TestMilvusRepository:
    @pytest.mark.asyncio
    async def test_upsert_search_get_delete_roundtrip(self, milvus_repo: MilvusRepository) -> None:
        document_id = str(uuid.uuid4())
        record = _make_record(document_id=document_id, seed=0.5)

        await milvus_repo.upsert_chunks([record])

        hits = await milvus_repo.search(
            query_vector=_vector(0.5), top_n=10, current_versions={document_id: 1}
        )
        assert any(hit.chunk_id == record.chunk_id for hit in hits)

        embeddings = await milvus_repo.get_by_chunk_ids([record.chunk_id])
        assert record.chunk_id in embeddings
        assert len(embeddings[record.chunk_id]) == EMBEDDING_DIM

        await milvus_repo.delete_by_document_id(document_id)
        hits_after_delete = await milvus_repo.search(
            query_vector=_vector(0.5), top_n=10, current_versions={document_id: 1}
        )
        assert not any(hit.chunk_id == record.chunk_id for hit in hits_after_delete)

    @pytest.mark.asyncio
    async def test_upsert_is_idempotent(self, milvus_repo: MilvusRepository) -> None:
        document_id = str(uuid.uuid4())
        record = _make_record(document_id=document_id, seed=0.7)

        await milvus_repo.upsert_chunks([record])
        await milvus_repo.upsert_chunks([record])

        embeddings = await milvus_repo.get_by_chunk_ids([record.chunk_id])
        assert record.chunk_id in embeddings

        hits = await milvus_repo.search(query_vector=_vector(0.7), top_n=50, current_versions={document_id: 1})
        matching = [hit for hit in hits if hit.chunk_id == record.chunk_id]
        assert len(matching) == 1

        await milvus_repo.delete_by_document_id(document_id)

    @pytest.mark.asyncio
    async def test_version_filter_returns_only_current_version(self, milvus_repo: MilvusRepository) -> None:
        document_id = str(uuid.uuid4())
        v1 = _make_record(document_id=document_id, content_version=1, seed=0.3)
        v2 = _make_record(document_id=document_id, content_version=2, seed=0.3)

        await milvus_repo.upsert_chunks([v1, v2])

        hits = await milvus_repo.search(query_vector=_vector(0.3), top_n=50, current_versions={document_id: 2})
        hit_ids = {hit.chunk_id for hit in hits}

        assert v2.chunk_id in hit_ids
        assert v1.chunk_id not in hit_ids

        await milvus_repo.delete_by_document_id(document_id)


class TestOpenSearchRepository:
    @pytest.mark.asyncio
    async def test_upsert_search_delete_roundtrip(self, opensearch_repo: OpenSearchRepository) -> None:
        document_id = str(uuid.uuid4())
        record = _make_record(document_id=document_id)

        await opensearch_repo.upsert_chunks([record])

        hits = await opensearch_repo.search(
            query_text="retry policy exponential backoff", top_n=10, current_versions={document_id: 1}
        )
        assert any(hit.chunk_id == record.chunk_id for hit in hits)

        await opensearch_repo.delete_by_document_id(document_id)
        hits_after_delete = await opensearch_repo.search(
            query_text="retry policy exponential backoff", top_n=10, current_versions={document_id: 1}
        )
        assert not any(hit.chunk_id == record.chunk_id for hit in hits_after_delete)

    @pytest.mark.asyncio
    async def test_upsert_is_idempotent(self, opensearch_repo: OpenSearchRepository) -> None:
        document_id = str(uuid.uuid4())
        record = _make_record(document_id=document_id)

        await opensearch_repo.upsert_chunks([record])
        await opensearch_repo.upsert_chunks([record])

        hits = await opensearch_repo.search(
            query_text="retry policy exponential backoff", top_n=50, current_versions={document_id: 1}
        )
        matching = [hit for hit in hits if hit.chunk_id == record.chunk_id]
        assert len(matching) == 1

        await opensearch_repo.delete_by_document_id(document_id)

    @pytest.mark.asyncio
    async def test_version_filter_returns_only_current_version(self, opensearch_repo: OpenSearchRepository) -> None:
        document_id = str(uuid.uuid4())
        v1 = _make_record(document_id=document_id, content_version=1)
        v2 = _make_record(document_id=document_id, content_version=2)

        await opensearch_repo.upsert_chunks([v1, v2])

        hits = await opensearch_repo.search(
            query_text="retry policy exponential backoff", top_n=50, current_versions={document_id: 2}
        )
        hit_ids = {hit.chunk_id for hit in hits}

        assert v2.chunk_id in hit_ids
        assert v1.chunk_id not in hit_ids

        await opensearch_repo.delete_by_document_id(document_id)
