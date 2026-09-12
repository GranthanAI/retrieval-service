import asyncio

from pymilvus import MilvusClient

from app.core.config import settings
from app.models.chunk_record import ChunkRecord
from app.stores.types import DenseSearchHit

# Sentinel encoding for Milvus's optional fields — see
# scripts/provision_milvus_collection.py's docstring for why these three
# fields are NOT nullable on the deployed server, despite LLD §4 declaring
# them so: neither v2.3.0 nor v2.4.15 accepts a real NULL insert for them.
# `None` on a ChunkRecord (the shared, honest representation OpenSearch
# still receives as a real null) is translated to these sentinels only at
# the Milvus boundary, never elsewhere.
_ABSENT_VARCHAR = ""
_ABSENT_PAGE_NUMBER = -1

_client: MilvusClient | None = None


def _get_client() -> MilvusClient:
    """Connects to the collection provisioned once by
    `scripts/provision_milvus_collection.py` (LLD §13.2) — never creates it.
    """
    global _client
    if _client is None:
        _client = MilvusClient(uri=f"http://{settings.MILVUS_HOST}:{settings.MILVUS_PORT}")
    return _client


def _to_row(record: ChunkRecord) -> dict:
    return {
        "chunk_id": record.chunk_id,
        "document_id": record.document_id,
        "content_version": record.content_version,
        "chunk_index": record.chunk_index,
        "owner_id": record.owner_id,
        "conversation_id": record.conversation_id if record.conversation_id is not None else _ABSENT_VARCHAR,
        "filename": record.filename,
        "page_number": record.page_number if record.page_number is not None else _ABSENT_PAGE_NUMBER,
        "section": record.section if record.section is not None else _ABSENT_VARCHAR,
        "content": record.content,
        "embedding": record.embedding,
    }


class MilvusRepository:
    """LLD §4 — exactly four operations, deliberately narrow. No method here
    creates or alters the collection (enforced by review convention and
    Phase 2's static grep check, LLD §13.2) — application code only ever
    connects and reads/writes.

    `MilvusClient`'s calls are synchronous; every blocking call is offloaded
    via `asyncio.to_thread`, the same pattern file-service already uses for
    its (also synchronous) MinIO client.
    """

    async def upsert_chunks(self, records: list[ChunkRecord]) -> None:
        """Upsert-by-primary-key (`chunk_id`) — a redelivered ingestion event
        for the same chunk overwrites rather than duplicates (HLD §11).
        """
        if not records:
            return

        rows = [_to_row(r) for r in records]

        def _upsert() -> None:
            client = _get_client()
            client.upsert(collection_name=settings.MILVUS_COLLECTION_NAME, data=rows)
            # Without an explicit flush, a search/query immediately after
            # this call can miss the just-written rows (segment growth is
            # async) — every other store operation below asks for
            # consistency_level="Strong" for the same reason.
            client.flush(collection_name=settings.MILVUS_COLLECTION_NAME)

        await asyncio.to_thread(_upsert)

    async def search(
        self, query_vector: list[float], top_n: int, current_versions: dict[str, int]
    ) -> list[DenseSearchHit]:
        """`current_versions`: {document_id: current_version} for every id in
        authorized_file_ids (HLD §13.3) — the filter expression built from
        this is `document_id IN (...) AND content_version == current_versions[document_id]`
        PER document_id, not a single flat top_n/document_ids pair (a
        document-level filter alone would let old-version chunks back in,
        HLD §13.3).
        """
        if not current_versions:
            return []

        clauses = [
            f'(document_id == "{doc_id}" and content_version == {version})'
            for doc_id, version in current_versions.items()
        ]
        filter_expr = " or ".join(clauses)

        def _search() -> list[DenseSearchHit]:
            results = _get_client().search(
                collection_name=settings.MILVUS_COLLECTION_NAME,
                data=[query_vector],
                anns_field="embedding",
                search_params={
                    "metric_type": settings.MILVUS_INDEX_METRIC_TYPE,
                    "params": {"nprobe": settings.MILVUS_SEARCH_NPROBE},
                },
                limit=top_n,
                filter=filter_expr,
                output_fields=["chunk_id"],
                timeout=settings.MILVUS_QUERY_TIMEOUT_SECONDS,
                consistency_level="Strong",
            )
            return [DenseSearchHit(chunk_id=hit["entity"]["chunk_id"], score=hit["distance"]) for hit in results[0]]

        return await asyncio.to_thread(_search)

    async def get_by_chunk_ids(self, chunk_ids: list[str]) -> dict[str, list[float]]:
        """HLD §18.1 — a primary-key `get` by chunk_id, NOT a fresh ANN
        search; fetches candidate embeddings for MMR's pairwise similarity.
        """
        if not chunk_ids:
            return {}

        quoted = ", ".join(f'"{cid}"' for cid in chunk_ids)
        filter_expr = f"chunk_id in [{quoted}]"

        def _query() -> dict[str, list[float]]:
            rows = _get_client().query(
                collection_name=settings.MILVUS_COLLECTION_NAME,
                filter=filter_expr,
                output_fields=["chunk_id", "embedding"],
                consistency_level="Strong",
            )
            return {row["chunk_id"]: row["embedding"] for row in rows}

        return await asyncio.to_thread(_query)

    async def delete_by_document_id(self, document_id: str) -> None:
        """HLD §21 — deletes ALL content_versions for the document,
        unconditionally (no version filter — matches file.deleted's own
        scope, HLD §8.2/§21).
        """

        def _delete() -> None:
            client = _get_client()
            client.delete(collection_name=settings.MILVUS_COLLECTION_NAME, filter=f'document_id == "{document_id}"')
            client.flush(collection_name=settings.MILVUS_COLLECTION_NAME)

        await asyncio.to_thread(_delete)
