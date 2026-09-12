import asyncio

from opensearchpy import OpenSearch

from app.core.config import settings
from app.models.chunk_record import ChunkRecord
from app.stores.types import BM25SearchHit

_client: OpenSearch | None = None


def _get_client() -> OpenSearch:
    """Connects to the index provisioned once by
    `scripts/provision_opensearch_index.py` (LLD §13.2's reasoning applied
    identically to OpenSearch) — never creates it.
    """
    global _client
    if _client is None:
        _client = OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","))
    return _client


class OpenSearchRepository:
    """LLD §5 — same narrow four-operation shape as MilvusRepository.

    opensearch-py's default client is synchronous; every blocking call is
    offloaded via `asyncio.to_thread`, matching MilvusRepository's pattern
    (and file-service's own precedent for a synchronous SDK, MinIO).
    """

    async def upsert_chunks(self, records: list[ChunkRecord]) -> None:
        """Indexed by `chunk_id` used directly as the OpenSearch document
        `_id` (not a mapped field lookup, LLD §5) — a redelivered ingestion
        event overwrites rather than duplicates, same idempotency property
        as Milvus's upsert-by-primary-key (HLD §11/§14).
        """
        if not records:
            return

        def _bulk_upsert() -> None:
            client = _get_client()
            actions: list[dict] = []
            for r in records:
                actions.append({"index": {"_index": settings.OPENSEARCH_INDEX_NAME, "_id": r.chunk_id}})
                actions.append(
                    {
                        "chunk_id": r.chunk_id,
                        "document_id": r.document_id,
                        "content_version": r.content_version,
                        "chunk_index": r.chunk_index,
                        "owner_id": r.owner_id,
                        "conversation_id": r.conversation_id,
                        "filename": r.filename,
                        "page_number": r.page_number,
                        "section": r.section,
                        "content": r.content,
                    }
                )
            client.bulk(body=actions, refresh=True)

        await asyncio.to_thread(_bulk_upsert)

    async def search(self, query_text: str, top_n: int, current_versions: dict[str, int]) -> list[BM25SearchHit]:
        """LLD §5 — `must: multi_match` over content/section/filename (each
        independently boosted), plus a `should`-of-`bool` per-document
        filter — equivalent to OR-ing per-document `(document_id AND
        content_version)` pairs, since OpenSearch's `terms` filter alone
        can't express "this version, for this document" as one flat clause
        the way Milvus's per-record scalar filter can. Same
        authorized+current-version scope as Milvus (HLD §13/§13.3).
        """
        if not current_versions:
            return []

        should_clauses = [
            {"bool": {"filter": [{"term": {"document_id": doc_id}}, {"term": {"content_version": version}}]}}
            for doc_id, version in current_versions.items()
        ]

        body = {
            "size": top_n,
            "query": {
                "bool": {
                    "must": {
                        "multi_match": {
                            "query": query_text,
                            "fields": ["content", "section^1.5", "filename^2.0"],
                        }
                    },
                    "should": should_clauses,
                    "minimum_should_match": 1,
                }
            },
        }

        def _search() -> list[BM25SearchHit]:
            client = _get_client()
            response = client.search(
                index=settings.OPENSEARCH_INDEX_NAME,
                body=body,
                request_timeout=settings.OPENSEARCH_QUERY_TIMEOUT_SECONDS,
            )
            return [BM25SearchHit(chunk_id=hit["_id"], score=hit["_score"]) for hit in response["hits"]["hits"]]

        return await asyncio.to_thread(_search)

    async def delete_by_document_id(self, document_id: str) -> None:
        def _delete() -> None:
            client = _get_client()
            client.delete_by_query(
                index=settings.OPENSEARCH_INDEX_NAME,
                body={"query": {"term": {"document_id": document_id}}},
                refresh=True,
            )

        await asyncio.to_thread(_delete)
