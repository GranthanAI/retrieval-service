import asyncio

from opensearchpy import OpenSearch

from app.core.config import settings
from app.core.exceptions import IndexMappingError, IndexMissingError
from app.stores import generations
from app.stores.index_schema import SCHEMA_V1, keyword_query_fields, mapping_problems
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


def _hit(hit: dict) -> BM25SearchHit:
    src = hit["_source"]
    return BM25SearchHit(
        chunk_id=hit["_id"], score=hit.get("_score") or 0.0, document_id=src["document_id"], filename=src["filename"], content=src["content"],
        page_number=src["page_number"], section=src["section"], chunk_index=src["chunk_index"], heading_path=src.get("heading_path"),
        page_start=src.get("page_start"), page_end=src.get("page_end"), block_type=src.get("block_type"), location=src.get("location"))


def _document(r: ChunkRecord, schema_version: str) -> dict:
    """The indexed document of a generation-2+ chunk. `content` is exactly the text that was embedded (heading path + body)."""
    return {
        "chunk_id": r.chunk_id, "document_id": r.document_id, "content_version": r.content_version, "chunk_index": r.chunk_index,
        "owner_id": r.owner_id, "conversation_id": r.conversation_id, "filename": r.filename, "page_number": r.page_number,
        "section": r.section, "content": r.content, "heading_path": r.heading_path, "page_start": r.page_start,
        "page_end": r.page_end, "block_type": r.block_type, "location": r.location, "generation": r.generation,
    }


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

        info = generations.current()
        index = info.opensearch_index

        def _bulk_upsert() -> None:
            client = _get_client()
            # Never let a write create the index: a bulk into a missing index auto-creates it with dynamic mapping.
            if not client.indices.exists(index=index):
                raise IndexMissingError(
                    f"OpenSearch index {index!r} does not exist; provision it with "
                    "scripts/provision_opensearch_index.py or scripts/generation.py create (writes never create it)"
                )
            actions: list[dict] = []
            for r in records:
                actions.append({"index": {"_index": index, "_id": r.chunk_id}})
                actions.append(
                    _document(r, info.schema_version)
                    if info.schema_version != SCHEMA_V1 else {
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

        # Scope is a `filter` (required, unscored), NOT a `should` with minimum_should_match: with scoring `should` clauses present
        # below, any one of them would otherwise satisfy the minimum and let an out-of-scope document in.
        bool_query: dict = {
            "must": {
                "multi_match": {
                    "query": query_text,
                    "fields": ["content", "section^1.5", "filename^2.0"],
                }
            },
            "filter": [{"bool": {"should": should_clauses, "minimum_should_match": 1}}],
        }
        if settings.OPENSEARCH_PHRASE_BOOST_ENABLED and query_text.strip():
            # Optional score boosts: a query that is (or contains) an exact heading or phrase ranks that section first.
            bool_query["should"] = [
                {"match_phrase": {"section": {"query": query_text, "boost": settings.OPENSEARCH_SECTION_PHRASE_BOOST}}},
                {"match_phrase": {"content": {"query": query_text, "boost": settings.OPENSEARCH_CONTENT_PHRASE_BOOST}}},
            ]
        body = {"size": top_n, "query": {"bool": bool_query}}
        info = generations.current()
        bool_query["must"]["multi_match"]["fields"] = keyword_query_fields(info.schema_version)

        def _search() -> list[BM25SearchHit]:
            client = _get_client()
            response = client.search(
                index=info.opensearch_read_target,
                body=body,
                request_timeout=settings.OPENSEARCH_QUERY_TIMEOUT_SECONDS,
            )
            return [_hit(hit) for hit in response["hits"]["hits"]]

        return await asyncio.to_thread(_search)

    async def document_chunks(self, document_id: str, content_version: int, limit: int) -> list[BM25SearchHit]:
        """Every chunk of ONE document version in document order (chunk_index ascending), up to `limit`. Exact filter on both fields; the
        caller has already established that the user may read the document and that this is its current version."""
        info = generations.current()
        body = {
            "size": limit,
            "sort": [{"chunk_index": "asc"}],
            "query": {"bool": {"filter": [{"term": {"document_id": document_id}}, {"term": {"content_version": content_version}}]}},
        }

        def _search() -> list[BM25SearchHit]:
            response = _get_client().search(index=info.opensearch_read_target, body=body, request_timeout=settings.OPENSEARCH_QUERY_TIMEOUT_SECONDS * 4)
            return [_hit(h) for h in response["hits"]["hits"]]

        return await asyncio.to_thread(_search)

    async def neighbours(self, document_id: str, content_version: int, chunk_index: int, section: str | None) -> list[BM25SearchHit]:
        """The chunks immediately before and after `chunk_index` in the same document version, kept only if they are in the same
        section (never across a section boundary)."""
        info = generations.current()
        body = {
            "size": 2, "sort": [{"chunk_index": "asc"}],
            "query": {"bool": {"filter": [{"term": {"document_id": document_id}}, {"term": {"content_version": content_version}},
                                          {"terms": {"chunk_index": [chunk_index - 1, chunk_index + 1]}}]}},
        }

        def _search() -> list[BM25SearchHit]:
            response = _get_client().search(index=info.opensearch_read_target, body=body, request_timeout=settings.OPENSEARCH_QUERY_TIMEOUT_SECONDS)
            return [h for h in (_hit(x) for x in response["hits"]["hits"]) if h.section == section]

        return await asyncio.to_thread(_search)

    async def delete_by_document_id(self, document_id: str) -> None:
        indexes = [g.opensearch_index for g in generations.all_live()]   # a deleted file must vanish from every live generation

        def _delete() -> None:
            client = _get_client()
            for index in indexes:
                if client.indices.exists(index=index):
                    client.delete_by_query(index=index, body={"query": {"term": {"document_id": document_id}}}, refresh=True)

        await asyncio.to_thread(_delete)

    async def verify_mapping(self) -> None:
        """Raises IndexMissingError / IndexMappingError (listing every difference) unless the index exists with the mapping the
        application requires. Used by startup and by /health/ready."""

        info = generations.current()

        def _verify() -> None:
            client = _get_client()
            index = info.opensearch_read_target
            if not client.indices.exists(index=index):
                raise IndexMissingError(f"OpenSearch index {index!r} does not exist; run scripts/provision_opensearch_index.py")
            problems = mapping_problems(client.indices.get_mapping(index=index), index, schema_version=info.schema_version)
            if problems:
                raise IndexMappingError(
                    f"OpenSearch index {index!r} has the wrong mapping ({'; '.join(problems)}). It was probably created by a first "
                    "write. Delete it and run scripts/provision_opensearch_index.py."
                )

        await asyncio.to_thread(_verify)

    async def ping(self) -> None:
        """Reachability probe for /health/ready (api mode). `info()` raises
        on failure, unlike `ping()` which just returns False."""
        await asyncio.to_thread(_get_client().info)
