import asyncio

from pymilvus import MilvusClient

from app.core.config import settings
from app.models.chunk_record import ChunkRecord
from app.stores import generations
from app.stores.generations import GenerationInfo
from app.stores.index_schema import SCHEMA_V1, milvus_output_fields
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


def _page(value: int | None) -> int | None:
    return value if value is not None and value != _ABSENT_PAGE_NUMBER else None


def _to_row(record: ChunkRecord, schema_version: str = SCHEMA_V1) -> dict:
    row = _base_row(record)
    if schema_version != SCHEMA_V1:
        row.update({
            "heading_path": record.heading_path or _ABSENT_VARCHAR,
            "page_start": record.page_start if record.page_start is not None else _ABSENT_PAGE_NUMBER,
            "page_end": record.page_end if record.page_end is not None else _ABSENT_PAGE_NUMBER,
            "block_type": record.block_type,
            "location": record.location or _ABSENT_VARCHAR,
            "generation": record.generation,
        })
    return row


def _base_row(record: ChunkRecord) -> dict:
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

        info = generations.current()
        rows = [_to_row(r, info.schema_version) for r in records]

        def _upsert() -> None:
            client = _get_client()
            client.upsert(collection_name=info.milvus_collection, data=rows)
            # Without an explicit flush, a search/query immediately after
            # this call can miss the just-written rows (segment growth is
            # async) — every other store operation below asks for
            # consistency_level="Strong" for the same reason.
            client.flush(collection_name=info.milvus_collection)

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

        info = generations.current()
        clauses = [
            f'(document_id == "{doc_id}" and content_version == {version})'
            for doc_id, version in current_versions.items()
        ]
        filter_expr = " or ".join(clauses)

        def _search() -> list[DenseSearchHit]:
            results = _get_client().search(
                collection_name=info.milvus_collection,
                data=[query_vector],
                anns_field="embedding",
                search_params={
                    "metric_type": settings.MILVUS_INDEX_METRIC_TYPE,
                    "params": {"ef": max(settings.MILVUS_HNSW_EF, top_n)},
                },
                limit=top_n,
                filter=filter_expr,
                output_fields=["chunk_id"] + [f for f in milvus_output_fields(info.schema_version) if f != "chunk_id"],
                timeout=settings.MILVUS_QUERY_TIMEOUT_SECONDS,
                consistency_level="Strong",
            )
            hits = []
            for hit in results[0]:
                entity = hit["entity"]
                # Sentinel decoding — the Milvus boundary's own absent-value
                # encoding (see _to_row's docstring above), decoded back to
                # None here so callers see the same "None means absent"
                # convention OpenSearch's hits already use natively.
                page_number = entity["page_number"]
                section = entity["section"]
                v2 = info.schema_version != SCHEMA_V1
                hits.append(
                    DenseSearchHit(
                        chunk_id=entity["chunk_id"],
                        score=hit["distance"],
                        document_id=entity["document_id"],
                        filename=entity["filename"],
                        content=entity["content"],
                        page_number=page_number if page_number != _ABSENT_PAGE_NUMBER else None,
                        section=section if section != _ABSENT_VARCHAR else None,
                        chunk_index=entity["chunk_index"],
                        heading_path=(entity.get("heading_path") or None) if v2 else None,
                        page_start=_page(entity.get("page_start")) if v2 else None,
                        page_end=_page(entity.get("page_end")) if v2 else None,
                        block_type=(entity.get("block_type") or None) if v2 else None,
                        location=(entity.get("location") or None) if v2 else None,
                    )
                )
            return hits

        return await asyncio.to_thread(_search)

    async def get_chunks_by_ids(self, chunk_ids: list[str]) -> list[tuple[DenseSearchHit, int]]:
        """Chunks by primary key with their content_version, for previous-turn candidates. The caller checks authorisation and that the
        version is the document's current one; this method does no access control."""
        if not chunk_ids:
            return []
        info = generations.current()
        quoted = ", ".join(f'"{cid}"' for cid in chunk_ids)
        fields = ["chunk_id", "content_version"] + [f for f in milvus_output_fields(info.schema_version) if f != "chunk_id"]

        def _query() -> list[tuple[DenseSearchHit, int]]:
            rows = _get_client().query(collection_name=info.milvus_collection, filter=f"chunk_id in [{quoted}]", output_fields=fields,
                                       timeout=settings.MILVUS_QUERY_TIMEOUT_SECONDS, consistency_level="Strong")
            by_id = {r["chunk_id"]: r for r in rows}
            v2 = info.schema_version != SCHEMA_V1
            out = []
            for cid in chunk_ids:                       # keep the caller's order (most relevant in the previous turn first)
                e = by_id.get(cid)
                if e is None:
                    continue
                out.append((DenseSearchHit(
                    chunk_id=e["chunk_id"], score=0.0, document_id=e["document_id"], filename=e["filename"], content=e["content"],
                    page_number=_page(e["page_number"]), section=(e["section"] or None), chunk_index=e["chunk_index"],
                    heading_path=(e.get("heading_path") or None) if v2 else None, page_start=_page(e.get("page_start")) if v2 else None,
                    page_end=_page(e.get("page_end")) if v2 else None, block_type=(e.get("block_type") or None) if v2 else None,
                    location=(e.get("location") or None) if v2 else None), e["content_version"]))
            return out

        return await asyncio.to_thread(_query)

    async def get_by_chunk_ids(self, chunk_ids: list[str]) -> dict[str, list[float]]:
        """HLD §18.1 — a primary-key `get` by chunk_id, NOT a fresh ANN
        search; fetches candidate embeddings for MMR's pairwise similarity.
        """
        if not chunk_ids:
            return {}

        quoted = ", ".join(f'"{cid}"' for cid in chunk_ids)
        filter_expr = f"chunk_id in [{quoted}]"

        collection = generations.current().milvus_collection

        def _query() -> dict[str, list[float]]:
            rows = _get_client().query(
                collection_name=collection,
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

        collections = [g.milvus_collection for g in generations.all_live()]   # a deleted file must vanish from every live generation

        def _delete() -> None:
            client = _get_client()
            for collection in collections:
                if client.has_collection(collection):
                    client.delete(collection_name=collection, filter=f'document_id == "{document_id}"')
                    client.flush(collection_name=collection)

        await asyncio.to_thread(_delete)

    async def ping(self) -> None:
        """Reachability probe for /health/ready (api mode) — raises if
        Milvus is unreachable."""
        await asyncio.to_thread(_get_client().has_collection, generations.current().milvus_collection)
