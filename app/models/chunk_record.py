from dataclasses import dataclass


@dataclass(frozen=True)
class ChunkRecord:
    """LLD §4/§5 — the single record shape both stores' `upsert_chunks`
    share, one per persisted chunk. Produced at ingestion time (LLD §6.2)
    and written to Milvus (dense vector + metadata) and OpenSearch (same
    metadata + full-text, no embedding) from the exact same object, so the
    two indexes can never drift on what a "chunk" contains.

    Lives under `app/models/` (not `app/query/` or `app/ingestion/`) so
    both packages — and `app/stores/` — can import it without a circular
    import; it is a plain dataclass, not a SQLAlchemy model, and is never
    imported by `app/db/base.py`.
    """

    chunk_id: str
    document_id: str
    content_version: int
    chunk_index: int
    owner_id: str
    conversation_id: str | None
    filename: str
    page_number: int | None
    section: str | None
    content: str
    embedding: list[float]
