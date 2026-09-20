from dataclasses import dataclass


@dataclass(frozen=True)
class DenseSearchHit:
    """One Milvus ANN search result (LLD §7/§18). `dense_rank`/`rrf_score`
    are computed by fusion.py (Phase 8) from this list's position, not
    stored here — a store-layer hit only knows its own chunk_id and score.

    Carries the chunk's provenance fields (document_id, filename, content,
    page_number, section, chunk_index) alongside the score — Phase 8's
    response_mapper needs these to build the final DocumentChunk, and
    fetching them here (already present on the Milvus record returned by
    the same ANN search) avoids a second per-chunk lookup after fusion/MMR.
    """

    chunk_id: str
    score: float
    document_id: str = ""
    filename: str = ""
    content: str = ""
    page_number: int | None = None
    section: str | None = None
    chunk_index: int = 0


@dataclass(frozen=True)
class BM25SearchHit:
    """One OpenSearch BM25 search result (LLD §7/§18) — same provenance
    fields as DenseSearchHit, for the same reason."""

    chunk_id: str
    score: float
    document_id: str = ""
    filename: str = ""
    content: str = ""
    page_number: int | None = None
    section: str | None = None
    chunk_index: int = 0
