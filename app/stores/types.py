from dataclasses import dataclass


@dataclass(frozen=True)
class DenseSearchHit:
    """One Milvus ANN search result (LLD §7/§18). `dense_rank`/`rrf_score`
    are computed by fusion.py (Phase 8) from this list's position, not
    stored here — a store-layer hit only knows its own chunk_id and score.
    """

    chunk_id: str
    score: float


@dataclass(frozen=True)
class BM25SearchHit:
    """One OpenSearch BM25 search result (LLD §7/§18)."""

    chunk_id: str
    score: float
