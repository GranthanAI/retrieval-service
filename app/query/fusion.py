from app.query.scoring import FusedCandidate
from app.stores.types import BM25SearchHit, DenseSearchHit


def _new_candidate(chunk_id: str, hit: DenseSearchHit | BM25SearchHit) -> FusedCandidate:
    return FusedCandidate(
        chunk_id=chunk_id,
        document_id=hit.document_id,
        filename=hit.filename,
        content=hit.content,
        page_number=hit.page_number,
        section=hit.section,
        chunk_index=hit.chunk_index,
    )


def reciprocal_rank_fusion(
    dense_hits: list[DenseSearchHit], bm25_hits: list[BM25SearchHit], k: int
) -> list[FusedCandidate]:
    """LLD §7.4 — standard Reciprocal Rank Fusion. A candidate present in
    only one input list still accumulates an `rrf_score` from that list
    alone (HLD §18.2) — no special-casing needed, `setdefault` plus
    independent accumulation naturally produces that.
    """
    scores: dict[str, FusedCandidate] = {}

    for rank, hit in enumerate(dense_hits, start=1):
        c = scores.setdefault(hit.chunk_id, _new_candidate(hit.chunk_id, hit))
        c.dense_rank, c.dense_score = rank, hit.score
        c.rrf_score += 1.0 / (k + rank)

    for rank, hit in enumerate(bm25_hits, start=1):
        c = scores.setdefault(hit.chunk_id, _new_candidate(hit.chunk_id, hit))
        c.bm25_rank, c.bm25_score = rank, hit.score
        c.rrf_score += 1.0 / (k + rank)

    return sorted(scores.values(), key=lambda c: c.rrf_score, reverse=True)
