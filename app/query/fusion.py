import re

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
        heading_path=hit.heading_path,
        page_start=hit.page_start,
        page_end=hit.page_end,
        block_type=hit.block_type,
        location=hit.location,
    )


def reciprocal_rank_fusion(
    dense_hits: list[DenseSearchHit], bm25_hits: list[BM25SearchHit], k: int,
    previous_hits: list[DenseSearchHit] | None = None, previous_weight: float = 1.0,
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

    # Chunks shown in the previous assistant turn: a third ranked list ("check again", "explain point 3"), already authorised and
    # version-checked by the caller.
    for rank, hit in enumerate(previous_hits or [], start=1):
        c = scores.setdefault(hit.chunk_id, _new_candidate(hit.chunk_id, hit))
        c.from_previous_turn = True
        c.rrf_score += previous_weight / (k + rank)

    return sorted(scores.values(), key=lambda c: c.rrf_score, reverse=True)


_STOP = frozenset(
    "a an and are as at be by do does for from how i in is it me of on or so tell that the this to was what when where which who why with you "
    "mein kya hai ka ki ke ko se par aur ye yeh woh batao bataiye karna".split()
)


def _words(text: str) -> list[str]:
    return re.findall(r"\w+", text.lower())


def boost_heading_matches(candidates: list[FusedCandidate], query: str, bonus: float) -> list[FusedCandidate]:
    """Adds `bonus` to the RRF score of every candidate whose section heading contains a word pair from the query verbatim, then
    re-sorts. "what is phase 6 about?" shares the pair "phase 6" with the heading "Phase 6: Payment gateway webhooks" and with no
    other. Rank fusion alone cannot do this: the section is BM25 rank 1, but its dense rank is poor because the twelve phase
    sections read alike, so the fused rank is mid-table. A pair made only of stop words never counts. `bonus` is one extra
    rank-1 vote (1 / (k + 1)); 0 disables it."""
    if bonus <= 0 or not candidates:
        return candidates
    q = _words(query)
    pairs = {(a, b) for a, b in zip(q, q[1:]) if not (a in _STOP and b in _STOP)}
    if not pairs:
        return candidates
    for c in candidates:
        w = _words(c.section or "")
        if any(pair in pairs for pair in zip(w, w[1:])):
            c.rrf_score += bonus
    return sorted(candidates, key=lambda c: c.rrf_score, reverse=True)
