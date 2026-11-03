import math

from app.core.config import settings
from app.query.scoring import FusedCandidate


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def normalise_relevance(pool: list[FusedCandidate]) -> None:
    """Min-max scales `rrf_score` to 0-1 within `pool` (sets `norm_relevance`). RRF scores are about 0.01-0.03 while the similarity
    term MMR subtracts is 0-1, so on raw scores diversity always wins. A pool whose scores are all equal is all-relevant (1.0)."""
    if not pool:
        return
    lo, hi = min(c.rrf_score for c in pool), max(c.rrf_score for c in pool)
    for c in pool:
        c.norm_relevance = 1.0 if hi == lo else (c.rrf_score - lo) / (hi - lo)


def select(
    candidates: list[FusedCandidate],
    embeddings: dict[str, list[float]],
    k: int,
    lambda_: float,
    relevance_floor: float = 0.0,
    seed: list[FusedCandidate] | None = None,
) -> list[FusedCandidate]:
    """LLD §7.5 — Maximal Marginal Relevance selection. Relevance is the pool-normalised `rrf_score` (0-1), so it is on the same scale
    as cosine similarity. Only candidates with normalised relevance >= `relevance_floor` are diversified over; if fewer than `k`
    qualify, the remainder is filled in relevance order. `seed` are already-chosen candidates (e.g. the per-file picks): they count
    toward `k` and toward the similarity penalty, and are returned first. `response_mapper.py` re-sorts the result by `rrf_score`
    (MMR decides membership only, HLD §18.2).
    """
    pool = candidates[: settings.MMR_CANDIDATE_POOL_SIZE]
    seeded = list(seed or [])
    if any(c.norm_relevance is None for c in pool + seeded):
        normalise_relevance(list({c.chunk_id: c for c in pool + seeded}.values()))
    selected: list[FusedCandidate] = list(seeded)
    taken = {c.chunk_id for c in selected}
    eligible = [c for c in pool if c.chunk_id not in taken and c.norm_relevance >= relevance_floor]
    rest = [c for c in pool if c.chunk_id not in taken and c.norm_relevance < relevance_floor]

    def mmr_score(c: FusedCandidate) -> float:
        if not selected:
            return c.norm_relevance
        max_sim = max(_cosine_similarity(embeddings[c.chunk_id], embeddings[s.chunk_id]) for s in selected)
        return lambda_ * c.norm_relevance - (1 - lambda_) * max_sim

    while eligible and len(selected) < k:
        best = max(eligible, key=mmr_score)
        best.mmr_score = mmr_score(best)  # internal tracking only, HLD §18.2 — never mapped to the proto
        selected.append(best)
        eligible.remove(best)

    for c in sorted(rest, key=lambda c: c.rrf_score, reverse=True):
        if len(selected) >= k:
            break
        selected.append(c)
    return selected


def best_per_file(pool: list[FusedCandidate], floor: float, k: int) -> list[FusedCandidate]:
    """Each file's top candidate whose normalised relevance is >= `floor`, best first, at most `k`. Used when the request names
    two or more files so one verbose file cannot crowd out the others."""
    best: dict[str, FusedCandidate] = {}
    for c in pool:  # pool is rrf-descending, so the first seen per file is its best
        if c.document_id not in best and (c.norm_relevance or 0.0) >= floor:
            best[c.document_id] = c
    return sorted(best.values(), key=lambda c: c.rrf_score, reverse=True)[:k]
