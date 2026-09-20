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


def select(
    candidates: list[FusedCandidate], embeddings: dict[str, list[float]], k: int, lambda_: float
) -> list[FusedCandidate]:
    """LLD §7.5 — Maximal Marginal Relevance selection. `rrf_score` is the
    relevance term MMR balances against diversity — NOT what ships on the
    wire (`response_mapper.py` re-sorts the selected set by `rrf_score`
    afterward, since MMR decides membership/order-of-selection only,
    HLD §18.2).
    """
    pool = candidates[: settings.MMR_CANDIDATE_POOL_SIZE]
    selected: list[FusedCandidate] = []
    remaining = list(pool)

    while remaining and len(selected) < k:

        def mmr_score(c: FusedCandidate) -> float:
            relevance = c.rrf_score
            if not selected:
                return relevance
            max_sim = max(
                _cosine_similarity(embeddings[c.chunk_id], embeddings[s.chunk_id]) for s in selected
            )
            return lambda_ * relevance - (1 - lambda_) * max_sim

        best = max(remaining, key=mmr_score)
        best.mmr_score = mmr_score(best)  # internal tracking only, HLD §18.2 — never mapped to the proto
        selected.append(best)
        remaining.remove(best)

    return selected
