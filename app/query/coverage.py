"""The weak-retrieval signal: how well do the returned passages cover the question?

With the cross-encoder on, `top_score` is the best reranker score (sigmoid of the model's logit, 0-1) among the selected passages.
With it off there is no relevance model, so the signal is derived from RANKS: the best selected candidate's RRF score divided by the
largest possible (rank 1 in both legs), which is 1.0 when both engines put the same chunk first and falls as it slips down either
list. Thresholds are settings, calibrated on the benchmark (docs/retrieval-quality.md)."""
import math

from app.core.config import settings
from app.grpc.retrieval_pb2 import Coverage
from app.query.scoring import FusedCandidate


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-max(-60.0, min(60.0, x))))


def rank_score(c: FusedCandidate) -> float:
    """Best-match rank in each leg, folded into 0-1 (1.0 = first in both)."""
    return c.rrf_score / (2.0 / (settings.FUSION_RRF_K + 1))


def signal(selected: list[FusedCandidate], reranked: bool) -> tuple[float | None, int]:
    """(top_score, coverage enum value). No candidates means NONE. Previous-turn candidates never raise the signal on their own."""
    if not settings.COVERAGE_ENABLED:
        return None, Coverage.COVERAGE_UNSPECIFIED
    if not selected:
        return 0.0, Coverage.NONE
    if reranked and all(c.rerank_score is not None for c in selected):
        top = max(c.rerank_score for c in selected)
        strong, weak = settings.COVERAGE_RERANK_STRONG_MIN, settings.COVERAGE_RERANK_WEAK_MIN
    else:
        top = max(min(1.0, rank_score(c)) for c in selected if not c.from_previous_turn or c.dense_rank or c.bm25_rank) if any(
            (not c.from_previous_turn) or c.dense_rank or c.bm25_rank for c in selected) else 0.0
        strong, weak = settings.COVERAGE_RANK_STRONG_MIN, settings.COVERAGE_RANK_WEAK_MIN
    if top >= strong:
        return top, Coverage.STRONG
    if top >= weak:
        return top, Coverage.WEAK
    return top, Coverage.NONE
