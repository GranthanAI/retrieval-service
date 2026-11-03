import re

from app.query.scoring import FusedCandidate

_SHINGLE = 3


def _shingles(text: str) -> set[tuple[str, ...]]:
    words = re.findall(r"\w+", text.lower())
    if len(words) < _SHINGLE:
        return {tuple(words)} if words else set()
    return {tuple(words[i : i + _SHINGLE]) for i in range(len(words) - _SHINGLE + 1)}


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def drop_near_duplicates(candidates: list[FusedCandidate], threshold: float) -> list[FusedCandidate]:
    """Keeps the first (highest-ranked) of any group of candidates whose word-3-shingle Jaccard similarity exceeds `threshold`.
    Input order is preserved. Overlapping chunk windows and repeated boilerplate otherwise fill the final set with one passage."""
    if threshold >= 1.0:
        return list(candidates)
    kept: list[FusedCandidate] = []
    kept_shingles: list[set] = []
    for c in candidates:
        sh = _shingles(c.content)
        if any(jaccard(sh, k) > threshold for k in kept_shingles):
            continue
        kept.append(c)
        kept_shingles.append(sh)
    return kept
