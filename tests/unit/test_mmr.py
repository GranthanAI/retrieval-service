from app.query import mmr
from app.query.scoring import FusedCandidate


def _candidate(chunk_id: str, rrf_score: float) -> FusedCandidate:
    return FusedCandidate(chunk_id=chunk_id, rrf_score=rrf_score)


class TestMMRSelection:
    def test_lambda_near_one_matches_pure_relevance_order(self) -> None:
        candidates = [_candidate("a", 0.9), _candidate("b", 0.5), _candidate("c", 0.1)]
        embeddings = {
            "a": [1.0, 0.0, 0.0],
            "b": [0.99, 0.01, 0.0],  # near-duplicate of a
            "c": [0.0, 0.0, 1.0],  # orthogonal
        }

        selected = mmr.select(candidates, embeddings, k=3, lambda_=0.999)

        assert [c.chunk_id for c in selected] == ["a", "b", "c"]  # pure relevance order preserved

    def test_lambda_near_zero_diversifies_away_from_near_duplicates(self) -> None:
        # "a" and "b" are near-duplicate vectors; "c" is orthogonal but has
        # lower raw relevance. With diversity dominating, "c" should be
        # picked before "b" (a near-duplicate of the already-selected "a").
        candidates = [_candidate("a", 0.9), _candidate("b", 0.85), _candidate("c", 0.5)]
        embeddings = {
            "a": [1.0, 0.0, 0.0],
            "b": [0.99, 0.01, 0.0],
            "c": [0.0, 1.0, 0.0],
        }

        selected = mmr.select(candidates, embeddings, k=2, lambda_=0.01)

        assert selected[0].chunk_id == "a"  # highest relevance always picked first (nothing selected yet)
        assert selected[1].chunk_id == "c"  # diversity dominates over b's higher raw relevance

    def test_selection_respects_k(self) -> None:
        candidates = [_candidate(str(i), 1.0 - i * 0.1) for i in range(10)]
        embeddings = {str(i): [float(i), 0.0] for i in range(10)}

        selected = mmr.select(candidates, embeddings, k=4, lambda_=0.7)

        assert len(selected) == 4

    def test_mmr_score_is_set_on_selected_candidates_for_internal_tracking_only(self) -> None:
        candidates = [_candidate("a", 0.9), _candidate("b", 0.5)]
        embeddings = {"a": [1.0, 0.0], "b": [0.0, 1.0]}

        selected = mmr.select(candidates, embeddings, k=2, lambda_=0.7)

        assert all(c.mmr_score is not None for c in selected)
