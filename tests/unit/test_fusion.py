from app.query import fusion
from app.query.fusion import reciprocal_rank_fusion
from app.stores.types import BM25SearchHit, DenseSearchHit


def _dense(chunk_id: str, score: float) -> DenseSearchHit:
    return DenseSearchHit(chunk_id=chunk_id, score=score, document_id="doc-1", filename="a.pdf", content="text")


def _bm25(chunk_id: str, score: float) -> BM25SearchHit:
    return BM25SearchHit(chunk_id=chunk_id, score=score, document_id="doc-1", filename="a.pdf", content="text")


class TestReciprocalRankFusion:
    def test_hand_computed_rrf_values_for_a_small_fixed_input(self) -> None:
        k = 60
        dense_hits = [_dense("a", 0.9), _dense("b", 0.8)]
        bm25_hits = [_bm25("b", 10.0), _bm25("a", 5.0)]

        fused = reciprocal_rank_fusion(dense_hits, bm25_hits, k=k)
        by_id = {c.chunk_id: c for c in fused}

        # a: dense_rank=1, bm25_rank=2 -> 1/61 + 1/62
        # b: dense_rank=2, bm25_rank=1 -> 1/62 + 1/61
        expected = 1.0 / (k + 1) + 1.0 / (k + 2)
        assert by_id["a"].rrf_score == expected
        assert by_id["b"].rrf_score == expected
        assert by_id["a"].dense_rank == 1
        assert by_id["a"].bm25_rank == 2
        assert by_id["b"].dense_rank == 2
        assert by_id["b"].bm25_rank == 1

    def test_candidate_present_in_only_one_list_still_gets_a_correct_rrf_score(self) -> None:
        k = 60
        dense_hits = [_dense("dense-only", 0.9)]
        bm25_hits: list[BM25SearchHit] = []

        fused = reciprocal_rank_fusion(dense_hits, bm25_hits, k=k)

        assert len(fused) == 1
        candidate = fused[0]
        assert candidate.rrf_score == 1.0 / (k + 1)
        assert candidate.dense_rank == 1
        assert candidate.bm25_rank is None
        assert candidate.bm25_score is None

    def test_sorted_descending_by_rrf_score(self) -> None:
        # Input order IS rank order (best first) — "first" should win.
        dense_hits = [_dense("first", 0.9), _dense("second", 0.1)]
        fused = reciprocal_rank_fusion(dense_hits, [], k=60)
        assert [c.chunk_id for c in fused] == ["first", "second"]

    def test_provenance_fields_carried_onto_the_fused_candidate(self) -> None:
        hit = DenseSearchHit(
            chunk_id="x",
            score=0.5,
            document_id="doc-9",
            filename="report.pdf",
            content="hello world",
            page_number=3,
            section="Intro",
            chunk_index=2,
        )
        fused = reciprocal_rank_fusion([hit], [], k=60)
        c = fused[0]
        assert c.document_id == "doc-9"
        assert c.filename == "report.pdf"
        assert c.content == "hello world"
        assert c.page_number == 3
        assert c.section == "Intro"
        assert c.chunk_index == 2


class TestHeadingMatchBoost:
    def _cands(self):
        from app.query.scoring import FusedCandidate

        return [
            FusedCandidate("a", section="Phase 5: Invoice generation", rrf_score=0.031),
            FusedCandidate("b", section="Phase 6: Payment gateway webhooks", rrf_score=0.027),
            FusedCandidate("c", section=None, rrf_score=0.026),
        ]

    def test_section_sharing_a_word_pair_with_the_query_moves_to_the_top(self):
        out = fusion.boost_heading_matches(self._cands(), "what is phase 6 about?", bonus=1 / 61)
        assert [c.chunk_id for c in out] == ["b", "a", "c"]

    def test_stop_word_only_pairs_and_disabled_bonus_change_nothing(self):
        assert [c.chunk_id for c in fusion.boost_heading_matches(self._cands(), "what is the", bonus=1 / 61)] == ["a", "b", "c"]
        assert [c.chunk_id for c in fusion.boost_heading_matches(self._cands(), "phase 6", bonus=0)] == ["a", "b", "c"]

    def test_hinglish_query_matches_too(self):
        out = fusion.boost_heading_matches(self._cands(), "Phase 6 mein kya karna hai?", bonus=1 / 61)
        assert out[0].chunk_id == "b"
