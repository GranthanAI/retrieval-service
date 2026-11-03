"""Phase 1 query-time behaviour: MMR floor/seed, near-duplicate removal, per-file balance, keyword query shape, token budget."""
import pytest

from app.core.config import settings
from app.query import dedupe, mmr, pipeline
from app.query.scoring import FusedCandidate
from app.stores import opensearch_repository as os_module
from app.stores.opensearch_repository import OpenSearchRepository
from evaluation.fakes import FakeOpenSearchClient


def _c(cid, score, doc="d1", content=None):
    return FusedCandidate(cid, document_id=doc, rrf_score=score, content=content or f"unique words for {cid} " * 5)


class TestMMR:
    def test_normalise_scales_to_unit_interval(self):
        pool = [_c("a", 0.03), _c("b", 0.02), _c("c", 0.01)]
        mmr.normalise_relevance(pool)
        assert [c.norm_relevance for c in pool] == pytest.approx([1.0, 0.5, 0.0])

    def test_equal_scores_are_all_fully_relevant(self):
        pool = [_c("a", 0.02), _c("b", 0.02)]
        mmr.normalise_relevance(pool)
        assert [c.norm_relevance for c in pool] == [1.0, 1.0]

    def test_floor_keeps_weak_candidates_out_of_diversification(self):
        pool = [_c("a", 0.03), _c("b", 0.029), _c("w", 0.001)]
        emb = {"a": [1, 0], "b": [1, 0.1], "w": [0, 1]}
        assert {c.chunk_id for c in mmr.select(pool, emb, 2, 0.1, relevance_floor=0.5)} == {"a", "b"}

    def test_weak_candidates_still_fill_k_when_too_few_qualify(self):
        pool = [_c("a", 0.03), _c("w1", 0.002), _c("w2", 0.001)]
        emb = {c.chunk_id: [1, 0] for c in pool}
        assert [c.chunk_id for c in mmr.select(pool, emb, 3, 0.7, relevance_floor=0.5)] == ["a", "w1", "w2"]

    def test_seed_counts_toward_k_and_is_returned_first(self):
        pool = [_c("a", 0.03), _c("b", 0.029), _c("c", 0.028)]
        mmr.normalise_relevance(pool)
        emb = {"a": [1, 0], "b": [0, 1], "c": [1, 1]}
        out = mmr.select(pool, emb, 2, 0.7, seed=[pool[2]])
        assert out[0].chunk_id == "c" and len(out) == 2

    def test_best_per_file_respects_floor_and_k(self):
        pool = [_c("a1", 0.03, "A"), _c("a2", 0.029, "A"), _c("b1", 0.02, "B"), _c("c1", 0.001, "C")]
        mmr.normalise_relevance(pool)
        assert [c.chunk_id for c in mmr.best_per_file(pool, 0.5, 10)] == ["a1", "b1"]
        assert [c.chunk_id for c in mmr.best_per_file(pool, 0.5, 1)] == ["a1"]


class TestDedupe:
    def test_drops_lower_ranked_near_duplicate_and_keeps_distinct(self):
        text = " ".join(f"token{i}" for i in range(60))
        near = text + " extra"
        out = dedupe.drop_near_duplicates([_c("a", 3, content=text), _c("b", 2, content=near), _c("c", 1, content="something else entirely different here")], 0.9)
        assert [c.chunk_id for c in out] == ["a", "c"]

    def test_threshold_one_disables(self):
        a = _c("a", 3, content="same same same same")
        assert len(dedupe.drop_near_duplicates([a, _c("b", 2, content="same same same same")], 1.0)) == 2


class TestKeywordQuery:
    async def _body(self, monkeypatch, enabled=True):
        fake = FakeOpenSearchClient()
        monkeypatch.setattr(os_module, "_client", fake)
        monkeypatch.setattr(settings, "OPENSEARCH_PHRASE_BOOST_ENABLED", enabled)
        await OpenSearchRepository().search("phase 6", 10, {"d1": 1})
        return fake.queries[0]["query"]["bool"]

    async def test_phrase_boosts_are_should_and_scope_is_a_filter(self, monkeypatch):
        b = await self._body(monkeypatch)
        assert "multi_match" in b["must"]
        boosts = {list(c["match_phrase"])[0]: list(c["match_phrase"].values())[0]["boost"] for c in b["should"]}
        assert boosts == {"section": 5.0, "content": 2.0}
        assert "minimum_should_match" not in b          # otherwise a phrase hit would satisfy it and widen the scope
        assert b["filter"][0]["bool"]["minimum_should_match"] == 1

    async def test_flag_off_removes_the_boosts(self, monkeypatch):
        assert "should" not in await self._body(monkeypatch, enabled=False)

    async def test_phrase_hit_outside_the_scope_is_not_returned(self, monkeypatch):
        fake = FakeOpenSearchClient()
        monkeypatch.setattr(os_module, "_client", fake)
        docs = {
            "in": {"document_id": "d1", "content_version": 1, "content": "alpha beta", "section": "x", "filename": "a"},
            "out": {"document_id": "d2", "content_version": 1, "content": "gamma delta", "section": "x", "filename": "b"},
        }
        fake.bulk([y for k, d in docs.items() for y in ({"index": {"_id": k}}, {**d, "chunk_id": k, "chunk_index": 0, "page_number": None})])
        hits = await OpenSearchRepository().search("gamma delta alpha", 10, {"d1": 1})
        assert [h.chunk_id for h in hits] == ["in"]


class TestTokenBudget:
    def test_fit_tokens_drops_from_the_tail_and_zero_means_unlimited(self):
        ranked = [_c(str(i), 1 - i / 10, content=" ".join(["w"] * 50)) for i in range(4)]
        assert len(pipeline._fit_tokens(ranked, 0)) == 4
        kept = pipeline._fit_tokens(ranked, 120)
        assert [c.chunk_id for c in kept] == ["0", "1"]
        assert pipeline._fit_tokens(ranked, 10) == []


def test_token_count_is_not_capped_or_padded():
    from app.query import tokens

    assert tokens.count(" ".join(["w"] * 50)) == 50
    assert tokens.count(" ".join(["w"] * 500)) == 500
    assert tokens.count("") == 0
