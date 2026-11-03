"""The benchmark's own metric functions: if these are wrong every reported number is wrong."""
import math

import pytest

from evaluation import metrics
from evaluation.metrics import QuestionResult, RankedChunk
from evaluation.questions import Question


def _q(evidence, pages=(), expected_files=(), category="phase"):
    return Question("q", category, "q?", ["a.md"], list(evidence), list(pages), list(expected_files))


def _c(text, file="a.md", page=None, end=None):
    return RankedChunk(file=file, content=text, page_start=page, page_end=end)


def _r(q, chunks, **kw):
    return QuestionResult(q, chunks, **kw)


def test_relevance_is_case_and_whitespace_insensitive_and_file_specific():
    q = _q([("a.md", "exit  criteria")])
    assert metrics.is_relevant(_c("The Exit\ncriteria hold"), q)
    assert not metrics.is_relevant(_c("The Exit criteria hold", file="b.md"), q)


def test_recall_counts_each_gold_evidence_once():
    q = _q([("a.md", "alpha"), ("a.md", "beta")])
    r = _r(q, [_c("x"), _c("alpha"), _c("y"), _c("z"), _c("w"), _c("beta")])
    assert metrics.recall_at(r, 5) == 0.5
    assert metrics.recall_at(r, 10) == 1.0


def test_mrr_uses_first_relevant_rank_and_zero_when_missing():
    q = _q([("a.md", "alpha")])
    assert metrics.rr_at(_r(q, [_c("x"), _c("x"), _c("alpha")])) == pytest.approx(1 / 3)
    assert metrics.rr_at(_r(q, [_c("x")])) == 0.0
    assert metrics.rr_at(_r(q, [_c("x")] * 10 + [_c("alpha")])) == 0.0  # rank 11 is outside @10


def test_ndcg_is_one_for_ideal_order_and_lower_when_relevant_chunk_is_late():
    q = _q([("a.md", "alpha")])
    assert metrics.ndcg_at(_r(q, [_c("alpha"), _c("x")])) == pytest.approx(1.0)
    assert metrics.ndcg_at(_r(q, [_c("x"), _c("x"), _c("alpha")])) == pytest.approx(1 / math.log2(4))


def test_page_attribution_needs_the_evidence_page_inside_the_reported_range():
    q = _q([("a.pdf", "fact")], pages=[5], category="page")
    assert metrics.page_correct(_r(q, [_c("fact", "a.pdf", page=5)]))
    assert metrics.page_correct(_r(q, [_c("fact", "a.pdf", page=3, end=5)]))
    assert not metrics.page_correct(_r(q, [_c("fact", "a.pdf", page=3)]))  # section start only: the known provenance bug
    assert not metrics.page_correct(_r(q, [_c("fact", "a.pdf")]))
    assert not metrics.page_correct(_r(q, [_c("nothing", "a.pdf", page=5)]))


def test_file_coverage_requires_a_relevant_chunk_per_expected_file():
    q = _q([("a.md", "alpha"), ("b.md", "beta")], expected_files=["a.md", "b.md"])
    assert metrics.file_coverage(_r(q, [_c("alpha"), _c("beta", "a.md")])) == 0.5
    assert metrics.file_coverage(_r(q, [_c("alpha"), _c("beta", "b.md")])) == 1.0


def test_summary_no_answer_precision_and_recall():
    ans = _q([("a.md", "alpha")])
    na1, na2 = _q([], category="unanswerable"), _q([], category="unanswerable")
    results = [
        _r(ans, [_c("alpha")]),                  # answered
        _r(na1, [], status="NO_MATCH"),          # correctly abstained
        _r(na2, [_c("junk")]),                   # should have abstained, did not
    ]
    m = metrics.summarise(results).metrics
    assert m["no_answer_precision"] == 1.0   # the only abstention was right
    assert m["no_answer_recall"] == 0.5      # one of two unanswerable questions caught


def test_weak_coverage_counts_as_abstention_once_the_field_exists():
    na = _q([], category="unanswerable")
    assert metrics.abstained(_r(na, [_c("junk")], coverage="WEAK"))
    assert not metrics.abstained(_r(na, [_c("junk")], coverage="STRONG"))
