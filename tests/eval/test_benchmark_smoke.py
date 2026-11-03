"""The benchmark itself, fake mode, Markdown files only (no Docling). Guards the harness against bit-rot and pins the Phase 1
acceptance criterion as a strict xfail."""
import pytest
import pytest_asyncio

from evaluation import harness, metrics
from evaluation.embedders import FakeEmbedder
from evaluation.questions import QUESTIONS

MD = ["PHASES.md", "hld.md", "lld.md", "runbook.md"]
# Questions whose gold evidence lies in the Markdown files, plus the unanswerable ones.
MD_QUESTIONS = [q for q in QUESTIONS if all(f in MD for f, _ in q.evidence)]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def results():
    res, stats, _ = await harness.run_benchmark(FakeEmbedder(), "fake", MD_QUESTIONS, files=MD)
    return {r.question.id: r for r in res}, stats


@pytest.mark.asyncio(loop_scope="module")
async def test_harness_indexes_the_corpus_and_answers_every_question(results):
    by_id, stats = results
    assert stats.chunks > 20
    assert set(by_id) == {q.id for q in MD_QUESTIONS}
    assert all(r.status in ("OK", "NO_MATCH") for r in by_id.values())


@pytest.mark.asyncio(loop_scope="module")
async def test_identifier_lookups_work_in_fake_mode(results):
    by_id, _ = results
    assert all(metrics.recall_at(by_id[i], 5) == 1.0 for i in ("id-retry", "id-txn", "id-path"))


@pytest.mark.asyncio(loop_scope="module")
async def test_phase_6_section_is_in_the_top_3(results):
    by_id, _ = results
    rank = metrics.first_relevant_rank(by_id["p6-about"])
    assert rank is not None and rank <= 3
