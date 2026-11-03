"""Metric definitions. Pure functions over ranked results; no I/O, so they are unit-tested directly (tests/unit/test_eval_metrics.py).

A "result" is the ranked chunk list returned by `pipeline.run`, as `RankedChunk`s. Relevance is text-based (see questions.py)."""
import math
import re
from dataclasses import dataclass, field

from evaluation.questions import Question


@dataclass
class RankedChunk:
    file: str
    content: str
    page_start: int | None = None
    page_end: int | None = None
    section: str = ""


@dataclass
class QuestionResult:
    question: Question
    chunks: list[RankedChunk]
    status: str = "OK"
    coverage: str | None = None  # WEAK/NONE/STRONG once Phase 3 adds the field; None before then
    latency_ms: float = 0.0
    note: str = ""


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def covers(chunk: RankedChunk, evidence: tuple[str, str]) -> bool:
    file, text = evidence
    return chunk.file == file and norm(text) in norm(chunk.content)


def is_relevant(chunk: RankedChunk, q: Question) -> bool:
    return any(covers(chunk, e) for e in q.evidence)


def recall_at(result: QuestionResult, k: int) -> float:
    """Fraction of gold evidences present in the top k chunks."""
    q = result.question
    top = result.chunks[:k]
    return sum(any(covers(c, e) for c in top) for e in q.evidence) / len(q.evidence)


def rr_at(result: QuestionResult, k: int = 10) -> float:
    for rank, c in enumerate(result.chunks[:k], start=1):
        if is_relevant(c, result.question):
            return 1.0 / rank
    return 0.0


def ndcg_at(result: QuestionResult, k: int = 10) -> float:
    """Binary-gain nDCG. A chunk is relevant if it holds any gold evidence; the ideal ranking has min(#evidence, k) relevant chunks."""
    q = result.question
    dcg = sum(1.0 / math.log2(rank + 1) for rank, c in enumerate(result.chunks[:k], start=1) if is_relevant(c, q))
    ideal = sum(1.0 / math.log2(rank + 1) for rank in range(1, min(len(q.evidence), k) + 1))
    return dcg / ideal if ideal else 0.0


def page_correct(result: QuestionResult, k: int = 5) -> bool:
    """The best relevant chunk in the top k reports a page range that contains a page where the evidence is printed."""
    q = result.question
    for c in result.chunks[:k]:
        if is_relevant(c, q):
            if c.page_start is None:
                return False
            end = c.page_end if c.page_end is not None else c.page_start
            return any(c.page_start <= p <= end for p in q.pages)
    return False


def file_coverage(result: QuestionResult, k: int = 10) -> float:
    """Fraction of the files the question needs that have a relevant chunk in the top k."""
    q = result.question
    top = result.chunks[:k]
    hit = {f for f in q.expected_files if any(c.file == f and is_relevant(c, q) for c in top)}
    return len(hit) / len(q.expected_files)


def abstained(result: QuestionResult) -> bool:
    """Did the system say 'nothing relevant'? Today only an empty result counts; once Phase 3 lands, coverage WEAK/NONE does too."""
    return not result.chunks or result.coverage in ("WEAK", "NONE")


@dataclass
class Summary:
    """metrics: name -> mean (None when no question applies). `fractions`: name -> (sum of per-question scores, number of questions),
    so a reader can see that 0.909 is 20/22 and that one question moves it by 4.5 points."""

    metrics: dict[str, float | None] = field(default_factory=dict)
    fractions: dict[str, tuple[float, int]] = field(default_factory=dict)
    by_category: dict[str, dict] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)

    def show(self, name: str) -> str:
        """'0.909 (20/22)'; ratios only for metrics that are a share of questions, 'n=22' for rank-based ones."""
        v = self.metrics.get(name)
        if v is None:
            return "n/a"
        total, n = self.fractions[name]
        if name.startswith(("mrr", "ndcg")):
            return f"{v:.3f} (n={n})"
        return f"{v:.3f} ({total:g}/{n})" if abs(total - round(total)) < 1e-9 else f"{v:.3f} ({total:.1f}/{n})"


def _mean(xs: list[float]) -> float | None:
    return round(sum(xs) / len(xs), 4) if xs else None


def summarise(results: list[QuestionResult]) -> Summary:
    answerable = [r for r in results if r.question.answerable]
    unanswerable = [r for r in results if not r.question.answerable]
    s = Summary()

    def put(name: str, scores: list[float]) -> None:
        s.metrics[name] = _mean(scores)
        s.fractions[name] = (round(sum(scores), 4), len(scores))

    put("recall@5", [recall_at(r, 5) for r in answerable])
    put("recall@8", [recall_at(r, 8) for r in answerable])
    put("recall@10", [recall_at(r, 10) for r in answerable])
    put("mrr@10", [rr_at(r) for r in answerable])
    put("ndcg@10", [ndcg_at(r) for r in answerable])
    put("identifier_exact_recall@5", [recall_at(r, 5) for r in answerable if r.question.category == "identifier"])
    pages = [r for r in answerable if r.question.pages]
    put("page_attribution_accuracy", [float(page_correct(r)) for r in pages])  # retrieved AND right page, over all page questions
    found = [r for r in pages if recall_at(r, 5) > 0]
    put("page_given_retrieved", [float(page_correct(r)) for r in found])       # right page, over page questions whose chunk was retrieved
    put("multi_file_coverage", [file_coverage(r) for r in answerable if r.question.expected_files])
    tp = sum(abstained(r) for r in unanswerable)          # abstained and there was nothing to find
    fp = sum(abstained(r) for r in answerable)            # abstained although the answer exists
    s.metrics["no_answer_precision"] = round(tp / (tp + fp), 4) if tp + fp else None   # of all abstentions, how many were right
    s.fractions["no_answer_precision"] = (tp, tp + fp)
    put("no_answer_recall", [float(abstained(r)) for r in unanswerable])               # of all unanswerable questions, how many were caught

    for cat in sorted({r.question.category for r in answerable}):
        rs = [r for r in answerable if r.question.category == cat]
        s.by_category[cat] = {
            "n": len(rs),
            "recall@5": _mean([recall_at(r, 5) for r in rs]), "recall@5_sum": round(sum(recall_at(r, 5) for r in rs), 4),
            "mrr@10": _mean([rr_at(r) for r in rs]), "ndcg@10": _mean([ndcg_at(r) for r in rs]),
        }
    s.counts = {"questions": len(results), "answerable": len(answerable), "unanswerable": len(unanswerable)}
    return s


def first_relevant_rank(result: QuestionResult, k: int = 10) -> int | None:
    for rank, c in enumerate(result.chunks[:k], start=1):
        if is_relevant(c, result.question):
            return rank
    return None
