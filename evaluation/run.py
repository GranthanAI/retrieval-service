"""Offline retrieval benchmark.

    uv run python -m evaluation.run --fake                 # CI: deterministic hash embedder + in-memory stores
    uv run python -m evaluation.run --real                 # all-MiniLM-L6-v2 + live Milvus/OpenSearch when reachable
    uv run python -m evaluation.run --real --label p1 --compare evaluation/results/baseline-real.json

Prints the metrics and writes them (with per-question detail) to evaluation/results/<label>-<mode>.json."""
import argparse
import asyncio
import json
import pathlib
import platform
import subprocess
import sys

from evaluation import harness, metrics
from evaluation.questions import QUESTIONS

RESULTS = pathlib.Path(__file__).resolve().parent / "results"


def _git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], capture_output=True, text=True, check=False).stdout.strip()
    except OSError:
        return ""


def _fmt(v) -> str:
    return "  n/a " if v is None else f"{v:6.3f}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fake", action="store_true", help="deterministic hash embedder, in-memory stores (CI)")
    mode.add_argument("--real", action="store_true", help="all-MiniLM-L6-v2 via sentence-transformers; live stores when reachable")
    ap.add_argument("--stores", choices=["auto", "fake", "real"], default="auto", help="--real only: force the store backend")
    ap.add_argument("--label", default="run", help="prefix of the result file name")
    ap.add_argument("--compare", type=pathlib.Path, help="a previous result JSON to print deltas against")
    ap.add_argument("--verbose", "-v", action="store_true", help="print one line per question")
    args = ap.parse_args(argv)

    import logging

    import structlog

    # The service logs one line per query; keep the benchmark output readable.
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))

    if args.fake:
        from evaluation.embedders import FakeEmbedder

        embedder, stores, mode_name = FakeEmbedder(), "fake", "fake"
    else:
        from evaluation.embedders import RealEmbedder

        embedder, mode_name = RealEmbedder(), "real"
        if args.stores == "auto":
            ok, why = harness.stores_reachable()
            stores = "real" if ok else "fake"
            if not ok:
                print(f"note: live Milvus/OpenSearch not reachable ({why}); using in-memory stores", file=sys.stderr)
        else:
            stores = args.stores

    results, stats, backend = asyncio.run(harness.run_benchmark(embedder, stores))
    summary = metrics.summarise(results)

    base = json.loads(args.compare.read_text()) if args.compare else None
    print(f"\nmode={mode_name} embedder={embedder.name} backend={backend}")
    print(f"corpus: {stats.files} files, {stats.chunks} chunks ({stats.seconds}s to ingest); questions: {summary.counts}")
    print(f"\n{'metric':<30}{'value':>8}" + (f"{'baseline':>10}{'delta':>9}" if base else ""))
    for k, v in summary.metrics.items():
        line = f"{k:<30}{_fmt(v):>8}"
        if base:
            b = base["metrics"].get(k)
            line += f"{_fmt(b):>10}" + (f"{v - b:+9.3f}" if v is not None and b is not None else "")
        print(line)
    print(f"\n{'category':<14}{'n':>3}{'R@5':>8}{'MRR@10':>8}")
    for cat, row in summary.by_category.items():
        print(f"{cat:<14}{row['n']:>3}{_fmt(row['recall@5']):>8}{_fmt(row['mrr@10']):>8}")
    print(f"\nlatency (pipeline.run, ms): p50={_pct([r.latency_ms for r in results], 50):.1f} p95={_pct([r.latency_ms for r in results], 95):.1f}")

    detail = []
    for r in results:
        rank = metrics.first_relevant_rank(r)
        detail.append({
            "id": r.question.id, "category": r.question.category, "query": r.question.query, "status": r.status,
            "first_relevant_rank": rank, "returned": len(r.chunks), "coverage": r.coverage,
            "top": [{"file": c.file, "section": c.section, "pages": [c.page_start, c.page_end], "relevant": metrics.is_relevant(c, r.question)} for c in r.chunks[:10]],
        })
        if args.verbose:
            print(f"  {r.question.id:<18} first_rel_rank={rank} returned={len(r.chunks)} status={r.status}")

    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"{args.label}-{mode_name}.json"
    out.write_text(json.dumps({
        "label": args.label, "mode": mode_name, "embedder": embedder.name, "backend": backend, "git": _git("rev-parse", "--short", "HEAD"),
        "python": platform.python_version(), "ingest": {"chunks": stats.chunks, "per_file": stats.chunks_per_file},
        "metrics": summary.metrics, "by_category": summary.by_category, "counts": summary.counts, "questions": detail,
    }, indent=2) + "\n")
    print(f"\nwrote {out}")
    return 0


def _pct(xs: list[float], p: int) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else 0.0


if __name__ == "__main__":
    sys.exit(main())
