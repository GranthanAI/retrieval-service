"""Offline retrieval benchmark.

    uv run python -m evaluation.run --fake                       # CI: deterministic hash embedder + in-memory stores
    uv run python -m evaluation.run --real                       # all-MiniLM-L6-v2 + live Milvus/OpenSearch when reachable
    uv run python -m evaluation.run --real --label p1 --compare evaluation/results/baseline-real.json
    uv run python -m evaluation.run --real --sets main,heldout \\
        --config boost0:HEADING_MATCH_BOOST=0 --config boost1:HEADING_MATCH_BOOST=1.0 --config k5:top_k=5 --config k8:top_k=8

Question sets: `main` (tuning set, questions.py) and `heldout` (heldout.py: report only, never tune against it; no per-question rows
are printed for it). The corpus is indexed once and every set is answered under every --config. A --config is
`name:KEY=VALUE[,KEY=VALUE...]` where KEY is a Settings field (or `top_k`, the request's top_k; default 10).
Results are written to evaluation/results/<label>-<mode>.json."""
import argparse
import asyncio
import json
import pathlib
import platform
import subprocess
import sys

from evaluation import harness, metrics
from evaluation.harness import Config

RESULTS = pathlib.Path(__file__).resolve().parent / "results"
TABLE = ["recall@5", "recall@8", "recall@10", "mrr@10", "ndcg@10", "identifier_exact_recall@5", "page_attribution_accuracy",
         "page_given_retrieved", "multi_file_coverage", "no_answer_precision", "no_answer_recall"]


def _git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], capture_output=True, text=True, check=False).stdout.strip()
    except OSError:
        return ""


def _parse_config(spec: str) -> Config:
    name, _, rest = spec.partition(":")
    overrides, top_k = {}, 10
    for item in filter(None, rest.split(",")):
        key, _, value = item.partition("=")
        if key == "top_k":
            top_k = int(value)
            continue
        from app.core.config import settings

        current = getattr(settings, key)  # AttributeError on a typo: fail loudly
        overrides[key] = type(current)(value) if not isinstance(current, bool) else value.lower() in ("1", "true", "yes")
    return Config(name, overrides, top_k)


def _questions(names: list[str]):
    from evaluation.heldout import HELDOUT
    from evaluation.questions import QUESTIONS

    known = {"main": QUESTIONS, "heldout": HELDOUT}
    return {n: known[n] for n in names}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fake", action="store_true", help="deterministic hash embedder, in-memory stores (CI)")
    mode.add_argument("--real", action="store_true", help="all-MiniLM-L6-v2 via sentence-transformers; live stores when reachable")
    ap.add_argument("--stores", choices=["auto", "fake", "real"], default="auto", help="--real only: force the store backend")
    ap.add_argument("--generation", default="1", help="comma list of index generations to build and evaluate: 1 (v1 parser/chunker/analyzer), 2 (v2)")
    ap.add_argument("--sets", default="main", help="comma list of: main, heldout")
    ap.add_argument("--config", action="append", default=[], help="name:KEY=VALUE[,KEY=VALUE]  (repeatable; default: the current settings)")
    ap.add_argument("--default-timeouts", action="store_true", help="--real: keep the service's 0.5 s Milvus/OpenSearch timeouts and report how often dense search degrades")
    ap.add_argument("--label", default="run", help="prefix of the result file name")
    ap.add_argument("--compare", type=pathlib.Path, help="a previous result JSON to print deltas against (first config, main set)")
    ap.add_argument("--verbose", "-v", action="store_true", help="one line per question (main set only)")
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

    set_names = [n.strip() for n in args.sets.split(",") if n.strip()]
    configs = [_parse_config(c) for c in args.config] or [Config()]
    out: dict = {}
    stats_by_gen: dict = {}
    for g in [int(x) for x in args.generation.split(",")]:
        res, stats, backend = asyncio.run(harness.run_benchmark(embedder, stores, _questions(set_names), configs,
                                                                default_timeouts=args.default_timeouts, generation=g))
        stats_by_gen[g] = stats
        for cfg_name, per_set in res.items():
            out[f"g{g}/{cfg_name}"] = per_set

    base = json.loads(args.compare.read_text()) if args.compare else None
    print(f"\nmode={mode_name} embedder={embedder.name} backend={backend}")
    record = {}
    for g, stats in stats_by_gen.items():
        toks = sorted(stats.tokens_per_chunk or [])
        extra = f"; tokens/chunk p50={toks[len(toks) // 2]} max={toks[-1]}" if toks else ""
        print(f"generation {g}: {stats.files} files, {stats.chunks} chunks ({stats.seconds}s to ingest){extra}")
    for g in stats_by_gen:
      for cfg in configs:
        for set_name in set_names:
            results = out[f"g{g}/{cfg.name}"][set_name]
            summary = metrics.summarise(results)
            print(f"\n=== generation={g} config={cfg.name} overrides={cfg.overrides or {}} top_k={cfg.top_k} set={set_name} {summary.counts} ===")
            b = (base or {}).get("runs", {}).get(f"g{g}/{cfg.name}/{set_name}", {}).get("metrics") if base else None
            for k in TABLE:
                line = f"{k:<28}{summary.show(k):<22}"
                if b and b.get(k) is not None and summary.metrics[k] is not None:
                    line += f"  baseline {b[k]:.3f}  delta {summary.metrics[k] - b[k]:+.3f}"
                print(line)
            print(f"\n{'category':<14}{'n':>3}{'R@5':>8}{'MRR@10':>8}{'nDCG@10':>9}")
            for cat, row in summary.by_category.items():
                print(f"{cat:<14}{row['n']:>3}{row['recall@5']:>8.3f}{row['mrr@10']:>8.3f}{row['ndcg@10']:>9.3f}")
            lat = [r.latency_ms for r in results]
            n_degraded = sum(r.note == "degraded" for r in results)
            print(f"questions that ran on one search engine only (timeout/failure; contaminates the numbers if > 0): {n_degraded}")
            print(f"latency (pipeline.run, ms): p50={_pct(lat, 50):.1f} p95={_pct(lat, 95):.1f}")
            detail = []
            for r in results:
                rank = metrics.first_relevant_rank(r)
                detail.append({"id": r.question.id, "category": r.question.category, "status": r.status, "first_relevant_rank": rank,
                               "returned": len(r.chunks), "coverage": r.coverage})
                if args.verbose and set_name == "main":
                    print(f"  {r.question.id:<22} first_rel_rank={rank} returned={len(r.chunks)} status={r.status}")
            record[f"g{g}/{cfg.name}/{set_name}"] = {
                "overrides": cfg.overrides or {}, "top_k": cfg.top_k, "metrics": summary.metrics,
                "fractions": {k: list(v) for k, v in summary.fractions.items()}, "by_category": summary.by_category, "counts": summary.counts, "degraded_questions": n_degraded,
                "questions": detail if set_name == "main" else [{"id": d["id"], "category": d["category"]} for d in detail],  # no per-question outcomes for held-out
            }

    if args.default_timeouts and harness.DENSE_SEARCH_MS:
        lat = sorted(ms for ms, _ in harness.DENSE_SEARCH_MS)
        failed = sum(f for _, f in harness.DENSE_SEARCH_MS)
        print(f"\nMilvus dense search with default timeouts: {len(lat)} searches, {failed} failed ({100 * failed / len(lat):.1f}%); "
              f"latency ms p50={_pct(lat, 50):.0f} p95={_pct(lat, 95):.0f} p99={_pct(lat, 99):.0f} max={lat[-1]:.0f}")
        record["_milvus_search"] = {"searches": len(lat), "failed": failed, "p50": _pct(lat, 50), "p95": _pct(lat, 95), "p99": _pct(lat, 99), "max": lat[-1]}
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"{args.label}-{mode_name}.json"
    path.write_text(json.dumps({
        "label": args.label, "mode": mode_name, "embedder": embedder.name, "backend": backend, "git": _git("rev-parse", "--short", "HEAD"),
        "python": platform.python_version(), "ingest": {f"g{g}": {"chunks": s.chunks, "per_file": s.chunks_per_file} for g, s in stats_by_gen.items()}, "runs": record,
    }, indent=2) + "\n")
    print(f"\nwrote {path}")
    return 0


def _pct(xs: list[float], p: int) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else 0.0


if __name__ == "__main__":
    sys.exit(main())
