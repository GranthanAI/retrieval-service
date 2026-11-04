"""Operate index generations (see app/stores/generations.py and docs/retrieval-quality.md).

    make generation-status
    make generation-bootstrap                 # once per environment: register the existing index as generation 1 and create the read alias
    make generation-create GENERATION=2       # explicitly create generation 2's Milvus collection and OpenSearch index, register it as `building`
    make reindex GENERATION=2                 # re-process every READY current document version into generation 2 (the active one is not touched)
    make activate GENERATION=2                # switch queries to generation 2, only if coverage is complete and the benchmark allows it
    uv run python scripts/generation.py retire 1       # mark a generation retired (data kept)
    uv run python scripts/generation.py drop 1 --yes   # delete a retired generation's physical indexes

Nothing here is run automatically. `activate` is deliberately never run by tests or agents against a shared environment."""
import argparse
import asyncio
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))  # so `uv run python scripts/<this>.py` finds `app` and `evaluation`

from sqlalchemy import select

from app.clients.embedding_client import EmbeddingClient
from app.core.config import settings
from app.core.enums import IngestionStatus
from app.db.session import get_session_factory
from app.ingestion import build, downloader
from app.ingestion import parser as parser_module
from app.ingestion import transcript as transcript_module
from app.models.document_current_version import DocumentCurrentVersion
from app.models.ingestion_job import IngestionJob
from app.repositories.generation_repository import GenerationRepository, to_info
from app.stores import generations
from app.stores.generations import ACTIVE, BUILDING, RETIRED, GenerationInfo
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository

SCRIPTS = pathlib.Path(__file__).resolve().parent


def _load(name: str):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _clients():
    from opensearchpy import OpenSearch
    from pymilvus import MilvusClient

    return MilvusClient(uri=f"http://{settings.MILVUS_HOST}:{settings.MILVUS_PORT}"), OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","))


async def cmd_status(args) -> int:
    async with get_session_factory()() as db:
        rows = await GenerationRepository.list_not_dropped(db)
    if not rows:
        print("no generations registered: the legacy index (generation 1, un-suffixed names) is in effect. Run `make generation-bootstrap`.")
        return 0
    for r in rows:
        async with get_session_factory()() as db:
            covered = len(await GenerationRepository.covered(db, r.generation))
        print(f"g{r.generation} {r.status:<8} parser={r.parser_version} chunker={r.chunker_version} analyzer={r.analyzer_version} "
              f"model={r.embedding_model}/{r.embedding_dimension} milvus={r.milvus_collection} opensearch={r.opensearch_index} "
              f"alias={r.opensearch_alias or '-'} documents_recorded={covered}")
    return 0


async def cmd_bootstrap(args) -> int:
    """Registers the existing un-suffixed index/collection as generation 1 (active) and points the read alias at it. Idempotent."""
    _, os_client = _clients()
    async with get_session_factory()() as db:
        if await GenerationRepository.get(db, 1):
            print("generation 1 is already registered; nothing to do.")
            return 0
        info = GenerationInfo(1, settings.MILVUS_COLLECTION_NAME, settings.OPENSEARCH_INDEX_NAME, "1", "1", "1",
                              settings.EMBEDDING_MODEL_EXPECTED, settings.EMBEDDING_DIMENSION_EXPECTED, ACTIVE,
                              opensearch_alias=settings.OPENSEARCH_ACTIVE_ALIAS)
        if not os_client.indices.exists(index=info.opensearch_index):
            print(f"OpenSearch index {info.opensearch_index!r} does not exist; provision it first (scripts/provision_opensearch_index.py)")
            return 1
        os_client.indices.put_alias(index=info.opensearch_index, name=settings.OPENSEARCH_ACTIVE_ALIAS)
        await GenerationRepository.register(db, info, notes="bootstrapped from the legacy un-suffixed index")
        await db.commit()
    print(f"registered generation 1 (active) -> {info.milvus_collection} / {info.opensearch_index}, alias {settings.OPENSEARCH_ACTIVE_ALIAS}")
    return 0


async def cmd_create(args) -> int:
    n = args.generation
    if n < 2:
        print("generation 1 is the legacy index; use bootstrap")
        return 1
    milvus_name, os_name = generations.names_for(n)
    milvus_client, os_client = _clients()
    made_m = _load("provision_milvus_collection").create_collection(milvus_client, milvus_name, "2")
    made_o = _load("provision_opensearch_index").create_index(os_client, os_name, "2")
    info = GenerationInfo(n, milvus_name, os_name, "2", "2", "2", settings.EMBEDDING_MODEL_EXPECTED, settings.EMBEDDING_DIMENSION_EXPECTED, BUILDING)
    async with get_session_factory()() as db:
        if not await GenerationRepository.get(db, n):
            await GenerationRepository.register(db, info, notes="created by scripts/generation.py create")
            await db.commit()
    print(f"generation {n}: collection {milvus_name} {'created' if made_m else 'already existed'}, index {os_name} "
          f"{'created' if made_o else 'already existed'}, registered as building")
    return 0


async def _ready_current_versions(db) -> list[IngestionJob]:
    stmt = (select(IngestionJob)
            .join(DocumentCurrentVersion, (DocumentCurrentVersion.document_id == IngestionJob.document_id)
                  & (DocumentCurrentVersion.current_version == IngestionJob.content_version))
            .where(IngestionJob.status == IngestionStatus.READY.value))
    return list((await db.execute(stmt)).scalars().all())


async def cmd_reindex(args) -> int:
    n = args.generation
    session_factory = get_session_factory()
    async with session_factory() as db:
        row = await GenerationRepository.get(db, n)
        if row is None or row.status != BUILDING:
            print(f"generation {n} must exist and be `building` (run `make generation-create GENERATION={n}`); it is {row.status if row else 'missing'}")
            return 1
        info = to_info(row)
        jobs = await _ready_current_versions(db)
    embedding_client = EmbeddingClient()
    if info.chunker_version != "1":
        await build.ensure_model_profile(embedding_client)
    print(f"reindexing {len(jobs)} READY current document versions into generation {n}")
    sem = asyncio.Semaphore(args.concurrency)
    milvus, opensearch = MilvusRepository(), OpenSearchRepository()
    failures: list[str] = []

    async def one(job: IngestionJob) -> None:
        async with sem:
            doc = str(job.document_id)
            try:
                raw = await downloader.fetch(job.storage_bucket, job.storage_key)
                transcript_doc = transcript_module.parse_transcript(raw)
                parsed = None if transcript_doc is not None else parser_module.parse(raw, job.filename)
                chunks = (transcript_module.chunk_transcript(transcript_doc) if transcript_doc is not None
                          else build.chunk_parsed(parsed, job.filename, info))
                if not chunks:
                    raise ValueError("no extractable text")
                vectors = await embedding_client.embed_chunks([c.content for c in chunks])
                records = build.make_records(chunks, vectors, document_id=doc, content_version=job.content_version, owner_id=str(job.owner_id),
                                             conversation_id=str(job.conversation_id) if job.conversation_id else None,
                                             filename=job.filename, info=info)
                with generations.use(info):                          # writes go to generation N only; the active one is not touched
                    await milvus.upsert_chunks(records)
                    await opensearch.upsert_chunks(records)
                async with session_factory() as db:
                    await GenerationRepository.record_document(db, n, job.document_id, job.content_version, len(records))
                    await db.commit()
                print(f"  ok   {doc} v{job.content_version} {len(records)} chunks")
            except Exception as exc:  # noqa: BLE001 - keep going; report at the end
                failures.append(f"{doc} v{job.content_version}: {type(exc).__name__}: {exc}")
                print(f"  FAIL {doc} v{job.content_version}: {type(exc).__name__}")

    await asyncio.gather(*(one(j) for j in jobs))
    await embedding_client.close()
    print(f"done: {len(jobs) - len(failures)} indexed, {len(failures)} failed")
    for f in failures:
        print("  -", f)
    return 1 if failures else 0


async def _benchmark_gate(active: GenerationInfo, candidate: GenerationInfo) -> tuple[bool, list[str]]:
    """The candidate must be at least as good as the active generation on BOTH sets for Recall@5, MRR@10 and nDCG@10."""
    from evaluation import harness, metrics
    from evaluation.embedders import RealEmbedder
    from evaluation.heldout import HELDOUT
    from evaluation.questions import QUESTIONS

    ok_stores, why = harness.stores_reachable()
    stores = "real" if ok_stores else "fake"
    if not ok_stores:
        print(f"note: live stores unreachable ({why}); benchmark runs on the in-memory stores")
    embedder = RealEmbedder()
    results = {}
    for label, info in (("active", active), ("candidate", candidate)):
        gen = 1 if info.chunker_version == "1" else 2
        out, _, _ = await harness.run_benchmark(embedder, stores, {"main": QUESTIONS, "heldout": HELDOUT}, generation=gen)
        results[label] = {s: metrics.summarise(r).metrics for s, r in out["default"].items()}
    problems = []
    for s in ("main", "heldout"):
        for k in ("recall@5", "mrr@10", "ndcg@10"):
            a, c = results["active"][s][k], results["candidate"][s][k]
            line = f"{s:8} {k:9} active {a:.3f}  candidate {c:.3f}"
            print("  " + line)
            if c < a - 1e-9:
                problems.append(line)
    return not problems, problems


async def cmd_activate(args) -> int:
    n = args.generation
    session_factory = get_session_factory()
    async with session_factory() as db:
        row, current = await GenerationRepository.get(db, n), await GenerationRepository.active(db)
        if row is None or row.status not in (BUILDING, RETIRED):
            print(f"generation {n} is {row.status if row else 'missing'}; only a building or retired generation can be activated")
            return 1
        if current is None:
            print("no active generation registered; run bootstrap first")
            return 1
        expected = {(str(j.document_id), j.content_version) for j in await _ready_current_versions(db)}
        covered = await GenerationRepository.covered(db, n)
    missing = expected - covered
    if missing:
        print(f"REFUSED: {len(missing)} READY current document version(s) are not in generation {n} (re-run `make reindex GENERATION={n}`), e.g. {sorted(missing)[:3]}")
        return 1
    print(f"coverage ok: all {len(expected)} READY current versions are in generation {n}. Benchmark gate (generation {n} vs active {current.generation}):")
    passed, problems = await _benchmark_gate(to_info(current), to_info(row))
    if not passed:
        print("REFUSED: generation", n, "is worse than the active generation on:\n  " + "\n  ".join(problems))
        return 1
    if args.dry_run:
        print("dry run: all checks passed; not switching.")
        return 0
    _, os_client = _clients()
    alias = settings.OPENSEARCH_ACTIVE_ALIAS
    actions = [{"remove": {"index": "*", "alias": alias}}] if os_client.indices.exists_alias(name=alias) else []
    actions.append({"add": {"index": row.opensearch_index, "alias": alias}})
    os_client.indices.update_aliases(body={"actions": actions})           # atomic: readers never see zero or two targets
    async with session_factory() as db:
        await GenerationRepository.activate(db, n, alias)
        await db.commit()
    print(f"generation {n} is now active (alias {alias} -> {row.opensearch_index}); generation {current.generation} is retired but kept. "
          f"Running processes pick it up within {settings.GENERATION_REFRESH_SECONDS:.0f} s.")
    return 0


async def cmd_retire(args) -> int:
    async with get_session_factory()() as db:
        row = await GenerationRepository.get(db, args.generation)
        if row is None or row.status == ACTIVE:
            print("generation must exist and not be active")
            return 1
        row.status = RETIRED
        await db.commit()
    print(f"generation {args.generation} retired (data kept)")
    return 0


async def cmd_drop(args) -> int:
    if not args.yes:
        print("refusing without --yes: this deletes the generation's Milvus collection and OpenSearch index")
        return 1
    async with get_session_factory()() as db:
        row = await GenerationRepository.get(db, args.generation)
        if row is None or row.status != RETIRED:
            print("only a retired generation can be dropped")
            return 1
        milvus_client, os_client = _clients()
        milvus_client.drop_collection(row.milvus_collection)
        os_client.indices.delete(index=row.opensearch_index, ignore_unavailable=True)
        await GenerationRepository.mark_dropped(db, args.generation)
        await db.commit()
    print(f"generation {args.generation} dropped")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("bootstrap")
    for name in ("create", "reindex", "activate", "retire", "drop"):
        p = sub.add_parser(name)
        p.add_argument("generation", type=int)
        if name == "reindex":
            p.add_argument("--concurrency", type=int, default=4)
        if name == "activate":
            p.add_argument("--dry-run", action="store_true", help="run every check but do not switch")
        if name == "drop":
            p.add_argument("--yes", action="store_true")
    args = ap.parse_args(argv)
    return asyncio.run({"status": cmd_status, "bootstrap": cmd_bootstrap, "create": cmd_create, "reindex": cmd_reindex,
                        "activate": cmd_activate, "retire": cmd_retire, "drop": cmd_drop}[args.cmd](args))


if __name__ == "__main__":
    sys.exit(main())
