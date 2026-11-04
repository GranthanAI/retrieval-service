"""Wires the real ingestion path (parse -> chunk -> embed -> ChunkRecord -> both repositories) and the real `pipeline.run` to either
in-memory fakes or live Milvus/OpenSearch, and runs the labelled questions.

What is faked, and why
    * Postgres lookups (current version / job status) and File Service permissions: the corpus is ingested in-process, so there is no
      job table to read; every file is READY at version 1 and every user is authorised.
    * the embedding *service*: the embedder runs in-process (see embedders.py).
    * Milvus / OpenSearch: only when the live stores are not reachable (or --stores fake). The repositories themselves always run.
Parsing, chunking, chunk ids, record construction, fusion, MMR and response mapping are the production code."""
import hashlib
import importlib.util
import pathlib
import pickle
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from unittest import mock

from app.clients.file_service_client import PermissionCheckResult
from app.core.config import settings
from app.core.enums import RetrievalScope
from app.grpc.retrieval_pb2 import GetRelevantChunksRequest, RetrievalStatus
from app.ingestion import build, parser
from app.ingestion.tokenization import ModelProfile, load_counter
from app.models.chunk_record import ChunkRecord
from app.query import pipeline
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores import generations
from app.stores import milvus_repository as milvus_module
from app.stores.generations import GenerationInfo
from app.stores import opensearch_repository as opensearch_module
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository
from evaluation import metrics
from evaluation.fakes import FakeMilvusClient, FakeOpenSearchClient
from evaluation.questions import QUESTIONS, Question

ROOT = pathlib.Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures"
CACHE = ROOT / ".cache"
OWNER = "00000000-0000-0000-0000-00000000e7a1"
CONTENT_VERSION = 1


def doc_id(filename: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"granthan-eval/{filename}"))


def fixture_files() -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(FIXTURES.iterdir()) if p.is_file() and not p.name.startswith("_")}


def _parse(raw: bytes, filename: str):
    """parser.parse, with a small on-disk cache for Docling (model load + layout analysis takes about a minute per PDF).
    The key includes the parser source so editing the parser invalidates it."""
    if not filename.lower().endswith(".pdf"):
        return parser.parse(raw, filename)
    src = pathlib.Path(parser.__file__).read_bytes() + pathlib.Path(parser.text_parsers.__file__).read_bytes()
    key = hashlib.sha256(raw + src).hexdigest()[:24]
    path = CACHE / f"parse-{key}.pkl"
    if path.exists():
        return pickle.loads(path.read_bytes())
    parsed = parser.parse(raw, filename)
    CACHE.mkdir(exist_ok=True)
    path.write_bytes(pickle.dumps(parsed))
    return parsed


# --------------------------------------------------------------------------------------------------------------------------
# Backends
# --------------------------------------------------------------------------------------------------------------------------
def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT.parent / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def stores_reachable() -> tuple[bool, str]:
    try:
        from opensearchpy import OpenSearch
        from pymilvus import MilvusClient

        MilvusClient(uri=f"http://{settings.MILVUS_HOST}:{settings.MILVUS_PORT}", timeout=3).list_collections()
        OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","), timeout=3).info()
        return True, ""
    except Exception as exc:  # noqa: BLE001 - any failure means "not available"
        return False, f"{type(exc).__name__}: {exc}"


@contextmanager
def backend(stores: str, default_timeouts: bool = False, info: GenerationInfo | None = None):
    """Yields (milvus_repo, opensearch_repo, description). `stores` is 'fake' or 'real'."""
    run_id = uuid.uuid4().hex[:8]
    saved = (settings.MILVUS_COLLECTION_NAME, settings.OPENSEARCH_INDEX_NAME, milvus_module._client, opensearch_module._client)
    saved_timeouts = (settings.MILVUS_QUERY_TIMEOUT_SECONDS, settings.OPENSEARCH_QUERY_TIMEOUT_SECONDS)
    try:
        if stores == "fake":
            milvus_module._client, opensearch_module._client = FakeMilvusClient(), FakeOpenSearchClient()
            yield MilvusRepository(), OpenSearchRepository(), "in-memory fakes (client level)"
        else:
            # The service's 0.5 s query timeouts are a latency SLO, not something to measure here: a timeout silently degrades the
            # query to one engine and would contaminate the quality numbers. Degradations are counted per question anyway.
            if not default_timeouts:
                settings.MILVUS_QUERY_TIMEOUT_SECONDS = settings.OPENSEARCH_QUERY_TIMEOUT_SECONDS = 10.0
            settings.MILVUS_COLLECTION_NAME = f"eval_chunks_{run_id}"
            settings.OPENSEARCH_INDEX_NAME = f"eval_chunks_{run_id}"
            milvus_module._client = opensearch_module._client = None
            # Created EXPLICITLY from the versioned schema of the generation being evaluated, never by a first write.
            version = info.schema_version if info else "1"
            from opensearchpy import OpenSearch  # noqa: PLC0415
            from pymilvus import MilvusClient  # noqa: PLC0415

            _load_script("provision_milvus_collection").create_collection(
                MilvusClient(uri=f"http://{settings.MILVUS_HOST}:{settings.MILVUS_PORT}"), settings.MILVUS_COLLECTION_NAME, version)
            _load_script("provision_opensearch_index").create_index(
                OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(",")), settings.OPENSEARCH_INDEX_NAME, version)
            try:
                yield MilvusRepository(), OpenSearchRepository(), f"live Milvus {settings.MILVUS_HOST}:{settings.MILVUS_PORT} + OpenSearch {settings.OPENSEARCH_HOSTS}"
            finally:
                try:
                    milvus_module._get_client().drop_collection(settings.MILVUS_COLLECTION_NAME)
                    opensearch_module._get_client().indices.delete(index=settings.OPENSEARCH_INDEX_NAME, ignore_unavailable=True)
                except Exception as exc:  # noqa: BLE001
                    print(f"warning: could not clean up eval collection/index {settings.MILVUS_COLLECTION_NAME}: {exc}")
    finally:
        settings.MILVUS_COLLECTION_NAME, settings.OPENSEARCH_INDEX_NAME, milvus_module._client, opensearch_module._client = saved
        settings.MILVUS_QUERY_TIMEOUT_SECONDS, settings.OPENSEARCH_QUERY_TIMEOUT_SECONDS = saved_timeouts


class _Session:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


class _Files:
    async def check_permissions(self, user_id, document_ids):
        return PermissionCheckResult(authorized=list(document_ids))


@contextmanager
def patched_state(files: list[str]):
    ids = {doc_id(f): CONTENT_VERSION for f in files}

    async def get_many(db, doc_ids):
        return {d: ids[d] for d in doc_ids if d in ids}

    async def statuses_for(db, versions):
        return {d: ("READY", 1) for d in versions}

    with mock.patch.object(DocumentCurrentVersionRepository, "get_many", staticmethod(get_many)), \
            mock.patch.object(IngestionJobRepository, "statuses_for", staticmethod(statuses_for)):
        yield


# --------------------------------------------------------------------------------------------------------------------------
# Ingestion and querying
# --------------------------------------------------------------------------------------------------------------------------
@dataclass
class IngestStats:
    files: int
    chunks: int
    seconds: float
    chunks_per_file: dict[str, int]
    chunk_texts: dict[str, list[str]]
    tokens_per_chunk: list[int] | None = None


def profile_info(generation: int, milvus_collection: str = "", opensearch_index: str = "") -> GenerationInfo:
    """What each benchmarked generation is: 1 = parser/chunker/analyzer v1 (the index as it was before Phase 2), 2 = v2 of all three."""
    v = "1" if generation == 1 else "2"
    return GenerationInfo(generation, milvus_collection, opensearch_index, parser_version=v, chunker_version=v, analyzer_version=v,
                          embedding_model="all-MiniLM-L6-v2", embedding_dimension=384, status="building")


async def ingest(corpus: dict[str, bytes], embedder, milvus: MilvusRepository, opensearch: OpenSearchRepository) -> IngestStats:
    """Same steps as `orchestrator._process` (parse, chunk, derive ids, embed, build ChunkRecords, write both stores), minus
    Postgres/MinIO/Kafka. Phase 2 changes those steps; keep this in lock-step (or extract the shared function) when it lands."""
    start, per_file, texts, tokens_per_chunk = time.perf_counter(), {}, {}, []
    for filename, raw in corpus.items():
        info = generations.current()
        parsed = _parse(raw, filename)
        chunks = build.chunk_parsed(parsed, filename, info)
        vectors = await embedder.embed_chunks([c.content for c in chunks])      # exactly the text that is indexed
        records = build.make_records(chunks, vectors, document_id=doc_id(filename), content_version=CONTENT_VERSION, owner_id=OWNER,
                                     conversation_id=None, filename=filename, info=info)
        await milvus.upsert_chunks(records)
        await opensearch.upsert_chunks(records)
        per_file[filename] = len(records)
        texts[filename] = [metrics.norm(c.content) for c in chunks]
        tokens_per_chunk.extend(build.runtime.counter.count(c.content) for c in chunks if build.runtime.counter)
    return IngestStats(len(corpus), sum(per_file.values()), round(time.perf_counter() - start, 2), per_file, texts, tokens_per_chunk)


def missing_gold(questions: list[Question], chunk_texts: dict[str, list[str]]) -> list[str]:
    """Gold evidence that no indexed chunk contains: a typo in questions.py, or a chunk boundary that splits the passage."""
    return [f"{q.id}: {file}: {text!r}" for q in questions for file, text in q.evidence
            if not any(metrics.norm(text) in t for t in chunk_texts.get(file, []))]


def _to_ranked(resp) -> list[metrics.RankedChunk]:
    out = []
    for c in resp.chunks:
        md = dict(c.metadata)
        page = md.get("page_number") or None
        start = int(md.get("page_start") or page) if (md.get("page_start") or page) else None
        end = int(md.get("page_end") or start) if start is not None else None
        out.append(metrics.RankedChunk(file=c.source_name, content=c.content, page_start=start, page_end=end, section=md.get("section", "")))
    return out


async def ask(q: Question, embedder, milvus, opensearch, top_k: int = 10) -> metrics.QuestionResult:
    req = GetRelevantChunksRequest(user_id="eval-user", conversation_id="eval-conv", query=q.query, top_k=top_k,
                                   scope=int(RetrievalScope.FILES), file_ids=[doc_id(f) for f in q.files], trace_id=f"eval-{q.id}")
    from app.core import metrics as service_metrics  # noqa: PLC0415

    def degraded() -> float:
        return sum(service_metrics.search_degraded_total.labels(engine=e)._value.get() for e in ("dense_only", "bm25_only"))

    before = degraded()
    t0 = time.perf_counter()
    resp = await pipeline.run(req, session_factory=_Session, file_service_client=_Files(), embedding_client=embedder,
                              milvus_repository=milvus, opensearch_repository=opensearch)
    ms = (time.perf_counter() - t0) * 1000
    coverage = None
    if "coverage" in resp.DESCRIPTOR.fields_by_name:  # added in Phase 3
        from app.grpc.retrieval_pb2 import Coverage  # noqa: PLC0415

        coverage = Coverage.Name(resp.coverage)
    return metrics.QuestionResult(q, _to_ranked(resp), status=RetrievalStatus.Name(resp.status), coverage=coverage, latency_ms=ms, top_score=(resp.top_score if resp.HasField("top_score") else None),
                                  note="degraded" if degraded() > before else "")


@dataclass
class Config:
    """One evaluated configuration: settings overrides (applied with mock.patch.object on `settings`) and the request's top_k."""

    name: str = "default"
    overrides: dict | None = None
    top_k: int = 10


@contextmanager
def applied(overrides: dict | None):
    if not overrides:
        yield
        return
    with mock.patch.multiple(settings, **overrides):
        yield


DENSE_SEARCH_MS: list[tuple[float, bool]] = []  # (elapsed ms, failed) for every Milvus search made by the queries of a run


def _timed(milvus: MilvusRepository) -> None:
    original = milvus.search

    async def search(*a, **kw):
        t0 = time.perf_counter()
        try:
            result = await original(*a, **kw)
        except BaseException:
            DENSE_SEARCH_MS.append(((time.perf_counter() - t0) * 1000, True))
            raise
        DENSE_SEARCH_MS.append(((time.perf_counter() - t0) * 1000, False))
        return result

    milvus.search = search


async def run_benchmark(embedder, stores: str, sets: dict[str, list[Question]], configs: list[Config] | None = None, files: list[str] | None = None,
                        default_timeouts: bool = False, generation: int = 1):
    """Indexes the corpus ONCE, then answers every question set under every config.
    Returns ({config name: {set name: [QuestionResult]}}, ingest stats, backend description). `files` limits the corpus (tests use the
    Markdown files only, which avoids the Docling parse); every question's gold evidence must then lie inside it."""
    configs = configs or [Config()]
    corpus = {n: b for n, b in fixture_files().items() if files is None or n in files}
    all_questions = [q for qs in sets.values() for q in qs]
    out: dict[str, dict[str, list[metrics.QuestionResult]]] = {}
    info = profile_info(generation)
    if info.chunker_version != "1":
        # chunker v2 sizes chunks with the embedding model's own tokenizer and max_seq_length (from GetModelInfo in production)
        build.runtime.profile = ModelProfile(embedder.name, embedder.dimension, embedder.max_seq_length, embedder.tokenizer_id)
        build.runtime.counter = load_counter(embedder.tokenizer_id)
    with backend(stores, default_timeouts, info) as (milvus, opensearch, description), patched_state(list(corpus)), \
            generations.use(GenerationInfo(**{**info.__dict__, "milvus_collection": settings.MILVUS_COLLECTION_NAME,
                                              "opensearch_index": settings.OPENSEARCH_INDEX_NAME})):
        stats = await ingest(corpus, embedder, milvus, opensearch)
        _timed(milvus)
        if bad := missing_gold(all_questions, stats.chunk_texts):
            raise SystemExit("gold evidence not found in any indexed chunk (fix the questions or the fixtures):" + "".join("\n  " + b for b in bad))
        for cfg in configs:
            with applied(cfg.overrides):
                out[cfg.name] = {name: [await ask(q, embedder, milvus, opensearch, top_k=cfg.top_k) for q in qs] for name, qs in sets.items()}
    return out, stats, description
