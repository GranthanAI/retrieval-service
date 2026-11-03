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
from app.ingestion import chunk_identity, chunker, parser
from app.models.chunk_record import ChunkRecord
from app.query import pipeline
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores import milvus_repository as milvus_module
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
def backend(stores: str):
    """Yields (milvus_repo, opensearch_repo, description). `stores` is 'fake' or 'real'."""
    run_id = uuid.uuid4().hex[:8]
    saved = (settings.MILVUS_COLLECTION_NAME, settings.OPENSEARCH_INDEX_NAME, milvus_module._client, opensearch_module._client)
    try:
        if stores == "fake":
            milvus_module._client, opensearch_module._client = FakeMilvusClient(), FakeOpenSearchClient()
            yield MilvusRepository(), OpenSearchRepository(), "in-memory fakes (client level)"
        else:
            settings.MILVUS_COLLECTION_NAME = f"eval_chunks_{run_id}"
            settings.OPENSEARCH_INDEX_NAME = f"eval_chunks_{run_id}"
            milvus_module._client = opensearch_module._client = None
            _load_script("provision_milvus_collection").main()
            _load_script("provision_opensearch_index").main()
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


async def ingest(corpus: dict[str, bytes], embedder, milvus: MilvusRepository, opensearch: OpenSearchRepository) -> IngestStats:
    """Same steps as `orchestrator._process` (parse, chunk, derive ids, embed, build ChunkRecords, write both stores), minus
    Postgres/MinIO/Kafka. Phase 2 changes those steps; keep this in lock-step (or extract the shared function) when it lands."""
    start, per_file, texts = time.perf_counter(), {}, {}
    for filename, raw in corpus.items():
        parsed = _parse(raw, filename)
        chunks = chunker.chunk(parsed)
        ids = [chunk_identity.derive_chunk_id(doc_id(filename), CONTENT_VERSION, c.chunk_index) for c in chunks]
        vectors = await embedder.embed_chunks([c.content for c in chunks])
        records = [
            ChunkRecord(chunk_id=cid, document_id=doc_id(filename), content_version=CONTENT_VERSION, chunk_index=c.chunk_index,
                        owner_id=OWNER, conversation_id=None, filename=filename, page_number=c.page_number, section=c.section,
                        content=c.content, embedding=v)
            for c, cid, v in zip(chunks, ids, vectors)
        ]
        await milvus.upsert_chunks(records)
        await opensearch.upsert_chunks(records)
        per_file[filename] = len(records)
        texts[filename] = [metrics.norm(c.content) for c in chunks]
    return IngestStats(len(corpus), sum(per_file.values()), round(time.perf_counter() - start, 2), per_file, texts)


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
    t0 = time.perf_counter()
    resp = await pipeline.run(req, session_factory=_Session, file_service_client=_Files(), embedding_client=embedder,
                              milvus_repository=milvus, opensearch_repository=opensearch)
    ms = (time.perf_counter() - t0) * 1000
    coverage = None
    if "coverage" in resp.DESCRIPTOR.fields_by_name:  # added in Phase 3
        from app.grpc.retrieval_pb2 import Coverage  # noqa: PLC0415

        coverage = Coverage.Name(resp.coverage)
    return metrics.QuestionResult(q, _to_ranked(resp), status=RetrievalStatus.Name(resp.status), coverage=coverage, latency_ms=ms)


async def run_benchmark(embedder, stores: str, questions: list[Question] | None = None, files: list[str] | None = None):
    """Returns (results, ingest stats, backend description). `files` limits the corpus (tests use the Markdown files only, which
    avoids the Docling parse); every question's gold evidence must then lie inside it."""
    questions = questions or QUESTIONS
    corpus = {n: b for n, b in fixture_files().items() if files is None or n in files}
    with backend(stores) as (milvus, opensearch, description), patched_state(list(corpus)):
        stats = await ingest(corpus, embedder, milvus, opensearch)
        if bad := missing_gold(questions, stats.chunk_texts):
            raise SystemExit("gold evidence not found in any indexed chunk (fix questions.py or the fixtures):\n  " + "\n  ".join(bad))
        results = [await ask(q, embedder, milvus, opensearch) for q in questions]
    return results, stats, description
