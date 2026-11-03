"""Reproductions of the Phase 1 retrieval bugs (docs/retrieval-quality.md, P1-1 .. P1-4).

Each test asserts the CORRECT behaviour and is marked strict-xfail, so it fails today and the suite stays green. When the fix lands
the test starts passing, strict mode turns that into a failure, and the fix commit removes the marker. Nothing here may be
loosened to make a fix pass: change the fix, or explain the design change in the phase report."""
import pytest

from app.clients.file_service_client import PermissionCheckResult
from app.core import metrics
from app.core.enums import RetrievalScope
from app.grpc.retrieval_pb2 import GetRelevantChunksRequest, RetrievalStatus
from app.query import mmr, pipeline
from app.query.scoring import FusedCandidate
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores import milvus_repository as milvus_module
from app.stores.milvus_repository import MilvusRepository
from app.stores.types import BM25SearchHit, DenseSearchHit
from evaluation.fakes import FakeMilvusClient

DOC = "11111111-1111-1111-1111-111111111111"


# --- P1-1 ---------------------------------------------------------------------------------------------------------------
@pytest.mark.xfail(strict=True, reason="P1-1: MMR mixes raw RRF scores (~0.01-0.03) with 0-1 cosine, so diversity dominates relevance")
def test_p1_1_mmr_keeps_both_strong_candidates_when_they_are_similar():
    strong = [FusedCandidate("a", rrf_score=0.0328), FusedCandidate("b", rrf_score=0.0323)]
    weak = [FusedCandidate(f"w{i}", rrf_score=0.012 - i * 0.0001) for i in range(30)]
    emb = {"a": [1.0, 0.0, 0.0], "b": [0.98, 0.2, 0.0]}                  # near-duplicates, both highly relevant
    emb.update({f"w{i}": [0.0, 0.0, 1.0] for i in range(30)})           # weak, but maximally different from "a"

    picked = {c.chunk_id for c in mmr.select(strong + weak, emb, k=2, lambda_=0.7)}

    assert picked == {"a", "b"}


# --- pipeline fakes (same seams as tests/unit/test_pipeline_status.py) ------------------------------------------------------
class _Session:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


class _Files:
    async def check_permissions(self, user_id, document_ids):
        return PermissionCheckResult(authorized=list(document_ids))


class _Embed:
    def __init__(self, fail=False):
        self.fail = fail

    async def embed_query(self, text):
        if self.fail:
            raise RuntimeError("embedding service down")
        return [1.0, 0.0]


def _hit(cls, i):
    return cls(chunk_id=f"c{i}", score=1.0 - i / 100, document_id=DOC, filename="a.md", content=" ".join(["word"] * 100), chunk_index=i)


class _Milvus:
    async def search(self, query_vector, top_n, current_versions):
        return [_hit(DenseSearchHit, i) for i in range(12)]

    async def get_by_chunk_ids(self, ids):
        return {i: [1.0, float(n)] for n, i in enumerate(ids)}


class _Search:
    async def search(self, query_text, top_n, current_versions):
        return [_hit(BM25SearchHit, i) for i in range(12)]


@pytest.fixture
def ready_document(monkeypatch):
    async def get_many(db, ids):
        return {d: 1 for d in ids}

    async def statuses_for(db, versions):
        return {d: ("READY", 12) for d in versions}

    monkeypatch.setattr(DocumentCurrentVersionRepository, "get_many", staticmethod(get_many))
    monkeypatch.setattr(IngestionJobRepository, "statuses_for", staticmethod(statuses_for))


async def _run(embed, **request_fields):
    request = GetRelevantChunksRequest(user_id="u", conversation_id="c", query="what is phase 6 about?", scope=int(RetrievalScope.FILES),
                                       file_ids=[DOC], trace_id="t", **request_fields)
    return await pipeline.run(request, session_factory=_Session, file_service_client=_Files(), embedding_client=embed,
                              milvus_repository=_Milvus(), opensearch_repository=_Search())


# --- P1-2 ---------------------------------------------------------------------------------------------------------------
@pytest.mark.xfail(strict=True, reason="P1-2: request.top_k is never read; MMR_FINAL_TOP_K (6) is always used")
async def test_p1_2_top_k_is_honoured(ready_document):
    response = await _run(_Embed(), top_k=3)
    assert len(response.chunks) == 3


@pytest.mark.xfail(strict=True, reason="P1-2: request.max_tokens is never read and total_tokens is not a real token count")
async def test_p1_2_max_tokens_drops_lowest_ranked_chunks(ready_document):
    response = await _run(_Embed(), top_k=10, max_tokens=250)  # each chunk is ~100+ tokens
    assert 0 < len(response.chunks) < 10
    assert response.total_tokens <= 250


# --- P1-3 ---------------------------------------------------------------------------------------------------------------
@pytest.mark.xfail(strict=True, reason="P1-3: HNSW index is searched with the IVF parameter nprobe; ef is never set")
async def test_p1_3_hnsw_search_sends_ef_not_nprobe(monkeypatch):
    fake = FakeMilvusClient(index_type="HNSW")
    monkeypatch.setattr(milvus_module, "_client", fake)

    await MilvusRepository().search([1.0, 0.0], top_n=40, current_versions={DOC: 1})

    params = fake.search_calls[0]["search_params"]["params"]
    assert "ef" in params and "nprobe" not in params
    assert params["ef"] >= 40


# --- P1-4 ---------------------------------------------------------------------------------------------------------------
@pytest.mark.xfail(strict=True, reason="P1-4: an embedding failure returns UNAVAILABLE although BM25 does not need a vector")
async def test_p1_4_embedding_failure_falls_back_to_bm25(ready_document):
    before = metrics.search_degraded_total.labels(engine="bm25_only")._value.get()

    response = await _run(_Embed(fail=True), top_k=5)

    assert response.status in (RetrievalStatus.OK, RetrievalStatus.NO_MATCH)
    assert len(response.chunks) > 0
    assert metrics.search_degraded_total.labels(engine="bm25_only")._value.get() == before + 1
