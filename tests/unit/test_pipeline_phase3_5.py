"""Retrieval-side behaviour of Phases 3 and 5: dense_query, previous-turn chunks, reranker integration and fallback, min_relevance_score,
coverage, neighbour expansion, the feedback log line, and GetDocumentChunks. All dependencies are fakes."""
import pytest
import structlog

from app.clients.file_service_client import PermissionCheckResult
from app.core import metrics
from app.core.config import settings
from app.core.enums import RetrievalScope
from app.grpc.retrieval_pb2 import Coverage, GetDocumentChunksRequest, GetRelevantChunksRequest, RetrievalStatus
from app.query import document_chunks, pipeline
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.types import BM25SearchHit, DenseSearchHit

D1, D2 = "d1", "d2"


class Session:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


class Files:
    def __init__(self, authorized=None):
        self.authorized = authorized

    async def check_permissions(self, user_id, document_ids):
        allowed = document_ids if self.authorized is None else [d for d in document_ids if d in self.authorized]
        return PermissionCheckResult(authorized=list(allowed))


def text(i, tag="x"):
    return f"passage {tag}{i} " + " ".join(f"{tag}{i}w{j}" for j in range(30))


def dense(i, doc=D1, section="S"):
    return DenseSearchHit(chunk_id=f"c{i}", score=1 - i / 100, document_id=doc, filename=f"{doc}.md", content=text(i), section=section, chunk_index=i)


def bm25(i, doc=D1, section="S"):
    return BM25SearchHit(chunk_id=f"c{i}", score=10 - i, document_id=doc, filename=f"{doc}.md", content=text(i), section=section, chunk_index=i)


class Embed:
    def __init__(self, rerank=None, boom=False):
        self.embedded, self._rerank, self.boom, self.rerank_calls = [], rerank, boom, 0
        if rerank is not None or boom:
            self.rerank = self._do_rerank

    async def embed_query(self, t):
        self.embedded.append(t)
        return [1.0, 0.0]

    async def _do_rerank(self, query, passages, timeout):
        self.rerank_calls += 1
        if self.boom:
            raise RuntimeError("reranker down")
        return self._rerank(passages)


class Milvus:
    def __init__(self, hits=None, previous=None):
        self.hits, self.previous = hits if hits is not None else [dense(i) for i in range(8)], previous or []

    async def search(self, query_vector, top_n, current_versions):
        return self.hits

    async def get_by_chunk_ids(self, ids):
        return {i: [1.0, float(n) * 0.01] for n, i in enumerate(ids)}

    async def get_chunks_by_ids(self, ids):
        return [(h, v) for h, v in self.previous if h.chunk_id in ids]


class Search:
    def __init__(self, hits=None, neighbours=None, doc_chunks=None):
        self.hits = hits if hits is not None else [bm25(i) for i in range(8)]
        self.queries, self._neighbours, self._doc_chunks = [], neighbours or {}, doc_chunks or {}

    async def search(self, query_text, top_n, current_versions):
        self.queries.append(query_text)
        return self.hits

    async def neighbours(self, document_id, version, chunk_index, section):
        return self._neighbours.get(chunk_index, [])

    async def document_chunks(self, document_id, version, limit):
        return self._doc_chunks.get(document_id, [])


@pytest.fixture(autouse=True)
def ready(monkeypatch):
    async def get_many(db, ids):
        return {d: 1 for d in ids}

    async def statuses_for(db, versions):
        return {d: ("READY", 8) for d in versions}

    monkeypatch.setattr(DocumentCurrentVersionRepository, "get_many", staticmethod(get_many))
    monkeypatch.setattr(IngestionJobRepository, "statuses_for", staticmethod(statuses_for))
    monkeypatch.setattr(settings, "DEDUPE_JACCARD_THRESHOLD", 1.0)
    monkeypatch.setattr(settings, "COVERAGE_ENABLED", True)


async def ask(embed=None, milvus=None, search=None, files=None, file_ids=(D1,), **fields):
    req = GetRelevantChunksRequest(user_id="u", conversation_id="c", query=fields.pop("query", "what is phase 6 about?"), top_k=fields.pop("top_k", 4),
                                   scope=int(RetrievalScope.FILES), file_ids=list(file_ids), trace_id="t", **fields)
    return await pipeline.run(req, session_factory=Session, file_service_client=files or Files(), embedding_client=embed or Embed(),
                              milvus_repository=milvus or Milvus(), opensearch_repository=search or Search())


def ids(resp):
    return [c.chunk_id for c in resp.chunks]


# ---- dense_query -------------------------------------------------------------------------------------------------------------
async def test_dense_leg_embeds_dense_query_and_keyword_leg_keeps_the_original_text():
    embed, search = Embed(), Search()
    await ask(embed=embed, search=search, query="Phase 6 mein kya karna hai?", dense_query="what does phase 6 involve")
    assert embed.embedded == ["what does phase 6 involve"] and search.queries == ["Phase 6 mein kya karna hai?"]


async def test_without_dense_query_the_query_is_embedded_as_before():
    embed = Embed()
    await ask(embed=embed, query="phase 6")
    assert embed.embedded == ["phase 6"]


# ---- previous-turn chunks ----------------------------------------------------------------------------------------------------
def prev(i, doc=D1, version=1):
    return (dense(i, doc), version)


async def test_previous_chunks_become_candidates_only_if_authorised_and_current(monkeypatch):
    milvus = Milvus(hits=[dense(0), dense(1)], previous=[prev(50), prev(51, doc="other"), prev(52, version=0)])
    resp = await ask(milvus=milvus, search=Search(hits=[bm25(0)]), previous_chunk_ids=["c50", "c51", "c52"], top_k=10)
    got = ids(resp)
    assert "c50" in got and "c51" not in got and "c52" not in got      # out-of-scope document and old version are dropped


async def test_previous_chunks_can_be_switched_off(monkeypatch):
    monkeypatch.setattr(settings, "PREVIOUS_CHUNKS_ENABLED", False)
    resp = await ask(milvus=Milvus(hits=[dense(0)], previous=[prev(50)]), search=Search(hits=[bm25(0)]), previous_chunk_ids=["c50"], top_k=10)
    assert "c50" not in ids(resp)


async def test_a_failure_fetching_previous_chunks_does_not_fail_the_query():
    class Broken(Milvus):
        async def get_chunks_by_ids(self, ids):
            raise RuntimeError("milvus hiccup")
    resp = await ask(milvus=Broken(), previous_chunk_ids=["c50"])
    assert resp.status == RetrievalStatus.OK and resp.chunks


# ---- reranker ----------------------------------------------------------------------------------------------------------------
def by_tag(passages):
    """Fake cross-encoder: passages containing 'passage x3' win, then x1, then the rest."""
    return [5.0 if "passage x3" in p else 2.0 if "passage x1" in p else -4.0 for p in passages]


async def test_reranker_scores_decide_the_order_and_are_returned_as_the_relevance(monkeypatch):
    monkeypatch.setattr(settings, "RERANK_ENABLED", True)
    resp = await ask(embed=Embed(rerank=by_tag), top_k=3)
    assert ids(resp)[:2] == ["c3", "c1"] and resp.chunks[0].relevance_score > resp.chunks[1].relevance_score > 0.5
    assert resp.HasField("top_score") and resp.top_score > 0.99 and resp.coverage == Coverage.STRONG


async def test_reranker_failure_falls_back_to_the_phase_1_path_with_a_metric(monkeypatch):
    monkeypatch.setattr(settings, "RERANK_ENABLED", True)
    before = metrics.rerank_fallback_total.labels(reason="RuntimeError")._value.get()
    baseline = await ask(embed=Embed())
    resp = await ask(embed=Embed(boom=True))
    assert ids(resp) == ids(baseline) and resp.status == RetrievalStatus.OK
    assert metrics.rerank_fallback_total.labels(reason="RuntimeError")._value.get() == before + 1


async def test_reranker_is_not_called_when_disabled(monkeypatch):
    embed = Embed(rerank=by_tag)
    await ask(embed=embed)
    assert embed.rerank_calls == 0


async def test_min_relevance_score_applies_to_the_reranker_score_only(monkeypatch):
    monkeypatch.setattr(settings, "RERANK_ENABLED", True)
    resp = await ask(embed=Embed(rerank=by_tag), top_k=5, min_relevance_score=0.5)
    assert ids(resp) == ["c3", "c1"]                                   # sigmoid(-4) = 0.018 is below 0.5
    monkeypatch.setattr(settings, "RERANK_ENABLED", False)
    plain = await ask(embed=Embed(), top_k=5, min_relevance_score=0.99)   # RRF scores are rank-derived: the threshold is ignored
    assert len(plain.chunks) == 5


async def test_reranked_bm25_only_path_orders_by_reranker(monkeypatch):
    monkeypatch.setattr(settings, "RERANK_ENABLED", True)

    class NoVector(Embed):
        async def embed_query(self, t):
            raise RuntimeError("embedding down")
    embed = NoVector(rerank=by_tag)
    resp = await ask(embed=embed, top_k=2)
    assert ids(resp) == ["c3", "c1"]


# ---- coverage ----------------------------------------------------------------------------------------------------------------
async def test_coverage_is_none_when_nothing_matched_and_strong_when_both_legs_agree():
    none = await ask(milvus=Milvus(hits=[]), search=Search(hits=[]))
    assert none.status == RetrievalStatus.NO_MATCH and none.coverage == Coverage.NONE
    strong = await ask()                              # the same chunk is first in both legs
    assert strong.coverage == Coverage.STRONG and strong.top_score == pytest.approx(1.0, abs=1e-6)


async def test_coverage_is_weak_when_the_two_engines_do_not_agree_on_any_chunk():
    resp = await ask(milvus=Milvus(hits=[dense(i) for i in range(1, 11)]), search=Search(hits=[bm25(i) for i in range(21, 31)]), top_k=2)
    assert resp.coverage == Coverage.WEAK and 0.3 <= resp.top_score < settings.COVERAGE_RANK_STRONG_MIN     # best vote: rank 1 in ONE leg only


async def test_coverage_can_be_disabled(monkeypatch):
    monkeypatch.setattr(settings, "COVERAGE_ENABLED", False)   # also the shipped default until the thresholds are calibrated
    resp = await ask()
    assert resp.coverage == Coverage.COVERAGE_UNSPECIFIED and not resp.HasField("top_score")


# ---- neighbour expansion -----------------------------------------------------------------------------------------------------
async def test_neighbours_are_attached_while_the_budget_allows(monkeypatch):
    monkeypatch.setattr(settings, "EXPAND_NEIGHBOURS", True)
    nb = {0: [bm25(100)]}
    resp = await ask(search=Search(neighbours=nb), top_k=1, max_tokens=0)
    assert ids(resp) == ["c0", "c100"]
    tight = await ask(search=Search(neighbours=nb), top_k=1, max_tokens=130)    # one chunk is about 100+ tokens: no room for the neighbour
    assert ids(tight) == ["c0"]


async def test_neighbours_are_off_by_default():
    resp = await ask(search=Search(neighbours={0: [bm25(100)]}), top_k=1)
    assert ids(resp) == ["c0"]


# ---- feedback log ------------------------------------------------------------------------------------------------------------
async def test_feedback_line_has_a_hash_stages_and_chunk_ids_but_no_query_text_by_default():
    with structlog.testing.capture_logs() as logs:
        await ask(query="secret question text", dense_query="english form")
    line = next(e for e in logs if e["event"] == "retrieval.feedback")
    assert line["rewrite_used"] is True and line["chunk_ids"] and set(line["stage_ms"]) >= {"embed", "search", "select"}
    assert len(line["query_hash"]) == 16 and "query" not in line and "dense_query" not in line
    assert "secret question text" not in str(line) and "passage" not in str(line)             # never document content either


async def test_feedback_line_includes_the_text_only_behind_the_flag(monkeypatch):
    monkeypatch.setattr(settings, "LOG_QUERY_TEXT", True)
    with structlog.testing.capture_logs() as logs:
        await ask(query="hello there")
    assert next(e for e in logs if e["event"] == "retrieval.feedback")["query"] == "hello there"


# ---- GetDocumentChunks -------------------------------------------------------------------------------------------------------
def doc_chunks(doc, n):
    return [BM25SearchHit(chunk_id=f"{doc}-{i}", score=0, document_id=doc, filename=f"{doc}.md", content=text(i), section="S", chunk_index=i) for i in range(n)]


async def full(files=None, file_ids=(D1, D2), max_tokens=0, search=None):
    req = GetDocumentChunksRequest(user_id="u", file_ids=list(file_ids), max_tokens=max_tokens, trace_id="t")
    return await document_chunks.run(req, session_factory=Session, file_service_client=files or Files(),
                                     opensearch_repository=search or Search(doc_chunks={D1: doc_chunks(D1, 3), D2: doc_chunks(D2, 2)}))


async def test_document_chunks_come_back_in_request_then_document_order():
    resp = await full(file_ids=(D2, D1))
    assert [c.chunk_id for c in resp.chunks] == ["d2-0", "d2-1", "d1-0", "d1-1", "d1-2"] and resp.status == RetrievalStatus.OK and not resp.truncated
    assert resp.total_tokens > 0 and resp.ready_count == 2


async def test_document_chunks_are_cut_at_a_chunk_boundary_and_flagged_when_max_tokens_is_reached():
    one = (await full(file_ids=(D1,))).total_tokens // 3
    resp = await full(file_ids=(D1,), max_tokens=one * 2 + 5)
    assert [c.chunk_id for c in resp.chunks] == ["d1-0", "d1-1"] and resp.truncated and resp.total_tokens <= one * 2 + 5


async def test_document_chunks_only_for_documents_the_user_may_read():
    resp = await full(files=Files(authorized=[D1]))
    assert {c.source_file_id for c in resp.chunks} == {D1} and resp.authorized_count == 1
    denied = await full(files=Files(authorized=[]))
    assert denied.status == RetrievalStatus.ACCESS_DENIED and not denied.chunks
    assert (await full(file_ids=())).status == RetrievalStatus.NO_SCOPE
