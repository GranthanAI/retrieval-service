"""Retrieval status contract: an empty answer always says WHY it is empty.

Every external dependency (File Service, embedding, Milvus, OpenSearch, Postgres lookups) is a
fake here, so this isolates the decision logic of `pipeline.run()`. The real-database and
real-store paths are covered by tests/integration.
"""

import uuid

import pytest

from app.clients.file_service_client import PermissionCheckResult
from app.core import metrics
from app.core.enums import RetrievalScope
from app.grpc.retrieval_pb2 import GetRelevantChunksRequest, RetrievalStatus
from app.query import pipeline
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.types import BM25SearchHit, DenseSearchHit


class _Session:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


def _session_factory():
    return _Session()


class _Files:
    def __init__(self, authorized=None, failed=False):
        self.authorized, self.failed = authorized, failed

    async def check_permissions(self, user_id, document_ids):
        if self.failed:
            return PermissionCheckResult(failed=True, error="ConnectError")
        allowed = document_ids if self.authorized is None else [d for d in document_ids if d in self.authorized]
        return PermissionCheckResult(authorized=list(allowed))


class _Embed:
    def __init__(self, fail=False):
        self.fail = fail

    async def embed_query(self, text):
        if self.fail:
            raise RuntimeError("embedding down")
        return [0.1, 0.2]


class _Milvus:
    def __init__(self, hits=None, boom=False):
        self.hits, self.boom, self.versions = hits or [], boom, None

    async def search(self, query_vector, top_n, current_versions):
        self.versions = dict(current_versions)
        if self.boom:
            raise RuntimeError("milvus down")
        return self.hits

    async def get_by_chunk_ids(self, ids):
        return {i: [1.0, 0.0] for i in ids}


class _Search:
    def __init__(self, hits=None, boom=False):
        self.hits, self.boom = hits or [], boom

    async def search(self, query_text, top_n, current_versions):
        if self.boom:
            raise RuntimeError("opensearch down")
        return self.hits


def _req(file_ids, scope=RetrievalScope.FILES):
    return GetRelevantChunksRequest(
        user_id="u1", conversation_id="c1", query="what is the access code?", top_k=5,
        scope=int(scope), file_ids=file_ids, trace_id="t-1",
    )


def _patch_state(monkeypatch, versions: dict, jobs: dict):
    async def get_many(db, ids):
        return {d: versions[d] for d in ids if d in versions}

    async def statuses_for(db, vers):
        return {d: jobs[d] for d in vers if d in jobs}

    monkeypatch.setattr(DocumentCurrentVersionRepository, "get_many", staticmethod(get_many))
    monkeypatch.setattr(IngestionJobRepository, "statuses_for", staticmethod(statuses_for))


async def _run(req, *, files=None, embed=None, milvus=None, search=None):
    return await pipeline.run(
        req,
        session_factory=_session_factory,
        file_service_client=files or _Files(),
        embedding_client=embed or _Embed(),
        milvus_repository=milvus or _Milvus(),
        opensearch_repository=search or _Search(),
    )


def _name(resp):
    return RetrievalStatus.Name(resp.status)


async def test_no_file_scope_is_reported():
    r = await _run(_req([]))
    assert _name(r) == "NO_SCOPE" and not r.chunks


async def test_unsupported_scope_is_reported():
    r = await _run(_req(["d1"], scope=RetrievalScope.ALL))
    assert _name(r) == "NO_SCOPE"


async def test_denied_or_missing_documents_are_access_denied_not_empty_success():
    r = await _run(_req(["d1"]), files=_Files(authorized=[]))
    assert _name(r) == "ACCESS_DENIED"
    assert r.requested_count == 1 and r.authorized_count == 0


async def test_permission_check_outage_is_unavailable_not_denied():
    r = await _run(_req(["d1"]), files=_Files(failed=True))
    assert _name(r) == "UNAVAILABLE"
    assert "permission" in r.status_detail


async def test_never_authorizes_everything_when_the_check_fails():
    # Fail closed: even though the document exists, an outage must not leak chunks.
    r = await _run(_req(["d1"]), files=_Files(failed=True), milvus=_Milvus(hits=[DenseSearchHit(chunk_id="c", score=1, document_id="d1", filename="a.pdf", content="x")]))
    assert not r.chunks


async def test_registered_but_never_ingested_is_not_indexed(monkeypatch):
    _patch_state(monkeypatch, versions={}, jobs={})
    assert _name(await _run(_req(["d1"]))) == "NOT_INDEXED"


async def test_current_version_still_being_indexed_is_indexing(monkeypatch):
    _patch_state(monkeypatch, versions={"d1": 2}, jobs={"d1": ("PROCESSING", None)})
    r = await _run(_req(["d1"]))
    assert _name(r) == "INDEXING" and r.authorized_count == 1 and r.ready_count == 0


async def test_partially_indexed_is_not_searchable(monkeypatch):
    _patch_state(monkeypatch, versions={"d1": 1}, jobs={"d1": ("PARTIALLY_INDEXED", 3)})
    assert _name(await _run(_req(["d1"]))) == "INDEXING"


async def test_failed_indexing_is_reported(monkeypatch):
    _patch_state(monkeypatch, versions={"d1": 1}, jobs={"d1": ("FAILED", None)})
    assert _name(await _run(_req(["d1"]))) == "INDEXING_FAILED"


async def test_embedding_outage_is_unavailable(monkeypatch):
    _patch_state(monkeypatch, versions={"d1": 1}, jobs={"d1": ("READY", 4)})
    assert _name(await _run(_req(["d1"]), embed=_Embed(fail=True))) == "UNAVAILABLE"


async def test_both_search_engines_down_is_unavailable(monkeypatch):
    _patch_state(monkeypatch, versions={"d1": 1}, jobs={"d1": ("READY", 4)})
    r = await _run(_req(["d1"]), milvus=_Milvus(boom=True), search=_Search(boom=True))
    assert _name(r) == "UNAVAILABLE"


async def test_indexed_document_without_a_relevant_passage_is_no_match(monkeypatch):
    _patch_state(monkeypatch, versions={"d1": 1}, jobs={"d1": ("READY", 4)})
    r = await _run(_req(["d1"]))
    assert _name(r) == "NO_MATCH" and r.ready_count == 1


async def test_chunks_are_ok_and_only_ready_versions_are_searched(monkeypatch):
    _patch_state(
        monkeypatch,
        versions={"d1": 3, "d2": 1},
        jobs={"d1": ("READY", 4), "d2": ("PROCESSING", None)},  # d2 is mid-indexing
    )
    milvus = _Milvus(hits=[DenseSearchHit(chunk_id="c1", score=0.9, document_id="d1", filename="a.pdf", content="the code is X")])
    r = await _run(_req(["d1", "d2"]), milvus=milvus, search=_Search(hits=[BM25SearchHit(chunk_id="c1", score=5, document_id="d1", filename="a.pdf", content="the code is X")]))
    assert _name(r) == "OK" and len(r.chunks) == 1
    assert (r.requested_count, r.authorized_count, r.ready_count) == (2, 2, 1)
    assert milvus.versions == {"d1": 3}  # the still-indexing document is never searched


async def test_partial_authorization_still_answers_from_the_accessible_document(monkeypatch):
    _patch_state(monkeypatch, versions={"d1": 1}, jobs={"d1": ("READY", 4)})
    milvus = _Milvus(hits=[DenseSearchHit(chunk_id="c1", score=0.9, document_id="d1", filename="a.pdf", content="x")])
    r = await _run(_req(["d1", "secret"]), files=_Files(authorized=["d1"]), milvus=milvus)
    assert _name(r) == "OK" and r.requested_count == 2 and r.authorized_count == 1


async def test_status_counter_is_incremented(monkeypatch):
    before = metrics.query_status_total.labels(status="ACCESS_DENIED")._value.get()
    await _run(_req(["d1"]), files=_Files(authorized=[]))
    assert metrics.query_status_total.labels(status="ACCESS_DENIED")._value.get() == before + 1


async def test_ids_are_deduplicated_in_counts():
    d = str(uuid.uuid4())
    r = await _run(_req([d, d, d]), files=_Files(authorized=[]))
    assert r.requested_count == 1
