"""Live OpenSearch (the root docker-compose OpenSearch 2.11): the phrase-boost keyword query and its scope guarantees.

These run against a throwaway index created with the REAL mapping from scripts/provision_opensearch_index.py (never against the
shared `retrieval_document_chunks`, whose mapping on a dev machine may have been auto-created and differ), and delete it afterwards.

Security property under test: a document outside the request's scope (not authorised, or not the current content_version) is NEVER
returned, even when it matches the query perfectly, including as an exact phrase or exact heading. The phrase boosts are `should`
clauses; if the per-document scope were a `should` clause with minimum_should_match=1 (the pre-Phase-1 shape), a phrase hit in an
out-of-scope document would satisfy that minimum and leak. `test_the_guard_is_sensitive_to_scope_in_should` proves these tests
would catch that regression."""
import importlib.util
import pathlib
import uuid

import pytest
import pytest_asyncio
from opensearchpy import OpenSearch

from app.core.config import settings
from app.models.chunk_record import ChunkRecord
from app.stores import opensearch_repository as os_module
from app.stores.opensearch_repository import OpenSearchRepository

HEADING = "Phase 6: Payment gateway webhooks"
PHRASE = "webhook ingestion from the payment gateway"


def _provisioned_mapping() -> dict:
    path = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "provision_opensearch_index.py"
    spec = importlib.util.spec_from_file_location("provision_opensearch_index", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.MAPPING


def _record(document_id: str, version: int, index: int, section: str, content: str) -> ChunkRecord:
    return ChunkRecord(
        chunk_id=str(uuid.uuid4()), document_id=document_id, content_version=version, chunk_index=index, owner_id=str(uuid.uuid4()),
        conversation_id=None, filename=f"{document_id[:4]}.md", page_number=None, section=section, content=content, embedding=[0.0],
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def live():
    """(repo, client, ids) with four chunks: in-scope current, out-of-scope document with an identical heading and phrase, and an
    in-scope document's OLD version carrying the same heading and phrase."""
    client = OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","))
    index = f"it_scope_{uuid.uuid4().hex[:8]}"
    saved = (settings.OPENSEARCH_INDEX_NAME, os_module._client)
    client.indices.create(index=index, body=_provisioned_mapping())
    settings.OPENSEARCH_INDEX_NAME, os_module._client = index, None
    try:
        doc_in, doc_out = str(uuid.uuid4()), str(uuid.uuid4())
        body = f"In this phase the team builds {PHRASE} with idempotent processing."
        current = _record(doc_in, 2, 0, HEADING, body + " Current version.")
        old = _record(doc_in, 1, 0, HEADING, body + " Superseded legacy ledger rewrite.")
        outsider = _record(doc_out, 1, 0, HEADING, body + " Another tenant's copy.")
        unrelated = _record(doc_in, 2, 1, "Open items", "Decide whether credit notes expire.")
        repo = OpenSearchRepository()
        await repo.upsert_chunks([current, old, outsider, unrelated])
        yield repo, client, {"doc_in": doc_in, "doc_out": doc_out, "current": current.chunk_id, "old": old.chunk_id,
                             "outsider": outsider.chunk_id, "unrelated": unrelated.chunk_id}
    finally:
        client.indices.delete(index=index, ignore_unavailable=True)
        settings.OPENSEARCH_INDEX_NAME, os_module._client = saved


def _ids(hits) -> list[str]:
    return [h.chunk_id for h in hits]


@pytest.mark.asyncio(loop_scope="module")
async def test_phrase_boost_query_runs_end_to_end_and_ranks_the_exact_heading_first(live):
    repo, _, ids = live
    hits = await repo.search(HEADING, 10, {ids["doc_in"]: 2})   # would raise on a 400 from OpenSearch if the body were malformed
    assert _ids(hits)[0] == ids["current"]


@pytest.mark.asyncio(loop_scope="module")
async def test_phrase_boost_actually_raises_the_score_of_the_exact_phrase(live):
    repo, _, ids = live
    boosted = {h.chunk_id: h.score for h in await repo.search(PHRASE, 10, {ids["doc_in"]: 2})}
    settings.OPENSEARCH_PHRASE_BOOST_ENABLED = False
    try:
        plain = {h.chunk_id: h.score for h in await repo.search(PHRASE, 10, {ids["doc_in"]: 2})}
    finally:
        settings.OPENSEARCH_PHRASE_BOOST_ENABLED = True
    assert boosted[ids["current"]] > plain[ids["current"]]


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("query", [HEADING, PHRASE, f'{HEADING} {PHRASE}', "phase 6 webhook ingestion gateway"])
async def test_out_of_scope_document_is_never_returned_even_on_an_exact_heading_or_phrase_match(live, query):
    repo, _, ids = live
    hits = await repo.search(query, 50, {ids["doc_in"]: 2})
    assert ids["outsider"] not in _ids(hits)
    assert {h.document_id for h in hits} <= {ids["doc_in"]}


@pytest.mark.asyncio(loop_scope="module")
async def test_scope_naming_only_the_other_document_returns_only_that_document(live):
    repo, _, ids = live
    hits = await repo.search(HEADING, 50, {ids["doc_out"]: 1})
    assert _ids(hits) == [ids["outsider"]]


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("query", [HEADING, PHRASE, "Superseded legacy ledger rewrite", "legacy ledger rewrite"])
async def test_an_older_content_version_is_never_returned(live, query):
    repo, _, ids = live
    hits = await repo.search(query, 50, {ids["doc_in"]: 2})
    assert ids["old"] not in _ids(hits)
    assert all(h.chunk_id != ids["old"] for h in hits)


@pytest.mark.asyncio(loop_scope="module")
async def test_older_version_is_returned_only_when_it_is_the_requested_version(live):
    repo, _, ids = live
    hits = await repo.search("legacy ledger rewrite", 50, {ids["doc_in"]: 1})
    assert _ids(hits) == [ids["old"]]


@pytest.mark.asyncio(loop_scope="module")
async def test_empty_scope_returns_nothing(live):
    repo, _, _ = live
    assert await repo.search(HEADING, 50, {}) == []


@pytest.mark.asyncio(loop_scope="module")
async def test_the_guard_is_sensitive_to_scope_in_should(live):
    """The regression these tests exist to catch: scope as `should` + minimum_should_match=1 alongside the phrase boosts. Run
    against the same live index, that shape DOES return the out-of-scope chunk, so the assertions above are not vacuous."""
    _, client, ids = live
    scope = {"bool": {"filter": [{"term": {"document_id": ids["doc_in"]}}, {"term": {"content_version": 2}}]}}
    regressed = {
        "size": 50,
        "query": {"bool": {
            "must": {"multi_match": {"query": HEADING, "fields": ["content", "section^1.5", "filename^2.0"]}},
            "should": [scope,
                       {"match_phrase": {"section": {"query": HEADING, "boost": 5.0}}},
                       {"match_phrase": {"content": {"query": HEADING, "boost": 2.0}}}],
            "minimum_should_match": 1,
        }},
    }
    leaked = {h["_id"] for h in client.search(index=settings.OPENSEARCH_INDEX_NAME, body=regressed)["hits"]["hits"]}
    assert ids["outsider"] in leaked


@pytest.mark.asyncio(loop_scope="module")
async def test_an_index_created_by_a_first_write_is_detected():
    """The environment bug that broke 7 integration tests: indexing a document into a missing index auto-creates it with dynamic
    mapping (document_id becomes text). verify_mapping must report that clearly."""
    from app.core.exceptions import IndexMappingError

    client = OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","))
    index = f"it_dynamic_{uuid.uuid4().hex[:8]}"
    saved = (settings.OPENSEARCH_INDEX_NAME, os_module._client)
    try:
        client.index(index=index, id="1", body={"document_id": str(uuid.uuid4()), "content_version": 1, "content": "x"}, refresh=True)
        settings.OPENSEARCH_INDEX_NAME, os_module._client = index, None
        with pytest.raises(IndexMappingError, match="document_id"):
            await OpenSearchRepository().verify_mapping()
    finally:
        client.indices.delete(index=index, ignore_unavailable=True)
        settings.OPENSEARCH_INDEX_NAME, os_module._client = saved


@pytest.mark.asyncio(loop_scope="module")
async def test_a_write_to_a_missing_index_does_not_create_it():
    from app.core.exceptions import IndexMissingError

    client = OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","))
    index = f"it_missing_{uuid.uuid4().hex[:8]}"
    saved = (settings.OPENSEARCH_INDEX_NAME, os_module._client)
    settings.OPENSEARCH_INDEX_NAME, os_module._client = index, None
    try:
        with pytest.raises(IndexMissingError):
            await OpenSearchRepository().upsert_chunks([_record(str(uuid.uuid4()), 1, 0, "s", "c")])
        assert not client.indices.exists(index=index)
    finally:
        client.indices.delete(index=index, ignore_unavailable=True)
        settings.OPENSEARCH_INDEX_NAME, os_module._client = saved
