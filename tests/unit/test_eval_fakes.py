"""The OpenSearch fake must follow the real engine's semantics where the service's query depends on them."""
from evaluation.fakes import FakeOpenSearchClient


def _client():
    c = FakeOpenSearchClient()
    docs = {
        "1": {"document_id": "d1", "content_version": 1, "content": "the retry worker reads MAX_RETRY_ATTEMPTS", "section": "Retry", "filename": "a.md"},
        "2": {"document_id": "d2", "content_version": 1, "content": "retry schedules for failed payments", "section": "Dunning", "filename": "b.md"},
        "3": {"document_id": "d1", "content_version": 2, "content": "retry worker old version", "section": "Retry", "filename": "a.md"},
    }
    c.bulk([x for i, d in docs.items() for x in ({"index": {"_id": i}}, d)])
    return c


def _ids(resp):
    return [h["_id"] for h in resp["hits"]["hits"]]


def test_should_clauses_count_toward_minimum_should_match():
    # Scope expressed as `should` + minimum_should_match=1 is satisfied by ANY should clause, so an extra scoring clause
    # (e.g. a phrase boost) silently widens the scope. The fake reproduces this on purpose: Phase 1 must keep scope in `filter`.
    scope = {"bool": {"filter": [{"term": {"document_id": "d1"}}, {"term": {"content_version": 1}}]}}
    phrase = {"match_phrase": {"content": {"query": "failed payments", "boost": 2}}}
    base = {"multi_match": {"query": "retry", "fields": ["content"]}}

    narrow = _client().search("i", {"size": 10, "query": {"bool": {"must": base, "should": [scope], "minimum_should_match": 1}}})
    assert _ids(narrow) == ["1"]

    widened = _client().search("i", {"size": 10, "query": {"bool": {"must": base, "should": [scope, phrase], "minimum_should_match": 1}}})
    assert set(_ids(widened)) == {"1", "2"}


def test_filter_scope_is_strict():
    scope = [{"term": {"document_id": "d1"}}, {"term": {"content_version": 1}}]
    q = {"bool": {"must": {"multi_match": {"query": "retry worker", "fields": ["content"]}}, "filter": scope}}
    assert _ids(_client().search("i", {"size": 10, "query": q})) == ["1"]


def test_exact_subfield_keeps_identifiers_whole_and_splits_parts():
    c = _client()
    whole = c.search("i", {"size": 5, "query": {"multi_match": {"query": "MAX_RETRY_ATTEMPTS", "fields": ["content.exact"]}}})
    assert _ids(whole) == ["1"]  # the query side keeps the identifier whole: documents sharing only the part "retry" do not match
    part = c.search("i", {"size": 5, "query": {"multi_match": {"query": "attempts", "fields": ["content.exact"]}}})
    assert _ids(part) == ["1"]
