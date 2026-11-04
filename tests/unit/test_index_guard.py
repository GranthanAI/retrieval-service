"""A dynamically created OpenSearch index (first write into a missing index) must be detected, and writes must never create one."""
import importlib.util
import pathlib

import pytest

from app.api import dependency_checks
from app.core.config import settings
from app.core.exceptions import IndexMappingError, IndexMissingError
from app.models.chunk_record import ChunkRecord
from app.stores import opensearch_repository as os_module
from app.stores.index_schema import mapping_problems
from app.stores.opensearch_repository import OpenSearchRepository


def _provisioned() -> dict:
    path = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "provision_opensearch_index.py"
    spec = importlib.util.spec_from_file_location("provision_opensearch_index", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {"idx": module.MAPPING}


def _dynamic() -> dict:
    """What OpenSearch builds when the first document arrives with no index: strings become text + .keyword, numbers long."""
    text = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    return {"idx": {"mappings": {"properties": {
        "chunk_id": text, "document_id": text, "conversation_id": text, "owner_id": text, "filename": text, "section": text, "content": text,
        "content_version": {"type": "long"}, "chunk_index": {"type": "long"}, "page_number": {"type": "long"}}}}}


class _Indices:
    def __init__(self, mapping=None):
        self.mapping, self.created = mapping, False

    def exists(self, index):
        return self.mapping is not None

    def get_mapping(self, index):
        return self.mapping

    def create(self, **kw):  # pragma: no cover - must never be called
        self.created = True


class _Client:
    def __init__(self, mapping=None):
        self.indices = _Indices(mapping)
        self.bulk_calls = 0

    def bulk(self, body, refresh=False):
        self.bulk_calls += 1


def test_provisioned_mapping_has_no_problems():
    assert mapping_problems(_provisioned(), "idx") == []


def test_dynamic_mapping_is_detected_and_every_wrong_field_is_named():
    problems = mapping_problems(_dynamic(), "idx")
    joined = "; ".join(problems)
    for field in ("document_id", "conversation_id", "chunk_id", "owner_id", "content_version", "chunk_index", "page_number"):
        assert f"field '{field}'" in joined
    assert "'long'" not in joined and "is long, expected integer" in joined


async def test_verify_mapping_raises_a_clear_error_for_a_dynamic_index(monkeypatch):
    monkeypatch.setattr(os_module, "_client", _Client(_dynamic()))
    with pytest.raises(IndexMappingError, match="wrong mapping.*document_id.*provision_opensearch_index"):
        await OpenSearchRepository().verify_mapping()


async def test_verify_mapping_raises_for_a_missing_index(monkeypatch):
    monkeypatch.setattr(os_module, "_client", _Client(None))
    with pytest.raises(IndexMissingError):
        await OpenSearchRepository().verify_mapping()


async def test_verify_mapping_passes_for_the_provisioned_mapping(monkeypatch):
    monkeypatch.setattr(os_module, "_client", _Client(_provisioned()))
    await OpenSearchRepository().verify_mapping()


async def test_a_write_into_a_missing_index_raises_and_never_reaches_bulk(monkeypatch):
    client = _Client(None)
    monkeypatch.setattr(os_module, "_client", client)
    record = ChunkRecord("c", "d", 1, 0, "o", None, "f.md", None, None, "text", [0.0])
    with pytest.raises(IndexMissingError):
        await OpenSearchRepository().upsert_chunks([record])
    assert client.bulk_calls == 0 and not client.indices.created


async def test_readiness_reports_the_reason_when_the_mapping_is_wrong(monkeypatch):
    monkeypatch.setattr(os_module, "_client", _Client(_dynamic()))

    async def ok():
        return None

    monkeypatch.setattr(OpenSearchRepository, "ping", lambda self: ok())
    assert await dependency_checks.check_opensearch() is False
    assert "document_id" in dependency_checks.last_errors["opensearch"]
