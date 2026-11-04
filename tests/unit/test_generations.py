"""Index generations: which collection/index a read or write targets, generation-2 documents, and the response metadata."""
import pytest

from app.core.config import settings
from app.models.chunk_record import ChunkRecord
from app.query import response_mapper
from app.query.scoring import FusedCandidate
from app.stores import generations
from app.stores import milvus_repository as milvus_module
from app.stores import opensearch_repository as os_module
from app.stores.generations import ACTIVE, BUILDING, GenerationInfo
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository
from evaluation.fakes import FakeMilvusClient, FakeOpenSearchClient

G2 = GenerationInfo(2, "chunks_g2", "chunks_g2", "2", "2", "2", "all-MiniLM-L6-v2", 384, BUILDING)


@pytest.fixture(autouse=True)
def clean_registry():
    generations.set_registered([])
    yield
    generations.set_registered([])


class RecordingMilvus(FakeMilvusClient):
    def __init__(self):
        super().__init__()
        self.calls = []

    def upsert(self, collection_name, data):
        self.calls.append(("upsert", collection_name, sorted(data[0])))
        super().upsert(collection_name, data)

    def search(self, collection_name, *a, **kw):
        self.calls.append(("search", collection_name, kw["output_fields"]))
        return super().search(collection_name, *a, **kw)

    def delete(self, collection_name, filter):
        self.calls.append(("delete", collection_name, None))
        super().delete(collection_name, filter)


class RecordingOpenSearch(FakeOpenSearchClient):
    def __init__(self):
        super().__init__()
        self.calls = []

    def bulk(self, body, refresh=False):
        self.calls.append(("bulk", body[0]["index"]["_index"], sorted(body[1])))
        return super().bulk(body, refresh)

    def search(self, index, body, request_timeout=None):
        self.calls.append(("search", index, body["query"]["bool"]["must"]["multi_match"]["fields"]))
        return super().search(index, body, request_timeout)

    def delete_by_query(self, index, body, refresh=False):
        self.calls.append(("delete", index, None))
        return super().delete_by_query(index, body, refresh)


@pytest.fixture
def stores(monkeypatch):
    m, o = RecordingMilvus(), RecordingOpenSearch()
    monkeypatch.setattr(milvus_module, "_client", m)
    monkeypatch.setattr(os_module, "_client", o)
    return m, o


def record(**kw) -> ChunkRecord:
    base = dict(chunk_id="c1", document_id="d1", content_version=1, chunk_index=0, owner_id="o", conversation_id=None, filename="plan.md",
                page_number=3, section="Exit criteria", content="plan.md › Phase 6 › Exit criteria\nreplay ten times", embedding=[1.0, 0.0],
                heading_path="Phase 6 › Exit criteria", page_start=3, page_end=5, block_type="text", location=None, generation=2)
    base.update(kw)
    return ChunkRecord(**base)


def test_without_a_registry_generation_1_is_the_legacy_unsuffixed_pair(monkeypatch):
    monkeypatch.setattr(settings, "MILVUS_COLLECTION_NAME", "legacy_c")
    monkeypatch.setattr(settings, "OPENSEARCH_INDEX_NAME", "legacy_i")
    info = generations.current()
    assert (info.generation, info.milvus_collection, info.opensearch_index, info.schema_version) == (1, "legacy_c", "legacy_i", "1")


def test_generation_names_follow_the_g_n_convention():
    assert generations.names_for(2) == (f"{settings.MILVUS_COLLECTION_NAME}_g2", f"{settings.OPENSEARCH_INDEX_NAME}_g2")


def test_current_is_the_active_generation_not_a_building_one():
    active = GenerationInfo(1, "c1", "i1", status=ACTIVE, opensearch_alias="alias")
    generations.set_registered([G2, active])
    assert generations.current().generation == 1 and generations.current().opensearch_read_target == "alias"
    assert [g.generation for g in generations.all_live()] == [1, 2]       # active first, building included (deletes must reach it)


async def test_writes_and_reads_target_the_pinned_generation_and_use_its_schema(stores):
    m, o = stores
    with generations.use(G2):
        await MilvusRepository().upsert_chunks([record()])
        await OpenSearchRepository().upsert_chunks([record()])
        await MilvusRepository().search([1.0, 0.0], 10, {"d1": 1})
        hits = await OpenSearchRepository().search("replay", 10, {"d1": 1})
    assert [c[:2] for c in m.calls] == [("upsert", "chunks_g2"), ("search", "chunks_g2")]
    assert [c[:2] for c in o.calls] == [("bulk", "chunks_g2"), ("search", "chunks_g2")]
    assert {"heading_path", "page_start", "page_end", "block_type", "location", "generation"} <= set(m.calls[0][2])
    assert {"heading_path", "page_start", "page_end", "block_type", "generation"} <= set(o.calls[0][2])
    assert "content.exact^2" in o.calls[1][2] and "section.english" in o.calls[1][2]            # generation-2 keyword fields
    assert hits and (hits[0].heading_path, hits[0].page_start, hits[0].page_end) == ("Phase 6 › Exit criteria", 3, 5)


async def test_generation_1_keeps_the_original_documents_and_query_fields(stores, monkeypatch):
    monkeypatch.setattr(settings, "MILVUS_COLLECTION_NAME", "legacy_c")
    monkeypatch.setattr(settings, "OPENSEARCH_INDEX_NAME", "legacy_i")
    m, o = stores
    await MilvusRepository().upsert_chunks([record(generation=1)])
    await OpenSearchRepository().upsert_chunks([record(generation=1)])
    await OpenSearchRepository().search("replay", 10, {"d1": 1})
    assert "heading_path" not in m.calls[0][2] and "heading_path" not in o.calls[0][2]
    assert o.calls[1][2] == ["content", "section^1.5", "filename^2.0"]


async def test_a_deleted_file_is_removed_from_every_live_generation(stores):
    m, o = stores
    generations.set_registered([GenerationInfo(1, "c1", "i1", status=ACTIVE), G2])
    await MilvusRepository().delete_by_document_id("d1")
    await OpenSearchRepository().delete_by_document_id("d1")
    assert sorted(c[1] for c in m.calls if c[0] == "delete") == ["c1", "chunks_g2"]
    assert sorted(c[1] for c in o.calls if c[0] == "delete") == ["chunks_g2", "i1"]


def test_response_metadata_exposes_the_new_provenance_only_when_present():
    old = FusedCandidate("c", page_number=5, section="Tasks", chunk_index=2)
    assert response_mapper._metadata(old) == {"page_number": "5", "section": "Tasks", "chunk_index": "2"}      # exactly as before
    new = FusedCandidate("c", page_number=3, section="Recon", chunk_index=1, heading_path="A › B", page_start=3, page_end=5,
                         block_type="table", location="Rows 1-40")
    md = response_mapper._metadata(new)
    assert (md["heading_path"], md["page_start"], md["page_end"], md["block_type"], md["location"]) == ("A › B", "3", "5", "table", "Rows 1-40")
