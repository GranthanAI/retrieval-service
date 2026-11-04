"""The single build path (parsed document -> chunks -> records) and the embedding model profile check."""
import pytest

from app.core.exceptions import EmbeddingModelMismatchError
from app.ingestion import build, text_parsers
from app.ingestion.tokenization import ModelProfile, WordTokenCounter, validate_profile
from app.stores.generations import GenerationInfo

G1 = GenerationInfo(1, "c", "i")
G2 = GenerationInfo(2, "c2", "i2", "2", "2", "2", status="building")
MD = b"# Plan\n\n## Phase 6: Webhooks\n\n#### Exit criteria\n\n- replay ten times gives one record\n"


@pytest.fixture(autouse=True)
def runtime():
    saved = (build.runtime.profile, build.runtime.counter)
    build.runtime.profile, build.runtime.counter = ModelProfile("all-MiniLM-L6-v2", 384, 256, "x/y"), WordTokenCounter()
    yield
    build.runtime.profile, build.runtime.counter = saved


def test_generation_1_uses_the_legacy_chunker_and_content_is_the_bare_body():
    chunks = build.chunk_parsed(text_parsers.parse_markdown(MD, "plan.md"), "plan.md", G1)
    assert chunks and all("›" not in c.content for c in chunks) and all(c.heading_path == [] for c in chunks)


def test_generation_2_prefixes_the_heading_path_and_the_records_carry_the_provenance():
    chunks = build.chunk_parsed(text_parsers.parse_markdown(MD, "plan.md"), "plan.md", G2)
    exit_chunk = next(c for c in chunks if c.section == "Exit criteria")
    assert "Phase 6" in exit_chunk.content
    records = build.make_records(chunks, [[0.0] * 2 for _ in chunks], document_id="d", content_version=1, owner_id="o",
                                 conversation_id=None, filename="plan.md", info=G2)
    rec = next(r for r in records if r.section == "Exit criteria")
    assert rec.content == exit_chunk.content                      # what is stored/indexed is exactly what was (to be) embedded
    assert rec.heading_path == "Plan › Phase 6: Webhooks › Exit criteria" and rec.generation == 2 and rec.location.startswith("Lines ")
    assert [r.chunk_id for r in records] == [r.chunk_id for r in build.make_records(chunks, [[0.0] * 2] * len(chunks), document_id="d", content_version=1,
                                                                                 owner_id="o", conversation_id=None, filename="plan.md", info=G2)]


def test_chunker_v2_without_a_model_profile_refuses_instead_of_guessing():
    build.runtime.profile = build.runtime.counter = None
    with pytest.raises(RuntimeError, match="model profile"):
        build.chunk_parsed(text_parsers.parse_markdown(MD, "plan.md"), "plan.md", G2)


def test_the_model_name_and_dimension_are_validated_against_the_expectation():
    validate_profile(ModelProfile("all-MiniLM-L6-v2", 384, 256, "x"))
    with pytest.raises(EmbeddingModelMismatchError):
        validate_profile(ModelProfile("bge-m3", 1024, 8192, "x"))
    with pytest.raises(EmbeddingModelMismatchError):
        validate_profile(ModelProfile("all-MiniLM-L6-v2", 768, 256, "x"))
