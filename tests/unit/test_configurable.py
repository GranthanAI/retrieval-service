"""Models and parsing are configuration: Docling options, the reranker (on/off, which one, its score thresholds), and the embedding model
(taken from the active index generation). Every choice must come from Settings (environment / .env) and fail loudly when it is invalid."""
import subprocess
import sys

import pytest
from pydantic import ValidationError

from app.clients import embedding_client
from app.core.config import Settings, settings
from app.core.exceptions import EmbeddingModelMismatchError
from app.grpc import embedding_pb2
from app.grpc.retrieval_pb2 import Coverage
from app.ingestion import docling_config, parser
from app.ingestion.parsed import ParserError
from app.ingestion.tokenization import ModelProfile, validate_profile
from app.query import coverage
from app.query.scoring import FusedCandidate
from app.stores import generations
from app.stores.generations import ACTIVE, GenerationInfo

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    generations.set_registered([])
    docling_config.reset()
    yield
    generations.set_registered([])
    docling_config.reset()


def env_settings(monkeypatch, **env) -> Settings:
    for k, v in env.items():
        monkeypatch.setenv(k, str(v))
    return Settings(_env_file=None)


# ---- Docling ------------------------------------------------------------------------------------------------------------------
def test_docling_defaults_reproduce_the_behaviour_before_it_was_configurable(monkeypatch):
    for k in ("DOCLING_ENABLED", "DOCLING_TABLE_STRUCTURE", "DOCLING_TABLE_MODE", "DOCLING_DEVICE", "DOCLING_NUM_THREADS", "DOCLING_DOCUMENT_TIMEOUT_SECONDS", "DOCLING_REUSE_CONVERTER"):
        monkeypatch.delenv(k, raising=False)
    s = Settings(_env_file=None)
    assert (s.DOCLING_ENABLED, s.DOCLING_TABLE_STRUCTURE, s.DOCLING_TABLE_MODE, s.DOCLING_DEVICE, s.DOCLING_NUM_THREADS,
            s.DOCLING_DOCUMENT_TIMEOUT_SECONDS, s.DOCLING_REUSE_CONVERTER) == (True, True, "", "auto", 0, 0.0, False)
    assert docling_config.parser_options_suffix() == ""            # default options leave parser_version unchanged


def test_docling_settings_come_from_the_environment_and_invalid_values_fail_at_startup(monkeypatch):
    s = env_settings(monkeypatch, DOCLING_ENABLED="false", DOCLING_TABLE_MODE="fast", DOCLING_DEVICE="cpu", DOCLING_NUM_THREADS="2")
    assert (s.DOCLING_ENABLED, s.DOCLING_TABLE_MODE, s.DOCLING_DEVICE, s.DOCLING_NUM_THREADS) == (False, "fast", "cpu", 2)
    for bad in ({"DOCLING_TABLE_MODE": "turbo"}, {"DOCLING_DEVICE": "tpu"}):
        with pytest.raises(ValidationError):
            env_settings(monkeypatch, **bad)
        monkeypatch.undo()


def test_pipeline_options_follow_the_settings(monkeypatch):
    monkeypatch.setattr(settings, "DOCLING_TABLE_STRUCTURE", True)
    monkeypatch.setattr(settings, "DOCLING_TABLE_MODE", "fast")
    monkeypatch.setattr(settings, "DOCLING_DEVICE", "cpu")
    monkeypatch.setattr(settings, "DOCLING_NUM_THREADS", 2)
    monkeypatch.setattr(settings, "DOCLING_DOCUMENT_TIMEOUT_SECONDS", 30.0)
    o = docling_config.pipeline_options()
    assert o.do_table_structure is True and o.table_structure_options.mode.value == "fast" and o.do_ocr is False
    assert o.accelerator_options.num_threads == 2 and str(o.accelerator_options.device.value if hasattr(o.accelerator_options.device, "value") else o.accelerator_options.device) == "cpu"
    assert o.document_timeout == 30.0
    monkeypatch.setattr(settings, "DOCLING_TABLE_STRUCTURE", False)
    assert docling_config.pipeline_options().do_table_structure is False
    assert docling_config.pipeline_options(do_ocr=True).do_ocr is True


def test_parse_affecting_options_are_recorded_in_the_parser_version_and_a_mismatch_is_detected(monkeypatch):
    monkeypatch.setattr(settings, "DOCLING_TABLE_STRUCTURE", False)
    monkeypatch.setattr(settings, "DOCLING_TABLE_MODE", "fast")
    assert docling_config.parser_options_suffix() == "+tables=off,tablemode=fast"
    assert docling_config.parser_mismatch("2") == "2+tables=off,tablemode=fast"            # generation built with defaults, settings changed since
    assert docling_config.parser_mismatch("2+tables=off,tablemode=fast") is None
    monkeypatch.setattr(settings, "DOCLING_TABLE_STRUCTURE", True)
    monkeypatch.setattr(settings, "DOCLING_TABLE_MODE", "")
    assert docling_config.parser_mismatch("2") is None


def test_with_docling_disabled_pdf_and_office_files_are_refused_clearly_and_text_still_works(monkeypatch):
    monkeypatch.setattr(settings, "DOCLING_ENABLED", False)
    for name in ("report.pdf", "plan.docx", "deck.pptx", "sheet.xlsx", "page.html"):
        with pytest.raises(ParserError, match="turned off"):
            parser.parse(b"%PDF-1.4 not parsed", name)
    assert parser.parse(b"# Title\n\nbody text", "notes.md").sections[0].text == "body text"
    assert parser.parse(b"a,b\n1,2\n", "t.csv").sections


def test_importing_the_parser_does_not_load_docling():
    code = f"import sys; sys.path.insert(0, {str(ROOT)!r}); import app.ingestion.parser; print('docling' in sys.modules, 'torch' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=ROOT, timeout=120)
    assert out.returncode == 0, out.stderr[-800:]
    assert out.stdout.strip().splitlines()[-1] == "False False"


def test_the_converter_is_built_per_file_by_default_and_once_when_reuse_is_on(monkeypatch):
    built = []

    class FakeDoc:
        document = object()

    class FakeConverter:
        def convert(self, stream):
            return FakeDoc()

    monkeypatch.setattr(docling_config, "make_converter", lambda do_ocr=False: built.append(1) or FakeConverter())
    monkeypatch.setattr(settings, "DOCLING_REUSE_CONVERTER", False)
    docling_config.convert(b"x", "a.pdf")
    docling_config.convert(b"x", "b.pdf")
    assert len(built) == 2
    built.clear()
    monkeypatch.setattr(settings, "DOCLING_REUSE_CONVERTER", True)
    docling_config.convert(b"x", "a.pdf")
    docling_config.convert(b"x", "b.pdf")
    assert len(built) == 1
    monkeypatch.setattr(settings, "DOCLING_ENABLED", False)
    with pytest.raises(ParserError):
        docling_config.convert(b"x", "c.pdf")


# ---- embedding model: the active generation decides --------------------------------------------------------------------------
E5 = GenerationInfo(2, "c2", "i2", "2", "2", "2", "intfloat/multilingual-e5-large", 1024, ACTIVE)


def test_without_a_registered_generation_the_expected_model_comes_from_settings():
    assert generations.expected_embedding() == (settings.EMBEDDING_MODEL_EXPECTED, settings.EMBEDDING_DIMENSION_EXPECTED)
    embedding_client._assert_model_matches(settings.EMBEDDING_MODEL_EXPECTED, settings.EMBEDDING_DIMENSION_EXPECTED)


def test_a_registered_generation_carries_its_own_model_and_the_wrong_one_is_refused():
    generations.set_registered([E5])
    assert generations.expected_embedding() == ("intfloat/multilingual-e5-large", 1024)
    embedding_client._assert_model_matches("intfloat/multilingual-e5-large", 1024)
    with pytest.raises(EmbeddingModelMismatchError, match="new generation"):
        embedding_client._assert_model_matches(settings.EMBEDDING_MODEL_EXPECTED, settings.EMBEDDING_DIMENSION_EXPECTED)
    with pytest.raises(EmbeddingModelMismatchError):
        embedding_client._assert_model_matches("intfloat/multilingual-e5-large", 384)       # right name, wrong size
    validate_profile(ModelProfile("intfloat/multilingual-e5-large", 1024, 512, "intfloat/multilingual-e5-large"))
    with pytest.raises(EmbeddingModelMismatchError):
        validate_profile(ModelProfile(settings.EMBEDDING_MODEL_EXPECTED, 384, 256, "x"))


def test_a_pinned_generation_is_what_a_reindex_validates_against():
    legacy_profile = ModelProfile(settings.EMBEDDING_MODEL_EXPECTED, 384, 256, "x")
    validate_profile(legacy_profile)                                                  # the active (legacy) generation
    with generations.use(E5):
        with pytest.raises(EmbeddingModelMismatchError):
            validate_profile(legacy_profile)                                          # a reindex into the e5 generation needs the e5 service
        validate_profile(ModelProfile("intfloat/multilingual-e5-large", 1024, 512, "x"))


# ---- reranker: on/off, which one, and its thresholds ---------------------------------------------------------------------------
class FakeStub:
    requests: list = []

    def __init__(self, channel):
        pass

    async def Rerank(self, request, timeout=None):
        FakeStub.requests.append(request)
        return embedding_pb2.RerankResponse(model=request.model or "default", scores=[0.5] * len(request.passages))


async def test_the_client_sends_the_chosen_reranker_only_when_one_is_chosen(monkeypatch):
    monkeypatch.setattr(embedding_client.embedding_pb2_grpc, "EmbeddingServiceStub", FakeStub)
    client = embedding_client.EmbeddingClient("localhost:1")
    monkeypatch.setattr(client, "_get_channel", lambda: None)
    FakeStub.requests.clear()
    await client.rerank("q", ["a", "b"], 1.0)
    await client.rerank("q", ["a", "b"], 1.0, model="cross-encoder/ms-marco-MiniLM-L-6-v2")
    assert [r.model for r in FakeStub.requests] == ["", "cross-encoder/ms-marco-MiniLM-L-6-v2"]


def test_reranker_choices_are_settings(monkeypatch):
    s = env_settings(monkeypatch, RERANK_ENABLED="true", RERANK_MODEL="BAAI/bge-reranker-v2-m3", RERANK_TOP_N="20")
    assert (s.RERANK_ENABLED, s.RERANK_MODEL, s.RERANK_TOP_N) == (True, "BAAI/bge-reranker-v2-m3", 20)
    for k in ("RERANK_ENABLED", "RERANK_MODEL", "RERANK_TOP_N"):
        monkeypatch.delenv(k)
    off = Settings(_env_file=None)
    assert off.RERANK_ENABLED is False and off.RERANK_MODEL == ""                    # off and server-default model unless chosen


def cand(score):
    return [FusedCandidate("c", rerank_score=score, rrf_score=0.03)]


def test_coverage_thresholds_are_per_reranker_and_an_uncalibrated_one_gives_a_score_but_no_judgement(monkeypatch):
    monkeypatch.setattr(settings, "COVERAGE_ENABLED", True)
    monkeypatch.setattr(settings, "RERANK_COVERAGE_THRESHOLDS", '{"model/a": {"weak_min": -9.0, "strong_min": -4.5}, "model/b": {"weak_min": 0.2, "strong_min": 0.6}}')
    monkeypatch.setattr(settings, "RERANK_MODEL", "model/a")
    assert coverage.signal(cand(-1.0), True) == (-1.0, Coverage.STRONG)
    assert coverage.signal(cand(-6.0), True) == (-6.0, Coverage.WEAK)
    assert coverage.signal(cand(-10.0), True) == (-10.0, Coverage.NONE)
    monkeypatch.setattr(settings, "RERANK_MODEL", "model/b")                         # same raw score, another model's scale
    assert coverage.signal(cand(-1.0), True) == (-1.0, Coverage.NONE)
    assert coverage.signal(cand(0.9), True) == (0.9, Coverage.STRONG)
    monkeypatch.setattr(settings, "RERANK_MODEL", "model/uncalibrated")
    assert coverage.signal(cand(0.9), True) == (0.9, Coverage.COVERAGE_UNSPECIFIED)
    monkeypatch.setattr(settings, "RERANK_MODEL", "")
    assert coverage.signal(cand(0.9), True)[1] == Coverage.COVERAGE_UNSPECIFIED       # key "default" has no entry
    assert coverage.signal([], True) == (0.0, Coverage.NONE)                          # nothing found is always certain


def test_a_malformed_threshold_table_fails_at_startup_not_at_the_first_query(monkeypatch):
    for bad in ("not json", '{"m": {"weak_min": 1}}', '{"m": {"weak_min": 5, "strong_min": 1}}', '[1,2]'):
        with pytest.raises(ValidationError):
            env_settings(monkeypatch, RERANK_COVERAGE_THRESHOLDS=bad)
        monkeypatch.undo()
    assert Settings(_env_file=None).RERANK_COVERAGE_THRESHOLDS                         # the shipped default is valid


def test_env_example_lists_only_real_settings_and_loads_as_valid_configuration(monkeypatch):
    """A key in .env.example that is not a Settings field is silently ignored by the app (MILVUS_SEARCH_NPROBE lingered there after it was
    removed), and a value that does not parse would only fail at deploy time."""
    import re

    keys = {}
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        m = re.match(r"^([A-Z][A-Z0-9_]+)=(.*)$", line)
        if m:
            keys[m.group(1)] = m.group(2)
    assert not sorted(k for k in keys if k not in Settings.model_fields), "keys in .env.example that are not settings"
    for k, v in keys.items():
        monkeypatch.setenv(k, v)
    Settings(_env_file=None)
    for k in ("RERANK_ENABLED", "DOCLING_ENABLED", "OCR_ENABLED", "CONTEXTUAL_CHUNKS_ENABLED", "COVERAGE_ENABLED", "EXPAND_NEIGHBOURS"):
        assert k in keys                                   # the on/off switches are all documented
