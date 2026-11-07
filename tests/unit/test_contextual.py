"""Contextual chunk text (generation 3): description written per chunk, bounded, never fatal, mock HTTP only (no live model calls)."""
import json

import httpx
import pytest

from app.core import metrics
from app.core.config import settings
from app.ingestion import build, contextual, text_parsers
from app.ingestion.tokenization import ModelProfile, WordTokenCounter
from app.stores.generations import GenerationInfo

G2 = GenerationInfo(2, "c2", "i2", "2", "2", "2", status="building")
G3 = GenerationInfo(3, "c3", "i3", "2", "2+ctx", "2", status="building")
MD = b"# Plan\n\n## Phase 6: Webhooks\n\n#### Exit criteria\n\n- replay ten times gives one record\n"


@pytest.fixture(autouse=True)
def runtime(monkeypatch):
    saved = (build.runtime.profile, build.runtime.counter)
    build.runtime.profile, build.runtime.counter = ModelProfile("all-MiniLM-L6-v2", 384, 256, "x/y"), WordTokenCounter()
    monkeypatch.setattr(settings, "CONTEXT_MAX_TOKENS", 30)
    monkeypatch.setattr(settings, "CONTEXTUAL_CHUNKS_ENABLED", True)      # the master switch is off by default
    yield
    build.runtime.profile, build.runtime.counter = saved
    build.set_context_writer(None)


class Fake:
    def __init__(self, text="This passage lists the exit criteria of Phase 6, the payment webhook phase of the plan.", boom=False):
        self.text, self.boom, self.calls = text, boom, []

    async def describe(self, *, filename, heading_path, outline, body):
        self.calls.append((filename, heading_path, outline, body))
        if self.boom:
            raise RuntimeError("model down")
        return self.text


def chunks_of(info=G3):
    parsed = text_parsers.parse_markdown(MD, "plan.md")
    return parsed, build.chunk_parsed(parsed, "plan.md", info)


def test_only_the_contextual_generation_reserves_room_and_asks_for_descriptions():
    assert contextual.reserve_tokens(G3) == 30 and contextual.reserve_tokens(G2) == 0
    assert contextual.is_contextual(G3) and not contextual.is_contextual(G2)


async def test_the_description_goes_between_the_heading_path_and_the_body_and_is_what_gets_embedded():
    parsed, chunks = chunks_of()
    build.set_context_writer(Fake())
    n = await build.contextualize(chunks, "plan.md", parsed, G3)
    exit_chunk = next(c for c in chunks if c.section == "Exit criteria")
    lines = exit_chunk.content.split("\n")
    assert n == len(chunks) and lines[0].startswith("plan.md › Plan › Phase 6")
    assert lines[1].startswith("This passage lists the exit criteria") and lines[2].startswith("- replay ten times")
    rec = build.make_records(chunks, [[0.0]] * len(chunks), document_id="d", content_version=1, owner_id="o", conversation_id=None,
                             filename="plan.md", info=G3)
    assert next(r for r in rec if r.section == "Exit criteria").content == exit_chunk.content


async def test_the_writer_gets_the_outline_and_heading_path_and_the_chunks_still_fit_the_model():
    parsed, chunks = chunks_of()
    w = Fake(text="word " * 500)                                     # absurdly long answer
    build.set_context_writer(w)
    await build.contextualize(chunks, "plan.md", parsed, G3)
    filename, path, outline, body = next(c for c in w.calls if "Exit criteria" in c[1])
    assert filename == "plan.md" and path[-1] == "Exit criteria" and "Phase 6: Webhooks" in outline and "replay" in body
    assert all(WordTokenCounter().count(c.content) <= 256 - 16 for c in chunks)


async def test_a_failing_writer_leaves_every_chunk_as_it_was_and_is_counted():
    parsed, chunks = chunks_of()
    before = [c.content for c in chunks]
    failed = metrics.contextual_chunks_total.labels(outcome="failed")._value.get()
    build.set_context_writer(Fake(boom=True))
    assert await build.contextualize(chunks, "plan.md", parsed, G3) == 0
    assert [c.content for c in chunks] == before
    assert metrics.contextual_chunks_total.labels(outcome="failed")._value.get() == failed + len(chunks)


async def test_the_master_switch_stops_every_call_even_for_a_contextual_generation(monkeypatch):
    monkeypatch.setattr(settings, "CONTEXTUAL_CHUNKS_ENABLED", False)
    parsed, chunks = chunks_of()
    before = [c.content for c in chunks]
    w = Fake()
    build.set_context_writer(w)
    assert await build.contextualize(chunks, "plan.md", parsed, G3) == 0
    assert w.calls == [] and [c.content for c in chunks] == before


async def test_generation_2_and_transcripts_never_call_the_writer():
    parsed, chunks = chunks_of(G2)
    w = Fake()
    build.set_context_writer(w)
    assert await build.contextualize(chunks, "plan.md", parsed, G2) == 0
    assert await build.contextualize(chunks, "t.json", None, G3) == 0
    assert w.calls == []


async def test_without_a_configured_model_the_generation_is_indexed_without_descriptions(monkeypatch):
    monkeypatch.setattr(settings, "CONTEXT_LLM_URL", "")
    parsed, chunks = chunks_of()
    before = [c.content for c in chunks]
    assert await build.contextualize(chunks, "plan.md", parsed, G3) == 0 and [c.content for c in chunks] == before


async def test_http_writer_posts_an_openai_style_request_and_reads_the_reply(monkeypatch):
    monkeypatch.setattr(settings, "CONTEXT_LLM_URL", "http://llm.test/v1/chat/completions")
    monkeypatch.setattr(settings, "CONTEXT_LLM_MODEL", "tiny")
    monkeypatch.setattr(settings, "CONTEXT_LLM_API_KEY", "k")
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"], seen["body"] = request.headers["authorization"], json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": '  "Describes the retry policy."  '}}]})

    writer = contextual.HttpContextWriter(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    out = await writer.describe(filename="a.md", heading_path=["A", "B"], outline=["A", "B"], body="the body text")
    assert seen["auth"] == "Bearer k" and seen["body"]["model"] == "tiny"
    assert "the body text" in seen["body"]["messages"][1]["content"] and "A > B" in seen["body"]["messages"][1]["content"]
    assert contextual.clean(out, WordTokenCounter(), 30) == "Describes the retry policy."


def test_the_http_writer_refuses_to_start_unconfigured(monkeypatch):
    monkeypatch.setattr(settings, "CONTEXT_LLM_URL", "")
    with pytest.raises(RuntimeError):
        contextual.HttpContextWriter()


def test_contextual_chunks_and_ocr_are_off_by_default():
    from app.core.config import Settings

    assert Settings.model_fields["CONTEXTUAL_CHUNKS_ENABLED"].default is False
    assert Settings.model_fields["OCR_ENABLED"].default is False
