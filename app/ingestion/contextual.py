"""Contextual chunk text: at ingestion, a cheap model writes one or two sentences that place each chunk in its document ("This passage is
the exit criteria of Phase 6, the payment webhook phase of the billing roadmap"). The sentences are stored WITH the chunk, embedded and
indexed with it (chunk.content = heading-path prefix, then the context, then the body), so a chunk that never repeats its subject can
still be found by it. Index generation 3 is built this way (chunker_version "2+ctx").

Off by default: `CONTEXTUAL_CHUNKS_ENABLED` is a master switch checked before any call, so a contextual generation alone never sends text to a model. The writer is a small protocol so tests use fakes; the real one calls an OpenAI-compatible
chat endpoint configured by `CONTEXT_LLM_*`. Nothing here ever fails ingestion: a chunk whose description cannot be written is indexed
without one. The description is built only from the text it is given and is length-capped (`CONTEXT_MAX_TOKENS`); the chunker reserves
that much room, so chunks still fit the embedding model. Document text is sent to the configured model only when the flag is on; it is
never logged."""
import asyncio
import re
from typing import Protocol

import httpx

from app.core import metrics
from app.core.config import settings
from app.core.logging import get_logger
from app.ingestion.chunker import Chunk
from app.ingestion.parsed import ParsedDocument
from app.ingestion.tokenization import TokenCounter
from app.stores.generations import GenerationInfo

logger = get_logger(__name__)

CONTEXTUAL_CHUNKER_VERSION = "2+ctx"

_SYSTEM = (
    "You write one or two short sentences that place a passage inside its document so a search engine can find it. "
    "Use only the information given. State what the passage is and where it sits (section, topic). Do not answer questions, "
    "do not add facts, do not quote the passage. Reply with the sentences only."
)


class ContextWriter(Protocol):
    async def describe(self, *, filename: str, heading_path: list[str], outline: list[str], body: str) -> str: ...


def is_contextual(info: GenerationInfo) -> bool:
    return info.chunker_version == CONTEXTUAL_CHUNKER_VERSION


def reserve_tokens(info: GenerationInfo) -> int:
    """Tokens the chunker must leave free in every chunk for the description."""
    return settings.CONTEXT_MAX_TOKENS if is_contextual(info) else 0


def outline_of(parsed: ParsedDocument, limit: int = 40) -> list[str]:
    """The document's headings in order, de-duplicated, as short context for the model."""
    seen: list[str] = []
    for section in parsed.sections:
        for h in section.heading_path or ([section.heading] if section.heading else []):
            if h and h not in seen:
                seen.append(h)
    return seen[:limit]


class HttpContextWriter:
    """OpenAI-compatible chat completions client (`CONTEXT_LLM_URL`, `CONTEXT_LLM_MODEL`, optional bearer `CONTEXT_LLM_API_KEY`)."""

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        if not settings.CONTEXT_LLM_URL or not settings.CONTEXT_LLM_MODEL:
            raise RuntimeError("contextual chunks need CONTEXT_LLM_URL and CONTEXT_LLM_MODEL")
        self._client = client or httpx.AsyncClient(timeout=settings.CONTEXT_LLM_TIMEOUT_SECONDS)

    async def describe(self, *, filename: str, heading_path: list[str], outline: list[str], body: str) -> str:
        headers = {"Authorization": f"Bearer {settings.CONTEXT_LLM_API_KEY}"} if settings.CONTEXT_LLM_API_KEY else {}
        user = (f"Document: {filename}\nOutline: {' | '.join(outline)}\nThis passage is under: {' > '.join(heading_path) or '(top level)'}\n\n"
                f"Passage:\n{body}")
        response = await self._client.post(
            settings.CONTEXT_LLM_URL, headers=headers,
            json={"model": settings.CONTEXT_LLM_MODEL, "temperature": 0, "max_tokens": settings.CONTEXT_MAX_TOKENS * 2,
                  "messages": [{"role": "system", "content": _SYSTEM}, {"role": "user", "content": user}]})
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    async def aclose(self) -> None:
        await self._client.aclose()


def clean(text: str, counter: TokenCounter, max_tokens: int) -> str:
    """One paragraph, no quotes/markdown noise, capped at `max_tokens` (cut at a word boundary)."""
    one = re.sub(r"\s+", " ", text).strip().strip("\"'`*# ")
    if counter.count(one) <= max_tokens:
        return one
    return counter.split(one, max_tokens)[0].rstrip(" ,;:-") if one else ""


def apply_context(chunk: Chunk, context: str) -> None:
    """chunk.content: `prefix \\n body` becomes `prefix \\n context \\n body` (the prefix is the first line, see chunker_v2)."""
    prefix, _, body = chunk.content.partition("\n")
    chunk.content = f"{prefix}\n{context}\n{body}"


async def add_context(chunks: list[Chunk], filename: str, parsed: ParsedDocument, writer: ContextWriter, counter: TokenCounter,
                      max_seq_length: int) -> int:
    """Writes the description of every chunk (bounded concurrency). Returns how many got one. A failure for one chunk leaves it as it
    was; the final length check keeps every chunk within the embedding model's limit."""
    outline = outline_of(parsed)
    sem = asyncio.Semaphore(settings.CONTEXT_LLM_CONCURRENCY)
    limit = max_seq_length - settings.CHUNK_MAX_MARGIN_V2
    written = 0

    async def one(chunk: Chunk) -> None:
        nonlocal written
        body = chunk.content.partition("\n")[2]
        try:
            async with sem:
                raw = await writer.describe(filename=filename, heading_path=chunk.heading_path, outline=outline, body=body)
            context = clean(raw, counter, settings.CONTEXT_MAX_TOKENS)
            if not context:
                metrics.contextual_chunks_total.labels(outcome="skipped").inc()
                return
            before = chunk.content
            apply_context(chunk, context)
            if counter.count(chunk.content) > limit:            # would not fit the model: keep the chunk as it was
                chunk.content = before
                metrics.contextual_chunks_total.labels(outcome="skipped").inc()
                return
            written += 1
            metrics.contextual_chunks_total.labels(outcome="written").inc()
        except Exception as exc:  # noqa: BLE001 - never fail ingestion over a description
            metrics.contextual_chunks_total.labels(outcome="failed").inc()
            logger.warning("contextual.chunk_failed", error=type(exc).__name__)

    await asyncio.gather(*(one(c) for c in chunks))
    return written
