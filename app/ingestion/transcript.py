"""Timestamp-preserving ingestion of YouTube transcripts.

File Service stores the canonical transcript as JSON (`schema: granthan.transcript.v1`, original segments with start/duration).
This module detects that object, parses it, and chunks it at sentence boundaries while keeping each chunk's time range.
Because the Milvus/OpenSearch schemas have no timestamp fields, the range travels in the existing `section` field as
``t=<start>-<end>`` (whole seconds; start rounded down, end rounded up) and is returned to callers through the gRPC
`metadata.section` they already receive. Nothing else in the pipeline changes: same embedder, same indexes, same chunk ids
(`derive_chunk_id(document, version, chunk_index)`), so re-delivery is idempotent and a re-index rebuilds from the canonical JSON.
"""

import json
import math
import re
from dataclasses import dataclass

from app.core.config import settings
from app.ingestion.chunker import Chunk
from app.ingestion.parser import ParserError

SCHEMA = "granthan.transcript.v1"
_WORDS_PER_TOKEN = 0.75                      # same approximation as chunker.py
_SENTENCE_END = re.compile(r"[.!?…。！？][\"')\]]*$")
_SECTION_RE = re.compile(r"^t=(\d+)-(\d+)$")


@dataclass(frozen=True)
class TranscriptSegment:
    start: float
    duration: float
    text: str


@dataclass(frozen=True)
class TranscriptDocument:
    video_id: str
    title: str | None
    language: str | None
    is_generated: bool | None
    segments: list[TranscriptSegment]


def parse_transcript(raw: bytes) -> TranscriptDocument | None:
    """None when `raw` is not a transcript (the normal document path then applies); ParserError when it claims to be one
    but is malformed (never silently indexed as something else)."""
    head = raw[:1].lstrip()
    if not raw.lstrip()[:1] == b"{":
        return None
    try:
        doc = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(doc, dict) or doc.get("schema") != SCHEMA:
        return None
    segs_raw = doc.get("segments")
    if not isinstance(segs_raw, list):
        raise ParserError("transcript has no segment list")
    segments: list[TranscriptSegment] = []
    for item in segs_raw:
        try:
            start, duration, text = float(item["start"]), float(item["duration"]), item["text"]
        except (KeyError, TypeError, ValueError):
            raise ParserError("transcript segment is malformed") from None
        if not isinstance(text, str) or not (math.isfinite(start) and math.isfinite(duration)) or start < 0 or duration < 0:
            raise ParserError("transcript segment is malformed")
        if text.strip():
            segments.append(TranscriptSegment(start, duration, text.strip()))
    return TranscriptDocument(
        video_id=str(doc.get("video_id") or ""), title=doc.get("title") if isinstance(doc.get("title"), str) else None,
        language=doc.get("language") if isinstance(doc.get("language"), str) else None,
        is_generated=doc.get("is_generated") if isinstance(doc.get("is_generated"), bool) else None, segments=segments,
    )


def format_section(start: float, end: float) -> str:
    return f"t={int(math.floor(start))}-{int(math.ceil(max(end, start)))}"


def parse_section(section: str | None) -> tuple[int, int] | None:
    """Inverse of `format_section` (used by consumers and tests). None when the section is not a time range."""
    m = _SECTION_RE.match(section or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _words(text: str) -> int:
    return len(text.split())


def chunk_transcript(doc: TranscriptDocument) -> list[Chunk]:
    """Windows of whole segments, sized by the same CHUNK_* token budgets as documents.
    A window closes at a sentence end once it reaches 60% of the target size, and always before exceeding the maximum; the next
    window re-includes trailing whole segments up to the configured overlap so a fact spanning a boundary stays findable.
    A single segment larger than the maximum is split by words with its time range interpolated."""
    target = max(1, int(settings.CHUNK_TARGET_TOKENS / _WORDS_PER_TOKEN))
    maximum = max(target, int(settings.CHUNK_MAX_TOKENS / _WORDS_PER_TOKEN))
    overlap = max(0, int(settings.CHUNK_OVERLAP_TOKENS / _WORDS_PER_TOKEN))

    # Normalise: oversized segments become several pieces with interpolated times.
    pieces: list[TranscriptSegment] = []
    for s in doc.segments:
        words = s.text.split()
        if len(words) <= maximum:
            pieces.append(s)
            continue
        step = max(1, target)
        n = math.ceil(len(words) / step)
        for k in range(n):
            part = words[k * step:(k + 1) * step]
            pieces.append(TranscriptSegment(s.start + s.duration * k / n, s.duration / n, " ".join(part)))

    chunks: list[Chunk] = []
    i = 0
    while i < len(pieces):
        j, count = i, 0
        while j < len(pieces):
            w = _words(pieces[j].text)
            if count and count + w > maximum:
                break
            count += w
            j += 1
            if count >= target * 0.6 and _SENTENCE_END.search(pieces[j - 1].text):
                break
            if count >= target:
                break
        window = pieces[i:j]
        start = window[0].start
        end = max(p.start + p.duration for p in window)
        chunks.append(Chunk(chunk_index=len(chunks), content=" ".join(p.text for p in window), page_number=None,
                            section=format_section(start, end)))
        if j >= len(pieces):
            break
        # next window: step back over whole trailing segments up to `overlap` words, but always make progress
        back, k = 0, j
        while k - 1 > i and back + _words(pieces[k - 1].text) <= overlap:
            k -= 1
            back += _words(pieces[k].text)
        i = k if k > i else j
    return chunks
