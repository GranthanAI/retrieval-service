from dataclasses import dataclass

from app.core.config import settings
from app.ingestion.parser import ParsedDocument, ParsedSection

# Rough words-per-token approximation used only to size chunks against the
# configured token budgets without a real tokenizer dependency in the
# ingestion path — HLD §10 specifies target/max/overlap in tokens but does
# not mandate a specific tokenizer; this is deliberately approximate.
_WORDS_PER_TOKEN = 0.75


@dataclass
class Chunk:
    """One structure-aware chunk, pre-embedding (HLD §10). `chunk_index` is
    assigned in emission order — the same input always produces the same
    sequence, which is what makes `chunk_identity.derive_chunk_id` (HLD §11)
    reproducible on a re-parse.
    """

    chunk_index: int
    content: str
    page_number: int | None
    section: str | None


def _tokens_to_words(tokens: int) -> int:
    return max(1, int(tokens / _WORDS_PER_TOKEN))


def _split_into_word_windows(words: list[str], target_words: int, max_words: int, overlap_words: int) -> list[str]:
    """Recursive/sliding split over a single section's words — never
    exceeds `max_words` per window, aims for `target_words`, and overlaps
    consecutive windows by `overlap_words` (HLD §10).
    """
    if len(words) <= max_words:
        return [" ".join(words)] if words else []

    windows: list[str] = []
    start = 0
    while start < len(words):
        end = min(start + target_words, len(words))
        windows.append(" ".join(words[start:end]))
        if end >= len(words):
            break
        start = end - overlap_words
    return windows


def chunk(parsed: ParsedDocument) -> list[Chunk]:
    """Structure-aware recursive chunking (HLD §10) — chunks never cross a
    section boundary; long sections are further split into overlapping
    windows sized against `CHUNK_TARGET_TOKENS`/`CHUNK_MAX_TOKENS`/
    `CHUNK_OVERLAP_TOKENS`. Page number and section heading are preserved
    per chunk (HLD §9's provenance requirement, threaded through to the
    gRPC response's `metadata` map, LLD §18.2).
    """
    target_words = _tokens_to_words(settings.CHUNK_TARGET_TOKENS)
    max_words = _tokens_to_words(settings.CHUNK_MAX_TOKENS)
    overlap_words = _tokens_to_words(settings.CHUNK_OVERLAP_TOKENS)

    chunks: list[Chunk] = []
    for section in parsed.sections:
        section: ParsedSection
        words = section.text.split()
        for window_text in _split_into_word_windows(words, target_words, max_words, overlap_words):
            if not window_text.strip():
                continue
            chunks.append(
                Chunk(
                    chunk_index=len(chunks),
                    content=window_text,
                    page_number=section.page_number,
                    section=section.heading,
                )
            )

    return chunks
