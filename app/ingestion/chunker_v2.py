"""Chunker v2 (chunker_version "2"): sizes chunks in the EMBEDDING MODEL's tokens, prefixes every chunk with its heading path, tracks a
page per block, and never splits a table mid-row.

Why it exists (see docs/retrieval-quality.md, P2-1..P2-4): v1 turned a 450-token target into 600 words (it divided by 0.75 instead of
multiplying), while all-MiniLM-L6-v2 reads only 256 wordpieces, so most of each chunk was invisible to vector search; it kept only the
latest heading, so the "Exit criteria" chunk of Phase 6 never said "Phase 6"; and it stamped a section with the page it started on.

Layout of every chunk's `content` (the one string that is embedded AND indexed in both stores):

    <filename> › <h1> › <h2> › <h3>
    <body>

Limits (tokens of the embedding model): target `CHUNK_TARGET_TOKENS_V2` (200) for the body, overlap `CHUNK_OVERLAP_TOKENS_V2` (30), hard
maximum `max_seq_length - CHUNK_MAX_MARGIN_V2` (16) for prefix + body together. Splitting order: paragraph, then line, then sentence,
then raw token windows as a last resort. After chunking, every chunk is re-counted and the run fails if any exceeds the maximum."""
import re
from dataclasses import dataclass

from app.core.config import settings
from app.ingestion.chunker import Chunk
from app.ingestion.parsed import ParsedBlock, ParsedDocument, ParsedSection
from app.ingestion.tokenization import TokenCounter, sentences

PATH_SEP = " › "
_MIN_BODY_ROOM = 64          # the prefix is shortened rather than leave less room than this for the body
_PARAGRAPH = re.compile(r"\n\s*\n")


class ChunkTooLargeError(ValueError):
    """A produced chunk is longer than the embedding model reads. Indicates a bug in splitting; ingestion must fail visibly."""


@dataclass
class _Piece:
    text: str
    tokens: int
    sep: str                 # what precedes this piece when joined to the previous one
    page: int | None
    location: str | None


def heading_path_of(section: ParsedSection) -> list[str]:
    return list(section.heading_path) or ([section.heading] if section.heading else [])


def build_prefix(filename: str, path: list[str], counter: TokenCounter, max_len: int) -> str:
    """`filename › h1 › h2 › h3`, shortened (filename first, then the outermost headings, finally the deepest one's tail) so that the
    body keeps at least _MIN_BODY_ROOM tokens."""
    parts = [filename] + path
    while True:
        prefix = PATH_SEP.join(p for p in parts if p)
        if counter.count(prefix + "\n") + _MIN_BODY_ROOM <= max_len:
            return prefix
        if len(parts) > 1:
            parts = parts[1:]
        else:
            return counter.split(parts[0], max(1, max_len - _MIN_BODY_ROOM - 2))[0]


def _fit(text: str, limit: int, counter: TokenCounter, sep: str, depth: int = 0) -> list[tuple[str, str]]:
    """Splits `text` into (piece, sep) units that each fit in `limit` tokens: paragraphs, then lines, then sentences, then token windows."""
    text = text.strip()
    if not text:
        return []
    if counter.count(text) <= limit:
        return [(text, sep)]
    if depth == 0:
        paras = [p for p in _PARAGRAPH.split(text) if p.strip()]
        if len(paras) > 1:
            return [u for i, p in enumerate(paras) for u in _fit(p, limit, counter, "\n\n" if i or sep == "\n\n" else sep, 1)]
    if depth <= 1:
        lines = [l for l in text.split("\n") if l.strip()]
        if len(lines) > 1:
            return [u for i, l in enumerate(lines) for u in _fit(l, limit, counter, "\n", 2)]
    if depth <= 2:
        sents = sentences(text)
        if len(sents) > 1:
            return [u for i, s in enumerate(sents) for u in _fit(s, limit, counter, sep if i == 0 else " ", 3)]
    return [(piece, sep if i == 0 else " ") for i, piece in enumerate(counter.split(text, limit))]


class _SectionChunker:
    def __init__(self, section: ParsedSection, filename: str, counter: TokenCounter, max_len: int, out: list[Chunk]) -> None:
        self.section, self.counter, self.max_len, self.out = section, counter, max_len, out
        self.path = heading_path_of(section)
        self.prefix = build_prefix(filename, self.path, counter, max_len)
        self.room = max_len - counter.count(self.prefix + "\n")
        self.target = min(settings.CHUNK_TARGET_TOKENS_V2, self.room)
        self.overlap = settings.CHUNK_OVERLAP_TOKENS_V2
        self.cur: list[_Piece] = []

    # ---- emission ------------------------------------------------------------------------------------------------------
    def _emit(self, body: str, *, page_start, page_end, block_type: str, location: str | None) -> None:
        content = f"{self.prefix}\n{body}"
        if self.counter.count(content) > self.max_len:      # the sum of piece counts drifted from the joined count: split again
            parts = self.counter.split(body, max(1, self.room - 2))
            if len(parts) > 1:
                for part in parts:
                    self._emit(part, page_start=page_start, page_end=page_end, block_type=block_type, location=location)
                return
            # cannot be made smaller: emit it and let the final check in chunk_v2 fail loudly (never loop, never truncate silently)
        self.out.append(Chunk(
            chunk_index=len(self.out), content=content, page_number=page_start, section=self.section.heading,
            heading_path=list(self.path), page_start=page_start, page_end=page_end, block_type=block_type, location=location))

    def _flush(self, keep_overlap: bool) -> None:
        if not self.cur:
            return
        body = "".join((p.sep if i else "") + p.text for i, p in enumerate(self.cur))
        pages = [p.page for p in self.cur if p.page is not None]
        locs = [p.location for p in self.cur if p.location]
        self._emit(body, page_start=min(pages) if pages else None, page_end=max(pages) if pages else None, block_type="text",
                   location=locs[0] if locs else self.section.location)
        tail: list[_Piece] = []
        if keep_overlap and self.overlap > 0:
            total = 0
            for p in reversed(self.cur):
                if total + p.tokens > self.overlap:
                    break
                tail.insert(0, p)
                total += p.tokens
            if len(tail) == len(self.cur):      # never carry the whole chunk: that would not make progress
                tail = tail[1:]
        self.cur = tail

    def _cur_tokens(self) -> int:
        return sum(p.tokens for p in self.cur)

    # ---- blocks --------------------------------------------------------------------------------------------------------
    def add_text(self, block: ParsedBlock) -> None:
        sep0 = "\n" if block.kind == "code" else "\n\n"
        for text, sep in _fit(block.text, self.room, self.counter, sep0):
            tokens = self.counter.count(text)
            if self.cur and block.page is not None and self.cur[-1].page is not None and block.page != self.cur[-1].page \
                    and self._cur_tokens() > self.target / 2:
                self._flush(keep_overlap=False)           # a new page starts a new chunk once the current one is over half full
            if self.cur and self._cur_tokens() + tokens > self.target:
                self._flush(keep_overlap=True)
            self.cur.append(_Piece(text, tokens, sep, block.page, block.location))

    def add_table(self, block: ParsedBlock) -> None:
        self._flush(keep_overlap=False)
        head = (f"{block.caption}\n" if block.caption else "") + (block.header or "")
        rows = block.rows
        loc_for = (lambda a, b: f"Rows {block.row_offset + a}-{block.row_offset + b - 1}") if block.header and block.row_offset else (lambda a, b: None)
        whole = head + ("\n" + "\n".join(rows) if rows else "")
        if self.counter.count(self.prefix + "\n" + whole) <= self.max_len:
            self._emit(whole, page_start=block.page, page_end=block.page, block_type="table",
                       location=loc_for(0, len(rows)) if rows else (block.location or self.section.location))
            return
        # The caption and header row are repeated on every row group so each chunk is understandable alone. They may take at most a third of
        # a chunk: a table whose header alone fills the chunk (seen in a real 84-page PDF) would otherwise leave no room for rows, and the row
        # splitting below degenerated into one-token fragments, each re-emitted with the full header (182 chunks became 3,495, 3,080 of them
        # exact duplicates). Only the START of an over-long header is repeated.
        head_cap = max(1, self.room // 3)
        if self.counter.count(head) > head_cap:
            head = self.counter.split(head, head_cap)[0]
        head_tokens = self.counter.count(head) + 1
        budget = self.room - head_tokens
        seen: set[str] = set()                                    # a table never emits the same chunk text twice

        def emit(body: str, a: int, b: int) -> None:
            if body not in seen:
                seen.add(body)
                self._emit(body, page_start=block.page, page_end=block.page, block_type="table", location=loc_for(a, b))

        i = 0
        while i < len(rows):
            j, used = i, 0
            while j < len(rows):
                t = self.counter.count(rows[j]) + 1
                if used + t > budget and j > i:
                    break
                used += t
                j += 1
            group = rows[i:j]
            if len(group) == 1 and used > budget:           # one row longer than the room: split the row itself, header repeated
                for part in self.counter.split(group[0], max(1, budget)):
                    emit(head + "\n" + part, i, j)
            else:
                emit(head + "\n" + "\n".join(group), i, j)
            i = j

    def run(self) -> None:
        blocks = self.section.blocks or [ParsedBlock(self.section.text, self.section.page_number)]
        for block in blocks:
            if block.kind == "table":
                self.add_table(block)
            elif block.text.strip():
                self.add_text(block)
        self._flush(keep_overlap=False)


def chunk_v2(parsed: ParsedDocument, filename: str, counter: TokenCounter, max_seq_length: int) -> list[Chunk]:
    max_len = max_seq_length - settings.CHUNK_MAX_MARGIN_V2
    out: list[Chunk] = []
    for section in parsed.sections:
        _SectionChunker(section, filename, counter, max_len, out).run()
    for c in out:
        n = counter.count(c.content)
        if n > max_len:
            raise ChunkTooLargeError(f"chunk {c.chunk_index} of {filename!r} has {n} tokens, maximum is {max_len}")
    return out
