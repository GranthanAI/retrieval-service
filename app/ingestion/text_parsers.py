"""Parsers for text-like files: plain text and source code, Markdown, CSV/TSV, and legacy .doc.

They produce `ParsedSection`s that the existing chunker splits further by token budget. Headings double as the citation label
("Lines 120-240", "Rows 41-80", or a Markdown heading), so an answer can point at where in the file it came from."""
import csv
import io
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from app.core.config import settings
from app.ingestion.parsed import ParsedBlock, ParsedDocument, ParsedSection, ParserError

_LINES_PER_SECTION = 120
_CHARS_PER_SECTION = 8_000
_ROWS_PER_SECTION = 40
_FENCE = re.compile(r"^\s*(```|~~~)")
_ATX_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")


def decode_text(raw: bytes, name: str = "file") -> str:
    """Bytes to text without guessing blindly: BOMs first, then UTF-8, then UTF-16 when it looks like it, else Windows-1252.
    Binary content (many NUL bytes) is refused rather than indexed as noise."""
    if raw.startswith(b"\xef\xbb\xbf"):
        text = raw[3:].decode("utf-8", errors="replace")
    elif raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        text = raw.decode("utf-16", errors="replace")
    else:
        if raw[:4096].count(b"\x00") > 8:
            raise ParserError(f"{name!r} looks like a binary file, not text")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("cp1252", errors="replace")
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    if len(text) > settings.PARSE_MAX_TEXT_CHARS:
        text = text[: settings.PARSE_MAX_TEXT_CHARS]          # bounded work: the head of a huge file is indexed
    return text


def parse_text(raw: bytes, name: str) -> ParsedDocument:
    text = decode_text(raw, name)
    if not text.strip():
        return ParsedDocument()
    lines = text.split("\n")
    blocks: list[tuple[int, int, str]] = []                  # (first line index, last line index + 1, body)
    start, buf, size = 0, [], 0
    for index, line in enumerate(lines):
        buf.append(line)
        size += len(line) + 1
        if len(buf) >= _LINES_PER_SECTION or size >= _CHARS_PER_SECTION:
            blocks.append((start, index + 1, "\n".join(buf)))
            start, buf, size = index + 1, [], 0
    if buf:
        blocks.append((start, len(lines), "\n".join(buf)))
    blocks = [(a, b, body.strip("\n")) for a, b, body in blocks if body.strip()]
    if len(blocks) == 1:                                     # fits in one block: no "Lines 1-N" label needed
        a, b, body = blocks[0]
        return ParsedDocument([ParsedSection(heading=None, page_number=None, text=body, location=f"Lines {a + 1}-{b}",
                                             blocks=[ParsedBlock(body, kind="code" if _looks_like_code(name) else "text")])])
    return ParsedDocument([
        ParsedSection(heading=f"Lines {a + 1}-{b}", page_number=None, text=body, location=f"Lines {a + 1}-{b}",
                      blocks=[ParsedBlock(body, kind="code" if _looks_like_code(name) else "text")])
        for a, b, body in blocks
    ])


_TABLE_SEPARATOR = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$")


def _looks_like_code(name: str) -> bool:
    return name.lower().rsplit(".", 1)[-1] not in ("txt", "md", "markdown", "rst", "text", "log", "csv", "tsv")


def _md_blocks(lines: list[tuple[int, str]]) -> list[ParsedBlock]:
    """Splits a section body into paragraphs, fenced code and pipe tables. A table is a run of lines starting with `|` whose second
    line is a separator row; it becomes one `table` block (header + separator + body rows) so the chunker can keep it whole."""
    blocks: list[ParsedBlock] = []
    i, n = 0, len(lines)
    para: list[str] = []

    def flush_para() -> None:
        text = "\n".join(para).strip()
        if text:
            blocks.append(ParsedBlock(text))
        para.clear()

    while i < n:
        line = lines[i][1]
        if _FENCE.match(line):
            flush_para()
            j = i + 1
            while j < n and not _FENCE.match(lines[j][1]):
                j += 1
            code = "\n".join(l for _, l in lines[i:min(j + 1, n)]).strip()
            if code:
                blocks.append(ParsedBlock(code, kind="code"))
            i = j + 1
            continue
        if line.lstrip().startswith("|") and i + 1 < n and _TABLE_SEPARATOR.match(lines[i + 1][1]):
            flush_para()
            j = i + 2
            while j < n and lines[j][1].lstrip().startswith("|"):
                j += 1
            header = lines[i][1].strip() + "\n" + lines[i + 1][1].strip()
            rows = [l.strip() for _, l in lines[i + 2:j]]
            blocks.append(ParsedBlock("\n".join([header] + rows), kind="table", header=header, rows=rows))
            i = j
            continue
        if not line.strip():
            flush_para()
        else:
            para.append(line)
        i += 1
    flush_para()
    return blocks


def parse_markdown(raw: bytes, name: str) -> ParsedDocument:
    """Sections split at every ATX heading. Each carries `heading` (deepest), `heading_path` (the whole hierarchy), `blocks`, and
    `location` (the line range of its body: Markdown has no pages, so a structural location is used instead)."""
    text = decode_text(raw, name)
    sections: list[ParsedSection] = []
    stack: list[tuple[int, str]] = []
    heading: str | None = None
    buf: list[tuple[int, str]] = []
    in_fence = False

    def flush() -> None:
        body = "\n".join(l for _, l in buf).strip()
        if body:
            filled = [no for no, l in buf if l.strip()]
            sections.append(ParsedSection(
                heading=heading, page_number=None, text=body, heading_path=[t for _, t in stack], blocks=_md_blocks(buf),
                location=f"Lines {filled[0]}-{filled[-1]}"))

    for no, line in enumerate(text.split("\n"), start=1):
        if _FENCE.match(line):
            in_fence = not in_fence
        match = None if in_fence else _ATX_HEADING.match(line)
        if match:
            flush()
            level, title = len(match.group(1)), match.group(2).strip()
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, title))
            heading, buf = title, []
        else:
            buf.append((no, line))
    flush()
    return ParsedDocument(sections)


def parse_csv(raw: bytes, name: str, delimiter: str = ",") -> ParsedDocument:
    text = decode_text(raw, name)
    if not text.strip():
        return ParsedDocument()
    csv.field_size_limit(10_000_000)
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|") if delimiter == "," else None
    except csv.Error:
        dialect = None
    reader = csv.reader(io.StringIO(text), dialect) if dialect else csv.reader(io.StringIO(text), delimiter=delimiter)
    try:
        rows = [r for r in reader if any(c.strip() for c in r)]
    except csv.Error as exc:
        raise ParserError(f"{name!r} is not valid CSV: {exc}") from exc
    if not rows:
        return ParsedDocument()
    header, body = rows[0], rows[1:]
    width = len(header)

    def render(chunk_rows: list[list[str]]) -> str:
        lines = ["| " + " | ".join(c.strip() for c in header) + " |", "|" + " --- |" * width]
        for r in chunk_rows:
            cells = (r + [""] * width)[: max(width, len(r))]
            lines.append("| " + " | ".join(c.strip().replace("\n", " ").replace("|", "\\|") for c in cells) + " |")
        return "\n".join(lines)

    if not body:
        only = render([])
        return ParsedDocument([ParsedSection(heading=None, page_number=None, text=only, blocks=[ParsedBlock(only, kind="table", header=only)])])
    sections: list[ParsedSection] = []
    total = 0
    for first in range(0, len(body), _ROWS_PER_SECTION):
        chunk_rows = body[first:first + _ROWS_PER_SECTION]
        rendered = render(chunk_rows)
        total += len(rendered)
        if total > settings.PARSE_MAX_TEXT_CHARS:
            sections.append(ParsedSection(None, None, f"[Only the first {first} of {len(body)} rows were indexed.]"))
            break
        header_lines = rendered.split("\n")[:2]
        loc = f"Rows {first + 1}-{first + len(chunk_rows)}"
        sections.append(ParsedSection(
            heading=loc, page_number=None, text=rendered, location=loc,
            blocks=[ParsedBlock(rendered, kind="table", header="\n".join(header_lines), rows=rendered.split("\n")[2:], row_offset=first + 1)]))
    return ParsedDocument(sections)


def parse_legacy_doc(raw: bytes, name: str) -> ParsedDocument:
    """Binary Word (.doc) through an external converter (antiword by default). It is not bundled with Python, so when it is
    missing the failure says what to do instead of producing an empty or garbled index."""
    command = shutil.which(settings.LEGACY_DOC_COMMAND)
    if command is None:
        raise ParserError(
            f"{name!r} is an old binary Word file (.doc) and this server has no converter for it. Save it as .docx or PDF and upload again."
        )
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "input.doc"
        path.write_bytes(raw)
        try:
            result = subprocess.run(
                [command, str(path)], capture_output=True, timeout=settings.LEGACY_DOC_TIMEOUT_SECONDS, check=False)
        except subprocess.TimeoutExpired as exc:
            raise ParserError(f"converting {name!r} took too long") from exc
    if result.returncode != 0 or not result.stdout.strip():
        raise ParserError(f"could not read {name!r} as a Word document; save it as .docx or PDF and upload again")
    return parse_text(result.stdout[: settings.PARSE_MAX_TEXT_CHARS], name)
