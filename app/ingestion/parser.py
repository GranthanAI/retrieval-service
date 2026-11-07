from app.core.config import settings
from app.ingestion import archive, docling_config, formats, ocr, text_parsers

# Docling (PDF, Word, PowerPoint, Excel, HTML) is configured in app/ingestion/docling_config.py and imported lazily, only when such a file is
# parsed: a worker that handles text, Markdown and CSV (DOCLING_ENABLED=false) never loads it.


# The data classes live in parsed.py (the text/archive parsers need them without importing this module); re-exported so existing
# imports of ParsedDocument / ParsedSection / ParserError from here keep working.
from app.ingestion.parsed import ParsedBlock, ParsedDocument, ParsedSection, ParserError  # noqa: E402,F401


def _parse_docling(raw_bytes: bytes, filename: str) -> ParsedDocument:
    """Docling wrapper (HLD §9) — extracts page number, heading hierarchy,
    paragraph boundaries, table structure, and section boundaries.

    Walks the parsed document in reading order (`iterate_items`), starting
    a new `ParsedSection` at every heading/title and accumulating body
    text (paragraphs, list items, tables rendered to markdown) underneath
    it until the next heading. A document with no heading before its first
    body content gets one implicit leading section (`heading=None`).
    """
    docling_config.ensure_enabled()
    from docling_core.types.doc import DocItemLabel, TableItem, TextItem

    heading_labels = {DocItemLabel.SECTION_HEADER, DocItemLabel.TITLE}
    body_labels = {DocItemLabel.TEXT, DocItemLabel.PARAGRAPH, DocItemLabel.LIST_ITEM, DocItemLabel.CAPTION, DocItemLabel.FOOTNOTE, DocItemLabel.CODE}
    try:
        document = docling_config.convert(raw_bytes, filename)
    except Exception as exc:
        raise ParserError(f"failed to parse {filename!r}: {exc}") from exc

    ext = filename.lower().rsplit(".", 1)[-1]
    paged = ext == "pdf"          # only PDFs have real pages; a .docx page_no would be a layout artefact, so it is never used
    slides = ext == "pptx"        # for slides the page_no is the slide number: a structural location, not a page

    sections: list[ParsedSection] = []
    stack: list[tuple[int, str]] = []          # (level, title): the heading hierarchy above the current position
    current_heading: str | None = None
    blocks: list[ParsedBlock] = []

    def _page(item) -> int | None:
        prov = getattr(item, "prov", None)
        return prov[0].page_no if prov else None

    def _flush() -> None:
        parts = [b.text for b in blocks if b.text.strip()]
        if not parts:
            return
        pages = [b.page for b in blocks if b.page is not None]
        loc = None
        if slides and pages:
            loc = f"Slide {min(pages)}" if min(pages) == max(pages) else f"Slides {min(pages)}-{max(pages)}"
        sections.append(ParsedSection(
            heading=current_heading, page_number=pages[0] if (paged and pages) else None, text="\n\n".join(parts),
            heading_path=[t for _, t in stack], blocks=list(blocks), location=loc))

    for item, _level in document.iterate_items():
        page_no = _page(item)
        page = page_no if paged else None
        loc = f"Slide {page_no}" if (slides and page_no is not None) else None

        if item.label in heading_labels:
            _flush()
            title = item.text if isinstance(item, TextItem) else None
            current_heading = title
            if title:
                level = 1 if item.label == DocItemLabel.TITLE else max(1, int(getattr(item, "level", 1) or 1))
                if item.label != DocItemLabel.TITLE and any(l == 0 for l, _ in stack):
                    level += 1          # content headings sit below a document title
                while stack and stack[-1][0] >= level:
                    stack.pop()
                stack.append((0 if item.label == DocItemLabel.TITLE else level, title))
            blocks = []
        elif isinstance(item, TableItem):
            md = item.export_to_markdown(document)
            lines = [ln for ln in md.split("\n") if ln.strip()]
            caption = None
            try:
                caption = item.caption_text(document) or None
            except Exception:  # noqa: BLE001 - captions are optional metadata
                pass
            header = "\n".join(lines[:2]) if len(lines) >= 2 else md
            blocks.append(ParsedBlock(md, page=page, kind="table", caption=caption, header=header, rows=lines[2:], location=loc))
        elif item.label in body_labels and isinstance(item, TextItem):
            blocks.append(ParsedBlock(item.text, page=page, kind="code" if item.label == DocItemLabel.CODE else "text", location=loc))

    _flush()

    parsed = ParsedDocument(sections=sections)
    if paged and settings.OCR_ENABLED:
        # Selective OCR (off by default): only pages with no usable text layer; never raises (see ocr.py).
        ocr.run(raw_bytes, filename, parsed, sorted(getattr(document, "pages", {}) or {}))
    return parsed


def parse_one(raw_bytes: bytes, filename: str) -> ParsedDocument:
    """One non-archive file, parsed according to its kind (see formats.py)."""
    kind = formats.classify(filename)
    if kind in ("pdf", "docling"):
        return _parse_docling(raw_bytes, filename)
    if kind == "markdown":
        return text_parsers.parse_markdown(raw_bytes, filename)
    if kind == "csv":
        return text_parsers.parse_csv(raw_bytes, filename, delimiter="	" if formats.extension(filename) == "tsv" else ",")
    if kind == "text":
        return text_parsers.parse_text(raw_bytes, filename)
    if kind == "legacy_doc":
        return text_parsers.parse_legacy_doc(raw_bytes, filename)
    raise ParserError(f"{filename!r}: this file type is not supported")


def parse(raw_bytes: bytes, filename: str) -> ParsedDocument:
    """Entry point used by the ingestion orchestrator. A .zip is opened and each readable file inside it is parsed with
    the rules for its own type (see archive.py for the safety limits)."""
    if formats.classify(filename) == "archive":
        return archive.parse_archive(raw_bytes, filename, parse_one)
    return parse_one(raw_bytes, filename)
