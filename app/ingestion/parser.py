from io import BytesIO

from app.ingestion import archive, formats, text_parsers
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.types.doc import DocItemLabel, TableItem, TextItem
from docling_core.types.io import DocumentStream

# OCR is for scanned/image-only pages — File Service's uploads are
# digitally-authored documents with a real text layer, so OCR only adds
# cost (model downloads, CPU time) with nothing to recover. Table structure
# detection stays on (HLD §9 requires table structure extraction).
_PDF_PIPELINE_OPTIONS = PdfPipelineOptions(do_ocr=False, do_table_structure=True)


# The data classes live in parsed.py (the text/archive parsers need them without importing this module); re-exported so existing
# imports of ParsedDocument / ParsedSection / ParserError from here keep working.
from app.ingestion.parsed import ParsedDocument, ParsedSection, ParserError  # noqa: E402,F401


_HEADING_LABELS = {DocItemLabel.SECTION_HEADER, DocItemLabel.TITLE}
_BODY_LABELS = {
    DocItemLabel.TEXT,
    DocItemLabel.PARAGRAPH,
    DocItemLabel.LIST_ITEM,
    DocItemLabel.CAPTION,
    DocItemLabel.FOOTNOTE,
    DocItemLabel.CODE,
}


def _parse_docling(raw_bytes: bytes, filename: str) -> ParsedDocument:
    """Docling wrapper (HLD §9) — extracts page number, heading hierarchy,
    paragraph boundaries, table structure, and section boundaries.

    Walks the parsed document in reading order (`iterate_items`), starting
    a new `ParsedSection` at every heading/title and accumulating body
    text (paragraphs, list items, tables rendered to markdown) underneath
    it until the next heading. A document with no heading before its first
    body content gets one implicit leading section (`heading=None`).
    """
    try:
        converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=_PDF_PIPELINE_OPTIONS)}
        )
        result = converter.convert(DocumentStream(name=filename, stream=BytesIO(raw_bytes)))
        document = result.document
    except Exception as exc:
        raise ParserError(f"failed to parse {filename!r}: {exc}") from exc

    sections: list[ParsedSection] = []
    current_heading: str | None = None
    current_page: int | None = None
    current_text_parts: list[str] = []

    def _flush() -> None:
        text = "\n\n".join(part for part in current_text_parts if part.strip())
        if text.strip():
            sections.append(ParsedSection(heading=current_heading, page_number=current_page, text=text))

    for item, _level in document.iterate_items():
        page_no = item.prov[0].page_no if getattr(item, "prov", None) else None

        if item.label in _HEADING_LABELS:
            _flush()
            current_heading = item.text if isinstance(item, TextItem) else None
            current_page = page_no
            current_text_parts = []
        elif isinstance(item, TableItem):
            current_text_parts.append(item.export_to_markdown(document))
            if current_page is None:
                current_page = page_no
        elif item.label in _BODY_LABELS and isinstance(item, TextItem):
            current_text_parts.append(item.text)
            if current_page is None:
                current_page = page_no

    _flush()

    return ParsedDocument(sections=sections)


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
