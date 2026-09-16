from dataclasses import dataclass, field
from io import BytesIO

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


@dataclass
class ParsedSection:
    """One heading-bounded section of the parsed document (HLD §9).
    `chunker.py` never splits a chunk across a `ParsedSection` boundary —
    this is what makes chunking structure-aware rather than a flat
    token-window split over the whole document.
    """

    heading: str | None
    page_number: int | None
    text: str


@dataclass
class ParsedDocument:
    sections: list[ParsedSection] = field(default_factory=list)


class ParserError(Exception):
    """Wraps any Docling parsing failure — the orchestrator maps this to
    `status=FAILED, error_code=PARSER_ERROR` (LLD §6.2)."""


_HEADING_LABELS = {DocItemLabel.SECTION_HEADER, DocItemLabel.TITLE}
_BODY_LABELS = {
    DocItemLabel.TEXT,
    DocItemLabel.PARAGRAPH,
    DocItemLabel.LIST_ITEM,
    DocItemLabel.CAPTION,
    DocItemLabel.FOOTNOTE,
    DocItemLabel.CODE,
}


def parse(raw_bytes: bytes, filename: str) -> ParsedDocument:
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
