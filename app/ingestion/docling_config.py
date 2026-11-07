"""Docling (PDF, Word, PowerPoint, Excel and HTML parsing) configured from settings, in one place.

Every choice is a `Settings` field, so it can come from the environment or `.env`:

    DOCLING_ENABLED                   false = PDF/Office files are refused with a clear message and Docling is never imported
    DOCLING_TABLE_STRUCTURE           table detection on/off (slower, and the source of tables kept whole in chunks)
    DOCLING_TABLE_MODE                "" (Docling's default) | "fast" | "accurate"
    DOCLING_DEVICE                    auto | cpu | cuda | mps | xpu
    DOCLING_NUM_THREADS               0 = Docling's default
    DOCLING_DOCUMENT_TIMEOUT_SECONDS  0 = none; otherwise Docling stops a document that takes longer
    DOCLING_REUSE_CONVERTER           build the converter once and reuse it (parsing is then serialised by a lock)

DOCLING_TABLE_STRUCTURE and DOCLING_TABLE_MODE change WHAT is extracted, so they are part of a generation's `parser_version`
(`parser_options_suffix()`); changing them means building a new index generation. The others only change speed or safety.

Docling is imported lazily, inside the functions below: the libraries pull in torch and cost tens of seconds at process start, which a
deployment that only handles text, Markdown and CSV (DOCLING_ENABLED=false) should not pay."""
import threading
from io import BytesIO

from app.core.config import settings
from app.ingestion.parsed import ParserError

DISABLED_MESSAGE = (
    "PDF and Office files can't be read on this server right now (document parsing is turned off). "
    "Upload a text, Markdown or CSV file instead, or ask an administrator to enable it."
)

_converter = None
_lock = threading.Lock()


def ensure_enabled() -> None:
    if not settings.DOCLING_ENABLED:
        raise ParserError(DISABLED_MESSAGE)


def parser_options_suffix() -> str:
    """"" when every parse-affecting option has its default, else e.g. "+tables=off,tablemode=fast". Appended to a generation's parser_version."""
    parts = []
    if not settings.DOCLING_TABLE_STRUCTURE:
        parts.append("tables=off")
    if settings.DOCLING_TABLE_MODE:
        parts.append(f"tablemode={settings.DOCLING_TABLE_MODE}")
    return ("+" + ",".join(parts)) if parts else ""


def parser_mismatch(recorded_parser_version: str) -> str | None:
    """None when the current Docling options give the parser_version a generation was created with; otherwise the version they give now."""
    now = recorded_parser_version.split("+")[0] + parser_options_suffix()
    return None if now == recorded_parser_version else now


def pipeline_options(do_ocr: bool = False):
    """Docling's PdfPipelineOptions built from the settings. Imports Docling."""
    from docling.datamodel.accelerator_options import AcceleratorOptions
    from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode

    options = PdfPipelineOptions(do_ocr=do_ocr, do_table_structure=settings.DOCLING_TABLE_STRUCTURE)
    if settings.DOCLING_TABLE_STRUCTURE and settings.DOCLING_TABLE_MODE:
        options.table_structure_options.mode = TableFormerMode(settings.DOCLING_TABLE_MODE)
    accelerator = {"device": settings.DOCLING_DEVICE}
    if settings.DOCLING_NUM_THREADS > 0:
        accelerator["num_threads"] = settings.DOCLING_NUM_THREADS
    options.accelerator_options = AcceleratorOptions(**accelerator)
    if settings.DOCLING_DOCUMENT_TIMEOUT_SECONDS > 0:
        options.document_timeout = settings.DOCLING_DOCUMENT_TIMEOUT_SECONDS
    return options


def make_converter(do_ocr: bool = False):
    from docling.datamodel.base_models import InputFormat
    from docling.document_converter import DocumentConverter, PdfFormatOption

    return DocumentConverter(format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options(do_ocr))})


def reset() -> None:
    """Drops the shared converter (tests, or after changing settings at run time)."""
    global _converter
    with _lock:
        _converter = None


def convert(raw: bytes, filename: str):
    """The Docling document for `raw`. Raises ParserError when parsing is disabled; any Docling error propagates for the caller to wrap."""
    ensure_enabled()
    from docling_core.types.io import DocumentStream

    stream = DocumentStream(name=filename, stream=BytesIO(raw))
    if not settings.DOCLING_REUSE_CONVERTER:
        return make_converter().convert(stream).document
    global _converter
    with _lock:                                   # the converter's pipelines are not known to be thread-safe: one document at a time
        if _converter is None:
            _converter = make_converter()
        return _converter.convert(stream).document
