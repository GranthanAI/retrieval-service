"""Selective OCR: recognise text only on the PDF pages that have no usable text layer.

The normal PDF path (`parser._parse_docling`) runs with OCR off, because digitally-authored documents have a text layer and OCR would
only add cost. A scanned or image-only page, though, comes out empty. With `OCR_ENABLED` this module finds those pages (fewer than
`OCR_MIN_CHARS_PER_PAGE` extracted characters), runs OCR on exactly those pages, and merges the recognised text back as blocks that carry
their real page number, so page-level provenance holds. Everything is off by default.

The OCR engine is injectable (`set_engine`): the default uses Docling's own OCR with `page_range`, which needs an OCR model (RapidOCR /
EasyOCR) that this service does not bundle. Without one the run is recorded as `engine_unavailable` and the document is indexed as it
was before: OCR can never make ingestion fail.

Metrics: pages seen / OCR'd / empty / failed, seconds per OCR'd page, and an extraction-quality score per page (share of the recognised
tokens that look like words), so garbage recognition is visible."""
import re
import time
from collections.abc import Callable
from dataclasses import dataclass

from app.core import metrics
from app.core.config import settings
from app.core.logging import get_logger
from app.ingestion.parsed import ParsedBlock, ParsedDocument, ParsedSection

logger = get_logger(__name__)

# (raw PDF bytes, filename, 1-based pages) -> {page: recognised text}. Raises if the engine cannot run.
OcrEngine = Callable[[bytes, str, list[int]], dict[int, str]]
_engine: OcrEngine | None = None


def set_engine(engine: OcrEngine | None) -> None:
    global _engine
    _engine = engine


@dataclass
class OcrReport:
    pages_total: int = 0
    pages_flagged: list[int] | None = None
    pages_recognised: list[int] | None = None
    seconds: float = 0.0
    status: str = "skipped"          # skipped | none_needed | ok | engine_unavailable | failed


def pages_needing_ocr(all_pages: list[int], chars_by_page: dict[int, int], min_chars: int, max_pages: int) -> list[int]:
    """Pages with fewer than `min_chars` extracted characters, in page order, at most `max_pages` (a cost cap per document)."""
    flagged = [p for p in sorted(all_pages) if chars_by_page.get(p, 0) < min_chars]
    return flagged[:max_pages]


def chars_by_page(parsed: ParsedDocument) -> dict[int, int]:
    counts: dict[int, int] = {}
    for section in parsed.sections:
        for block in section.blocks:
            if block.page is not None:
                counts[block.page] = counts.get(block.page, 0) + len(block.text.strip())
    return counts


def quality(text: str) -> float:
    """Share of whitespace-separated tokens that look like words (letters/digits with ordinary punctuation). Near 1.0 for clean text,
    low for recognition noise. A proxy: there is no ground truth at ingestion time."""
    tokens = text.split()
    if not tokens:
        return 0.0
    good = sum(1 for t in tokens if re.fullmatch(r"[\w][\w'’\-.,;:!?()%/$]*", t) and re.search(r"[A-Za-z0-9ऀ-ॿ]", t))
    return good / len(tokens)


def merge(parsed: ParsedDocument, recognised: dict[int, str]) -> None:
    """Adds one section per recognised page, in page order, under the heading path in effect at that page (the last section that starts
    on or before it). Text blocks carry the real page number."""
    starts = sorted(
        (min((b.page for b in s.blocks if b.page is not None), default=s.page_number or 0), i) for i, s in enumerate(parsed.sections))
    for page in sorted(recognised):
        text = recognised[page].strip()
        if not text:
            continue
        before = [i for start, i in starts if start <= page]
        anchor = parsed.sections[before[-1]] if before else None
        path = list(anchor.heading_path) if anchor else []
        section = ParsedSection(
            heading=anchor.heading if anchor else None, page_number=page, text=text, heading_path=path,
            blocks=[ParsedBlock(text, page=page, kind="text")])
        position = next((idx for idx, s in enumerate(parsed.sections)
                         if min((b.page for b in s.blocks if b.page is not None), default=s.page_number or 0) > page), len(parsed.sections))
        parsed.sections.insert(position, section)


def run(raw: bytes, filename: str, parsed: ParsedDocument, all_pages: list[int]) -> OcrReport:
    """Finds the pages without a text layer, OCRs them, merges the result into `parsed`. Never raises."""
    report = OcrReport(pages_total=len(all_pages))
    if not settings.OCR_ENABLED:
        return report
    flagged = pages_needing_ocr(all_pages, chars_by_page(parsed), settings.OCR_MIN_CHARS_PER_PAGE, settings.OCR_MAX_PAGES_PER_DOCUMENT)
    report.pages_flagged = flagged
    metrics.ocr_pages_total.labels(outcome="seen").inc(len(all_pages))
    if not flagged:
        report.status = "none_needed"
        return report
    engine = _engine or _docling_engine
    start = time.perf_counter()
    try:
        recognised = engine(raw, filename, flagged)
    except Exception as exc:  # noqa: BLE001 - OCR must never fail ingestion
        report.status = "engine_unavailable" if isinstance(exc, (ImportError, RuntimeError, OSError)) else "failed"
        metrics.ocr_pages_total.labels(outcome=report.status).inc(len(flagged))
        logger.warning("ocr.failed", status=report.status, pages=len(flagged), error=type(exc).__name__)
        return report
    report.seconds = time.perf_counter() - start
    per_page = report.seconds / max(1, len(flagged))
    good: list[int] = []
    for page in flagged:
        text = recognised.get(page, "")
        if text.strip():
            good.append(page)
            metrics.ocr_page_seconds.observe(per_page)
            metrics.ocr_quality.observe(quality(text))
        else:
            metrics.ocr_pages_total.labels(outcome="empty").inc()
    metrics.ocr_pages_total.labels(outcome="recognised").inc(len(good))
    merge(parsed, {p: recognised[p] for p in good})
    report.pages_recognised, report.status = good, "ok"
    logger.info("ocr.done", filename_ext=filename.rsplit(".", 1)[-1], flagged=len(flagged), recognised=len(good), seconds=round(report.seconds, 2))
    return report


def _docling_engine(raw: bytes, filename: str, pages: list[int]) -> dict[int, str]:
    """Docling's own OCR on exactly `pages` (contiguous runs converted together). Needs an OCR model installed; otherwise Docling raises
    and `run` records `engine_unavailable`."""
    from io import BytesIO

    from docling.datamodel.base_models import InputFormat
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling_core.types.doc import DocItemLabel, TextItem
    from docling_core.types.io import DocumentStream

    from app.ingestion import docling_config

    options = docling_config.pipeline_options(do_ocr=True)       # same device / threads / timeout as normal parsing
    options.do_table_structure = False
    options.force_backend_text = False
    converter = DocumentConverter(format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)})
    out: dict[int, list[str]] = {p: [] for p in pages}
    runs: list[tuple[int, int]] = []
    for p in pages:
        if runs and runs[-1][1] == p - 1:
            runs[-1] = (runs[-1][0], p)
        else:
            runs.append((p, p))
    for first, last in runs:
        result = converter.convert(DocumentStream(name=filename, stream=BytesIO(raw)), page_range=(first, last))
        for item, _ in result.document.iterate_items():
            prov = getattr(item, "prov", None)
            if prov and isinstance(item, TextItem) and item.label != DocItemLabel.PAGE_FOOTER and prov[0].page_no in out:
                out[prov[0].page_no].append(item.text)
    return {p: "\n\n".join(parts) for p, parts in out.items()}
