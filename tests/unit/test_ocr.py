"""Selective OCR: page detection, merge with page provenance, never failing ingestion, off by default. The OCR engine is always a fake."""
import pytest

from app.core import metrics
from app.core.config import settings
from app.ingestion import ocr
from app.ingestion.parsed import ParsedBlock, ParsedDocument, ParsedSection


@pytest.fixture(autouse=True)
def reset(monkeypatch):
    monkeypatch.setattr(settings, "OCR_ENABLED", True)
    yield
    ocr.set_engine(None)


def doc_with_pages(*pages_text: tuple[int, str]) -> ParsedDocument:
    secs = [ParsedSection("Part", p, t, heading_path=["Doc", "Part"], blocks=[ParsedBlock(t, page=p)]) for p, t in pages_text]
    return ParsedDocument(secs)


def test_pages_with_too_little_text_are_flagged_in_page_order_and_capped():
    counts = {1: 500, 2: 3, 4: 0, 5: 19, 6: 20}
    assert ocr.pages_needing_ocr([1, 2, 3, 4, 5, 6], counts, min_chars=20, max_pages=10) == [2, 3, 4, 5]
    assert ocr.pages_needing_ocr([1, 2, 3, 4, 5, 6], counts, min_chars=20, max_pages=2) == [2, 3]


def test_only_the_flagged_pages_are_sent_to_the_engine_and_the_text_keeps_its_page_number():
    parsed = doc_with_pages((1, "a real text layer, plenty of characters here"), (3, "another page with a real text layer, long enough"))
    seen = {}

    def engine(raw, filename, pages):
        seen["pages"] = pages
        return {2: "Recognised text of the scanned page two."}

    ocr.set_engine(engine)
    report = ocr.run(b"%PDF", "scan.pdf", parsed, [1, 2, 3])
    assert seen["pages"] == [2] and report.status == "ok" and report.pages_recognised == [2]
    middle = parsed.sections[1]                                                      # inserted in page order, between page 1 and page 3
    assert middle.page_number == 2 and middle.blocks[0].page == 2 and "scanned page two" in middle.text
    assert middle.heading_path == ["Doc", "Part"]                                     # under the heading path in effect at that page


def test_a_document_with_a_text_layer_on_every_page_never_calls_the_engine():
    parsed = doc_with_pages((1, "x" * 100), (2, "y" * 100))

    def engine(*a):
        raise AssertionError("must not be called")

    ocr.set_engine(engine)
    assert ocr.run(b"", "f.pdf", parsed, [1, 2]).status == "none_needed" and len(parsed.sections) == 2


def test_off_by_default_does_nothing(monkeypatch):
    monkeypatch.setattr(settings, "OCR_ENABLED", False)
    ocr.set_engine(lambda *a: (_ for _ in ()).throw(AssertionError("must not be called")))
    parsed = doc_with_pages((1, ""))
    assert ocr.run(b"", "f.pdf", parsed, [1, 2]).status == "skipped" and len(parsed.sections) == 1
    assert Settings_default_is_off()


def Settings_default_is_off() -> bool:
    from app.core.config import Settings

    return Settings.model_fields["OCR_ENABLED"].default is False


@pytest.mark.parametrize("exc,status", [(ImportError("no rapidocr"), "engine_unavailable"), (RuntimeError("no model"), "engine_unavailable"),
                                        (ValueError("boom"), "failed")])
def test_an_engine_error_is_recorded_and_never_raised(exc, status):
    def engine(*a):
        raise exc

    ocr.set_engine(engine)
    parsed = doc_with_pages((1, ""))
    before = metrics.ocr_pages_total.labels(outcome=status)._value.get()
    report = ocr.run(b"", "f.pdf", parsed, [1])
    assert report.status == status and len(parsed.sections) == 1                       # indexed exactly as before
    assert metrics.ocr_pages_total.labels(outcome=status)._value.get() == before + 1


def test_empty_recognition_adds_nothing_and_is_counted():
    ocr.set_engine(lambda raw, name, pages: {1: "   "})
    parsed = doc_with_pages()
    before = metrics.ocr_pages_total.labels(outcome="empty")._value.get()
    assert ocr.run(b"", "f.pdf", parsed, [1]).pages_recognised == [] and parsed.sections == []
    assert metrics.ocr_pages_total.labels(outcome="empty")._value.get() == before + 1


def test_extraction_quality_separates_text_from_noise():
    assert ocr.quality("The quick brown fox jumps over 13 lazy dogs.") > 0.95
    assert ocr.quality("~~ ## @@ ^^ || \\\\ ¦¦ ¬¬") < 0.2
    assert ocr.quality("") == 0.0


def test_quality_and_time_metrics_are_recorded_per_recognised_page():
    ocr.set_engine(lambda raw, name, pages: {p: "clean recognised words " * 5 for p in pages})
    before = metrics.ocr_quality._sum.get()
    ocr.run(b"", "f.pdf", doc_with_pages(), [1, 2])
    assert metrics.ocr_quality._sum.get() > before + 1.5


def test_a_real_pdf_with_an_image_only_page_is_detected_without_ocr_and_ocr_fills_it(monkeypatch):
    """Real Docling parse of a generated PDF whose page 2 has no text layer at all (no engine needed to DETECT it)."""
    import importlib.util
    import pathlib

    from app.ingestion import parser

    gen = pathlib.Path(__file__).resolve().parents[2] / "evaluation" / "fixtures" / "_generate.py"
    spec = importlib.util.spec_from_file_location("_gen", gen)
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    raw = g.build_pdf([["# Report", "", "Text on the first page, with a real text layer."], [], ["# Appendix", "", "Text on the third page as well."]])

    calls = {}
    ocr.set_engine(lambda r, name, pages: calls.setdefault("pages", pages) and {pages[0]: "Scanned words recognised on page two."})
    parsed = parser.parse(raw, "scan.pdf")
    assert calls["pages"] == [2]
    page2 = [s for s in parsed.sections if any(b.page == 2 for b in s.blocks)]
    assert page2 and "Scanned words" in page2[0].text
    assert [min(b.page for b in s.blocks if b.page) for s in parsed.sections] == sorted(min(b.page for b in s.blocks if b.page) for s in parsed.sections)
