"""Parsing of the file types beyond PDF: text and code, Markdown, CSV, Word/Excel/PowerPoint/HTML (Docling), legacy .doc and zip."""
import io
import zipfile

import pytest

from app.core.config import settings
from app.ingestion import archive, chunker, formats, parser
from app.ingestion.parser import ParserError


def zipped(files: dict, extra=None) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)
        if extra:
            extra(zf)
    return buf.getvalue()


def headings(doc):
    return [s.heading for s in doc.sections]


def text_of(doc):
    return "\n".join(s.text for s in doc.sections)


# ─── what is recognised ──────────────────────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name,kind", [
    ("a.pdf", "pdf"), ("A.PDF", "pdf"), ("r.docx", "docling"), ("s.pptx", "docling"), ("t.xlsx", "docling"), ("p.html", "docling"),
    ("n.md", "markdown"), ("d.csv", "csv"), ("d.tsv", "csv"), ("old.doc", "legacy_doc"), ("x.zip", "archive"),
    ("a.txt", "text"), ("main.py", "text"), ("App.tsx", "text"), ("lib.rs", "text"), ("query.sql", "text"), ("conf.yaml", "text"),
    ("Dockerfile", "text"), ("Makefile", "text"), ("logo.png", "unsupported"), ("tool.exe", "unsupported"), ("noext", "unsupported"),
    (".env", "unsupported"),
])
def test_file_kinds(name, kind):
    assert formats.classify(name) == kind


# ─── text, code, markdown, csv ───────────────────────────────────────────────────────────────────────────────────────────


def test_a_small_text_or_code_file_is_one_section_a_long_one_is_labelled_by_line_range():
    assert headings(parser.parse(b"def f():\n    return 1\n", "main.py")) == [None]
    long = parser.parse(("line of code\n" * 300).encode(), "big.py")
    assert headings(long)[:2] == ["Lines 1-120", "Lines 121-240"] and len(long.sections) == 3
    assert "line of code" in long.sections[0].text


def test_text_encodings_are_decoded_not_mangled():
    assert "café" in text_of(parser.parse("café au lait".encode("utf-8"), "a.txt"))
    assert "café" in text_of(parser.parse("café".encode("utf-16"), "b.txt"))
    assert "café" in text_of(parser.parse("café".encode("cp1252"), "c.txt"))
    assert "hello" in text_of(parser.parse(b"\xef\xbb\xbfhello", "d.txt"))               # BOM stripped


def test_binary_content_is_refused_not_indexed_as_noise():
    with pytest.raises(ParserError, match="binary"):
        parser.parse(b"\x00\x01\x02" * 4000, "fake.txt")


def test_empty_files_parse_to_nothing_so_ingestion_reports_no_text():
    assert parser.parse(b"   \n\n", "empty.txt").sections == []
    assert parser.parse(b"", "empty.csv").sections == []


def test_markdown_splits_on_headings_but_not_inside_code_fences():
    md = b"intro text\n\n# Title\n\nbody\n\n```\n# comment, not a heading\n```\n\n## Sub\n\nmore text\n"
    doc = parser.parse(md, "a.md")
    assert headings(doc) == [None, "Title", "Sub"]
    assert "# comment, not a heading" in doc.sections[1].text


def test_csv_becomes_a_markdown_table_per_row_group_with_the_header_repeated():
    rows = "\n".join(f"item{i},{i}" for i in range(100))
    doc = parser.parse(f"name,qty\n{rows}".encode(), "stock.csv")
    assert headings(doc) == ["Rows 1-40", "Rows 41-80", "Rows 81-100"]
    for section in doc.sections:
        assert section.text.startswith("| name | qty |")
    assert "| item41 | 41 |" in doc.sections[1].text
    tsv = parser.parse(b"a\tb\n1\t2\n", "x.tsv")
    assert "| 1 | 2 |" in tsv.sections[0].text


def test_chunker_accepts_the_new_sections():
    doc = parser.parse(("word " * 4000).encode(), "long.txt")
    assert len(chunker.chunk(doc)) > 1


# ─── Word / Excel / PowerPoint / HTML through Docling ──────────────────────────────────────────────────────────────────


def office_files():
    import docx
    import openpyxl
    import pptx
    out = {}
    d = docx.Document()
    d.add_heading("Quarterly plan", 1)
    d.add_paragraph("The revenue limit is 42 units.")
    b = io.BytesIO()
    d.save(b)
    out["plan.docx"] = b.getvalue()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["name", "qty"])
    ws.append(["bolt", 3])
    b = io.BytesIO()
    wb.save(b)
    out["stock.xlsx"] = b.getvalue()
    p = pptx.Presentation()
    s = p.slides.add_slide(p.slide_layouts[1])
    s.shapes.title.text = "Roadmap"
    s.placeholders[1].text = "Ship v2 in March"
    b = io.BytesIO()
    p.save(b)
    out["deck.pptx"] = b.getvalue()
    out["page.html"] = b"<html><body><h1>Title</h1><p>Body text here.</p></body></html>"
    return out


@pytest.mark.parametrize("name,needle", [("plan.docx", "42 units"), ("stock.xlsx", "bolt"), ("deck.pptx", "March"), ("page.html", "Body text")])
def test_office_and_html_documents_are_read(name, needle):
    assert needle in text_of(parser.parse(office_files()[name], name))


def test_legacy_doc_without_a_converter_fails_with_an_actionable_message(monkeypatch):
    monkeypatch.setattr(settings, "LEGACY_DOC_COMMAND", "definitely-not-installed-converter")
    with pytest.raises(ParserError, match=r"\.docx or PDF"):
        parser.parse(b"\xd0\xcf\x11\xe0 not really", "old.doc")


def test_unsupported_types_fail_clearly():
    with pytest.raises(ParserError, match="not supported"):
        parser.parse(b"\x89PNG....", "image.png")


# ─── zip archives ────────────────────────────────────────────────────────────────────────────────────────────────────────


def test_every_readable_file_in_a_zip_is_indexed_with_its_path_as_the_citation_label():
    z = zipped({
        "README.md": b"# Project\n\nThis is the readme.\n",
        "src/app/main.py": b"def run():\n    return 'hello from main'\n",
        "docs/notes.txt": b"meeting notes about the limit of 42",
        "data/stock.csv": b"name,qty\nbolt,3\n",
        "plan.docx": office_files()["plan.docx"],
    })
    doc = parser.parse(z, "bundle.zip")
    labels = headings(doc)
    assert labels[0] == "Archive contents"
    assert "README.md › Project" in labels and "src/app/main.py" in labels and "docs/notes.txt" in labels
    assert any(l.startswith("data/stock.csv") for l in labels) and any(l.startswith("plan.docx") for l in labels)
    overview = doc.sections[0].text
    for path in ("README.md", "src/app/main.py", "docs/notes.txt", "data/stock.csv", "plan.docx"):
        assert path in overview
    assert "hello from main" in text_of(doc) and "42 units" in text_of(doc)


def test_noise_and_unsupported_members_are_skipped_and_reported():
    z = zipped({"a.txt": b"keep me", "node_modules/pkg/index.js": b"x = 1", ".git/config": b"[core]", "__MACOSX/._a.txt": b"junk",
                "image.png": b"\x89PNG", "tool.exe": b"MZ", ".env": b"SECRET=1"})
    doc = parser.parse(z, "mixed.zip")
    assert "a.txt" in doc.sections[0].text and "node_modules" not in text_of(doc) and "SECRET" not in text_of(doc)
    assert "2 skipped (unsupported type)" in doc.sections[0].text                      # the png and the exe, not the noise


def test_an_archive_with_nothing_readable_fails_with_the_reasons():
    with pytest.raises(ParserError, match="no readable files.*unsupported type"):
        parser.parse(zipped({"a.png": b"\x89PNG", "b.exe": b"MZ"}), "images.zip")
    with pytest.raises(ParserError, match="not a valid zip"):
        parser.parse(b"this is not a zip at all", "broken.zip")


def test_a_broken_member_does_not_fail_the_archive():
    z = zipped({"good.txt": b"fine content here", "bad.txt": b"\x00\x01\x02" * 4000})
    doc = parser.parse(z, "partial.zip")
    assert "fine content here" in text_of(doc) and "1 skipped (unreadable)" in doc.sections[0].text


def test_nested_zips_are_opened_up_to_the_depth_limit(monkeypatch):
    inner = zipped({"deep.txt": b"inside the inner archive"})
    outer = zipped({"inner.zip": inner, "top.txt": b"top level"})
    doc = parser.parse(outer, "outer.zip")
    assert "inside the inner archive" in text_of(doc) and any(h and h.startswith("inner.zip/deep.txt") for h in headings(doc))
    monkeypatch.setattr(settings, "ARCHIVE_MAX_DEPTH", 1)
    doc = parser.parse(outer, "outer.zip")
    assert "inside the inner archive" not in text_of(doc) and "nested too deep" in doc.sections[0].text


# ─── zip safety ──────────────────────────────────────────────────────────────────────────────────────────────────────────


def test_path_traversal_names_are_neutralised_and_nothing_is_written_to_disk(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    z = zipped({"../../evil.txt": b"escape attempt", "/abs/path.txt": b"absolute", "C:\\win\\x.txt": b"drive", "ok/../fine.txt": b"dots"})
    doc = parser.parse(z, "slip.zip")
    shown = doc.sections[0].text
    assert "../" not in shown and "evil" not in "".join(h or "" for h in headings(doc)[1:])
    assert "abs/path.txt" in shown and "win/x.txt" in shown                           # absolute and drive prefixes are stripped
    assert list(tmp_path.iterdir()) == []                                              # members are read in memory, never extracted
    assert archive.safe_member_path("a/../b") is None and archive.safe_member_path("..") is None
    assert archive.safe_member_path("/etc/passwd") == "etc/passwd" and archive.safe_member_path("") is None


def test_too_many_entries_is_refused(monkeypatch):
    monkeypatch.setattr(settings, "ARCHIVE_MAX_ENTRIES", 10)
    with pytest.raises(ParserError, match="entries"):
        parser.parse(zipped({f"f{i}.txt": b"x" for i in range(11)}), "many.zip")


def test_a_decompression_bomb_is_skipped_without_being_expanded(monkeypatch):
    bomb = zipped({"zeros.txt": b"0" * 60_000_000, "ok.txt": b"small and fine"})
    assert len(bomb) < 200_000                                                          # tiny on disk, 60 MB when expanded
    monkeypatch.setattr(settings, "ARCHIVE_MAX_MEMBER_BYTES", 100_000_000)
    doc = parser.parse(bomb, "bomb.zip")
    assert "small and fine" in text_of(doc) and "zeros" not in "".join(h or "" for h in headings(doc)[1:])
    assert "suspicious compression ratio" in doc.sections[0].text


def test_member_size_and_total_size_caps(monkeypatch):
    monkeypatch.setattr(settings, "ARCHIVE_MAX_MEMBER_BYTES", 1_000)
    doc = parser.parse(zipped({"big.txt": b"a" * 5_000, "small.txt": b"tiny"}), "caps.zip")
    assert "small.txt" in doc.sections[0].text and "1 skipped (too large)" in doc.sections[0].text
    monkeypatch.setattr(settings, "ARCHIVE_MAX_MEMBER_BYTES", 50_000_000)
    monkeypatch.setattr(settings, "ARCHIVE_MAX_TOTAL_BYTES", 2_000)
    doc = parser.parse(zipped({f"f{i}.txt": b"x" * 900 for i in range(6)}), "total.zip")
    assert "archive size limit reached" in doc.sections[0].text


def test_symbolic_links_are_skipped():
    def add(zf):
        info = zipfile.ZipInfo("link.txt")
        info.external_attr = 0o120777 << 16                                             # a symbolic link
        zf.writestr(info, "target.txt")
    doc = parser.parse(zipped({"real.txt": b"real content"}, extra=add), "links.zip")
    assert "real content" in text_of(doc) and "1 skipped (symbolic link)" in doc.sections[0].text


def test_heavy_documents_inside_an_archive_are_capped(monkeypatch):
    monkeypatch.setattr(settings, "ARCHIVE_MAX_DOCUMENT_MEMBERS", 1)
    files = {"a.docx": office_files()["plan.docx"], "b.docx": office_files()["plan.docx"], "notes.txt": b"plain text is not limited"}
    doc = parser.parse(zipped(files), "docs.zip")
    assert "1 skipped (too many documents)" in doc.sections[0].text and "plain text is not limited" in text_of(doc)
