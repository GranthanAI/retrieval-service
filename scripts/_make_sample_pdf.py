"""One-off helper — writes tests/fixtures/sample.pdf, a minimal hand-built
single-page PDF with plain text content, no external PDF library dependency
needed. Not part of the application; run once to (re)generate the fixture.
"""

import pathlib

TEXT_LINES = [
    "Retrieval Service Sample Document",
    "",
    "This document exists purely to exercise the ingestion pipeline end to end.",
    "It describes the retry policy used by background workers in this system.",
    "The retry policy uses exponential backoff with jitter between attempts.",
    "A worker retries a failed job up to three times before giving up.",
]


def _escape(text: str) -> str:
    return text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def build_pdf() -> bytes:
    content_lines = ["BT", "/F1 12 Tf", "50 750 Td", "14 TL"]
    for i, line in enumerate(TEXT_LINES):
        if i > 0:
            content_lines.append("T*")
        content_lines.append(f"({_escape(line)}) Tj")
    content_lines.append("ET")
    content_stream = "\n".join(content_lines).encode("latin-1")

    objects = []
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objects.append(
        b"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 4 0 R >> >> "
        b"/MediaBox [0 0 612 792] /Contents 5 0 R >>"
    )
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects.append(f"<< /Length {len(content_stream)} >>\nstream\n".encode("latin-1") + content_stream + b"\nendstream")

    out = bytearray()
    out += b"%PDF-1.4\n"
    offsets = [0]
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode("latin-1") + obj + b"\nendobj\n"

    xref_offset = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode("latin-1")
    out += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        out += f"{off:010d} 00000 n \n".encode("latin-1")
    out += b"trailer\n"
    out += f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n".encode("latin-1")
    out += b"startxref\n"
    out += f"{xref_offset}\n".encode("latin-1")
    out += b"%%EOF"

    return bytes(out)


def main() -> None:
    fixtures_dir = pathlib.Path(__file__).resolve().parents[1] / "tests" / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    (fixtures_dir / "sample.pdf").write_bytes(build_pdf())
    print(f"wrote {fixtures_dir / 'sample.pdf'}")


if __name__ == "__main__":
    main()
