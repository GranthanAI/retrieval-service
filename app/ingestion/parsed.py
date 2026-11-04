from dataclasses import dataclass, field


@dataclass
class ParsedBlock:
    """One block of body content inside a section, with where it came from. Blocks let the chunker keep a page per block (a section
    that spans pages 3-5 is not "page 3") and keep tables whole.

    kind: "text" | "table" | "code".
    For a table, `text` is the whole rendered table (markdown), `header` its header line and separator, `rows` the body lines, `caption`
    its caption when the format has one, and `row_offset` the 1-based number of the first body row (used for "Rows a-b" locations).
    `page` is a real page number (PDF only) or None; formats without pages use `location` instead (a slide, a row range). A page number
    is never invented."""

    text: str
    page: int | None = None
    kind: str = "text"
    caption: str | None = None
    header: str | None = None
    rows: list[str] = field(default_factory=list)
    row_offset: int = 1
    location: str | None = None


@dataclass
class ParsedSection:
    """One heading-bounded section of the parsed document (HLD §9).
    `chunker.py` never splits a chunk across a `ParsedSection` boundary —
    this is what makes chunking structure-aware rather than a flat
    token-window split over the whole document.

    `heading` is the deepest heading (what was always stored as `section`); `heading_path` is the full hierarchy from the top, e.g.
    ["billing-service roadmap", "billing-service: coding phases", "Phase 6: Payment gateway webhooks", "Exit criteria"].
    `blocks` carries per-block pages and tables; parsers that do not fill it leave the chunker to treat `text` as one block.
    """

    heading: str | None
    page_number: int | None
    text: str
    heading_path: list[str] = field(default_factory=list)
    blocks: list[ParsedBlock] = field(default_factory=list)
    location: str | None = None


@dataclass
class ParsedDocument:
    sections: list[ParsedSection] = field(default_factory=list)


class ParserError(Exception):
    """Any parsing failure — the orchestrator maps this to
    `status=FAILED, error_code=PARSER_ERROR` (LLD §6.2) and shows the message to the user."""
