from dataclasses import dataclass, field


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
    """Any parsing failure — the orchestrator maps this to
    `status=FAILED, error_code=PARSER_ERROR` (LLD §6.2) and shows the message to the user."""
