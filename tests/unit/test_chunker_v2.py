"""Chunker v2: embedding-model-token sizing, heading-path prefix, page tracking, whole tables, parsers' heading hierarchy."""
import pytest

from app.core.config import settings
from app.ingestion import text_parsers
from app.ingestion.chunker_v2 import PATH_SEP, ChunkTooLargeError, build_prefix, chunk_v2
from app.ingestion.parsed import ParsedBlock, ParsedDocument, ParsedSection

MAX_SEQ = 96            # => hard maximum 80 tokens (prefix + body) with the default margin of 16


class WordCounter:
    """One token per whitespace word: deterministic and tokenizer-free."""

    name = "test-words"

    def count(self, text: str) -> int:
        return len(text.split())

    def split(self, text: str, max_tokens: int) -> list[str]:
        words = text.split()
        return [" ".join(words[i:i + max_tokens]) for i in range(0, len(words), max_tokens)]


@pytest.fixture(autouse=True)
def small_limits(monkeypatch):
    monkeypatch.setattr(settings, "CHUNK_TARGET_TOKENS_V2", 30)
    monkeypatch.setattr(settings, "CHUNK_OVERLAP_TOKENS_V2", 6)
    monkeypatch.setattr(settings, "CHUNK_MAX_MARGIN_V2", 16)


def words(n: int, tag: str = "w") -> str:
    return " ".join(f"{tag}{i}" for i in range(n))


def doc(*sections: ParsedSection) -> ParsedDocument:
    return ParsedDocument(list(sections))


def run(parsed, filename="plan.md"):
    return chunk_v2(parsed, filename, WordCounter(), MAX_SEQ)


def test_every_chunk_carries_the_full_heading_path_and_the_file_name():
    s = ParsedSection("Exit criteria", None, "- done when green", heading_path=["roadmap", "coding phases", "Phase 6: Webhooks", "Exit criteria"])
    chunk = run(doc(s))[0]
    first_line = chunk.content.split("\n")[0]
    assert first_line == PATH_SEP.join(["plan.md", "roadmap", "coding phases", "Phase 6: Webhooks", "Exit criteria"])
    assert "Phase 6" in chunk.content and chunk.heading_path[-1] == "Exit criteria" and chunk.section == "Exit criteria"


def test_no_chunk_exceeds_the_maximum_even_for_one_giant_unbroken_paragraph():
    giant = ParsedSection("Big", None, words(500), heading_path=["Big"])
    chunks = run(doc(giant))
    assert len(chunks) > 5 and all(WordCounter().count(c.content) <= MAX_SEQ - 16 for c in chunks)


def test_split_order_is_paragraph_then_sentence_then_tokens():
    paras = "\n\n".join(f"Para {i}. " + words(20, f"p{i}x") for i in range(6))
    chunks = run(doc(ParsedSection("S", None, paras, heading_path=["S"])))
    assert len(chunks) >= 3
    # chunks begin at paragraph starts, never mid-paragraph (each paragraph fits the target)
    assert all(c.content.split("\n", 1)[1].startswith("Para ") or c.content.split("\n", 1)[1].split()[0].startswith("p") for c in chunks)
    long_sentences = ParsedSection("T", None, ". ".join(f"Sentence {i} " + words(25, f"s{i}") for i in range(8)) + ".", heading_path=["T"])
    assert all(WordCounter().count(c.content) <= MAX_SEQ - 16 for c in run(doc(long_sentences)))


def test_consecutive_text_chunks_overlap_by_at_most_the_overlap_budget():
    text = "\n\n".join(words(4, f"q{i}x") for i in range(20))          # small units, so a tail of at most 6 tokens can be carried
    chunks = run(doc(ParsedSection("S", None, text, heading_path=["S"])))
    assert len(chunks) > 2
    for a, b in zip(chunks, chunks[1:]):
        body_a, body_b = a.content.split("\n", 1)[1].split(), b.content.split("\n", 1)[1].split()
        shared = [w for w in body_b[:8] if w in body_a]
        assert 0 < len(shared) <= settings.CHUNK_OVERLAP_TOKENS_V2


def test_a_section_spanning_pages_records_a_page_range_instead_of_its_first_page():
    blocks = [ParsedBlock(words(5, "a"), page=3), ParsedBlock(words(5, "b"), page=4), ParsedBlock(words(5, "c"), page=5)]
    chunk = run(doc(ParsedSection("Recon", 3, "x", heading_path=["Recon"], blocks=blocks)))[0]
    assert (chunk.page_start, chunk.page_end) == (3, 5)


def test_a_new_page_starts_a_new_chunk_once_the_current_one_is_over_half_full():
    blocks = [ParsedBlock(words(20, "a"), page=3), ParsedBlock(words(5, "b"), page=4)]          # 20 > 30/2
    chunks = run(doc(ParsedSection("R", 3, "x", heading_path=["R"], blocks=blocks)))
    assert [(c.page_start, c.page_end) for c in chunks] == [(3, 3), (4, 4)]


def test_pages_are_never_invented_for_formats_without_them():
    s = ParsedSection("Part", None, words(10), heading_path=["Part"], location="Lines 4-9")
    c = run(doc(s))[0]
    assert (c.page_number, c.page_start, c.page_end, c.location) == (None, None, None, "Lines 4-9")


def test_a_small_table_is_one_chunk_marked_as_a_table():
    header = "| plan | storage |\n| --- | --- |"
    rows = ["| Starter | 50 GB |", "| Team | 500 GB |"]
    t = ParsedBlock("\n".join([header] + rows), page=6, kind="table", header=header, rows=rows, caption="Pricing")
    chunks = run(doc(ParsedSection("Pricing", 6, "x", heading_path=["Pricing"], blocks=[t])))
    assert len(chunks) == 1 and chunks[0].block_type == "table" and chunks[0].page_start == 6
    assert "Pricing" in chunks[0].content and all(r in chunks[0].content for r in rows)


def test_a_large_table_splits_into_row_groups_that_repeat_header_and_caption_and_never_cut_a_row():
    header = "| id | customer | status |\n| --- | --- | --- |"
    rows = [f"| INV-{i:04d} | Customer number {i} | paid |" for i in range(60)]
    t = ParsedBlock("\n".join([header] + rows), kind="table", header=header, rows=rows, caption="Invoices", row_offset=1)
    chunks = run(doc(ParsedSection(None, None, "x", blocks=[t])), filename="invoices.csv")
    assert len(chunks) > 3 and all(c.block_type == "table" for c in chunks)
    seen = []
    for c in chunks:
        lines = c.content.split("\n")
        assert lines[0] == "invoices.csv" and "Invoices" in lines and header.split("\n")[0] in lines     # prefix, caption, header on every group
        seen += [l for l in lines if l.startswith("| INV-")]
        assert all(l.endswith("| paid |") for l in lines if l.startswith("| INV-"))                       # no row cut in half
        assert WordCounter().count(c.content) <= MAX_SEQ - 16
    assert seen == rows                                                                                   # every row exactly once, in order
    assert [c.location for c in chunks][0].startswith("Rows 1-")


def test_an_overlong_heading_path_is_shortened_so_the_body_keeps_room():
    path = [f"heading level {i} " + words(20, f"h{i}") for i in range(4)]
    prefix = build_prefix("a-very-long-file-name.md", path, WordCounter(), MAX_SEQ - 16)
    assert WordCounter().count(prefix + "\n") + 64 <= MAX_SEQ - 16 or len(prefix.split()) < 16
    chunks = run(doc(ParsedSection("deep", None, words(40), heading_path=path)), filename="a-very-long-file-name.md")
    assert all(WordCounter().count(c.content) <= MAX_SEQ - 16 for c in chunks)


def test_a_counter_that_disagrees_with_the_final_check_fails_loudly():
    class Liar(WordCounter):
        calls = 0

        def count(self, text):
            Liar.calls += 1
            return len(text.split()) if Liar.calls < 10_000 and "FINAL" not in text else 10**6
    with pytest.raises(ChunkTooLargeError):
        chunk_v2(doc(ParsedSection("S", None, "FINAL words", heading_path=["S"])), "f.md", Liar(), MAX_SEQ)


# ---- parsers ----------------------------------------------------------------------------------------------------------------
MD = b"""# Roadmap

intro text

## Coding phases

### Phase 6: Webhooks

builds webhook ingestion.

#### Exit criteria

- replay ten times gives one record

| a | b |
| --- | --- |
| 1 | 2 |

```
code # not a heading
```

### Phase 7: Metering

meter things
"""


def test_markdown_keeps_the_whole_heading_hierarchy_blocks_and_line_locations():
    sections = text_parsers.parse_markdown(MD, "plan.md").sections
    exit_ = next(s for s in sections if s.heading == "Exit criteria")
    assert exit_.heading_path == ["Roadmap", "Coding phases", "Phase 6: Webhooks", "Exit criteria"]
    assert [b.kind for b in exit_.blocks] == ["text", "table", "code"]
    assert exit_.blocks[1].header.startswith("| a | b |") and exit_.blocks[1].rows == ["| 1 | 2 |"]
    assert exit_.location.startswith("Lines ")
    p7 = next(s for s in sections if s.heading == "Phase 7: Metering")
    assert p7.heading_path == ["Roadmap", "Coding phases", "Phase 7: Metering"]       # the Phase 6 branch was popped


def test_csv_sections_are_table_blocks_with_row_offsets():
    rows = "\n".join(["id,name"] + [f"{i},n{i}" for i in range(1, 90)])
    sections = text_parsers.parse_csv(rows.encode(), "x.csv").sections
    assert len(sections) == 3 and sections[1].blocks[0].kind == "table" and sections[1].blocks[0].row_offset == 41
    assert sections[1].location == "Rows 41-80" and sections[1].blocks[0].header.startswith("| id | name |")


def test_plain_text_gets_a_line_range_location_and_no_page():
    s = text_parsers.parse_text(("line\n" * 300).encode(), "notes.txt").sections
    assert s[0].location == "Lines 1-120" and s[0].page_number is None
