import json

import pytest

from app.core.config import settings
from app.ingestion import transcript as t
from app.ingestion.parser import ParserError


def doc_bytes(segments, **extra):
    return json.dumps({"schema": t.SCHEMA, "video_id": "dQw4w9WgXcQ", "title": "T", "language": "en", "is_generated": False,
                       "segments": [{"start": s, "duration": d, "text": x} for s, d, x in segments], **extra}, ensure_ascii=False).encode()


def make(n, words_per=10, step=4.0, sentence=True):
    return [(i * step, step, " ".join(f"w{i}_{k}" for k in range(words_per)) + ("." if sentence else "")) for i in range(n)]


def chunks_for(segments):
    return t.chunk_transcript(t.parse_transcript(doc_bytes(segments)))


# ── detection and parsing ────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", [b"%PDF-1.4 ...", b"plain text", b"", b"{not json", b"[1,2]", b'{"schema":"other"}', b'{"a":1}', b"\xfc\xff"])
def test_non_transcripts_are_left_to_the_normal_document_path(raw):
    assert t.parse_transcript(raw) is None


def test_a_transcript_is_parsed_with_metadata_and_blank_segments_dropped():
    d = t.parse_transcript(doc_bytes([(0, 1, "hello"), (1, 1, "   "), (2, 1, "wörld ✓")]))
    assert (d.video_id, d.title, d.language, d.is_generated) == ("dQw4w9WgXcQ", "T", "en", False)
    assert [s.text for s in d.segments] == ["hello", "wörld ✓"]


@pytest.mark.parametrize("bad", [
    b'{"schema":"granthan.transcript.v1","segments":"x"}',
    b'{"schema":"granthan.transcript.v1","segments":[{"start":"a","duration":1,"text":"x"}]}',
    b'{"schema":"granthan.transcript.v1","segments":[{"start":-1,"duration":1,"text":"x"}]}',
    b'{"schema":"granthan.transcript.v1","segments":[{"start":0,"duration":1}]}',
    b'{"schema":"granthan.transcript.v1","segments":[{"start":0,"duration":1,"text":5}]}',
])
def test_a_malformed_transcript_fails_loudly_instead_of_being_indexed_as_something_else(bad):
    with pytest.raises(ParserError):
        t.parse_transcript(bad)


# ── chunking ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def test_section_encoding_round_trips_and_rounds_outward():
    assert t.format_section(12.9, 20.1) == "t=12-21"
    assert t.parse_section("t=12-21") == (12, 21)
    assert t.parse_section("Intro") is None and t.parse_section(None) is None and t.parse_section("t=a-b") is None


def test_every_chunk_carries_its_time_range_and_ranges_follow_the_video_order():
    cs = chunks_for(make(200))
    assert len(cs) > 3
    ranges = [t.parse_section(c.section) for c in cs]
    assert all(r and r[0] <= r[1] for r in ranges)
    assert [r[0] for r in ranges] == sorted(r[0] for r in ranges)
    assert [c.chunk_index for c in cs] == list(range(len(cs)))
    assert all(c.page_number is None for c in cs)


def test_a_chunk_time_range_covers_exactly_the_segments_it_contains():
    segs = make(100)
    cs = chunks_for(segs)
    by_word = {s[2].split()[0]: (s[0], s[0] + s[1]) for s in segs}
    for c in cs:
        starts = [by_word[w] for w in c.content.split() if w in by_word]
        lo, hi = t.parse_section(c.section)
        assert lo == int(starts[0][0]) and hi == int(-(-starts[-1][1] // 1))


def test_chunks_never_exceed_the_maximum_and_aim_for_the_target(monkeypatch):
    monkeypatch.setattr(settings, "CHUNK_TARGET_TOKENS", 90)
    monkeypatch.setattr(settings, "CHUNK_MAX_TOKENS", 120)
    monkeypatch.setattr(settings, "CHUNK_OVERLAP_TOKENS", 15)
    cs = chunks_for(make(300, words_per=7))
    maximum = int(120 / 0.75)
    assert all(len(c.content.split()) <= maximum for c in cs)
    sizes = [len(c.content.split()) for c in cs[:-1]]
    assert sum(sizes) / len(sizes) > 0.5 * int(90 / 0.75)


def test_windows_close_at_sentence_ends_when_possible(monkeypatch):
    monkeypatch.setattr(settings, "CHUNK_TARGET_TOKENS", 60)
    monkeypatch.setattr(settings, "CHUNK_MAX_TOKENS", 200)
    monkeypatch.setattr(settings, "CHUNK_OVERLAP_TOKENS", 0)
    cs = chunks_for(make(60, words_per=9))
    assert all(c.content.rstrip().endswith(".") for c in cs)


def test_overlap_repeats_trailing_segments_so_boundary_facts_remain_findable(monkeypatch):
    monkeypatch.setattr(settings, "CHUNK_TARGET_TOKENS", 60)
    monkeypatch.setattr(settings, "CHUNK_MAX_TOKENS", 120)
    monkeypatch.setattr(settings, "CHUNK_OVERLAP_TOKENS", 30)
    cs = chunks_for(make(80, words_per=10))
    shared = set(cs[0].content.split()) & set(cs[1].content.split())
    assert shared
    r0, r1 = t.parse_section(cs[0].section), t.parse_section(cs[1].section)
    assert r1[0] < r0[1]                                               # the ranges overlap in time as well


def test_all_transcript_text_is_present_in_some_chunk_and_nothing_is_invented():
    segs = make(150)
    cs = chunks_for(segs)
    all_words = {w for _, _, x in segs for w in x.split()}
    chunk_words = {w for c in cs for w in c.content.split()}
    assert all_words == chunk_words


def test_progress_is_guaranteed_with_an_overlap_budget_larger_than_a_chunk(monkeypatch):
    monkeypatch.setattr(settings, "CHUNK_OVERLAP_TOKENS", 10_000)
    cs = chunks_for(make(500, words_per=3))
    assert len(cs) < 500 and cs[-1].content


def test_one_huge_segment_is_split_by_words_with_interpolated_times():
    huge = " ".join(f"x{i}" for i in range(5000))
    cs = chunks_for([(100.0, 500.0, huge)])
    assert len(cs) > 1
    ranges = [t.parse_section(c.section) for c in cs]
    assert ranges[0][0] == 100 and ranges[-1][1] >= 599 and ranges == sorted(ranges)
    assert {w for c in cs for w in c.content.split()} == set(huge.split())


def test_chunking_is_deterministic_so_redelivery_reproduces_the_same_chunks():
    segs = make(120)
    a, b = chunks_for(segs), chunks_for(segs)
    assert [(c.chunk_index, c.content, c.section) for c in a] == [(c.chunk_index, c.content, c.section) for c in b]


def test_unicode_and_original_text_are_preserved_exactly():
    cs = chunks_for([(0, 2, "नमस्ते दुनिया."), (2, 2, "日本語 😀 ok.")])
    assert cs[0].content == "नमस्ते दुनिया. 日本語 😀 ok."


def test_an_empty_transcript_yields_no_chunks_so_the_existing_no_text_rule_fails_it():
    assert t.chunk_transcript(t.parse_transcript(doc_bytes([(0, 1, "  ")]))) == []
