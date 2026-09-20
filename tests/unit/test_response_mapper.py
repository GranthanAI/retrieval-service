import pytest

from app.query import response_mapper
from app.query.scoring import FusedCandidate


def _candidate(chunk_id: str, rrf_score: float, mmr_score: float | None = None) -> FusedCandidate:
    return FusedCandidate(
        chunk_id=chunk_id,
        document_id="doc-1",
        filename="report.pdf",
        content="hello world " * 5,
        page_number=2,
        section="Intro",
        chunk_index=1,
        rrf_score=rrf_score,
        mmr_score=mmr_score,
    )


class TestResponseMapper:
    def test_relevance_score_is_always_rrf_score_never_mmr_score(self) -> None:
        candidates = [_candidate("a", rrf_score=0.5, mmr_score=0.9)]
        response = response_mapper.to_proto(candidates, "req-1")
        # proto `float` is 32-bit — approx accounts for the precision loss,
        # what matters is it's rrf_score (0.5), nowhere near mmr_score (0.9).
        assert response.chunks[0].relevance_score == pytest.approx(0.5)

    def test_relevance_score_matches_rrf_score_across_degraded_shapes_too(self) -> None:
        # Degraded-path candidate: no mmr_score at all (MMR was skipped).
        candidates = [_candidate("a", rrf_score=0.3, mmr_score=None)]
        response = response_mapper.to_proto(candidates, "req-1")
        assert response.chunks[0].relevance_score == pytest.approx(0.3)

    def test_field_mapping(self) -> None:
        candidates = [_candidate("chunk-1", rrf_score=0.7)]
        response = response_mapper.to_proto(candidates, "req-42")

        chunk = response.chunks[0]
        assert chunk.chunk_id == "chunk-1"
        assert chunk.source_file_id == "doc-1"
        assert chunk.source_name == "report.pdf"
        assert chunk.metadata["page_number"] == "2"
        assert chunk.metadata["section"] == "Intro"
        assert chunk.metadata["chunk_index"] == "1"
        assert response.request_id == "req-42"

    def test_empty_response_has_no_chunks_and_zero_tokens(self) -> None:
        response = response_mapper.empty_response("req-1")
        assert list(response.chunks) == []
        assert response.total_tokens == 0
        assert response.request_id == "req-1"

    def test_none_page_number_maps_to_empty_string_metadata(self) -> None:
        candidate = FusedCandidate(chunk_id="a", page_number=None, rrf_score=0.1)
        response = response_mapper.to_proto([candidate], "req-1")
        assert response.chunks[0].metadata["page_number"] == ""
