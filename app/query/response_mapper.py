from app.core import metrics
from app.grpc.retrieval_pb2 import DocumentChunk, GetRelevantChunksResponse, RetrievalStatus
from app.query.scoring import FusedCandidate

# No real tokenizer dependency — HLD §18.2 permits "the same tokenizer the
# target LLM provider uses (or a reasonable proxy)". This proxy matches
# chunker.py's own words-per-token approximation for consistency across the
# codebase, not a claim of exactness.
_WORDS_PER_TOKEN = 0.75


def _count_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, int(len(text.split()) / _WORDS_PER_TOKEN))


def empty_response(
    request_id: str,
    status: int = RetrievalStatus.NO_MATCH,
    detail: str = "",
    *,
    requested_count: int = 0,
    authorized_count: int = 0,
    ready_count: int = 0,
) -> GetRelevantChunksResponse:
    """HLD §17's "empty is not an error" posture — the single code path
    every short-circuit and degrade path in `pipeline.py` returns through.

    "Empty" is no longer ambiguous: `status` says WHY nothing was returned (no scope, access
    denied, still indexing, dependency down, timeout, or simply nothing relevant), so the
    caller can tell "the document has no answer" from "the document could not be searched".
    """
    metrics.query_status_total.labels(status=RetrievalStatus.Name(status)).inc()
    return GetRelevantChunksResponse(
        chunks=[],
        total_tokens=0,
        request_id=request_id,
        status=status,
        status_detail=detail,
        requested_count=requested_count,
        authorized_count=authorized_count,
        ready_count=ready_count,
    )


def to_proto(
    chunks: list[FusedCandidate],
    request_id: str,
    *,
    requested_count: int = 0,
    authorized_count: int = 0,
    ready_count: int = 0,
) -> GetRelevantChunksResponse:
    """HLD §18.2 — `relevance_score` is always `rrf_score`, never
    `mmr_score` (selection-order-dependent, not a fit for a field read as
    "how relevant is this chunk in isolation"). `chunks` must already be
    sorted by `rrf_score` descending by the caller (`pipeline.py` step 8) —
    LLM Service's Context Window Manager trims from the tail without
    re-sorting.
    """
    if not chunks:
        return empty_response(
            request_id,
            RetrievalStatus.NO_MATCH,
            "documents are indexed but no passage matched the query",
            requested_count=requested_count,
            authorized_count=authorized_count,
            ready_count=ready_count,
        )
    metrics.query_status_total.labels(status="OK").inc()
    return GetRelevantChunksResponse(
        chunks=[
            DocumentChunk(
                chunk_id=c.chunk_id,
                content=c.content,
                source_file_id=c.document_id,
                source_name=c.filename,
                relevance_score=c.rrf_score,
                metadata={
                    "page_number": str(c.page_number) if c.page_number is not None else "",
                    "section": c.section or "",
                    "chunk_index": str(c.chunk_index),
                },
            )
            for c in chunks
        ],
        total_tokens=sum(_count_tokens(c.content) for c in chunks),
        request_id=request_id,
        status=RetrievalStatus.OK,
        requested_count=requested_count,
        authorized_count=authorized_count,
        ready_count=ready_count,
    )
