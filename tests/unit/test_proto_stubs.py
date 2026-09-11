from app.grpc.retrieval_pb2 import GetRelevantChunksRequest, RetrievalScope


def test_request_fields_roundtrip() -> None:
    request = GetRelevantChunksRequest(
        user_id="user-1",
        conversation_id="conv-1",
        query="what is the retry policy?",
        top_k=5,
        scope=RetrievalScope.FILES,
        file_ids=["doc-a", "doc-b"],
        min_relevance_score=0.5,
        max_tokens=2000,
        trace_id="trace-1",
    )

    assert request.user_id == "user-1"
    assert request.query == "what is the retry policy?"
    assert request.top_k == 5
    assert request.scope == RetrievalScope.FILES
    assert list(request.file_ids) == ["doc-a", "doc-b"]
    assert request.trace_id == "trace-1"
