import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.clients.file_service_client import FileServiceClient
from app.core import metrics
from app.core.config import settings
from app.core.enums import RetrievalScope
from app.core.logging import get_logger
from app.core.enums import IngestionStatus
from app.grpc.retrieval_pb2 import GetRelevantChunksRequest, GetRelevantChunksResponse, RetrievalStatus
from app.query import bm25_search, candidate_embeddings, dedupe, dense_search, fusion, mmr, response_mapper, scope_guard, tokens
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository

logger = get_logger(__name__)


async def run(
    request: GetRelevantChunksRequest,
    *,
    session_factory: async_sessionmaker,
    file_service_client: FileServiceClient,
    embedding_client: EmbeddingClient,
    milvus_repository: MilvusRepository,
    opensearch_repository: OpenSearchRepository,
) -> GetRelevantChunksResponse:
    """LLD §7.2 — full orchestration, matching HLD §18/§26.2's frozen
    diagram exactly. Every external call goes through an injected client
    (constructor args here, not module-level singletons) so this phase's
    tests use fakes standing in for every one of them.
    """
    scope = RetrievalScope(request.scope)
    requested = list(dict.fromkeys(request.file_ids))  # distinct, order preserved
    counts = {"requested_count": len(requested)}
    base = {"trace_id": request.trace_id, "requested": len(requested)}

    def done(status: int, detail: str, **extra):
        # One structured line per query: why it ended the way it did. No query text, no content.
        logger.info("retrieval.query_result", status=RetrievalStatus.Name(status), detail=detail, **base, **extra)

    # 1. Scope check (HLD §16.3/§16.4) — SHORT-CIRCUIT #1
    if not scope_guard.check(scope, list(request.file_ids)):
        reason = "no_scope" if scope != RetrievalScope.FILES else "no_file_ids"
        metrics.query_short_circuit_total.labels(reason=reason).inc()
        done(RetrievalStatus.NO_SCOPE, reason)
        return response_mapper.empty_response(
            request.trace_id, RetrievalStatus.NO_SCOPE, "no file scope to search", **counts
        )

    # 2. Authorization (HLD §16.2) — SHORT-CIRCUIT #2. Fails closed, but the reason is reported:
    # "the permission check could not run" is not the same as "you may not read these files".
    permission = await file_service_client.check_permissions(user_id=request.user_id, document_ids=requested)
    authorized_file_ids = permission.authorized
    counts["authorized_count"] = len(authorized_file_ids)
    if permission.failed:
        metrics.query_short_circuit_total.labels(reason="permission_check_failed").inc()
        done(RetrievalStatus.UNAVAILABLE, "permission_check_failed", error=permission.error, authorized=0)
        return response_mapper.empty_response(
            request.trace_id,
            RetrievalStatus.UNAVAILABLE,
            "the document permission check could not be completed",
            **counts,
        )
    if not authorized_file_ids:
        metrics.query_short_circuit_total.labels(reason="no_authorized_ids").inc()
        done(RetrievalStatus.ACCESS_DENIED, "no_authorized_ids", authorized=0)
        return response_mapper.empty_response(
            request.trace_id,
            RetrievalStatus.ACCESS_DENIED,
            "none of the requested documents exist or are accessible to this user",
            **counts,
        )

    # 3a. Version resolution (HLD §13.3 — local Postgres lookup, not File Service), restricted to
    # versions whose ingestion actually completed. The current version is registered when the
    # upload event is consumed (before indexing), so "has a current version" alone does not mean
    # "has chunks".
    async with session_factory() as db:
        registered = await DocumentCurrentVersionRepository.get_many(db, authorized_file_ids)
        job_state = await IngestionJobRepository.statuses_for(db, registered) if registered else {}

    current_versions: dict[str, int] = {}
    indexing = failed = unregistered = 0
    for doc in authorized_file_ids:
        if doc not in registered:
            unregistered += 1
            continue
        status, _chunks = job_state.get(doc, (None, None))
        if status == IngestionStatus.READY.value:
            current_versions[doc] = registered[doc]
        elif status == IngestionStatus.FAILED.value:
            failed += 1
        else:
            indexing += 1
    counts["ready_count"] = len(current_versions)
    doc_versions = {d: v for d, v in current_versions.items()}

    if not current_versions:
        if failed:
            status, detail = RetrievalStatus.INDEXING_FAILED, "indexing of the document failed"
        elif indexing:
            status, detail = RetrievalStatus.INDEXING, "the document is still being indexed"
        else:
            status, detail = RetrievalStatus.NOT_INDEXED, "the document has not been indexed yet"
        metrics.query_short_circuit_total.labels(reason=RetrievalStatus.Name(status).lower()).inc()
        done(status, detail, authorized=len(authorized_file_ids), indexing=indexing, failed=failed, unregistered=unregistered)
        return response_mapper.empty_response(request.trace_id, status, detail, **counts)

    # Result size: the request's top_k (capped), else the configured default. `min_relevance_score` is deliberately NOT applied:
    # RRF scores are rank-derived (about 0.01-0.03), so a threshold on them means nothing. It will apply to the reranker score
    # (Phase 3) and is ignored until then.
    top_k = min(request.top_k, settings.RETRIEVAL_MAX_TOP_K) if request.top_k > 0 else settings.MMR_FINAL_TOP_K
    top_k = max(1, top_k)

    # 3b. Query embedding (HLD §12). The keyword leg needs no vector, so a failure here degrades to BM25-only instead of failing.
    query_vector = None
    try:
        query_vector = await embedding_client.embed_query(request.query)
    except Exception as exc:
        logger.warning("pipeline.query_embedding_failed", error=str(exc))
        if not settings.EMBEDDING_FAILURE_FALLBACK_BM25:
            done(RetrievalStatus.UNAVAILABLE, "query_embedding_failed", ready=len(current_versions))
            return response_mapper.empty_response(
                request.trace_id, RetrievalStatus.UNAVAILABLE, "the query could not be embedded", **counts
            )

    # 4. Dense + BM25 search, concurrently — degradation matrix (LLD §7.2)
    async def _no_vector():
        raise RuntimeError("no query vector: embedding failed")

    dense_result, bm25_result = await asyncio.gather(
        dense_search.search(milvus_repository, query_vector, settings.MILVUS_SEARCH_TOP_N, current_versions)
        if query_vector is not None
        else _no_vector(),
        bm25_search.search(opensearch_repository, request.query, settings.OPENSEARCH_SEARCH_TOP_N, current_versions),
        return_exceptions=True,
    )

    dense_failed = isinstance(dense_result, BaseException)
    bm25_failed = isinstance(bm25_result, BaseException)

    if dense_failed and bm25_failed:
        done(RetrievalStatus.UNAVAILABLE, "both_search_engines_failed", ready=len(current_versions))
        return response_mapper.empty_response(
            request.trace_id, RetrievalStatus.UNAVAILABLE, "the search engines are unavailable", **counts
        )

    dense_hits = [] if dense_failed else dense_result
    bm25_hits = [] if bm25_failed else bm25_result

    if dense_failed and not bm25_failed:
        # No dense leg (Milvus down, or the query could not be embedded): BM25-only candidates ranked by rrf_score (RRF degrades to
        # a single-list rank-score). MMR is skipped — no candidate embeddings are available. Near-duplicates are still dropped.
        metrics.search_degraded_total.labels(engine="bm25_only").inc()
        fused = fusion.reciprocal_rank_fusion([], bm25_hits, k=settings.FUSION_RRF_K)
        fused = fusion.boost_heading_matches(fused, request.query, settings.HEADING_MATCH_BOOST / (settings.FUSION_RRF_K + 1))
        fused = dedupe.drop_near_duplicates(fused, settings.DEDUPE_JACCARD_THRESHOLD)
        sorted_selected = _fit_tokens(sorted(fused[:top_k], key=lambda c: c.rrf_score, reverse=True), request.max_tokens)
        done(RetrievalStatus.OK if sorted_selected else RetrievalStatus.NO_MATCH, "bm25_only",
             chunks=len(sorted_selected), versions=doc_versions, embedding_failed=query_vector is None)
        return response_mapper.to_proto(sorted_selected, request.trace_id, **counts)

    if bm25_failed and not dense_failed:
        # OpenSearch fails, Milvus succeeds: dense-only candidates — still the full RRF/MMR path below.
        metrics.search_degraded_total.labels(engine="dense_only").inc()

    # Both succeeded, or OpenSearch-only-failed (same code path — the degraded row is just this one with the other list empty,
    # LLD §7.2's closing paragraph).
    fused = fusion.reciprocal_rank_fusion(dense_hits, bm25_hits, k=settings.FUSION_RRF_K)
    fused = fusion.boost_heading_matches(fused, request.query, settings.HEADING_MATCH_BOOST / (settings.FUSION_RRF_K + 1))
    fused = dedupe.drop_near_duplicates(fused, settings.DEDUPE_JACCARD_THRESHOLD)

    pool = fused[: settings.MMR_CANDIDATE_POOL_SIZE]
    embeddings = await candidate_embeddings.fetch(milvus_repository, [c.chunk_id for c in pool])
    mmr.normalise_relevance(pool)

    floor = settings.MMR_RELEVANCE_FLOOR
    reserved: list = []
    if settings.BALANCE_ACROSS_FILES and len(current_versions) >= 2:
        reserved = mmr.best_per_file(pool, floor, top_k)
    selected = mmr.select(pool, embeddings, k=top_k, lambda_=settings.MMR_LAMBDA, relevance_floor=floor, seed=reserved)
    sorted_selected = _fit_tokens(sorted(selected, key=lambda c: c.rrf_score, reverse=True), request.max_tokens)

    done(
        RetrievalStatus.OK if sorted_selected else RetrievalStatus.NO_MATCH,
        "ok" if sorted_selected else "no_relevant_chunks",
        chunks=len(sorted_selected),
        dense_hits=len(dense_hits),
        bm25_hits=len(bm25_hits),
        versions=doc_versions,
    )
    return response_mapper.to_proto(sorted_selected, request.trace_id, **counts)


def _fit_tokens(ranked: list, max_tokens: int) -> list:
    """Drops the lowest-ranked chunks until the total token count fits `max_tokens` (0 = no limit). `ranked` is best-first.
    If not even the best chunk fits, the result is empty."""
    if max_tokens <= 0:
        return ranked
    kept = list(ranked)
    while kept and sum(tokens.count(c.content) for c in kept) > max_tokens:
        kept.pop()
    return kept
