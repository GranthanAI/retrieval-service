import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.clients.file_service_client import FileServiceClient
from app.core import metrics
from app.core.config import settings
from app.core.enums import RetrievalScope
from app.core.logging import get_logger
from app.grpc.retrieval_pb2 import GetRelevantChunksRequest, GetRelevantChunksResponse
from app.query import bm25_search, candidate_embeddings, dense_search, fusion, mmr, response_mapper, scope_guard
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
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

    # 1. Scope check (HLD §16.3/§16.4) — SHORT-CIRCUIT #1
    if not scope_guard.check(scope, list(request.file_ids)):
        reason = "no_scope" if scope != RetrievalScope.FILES else "no_file_ids"
        metrics.query_short_circuit_total.labels(reason=reason).inc()
        return response_mapper.empty_response(request.trace_id)

    # 2. Authorization (HLD §16.2) — SHORT-CIRCUIT #2
    authorized_file_ids = await file_service_client.batch_check_permissions(
        user_id=request.user_id, document_ids=list(request.file_ids)
    )
    if not authorized_file_ids:
        metrics.query_short_circuit_total.labels(reason="no_authorized_ids").inc()
        return response_mapper.empty_response(request.trace_id)

    # 3a. Version resolution (HLD §13.3 — local Postgres lookup, not File Service)
    async with session_factory() as db:
        current_versions = await DocumentCurrentVersionRepository.get_many(db, authorized_file_ids)

    if not current_versions:
        # Every authorized id has no completed/known ingestion — nothing to
        # search. Short-circuited here (not left to fall through to two
        # empty store searches) so an embed_query call isn't wasted on a
        # request that structurally cannot produce results.
        return response_mapper.empty_response(request.trace_id)

    # 3b. Query embedding (HLD §12) — hard dependency for both search legs,
    # no partial-degrade path exists once this fails.
    try:
        query_vector = await embedding_client.embed_query(request.query)
    except Exception as exc:
        logger.warning("pipeline.query_embedding_failed", error=str(exc))
        return response_mapper.empty_response(request.trace_id)

    # 4. Dense + BM25 search, concurrently — degradation matrix (LLD §7.2)
    dense_result, bm25_result = await asyncio.gather(
        dense_search.search(milvus_repository, query_vector, settings.MILVUS_SEARCH_TOP_N, current_versions),
        bm25_search.search(opensearch_repository, request.query, settings.OPENSEARCH_SEARCH_TOP_N, current_versions),
        return_exceptions=True,
    )

    dense_failed = isinstance(dense_result, BaseException)
    bm25_failed = isinstance(bm25_result, BaseException)

    if dense_failed and bm25_failed:
        return response_mapper.empty_response(request.trace_id)

    dense_hits = [] if dense_failed else dense_result
    bm25_hits = [] if bm25_failed else bm25_result

    if dense_failed and not bm25_failed:
        # Milvus fails, OpenSearch succeeds: BM25-only candidates, ranked by
        # rrf_score (RRF degrades to a single-list rank-score, not true
        # fusion). MMR is SKIPPED — no candidate embeddings available,
        # since Milvus is both the dense search engine AND the only
        # candidate-embedding store (HLD §18.1). Top-K taken directly by
        # rank — fused is already rrf_score-descending, so the final sort
        # below is a no-op, matching LLD §7.2's own note.
        metrics.search_degraded_total.labels(engine="bm25_only").inc()
        fused = fusion.reciprocal_rank_fusion([], bm25_hits, k=settings.FUSION_RRF_K)
        selected = fused[: settings.MMR_FINAL_TOP_K]
        sorted_selected = sorted(selected, key=lambda c: c.rrf_score, reverse=True)
        return response_mapper.to_proto(sorted_selected, request.trace_id)

    if bm25_failed and not dense_failed:
        # OpenSearch fails, Milvus succeeds: dense-only candidates — still
        # the full RRF/MMR path below, since Milvus (and therefore
        # candidate embeddings) is available either way.
        metrics.search_degraded_total.labels(engine="dense_only").inc()

    # Both succeeded, or OpenSearch-only-failed (same code path — the
    # degraded row is just this one with the other list empty, LLD §7.2's
    # closing paragraph).
    fused = fusion.reciprocal_rank_fusion(dense_hits, bm25_hits, k=settings.FUSION_RRF_K)

    pool = fused[: settings.MMR_CANDIDATE_POOL_SIZE]
    embeddings = await candidate_embeddings.fetch(milvus_repository, [c.chunk_id for c in pool])

    selected = mmr.select(fused, embeddings, k=settings.MMR_FINAL_TOP_K, lambda_=settings.MMR_LAMBDA)
    sorted_selected = sorted(selected, key=lambda c: c.rrf_score, reverse=True)

    return response_mapper.to_proto(sorted_selected, request.trace_id)
