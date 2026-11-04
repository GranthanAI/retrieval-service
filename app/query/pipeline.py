import asyncio
import hashlib
import time

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.clients.file_service_client import FileServiceClient
from app.core import metrics
from app.core.config import settings
from app.core.enums import RetrievalScope
from app.core.logging import get_logger
from app.core.enums import IngestionStatus
from app.grpc.retrieval_pb2 import Coverage, GetRelevantChunksRequest, GetRelevantChunksResponse, RetrievalStatus
from app.query import (
    bm25_search, candidate_embeddings, coverage, dedupe, dense_search, fusion, mmr, response_mapper, scope_guard, tokens)
from app.query.scoring import FusedCandidate
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

    # Result size: the request's top_k (capped), else the configured default.
    top_k = min(request.top_k, settings.RETRIEVAL_MAX_TOP_K) if request.top_k > 0 else settings.MMR_FINAL_TOP_K
    top_k = max(1, top_k)
    clock = _Clock()
    reranking = settings.RERANK_ENABLED and hasattr(embedding_client, "rerank")
    dense_n = max(settings.MILVUS_SEARCH_TOP_N, settings.RERANK_LEG_TOP_N) if reranking else settings.MILVUS_SEARCH_TOP_N
    bm25_n = max(settings.OPENSEARCH_SEARCH_TOP_N, settings.RERANK_LEG_TOP_N) if reranking else settings.OPENSEARCH_SEARCH_TOP_N

    # 3b. Query embedding (HLD §12). The dense leg embeds `dense_query` (a standalone rewrite, e.g. in English) when the caller sent
    # one; the keyword leg always uses `query`. The keyword leg needs no vector, so an embedding failure degrades to BM25-only.
    dense_text = request.dense_query if (request.HasField("dense_query") and request.dense_query.strip()) else request.query
    query_vector = None
    try:
        query_vector = await embedding_client.embed_query(dense_text)
    except Exception as exc:
        logger.warning("pipeline.query_embedding_failed", error=str(exc))
        if not settings.EMBEDDING_FAILURE_FALLBACK_BM25:
            done(RetrievalStatus.UNAVAILABLE, "query_embedding_failed", ready=len(current_versions))
            return response_mapper.empty_response(
                request.trace_id, RetrievalStatus.UNAVAILABLE, "the query could not be embedded", **counts
            )
    clock.lap("embed")

    # 4. Dense + BM25 search (+ the previous turn's chunks), concurrently — degradation matrix (LLD §7.2)
    async def _no_vector():
        raise RuntimeError("no query vector: embedding failed")

    dense_result, bm25_result, previous_result = await asyncio.gather(
        dense_search.search(milvus_repository, query_vector, dense_n, current_versions) if query_vector is not None else _no_vector(),
        bm25_search.search(opensearch_repository, request.query, bm25_n, current_versions),
        _previous_candidates(request, milvus_repository, current_versions),
        return_exceptions=True,
    )
    clock.lap("search")

    dense_failed = isinstance(dense_result, BaseException)
    bm25_failed = isinstance(bm25_result, BaseException)

    if dense_failed and bm25_failed:
        done(RetrievalStatus.UNAVAILABLE, "both_search_engines_failed", ready=len(current_versions))
        return response_mapper.empty_response(
            request.trace_id, RetrievalStatus.UNAVAILABLE, "the search engines are unavailable", **counts
        )

    dense_hits = [] if dense_failed else dense_result
    bm25_hits = [] if bm25_failed else bm25_result
    previous_hits = [] if isinstance(previous_result, BaseException) else previous_result

    fused = fusion.reciprocal_rank_fusion(dense_hits, bm25_hits, k=settings.FUSION_RRF_K, previous_hits=previous_hits,
                                          previous_weight=settings.PREVIOUS_CHUNKS_WEIGHT)
    fused = fusion.boost_heading_matches(fused, request.query, settings.HEADING_MATCH_BOOST / (settings.FUSION_RRF_K + 1))
    fused = dedupe.drop_near_duplicates(fused, settings.DEDUPE_JACCARD_THRESHOLD)

    # Cross-encoder rerank of the best RERANK_TOP_N fused candidates. On timeout/error: the Phase 1 path, with a metric.
    reranked = False
    if reranking:
        top = fused[: settings.RERANK_TOP_N]
        reranked = await _rerank(embedding_client, request.query, top)
        if reranked:
            fused = sorted(top, key=lambda c: c.rerank_score, reverse=True)
    clock.lap("rerank")

    if dense_failed and not bm25_failed:
        # No dense leg (Milvus down, or the query could not be embedded): keyword candidates only. MMR is skipped — no candidate
        # embeddings are available. Ranked by reranker score when it ran, else by RRF.
        metrics.search_degraded_total.labels(engine="bm25_only").inc()
        selected = fused[:top_k]
        mode = "bm25_only"
    else:
        if bm25_failed:
            metrics.search_degraded_total.labels(engine="dense_only").inc()
        pool = fused[: settings.RERANK_TOP_N if reranked else settings.MMR_CANDIDATE_POOL_SIZE]
        embeddings = await candidate_embeddings.fetch(milvus_repository, [c.chunk_id for c in pool])
        mmr.normalise_relevance(pool)
        floor = settings.MMR_RELEVANCE_FLOOR
        reserved: list = []
        if settings.BALANCE_ACROSS_FILES and len(current_versions) >= 2:
            reserved = mmr.best_per_file(pool, floor, top_k)
        selected = mmr.select(pool, embeddings, k=top_k, lambda_=settings.MMR_LAMBDA, relevance_floor=floor, seed=reserved)
        mode = "ok"
    clock.lap("select")

    def _order(c) -> float:
        return c.rerank_score if reranked and c.rerank_score is not None else c.rrf_score

    # `min_relevance_score` applies to the reranker score only (RRF scores are rank-derived and mean nothing as a threshold).
    if reranked and request.min_relevance_score > 0:
        selected = [c for c in selected if c.rerank_score is not None and c.rerank_score >= request.min_relevance_score]
    top_score, coverage_value = coverage.signal(selected, reranked)
    ranked = sorted(selected, key=_order, reverse=True)
    if settings.EXPAND_NEIGHBOURS and ranked:
        ranked = await _expand_neighbours(ranked, opensearch_repository, current_versions, request.max_tokens)
    sorted_selected = _fit_tokens(ranked, request.max_tokens)
    clock.lap("finish")

    status = RetrievalStatus.OK if sorted_selected else RetrievalStatus.NO_MATCH
    done(status, mode if mode != "ok" else ("ok" if sorted_selected else "no_relevant_chunks"), chunks=len(sorted_selected),
         dense_hits=len(dense_hits), bm25_hits=len(bm25_hits), versions=doc_versions, reranked=reranked)
    _feedback(request, sorted_selected, clock, coverage_value, top_score, reranked, bool(previous_hits))
    return response_mapper.to_proto(sorted_selected, request.trace_id, **counts, top_score=top_score, coverage=coverage_value,
                                    score_of=_order)


class _Clock:
    """Per-stage latency in ms for the feedback log line."""

    def __init__(self) -> None:
        self._t = time.perf_counter()
        self.ms: dict[str, float] = {}

    def lap(self, name: str) -> None:
        now = time.perf_counter()
        self.ms[name] = round((now - self._t) * 1000, 1)
        self._t = now


def _feedback(request, selected, clock: _Clock, coverage_value: int, top_score, reranked: bool, previous_used: bool) -> None:
    """One structured line per retrieval, for turning bad answers into benchmark cases (docs/retrieval-quality.md). Never logs document
    content; the query text only when LOG_QUERY_TEXT is on, otherwise a hash."""
    fields = {
        "trace_id": request.trace_id,
        "query_hash": hashlib.sha256(request.query.encode("utf-8")).hexdigest()[:16],
        "rewrite_used": request.HasField("dense_query") and bool(request.dense_query.strip()),
        "previous_chunks_used": previous_used,
        "reranked": reranked,
        "chunk_ids": [c.chunk_id for c in selected],
        "scores": [round(c.rerank_score if reranked and c.rerank_score is not None else c.rrf_score, 4) for c in selected],
        "coverage": Coverage.Name(coverage_value),
        "top_score": None if top_score is None else round(top_score, 4),
        "stage_ms": clock.ms,
    }
    if settings.LOG_QUERY_TEXT:
        fields["query"] = request.query
        if request.HasField("dense_query"):
            fields["dense_query"] = request.dense_query
    logger.info("retrieval.feedback", **fields)


async def _previous_candidates(request, milvus_repository, current_versions: dict[str, int]) -> list:
    """The previous turn's chunks as extra candidates, after checking that each belongs to a document the user may read (it is in
    `current_versions`, which is already authorised and READY) and that its content_version is still the current one."""
    if not settings.PREVIOUS_CHUNKS_ENABLED or not request.previous_chunk_ids:
        return []
    ids = list(dict.fromkeys(request.previous_chunk_ids))[: settings.PREVIOUS_CHUNKS_MAX]
    rows = await milvus_repository.get_chunks_by_ids(ids)
    return [hit for hit, version in rows if current_versions.get(hit.document_id) == version]


async def _rerank(embedding_client, query: str, candidates: list) -> bool:
    """Scores `candidates` in place (rerank_score = the reranker's own score, used as is: only its order and the per-model thresholds in the
    settings matter). False (and a metric) if it timed out or failed."""
    if not candidates:
        return False
    try:
        scores = await asyncio.wait_for(
            embedding_client.rerank(query, [c.content for c in candidates], settings.RERANK_TIMEOUT_SECONDS),
            timeout=settings.RERANK_TIMEOUT_SECONDS + 0.2)
    except Exception as exc:  # noqa: BLE001 - timeout, UNIMPLEMENTED, connection: all fall back to the Phase 1 path
        metrics.rerank_fallback_total.labels(reason=type(exc).__name__).inc()
        logger.warning("pipeline.rerank_failed", error=type(exc).__name__)
        return False
    for c, score in zip(candidates, scores):
        c.rerank_score = score
    return True


async def _expand_neighbours(ranked: list, opensearch_repository, current_versions: dict[str, int], max_tokens: int) -> list:
    """Adds the previous and next chunk of the same section after each selected chunk while `max_tokens` allows (never whole documents).
    Neighbours are marked so they sort next to their chunk; a failure to fetch them just returns the original list."""
    budget = max_tokens if max_tokens > 0 else 10**9
    used = sum(tokens.count(c.content) for c in ranked)
    have = {c.chunk_id for c in ranked}
    out: list = []
    try:
        for c in ranked:
            out.append(c)
            version = current_versions.get(c.document_id)
            if version is None:
                continue
            for neighbour in await opensearch_repository.neighbours(c.document_id, version, c.chunk_index, c.section):
                cost = tokens.count(neighbour.content)
                if neighbour.chunk_id in have or used + cost > budget:
                    continue
                have.add(neighbour.chunk_id)
                used += cost
                out.append(FusedCandidate(
                    chunk_id=neighbour.chunk_id, document_id=neighbour.document_id, filename=neighbour.filename, content=neighbour.content,
                    page_number=neighbour.page_number, section=neighbour.section, chunk_index=neighbour.chunk_index,
                    heading_path=neighbour.heading_path, page_start=neighbour.page_start, page_end=neighbour.page_end,
                    block_type=neighbour.block_type, location=neighbour.location, rrf_score=c.rrf_score * 0.5,
                    rerank_score=None if c.rerank_score is None else c.rerank_score * 0.5))
    except Exception as exc:  # noqa: BLE001
        logger.warning("pipeline.neighbour_expansion_failed", error=type(exc).__name__)
        return ranked
    return out


def _fit_tokens(ranked: list, max_tokens: int) -> list:
    """Drops the lowest-ranked chunks until the total token count fits `max_tokens` (0 = no limit). `ranked` is best-first.
    If not even the best chunk fits, the result is empty."""
    if max_tokens <= 0:
        return ranked
    kept = list(ranked)
    while kept and sum(tokens.count(c.content) for c in kept) > max_tokens:
        kept.pop()
    return kept
