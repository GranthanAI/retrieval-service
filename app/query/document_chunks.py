"""GetDocumentChunks: the whole text of small documents, for callers that would rather send a document in full than guess which passages
matter. Same access rules as GetRelevantChunks: only documents the user may read, only their current READY version, and the same
reasons when there is nothing to return."""
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.file_service_client import FileServiceClient
from app.core import metrics
from app.core.config import settings
from app.core.enums import IngestionStatus
from app.core.logging import get_logger
from app.grpc.retrieval_pb2 import DocumentChunk, GetDocumentChunksRequest, GetDocumentChunksResponse, RetrievalStatus
from app.query import tokens
from app.query.response_mapper import _metadata
from app.query.scoring import FusedCandidate
from app.repositories.document_current_version_repository import DocumentCurrentVersionRepository
from app.repositories.ingestion_job_repository import IngestionJobRepository
from app.stores.opensearch_repository import OpenSearchRepository

logger = get_logger(__name__)


def _empty(request: GetDocumentChunksRequest, status: int, detail: str, **counts) -> GetDocumentChunksResponse:
    metrics.query_status_total.labels(status=RetrievalStatus.Name(status)).inc()
    return GetDocumentChunksResponse(request_id=request.trace_id, status=status, status_detail=detail, **counts)


async def run(
    request: GetDocumentChunksRequest,
    *,
    session_factory: async_sessionmaker,
    file_service_client: FileServiceClient,
    opensearch_repository: OpenSearchRepository,
) -> GetDocumentChunksResponse:
    requested = list(dict.fromkeys(request.file_ids))
    counts = {"requested_count": len(requested)}
    if not requested:
        return _empty(request, RetrievalStatus.NO_SCOPE, "no file scope to read", **counts)

    permission = await file_service_client.check_permissions(user_id=request.user_id, document_ids=requested)
    counts["authorized_count"] = len(permission.authorized)
    if permission.failed:
        return _empty(request, RetrievalStatus.UNAVAILABLE, "the document permission check could not be completed", **counts)
    if not permission.authorized:
        return _empty(request, RetrievalStatus.ACCESS_DENIED, "none of the requested documents exist or are accessible to this user", **counts)

    async with session_factory() as db:
        registered = await DocumentCurrentVersionRepository.get_many(db, permission.authorized)
        job_state = await IngestionJobRepository.statuses_for(db, registered) if registered else {}
    versions: dict[str, int] = {}
    indexing = failed = 0
    for doc in requested:                                             # request order = output order
        if doc not in permission.authorized or doc not in registered:
            continue
        status, _ = job_state.get(doc, (None, None))
        if status == IngestionStatus.READY.value:
            versions[doc] = registered[doc]
        elif status == IngestionStatus.FAILED.value:
            failed += 1
        else:
            indexing += 1
    counts["ready_count"] = len(versions)
    if not versions:
        status, detail = ((RetrievalStatus.INDEXING_FAILED, "indexing of the document failed") if failed else
                          (RetrievalStatus.INDEXING, "the document is still being indexed") if indexing else
                          (RetrievalStatus.NOT_INDEXED, "the document has not been indexed yet"))
        return _empty(request, status, detail, **counts)

    out: list[DocumentChunk] = []
    total = 0
    truncated = False
    budget = request.max_tokens if request.max_tokens > 0 else None
    for doc, version in versions.items():
        hits = await opensearch_repository.document_chunks(doc, version, settings.DOCUMENT_CHUNKS_MAX)
        for h in hits:
            cost = tokens.count(h.content)
            if budget is not None and total + cost > budget:
                truncated = True                                      # cut at a chunk boundary, never mid-chunk
                break
            total += cost
            c = FusedCandidate(chunk_id=h.chunk_id, document_id=h.document_id, filename=h.filename, content=h.content,
                               page_number=h.page_number, section=h.section, chunk_index=h.chunk_index, heading_path=h.heading_path,
                               page_start=h.page_start, page_end=h.page_end, block_type=h.block_type, location=h.location)
            out.append(DocumentChunk(chunk_id=c.chunk_id, content=c.content, source_file_id=c.document_id, source_name=c.filename,
                                     relevance_score=0.0, metadata=_metadata(c)))
        if truncated:
            break
    metrics.query_status_total.labels(status="OK").inc()
    logger.info("retrieval.document_chunks", trace_id=request.trace_id, files=len(versions), chunks=len(out), total_tokens=total, truncated=truncated)
    return GetDocumentChunksResponse(chunks=out, total_tokens=total, request_id=request.trace_id, status=RetrievalStatus.OK,
                                     truncated=truncated, **counts)
