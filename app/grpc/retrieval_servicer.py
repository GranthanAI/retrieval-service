import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.clients.file_service_client import FileServiceClient
from app.core import metrics
from app.core.config import settings
from app.core.logging import get_logger
from app.grpc import retrieval_pb2_grpc
from app.grpc.retrieval_pb2 import GetRelevantChunksRequest, GetRelevantChunksResponse
from app.query import pipeline, response_mapper
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository

logger = get_logger(__name__)


class RetrievalServicer(retrieval_pb2_grpc.RetrievalServiceServicer):
    """HLD §6/§18 — implements `GetRelevantChunks`. The servicer itself
    never raises a gRPC error back to LLM Service for a pipeline-internal
    failure (HLD §17's "never let an internal failure block the whole
    request") — this is the single outermost guard, on top of the narrower
    fallbacks already inside `pipeline.run()` (LLD §7.2).

    Every real client (Phase 3/7/2's `embedding_client`/`file_service_client`/
    `milvus_repository`/`opensearch_repository`) is constructed once in
    `lifespan.py` and passed in here — the servicer holds no globals of its
    own, matching `pipeline.run()`'s own injected-dependency shape from
    Phase 8, just with real objects instead of fakes.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker,
        file_service_client: FileServiceClient,
        embedding_client: EmbeddingClient,
        milvus_repository: MilvusRepository,
        opensearch_repository: OpenSearchRepository,
    ) -> None:
        self._session_factory = session_factory
        self._file_service_client = file_service_client
        self._embedding_client = embedding_client
        self._milvus_repository = milvus_repository
        self._opensearch_repository = opensearch_repository

    async def GetRelevantChunks(self, request: GetRelevantChunksRequest, context) -> GetRelevantChunksResponse:
        with metrics.query_latency_histogram.time():
            try:
                return await asyncio.wait_for(
                    pipeline.run(
                        request,
                        session_factory=self._session_factory,
                        file_service_client=self._file_service_client,
                        embedding_client=self._embedding_client,
                        milvus_repository=self._milvus_repository,
                        opensearch_repository=self._opensearch_repository,
                    ),
                    timeout=settings.QUERY_PIPELINE_DEADLINE_SECONDS,
                )
            except TimeoutError:
                # asyncio.TimeoutError is builtins.TimeoutError as of Python
                # 3.11 — LLD §7.1 degrades, never raises, on a deadline miss.
                metrics.query_timeout_counter.inc()
                logger.warning("retrieval_servicer.deadline_exceeded", trace_id=request.trace_id)
                return response_mapper.empty_response(request.trace_id)
