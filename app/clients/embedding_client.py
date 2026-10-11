import grpc

from app.core import metrics
from app.core.config import settings
from app.core.exceptions import EmbeddingModelMismatchError
from app.core.logging import get_logger
from app.grpc import embedding_pb2, embedding_pb2_grpc

logger = get_logger(__name__)


def _assert_model_matches(model: str, dimension: int) -> None:
    """HLD §12 — embedding-service publishes its model identity in every
    response; assert it matches this service's own configured expectation
    and refuse to index/query on a mismatch, as a cheap runtime guard
    against a silent embedding-service model upgrade breaking the index.
    """
    if model != settings.EMBEDDING_MODEL_EXPECTED or dimension != settings.EMBEDDING_DIMENSION_EXPECTED:
        metrics.embedding_model_mismatch_total.inc()
        raise EmbeddingModelMismatchError(
            f"embedding-service returned model={model!r} dimension={dimension}, "
            f"expected model={settings.EMBEDDING_MODEL_EXPECTED!r} "
            f"dimension={settings.EMBEDDING_DIMENSION_EXPECTED}"
        )


class EmbeddingClient:
    """LLD §8.1 — both embedding call shapes (single for query, batch for
    ingestion) against embedding-service's gRPC API (HLD §12): the same
    external service both paths call is what actually guarantees
    index/query embedding consistency, not an in-process model instance.
    """

    def __init__(self, target: str | None = None) -> None:
        self._target = target or settings.EMBEDDING_SERVICE_GRPC_TARGET
        self._channel: grpc.aio.Channel | None = None

    def _get_channel(self) -> grpc.aio.Channel:
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(self._target)
        return self._channel

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    async def embed_query(self, text: str) -> list[float]:
        """Single call, ON the query path — EMBEDDING_QUERY_TIMEOUT_SECONDS,
        retried up to EMBEDDING_RETRY_ATTEMPTS times (HLD §12/§17). Raises on
        final failure or a model mismatch — the query pipeline's own
        degrade-to-empty-response policy (LLD §7.2 step 3b) lives in the
        caller, not here.
        """
        stub = embedding_pb2_grpc.EmbeddingServiceStub(self._get_channel())
        request = embedding_pb2.EmbeddingRequest(text=text)

        last_error: Exception | None = None
        for attempt in range(settings.EMBEDDING_RETRY_ATTEMPTS + 1):
            try:
                response = await stub.GenerateEmbedding(request, timeout=settings.EMBEDDING_QUERY_TIMEOUT_SECONDS)
                _assert_model_matches(response.model, response.dimension)
                return list(response.embedding)
            except EmbeddingModelMismatchError:
                raise
            except grpc.aio.AioRpcError as exc:
                last_error = exc
                logger.warning("embedding_client.embed_query.retry", attempt=attempt, error=str(exc))
        assert last_error is not None
        raise last_error

    async def embed_chunks(self, texts: list[str]) -> list[list[float]]:
        """Batched per EMBEDDING_BATCH_SIZE, off the query path —
        EMBEDDING_INGESTION_TIMEOUT_SECONDS per batch call, generous since
        ingestion has no caller-imposed deadline (HLD §17's SLA is
        query-path only). Returns one vector per input text, same order.
        """
        if not texts:
            return []

        stub = embedding_pb2_grpc.EmbeddingServiceStub(self._get_channel())
        vectors: list[list[float]] = []

        for start in range(0, len(texts), settings.EMBEDDING_BATCH_SIZE):
            batch = texts[start : start + settings.EMBEDDING_BATCH_SIZE]
            request = embedding_pb2.BatchEmbeddingRequest(texts=batch)

            last_error: Exception | None = None
            for attempt in range(settings.EMBEDDING_RETRY_ATTEMPTS + 1):
                try:
                    response = await stub.GenerateEmbeddings(
                        request, timeout=settings.EMBEDDING_INGESTION_TIMEOUT_SECONDS
                    )
                    _assert_model_matches(response.model, response.dimension)
                    vectors.extend(list(v.values) for v in response.embeddings)
                    break
                except EmbeddingModelMismatchError:
                    raise
                except grpc.aio.AioRpcError as exc:
                    last_error = exc
                    logger.warning("embedding_client.embed_chunks.retry", attempt=attempt, error=str(exc))
            else:
                assert last_error is not None
                raise last_error

        return vectors
