import asyncio
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.clients.embedding_client import EmbeddingClient
from app.core.config import settings
from app.core.logging import get_logger
from app.ingestion.orchestrator import run as run_orchestrator

logger = get_logger(__name__)

_STOP = object()


class BoundedIngestionPool:
    """LLD §6.1.1 — neither Kafka consumer calls `orchestrator.run()`
    directly from its own poll loop; an upload burst would otherwise let
    ingestion concurrency grow unbounded with Kafka lag, hammering
    Docling's CPU, embedding-service, Milvus, OpenSearch, and Postgres all
    at once with no backpressure.

    If the queue is full, `enqueue()` blocks — the intended backpressure:
    consumer-group lag grows visibly (an observable, alertable signal)
    instead of the process silently accumulating unbounded in-flight work.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker,
        embedding_client: EmbeddingClient,
        max_concurrency: int | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._embedding_client = embedding_client
        self._max_concurrency = max_concurrency or settings.INGESTION_MAX_CONCURRENCY
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=self._max_concurrency * 2)
        self._workers: list[asyncio.Task] = []

    async def start(self) -> None:
        self._workers = [asyncio.create_task(self._worker_loop()) for _ in range(self._max_concurrency)]

    async def stop(self) -> None:
        for _ in self._workers:
            await self._queue.put(_STOP)
        await asyncio.gather(*self._workers)
        self._workers = []

    async def enqueue(self, document_id: UUID, content_version: int) -> None:
        await self._queue.put((document_id, content_version))

    def qsize(self) -> int:
        return self._queue.qsize()

    async def _worker_loop(self) -> None:
        while True:
            item = await self._queue.get()
            if item is _STOP:
                self._queue.task_done()
                return
            document_id, content_version = item
            try:
                await run_orchestrator(self._session_factory, document_id, content_version, self._embedding_client)
            except Exception:
                logger.exception(
                    "ingestion_pool.orchestrator_failed", document_id=str(document_id), content_version=content_version
                )
            finally:
                self._queue.task_done()
