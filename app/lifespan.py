import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.clients.embedding_client import EmbeddingClient
from app.core.config import settings
from app.core.logging import get_logger
from app.db.postgres import dispose_engine, get_engine
from app.db.session import get_session_factory
from app.events.consumers.file_deleted_consumer import run_file_deleted_consumer
from app.events.consumers.file_processing_retry_consumer import run_file_processing_retry_consumer
from app.events.consumers.file_uploaded_consumer import run_file_uploaded_consumer
from app.events.ingestion_pool import BoundedIngestionPool
from app.events.producers.processing_status_producer import start_producer, stop_producer
from app.workers.outbox_worker import run_outbox_worker

logger = get_logger(__name__)


async def _startup_api(app: FastAPI) -> None:
    """DEPLOYMENT_MODE=api branch (LLD §9): DB pool, gRPC server. The gRPC
    server itself lands in Phase 9 — the DB engine (needed for the
    permission-check/version-resolution reads the query pipeline will do)
    is real from here on.
    """
    app.state.db_engine = get_engine()
    logger.info("lifespan.api.startup")


async def _startup_worker(app: FastAPI) -> None:
    """DEPLOYMENT_MODE=worker branch (LLD §9): DB pool, Kafka consumers,
    bounded ingestion pool (LLD §6.1.1), outbox worker (LLD §3.3). The
    reconciliation sweep lands in Phase 6.
    """
    app.state.db_engine = get_engine()
    session_factory = get_session_factory()

    embedding_client = EmbeddingClient()
    app.state.embedding_client = embedding_client

    pool = BoundedIngestionPool(session_factory, embedding_client)
    await pool.start()
    app.state.ingestion_pool = pool

    producer = await start_producer()
    app.state.kafka_producer = producer

    stop_events: dict[str, asyncio.Event] = {}
    tasks: dict[str, asyncio.Task] = {}

    def _spawn(name: str, coro) -> None:
        stop_event = asyncio.Event()
        stop_events[name] = stop_event
        tasks[name] = asyncio.create_task(coro(stop_event))

    _spawn("file_uploaded_consumer", lambda se: run_file_uploaded_consumer(session_factory, pool, se))
    _spawn(
        "file_processing_retry_consumer", lambda se: run_file_processing_retry_consumer(session_factory, pool, se)
    )
    _spawn("file_deleted_consumer", lambda se: run_file_deleted_consumer(session_factory, se))
    _spawn("outbox_worker", lambda se: run_outbox_worker(session_factory, producer, se))

    app.state.worker_stop_events = stop_events
    app.state.worker_tasks = tasks

    logger.info("lifespan.worker.startup")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup/shutdown wiring for the Retrieval Service, branched on
    DEPLOYMENT_MODE (LLD §9). The FastAPI health app itself runs in every
    mode; only the mode-specific branch below differs. Never both a gRPC
    server and Kafka consumers in the same real deployment (api != worker),
    though DEPLOYMENT_MODE=all runs both for local/dev convenience.
    """
    logger.info("retrieval_service.startup.begin", deployment_mode=settings.DEPLOYMENT_MODE)

    if settings.DEPLOYMENT_MODE in ("api", "all"):
        await _startup_api(app)
    if settings.DEPLOYMENT_MODE in ("worker", "all"):
        await _startup_worker(app)

    logger.info("retrieval_service.startup.complete")
    try:
        yield
    finally:
        logger.info("retrieval_service.shutdown.begin")
        if settings.DEPLOYMENT_MODE in ("worker", "all"):
            for stop_event in app.state.worker_stop_events.values():
                stop_event.set()
            await asyncio.gather(*app.state.worker_tasks.values())
            await app.state.ingestion_pool.stop()
            await app.state.embedding_client.close()
            await stop_producer()
        await dispose_engine()
        logger.info("retrieval_service.shutdown.complete")
