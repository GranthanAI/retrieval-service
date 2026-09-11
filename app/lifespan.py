from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


async def _startup_api() -> None:
    """DEPLOYMENT_MODE=api branch (LLD §9): DB pool, gRPC server.

    Stub at Phase 0 — no real clients/servers constructed yet. Phase 9 wires
    the real gRPC server; Phase 1 wires the DB pool.
    """
    logger.info("lifespan.api.startup")


async def _startup_worker() -> None:
    """DEPLOYMENT_MODE=worker branch (LLD §9): DB pool, Milvus/OpenSearch/
    embedding/File-Service clients, Kafka consumers, bounded ingestion pool,
    outbox_worker, reconciliation sweep.

    Stub at Phase 0 — real wiring lands in Phases 1-6.
    """
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
        await _startup_api()
    if settings.DEPLOYMENT_MODE in ("worker", "all"):
        await _startup_worker()

    logger.info("retrieval_service.startup.complete")
    try:
        yield
    finally:
        logger.info("retrieval_service.shutdown.begin")
        logger.info("retrieval_service.shutdown.complete")
