import asyncio
from collections.abc import Awaitable, Callable

from sqlalchemy import text

from app.core.config import settings
from app.core.logging import get_logger
from app.db.session import get_session_factory
from app.events.producers.processing_status_producer import get_producer
from app.ingestion import downloader
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository

logger = get_logger(__name__)


async def _within_timeout(name: str, awaitable: Awaitable[object]) -> bool:
    try:
        await asyncio.wait_for(awaitable, timeout=settings.HEALTH_CHECK_TIMEOUT_SECONDS)
        return True
    except Exception as exc:
        logger.warning("health.ready.check_failed", dependency=name, error=str(exc))
        return False


async def check_postgres() -> bool:
    async def _ping() -> None:
        async with get_session_factory()() as db:
            await db.execute(text("SELECT 1"))

    return await _within_timeout("postgres", _ping())


async def check_milvus() -> bool:
    return await _within_timeout("milvus", MilvusRepository().ping())


async def check_opensearch() -> bool:
    return await _within_timeout("opensearch", OpenSearchRepository().ping())


async def check_minio() -> bool:
    return await _within_timeout("minio", downloader.ping())


async def check_kafka() -> bool:
    async def _ping() -> None:
        await get_producer().client.fetch_all_metadata()

    return await _within_timeout("kafka", _ping())


CHECKS: dict[str, Callable[[], Awaitable[bool]]] = {
    "postgres": check_postgres,
    "milvus": check_milvus,
    "opensearch": check_opensearch,
    "minio": check_minio,
    "kafka": check_kafka,
}

# LLD §9 / Implementation Plan Phase 11: readiness is mode-aware — each
# deployment is only "not ready" for dependencies its own workload actually
# uses. A Kafka outage must not take retrieval-api pods NotReady (they never
# touch Kafka), nor a Milvus outage the workers' (they never query it).
_API_CHECKS = ("postgres", "milvus", "opensearch")
_WORKER_CHECKS = ("postgres", "minio", "kafka")


def checks_for_mode(mode: str) -> list[str]:
    if mode == "api":
        return list(_API_CHECKS)
    if mode == "worker":
        return list(_WORKER_CHECKS)
    return list(dict.fromkeys(_API_CHECKS + _WORKER_CHECKS))  # "all": union, order-preserving
