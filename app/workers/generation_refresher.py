import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import settings
from app.core.logging import get_logger
from app.repositories import generation_repository
from app.stores import generations

logger = get_logger(__name__)


async def refresh_once(session_factory: async_sessionmaker) -> None:
    """Loads the generation registry into the in-process cache. A failure (database down, migration not applied) leaves the previous
    cache, or the legacy generation 1 on a fresh process, in effect: retrieval keeps working on the last known generation."""
    try:
        infos = await generation_repository.refresh(session_factory)
        logger.info("generations.refreshed", active=generations.current().generation, registered=[g.generation for g in infos])
    except Exception as exc:  # noqa: BLE001
        logger.warning("generations.refresh_failed", error=str(exc), in_effect=generations.current().generation)


async def run_generation_refresher(session_factory: async_sessionmaker, stop_event: asyncio.Event) -> None:
    """Picks up an activation made by `make activate` in another process within GENERATION_REFRESH_SECONDS."""
    while not stop_event.is_set():
        await refresh_once(session_factory)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.GENERATION_REFRESH_SECONDS)
        except asyncio.TimeoutError:
            pass
