import asyncio

from fastapi import APIRouter, Response, status
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api import dependency_checks
from app.core.config import settings

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    """Process-alive check only — no dependency calls, so a slow dependency
    can never trigger a restart loop that fixes nothing."""
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness(response: Response) -> dict[str, object]:
    """Mode-aware (LLD §9): 200 only if every dependency this deployment's
    own workload needs responds within HEALTH_CHECK_TIMEOUT_SECONDS."""
    names = dependency_checks.checks_for_mode(settings.DEPLOYMENT_MODE)
    outcomes = await asyncio.gather(*(dependency_checks.CHECKS[name]() for name in names))
    checks = dict(zip(names, outcomes))

    if not all(checks.values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready", "checks": checks}
    return {"status": "ready", "checks": checks}


@router.get("/metrics")
async def metrics() -> PlainTextResponse:
    """Prometheus scrape endpoint for app/core/metrics.py (HLD §24)."""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
