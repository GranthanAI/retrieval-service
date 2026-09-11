from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    """Process-alive check only — no dependency calls.

    Dependency checks (Postgres/Milvus/OpenSearch/Kafka/MinIO, mode-aware)
    land on GET /health/ready in Phase 11 once those clients actually exist.
    """
    return {"status": "alive"}
