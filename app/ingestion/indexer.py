import asyncio
from dataclasses import dataclass
from enum import Enum

from app.core.enums import StageStatus
from app.models.chunk_record import ChunkRecord
from app.stores.milvus_repository import MilvusRepository
from app.stores.opensearch_repository import OpenSearchRepository


class IndexTarget(str, Enum):
    """LLD §6.2/§6.3 — which store(s) a `write()` call should touch. A
    `PARTIALLY_INDEXED` retry (LLD §6.3) targets only the previously-failed
    store, never re-writing the one that already succeeded.
    """

    MILVUS = "MILVUS"
    OPENSEARCH = "OPENSEARCH"
    BOTH = "BOTH"


@dataclass
class IndexResult:
    """Per-target outcome of one `write()` call — the orchestrator (LLD
    §6.2) uses this to set `milvus_status`/`opensearch_status` independently
    and then call `ingestion_job_repository.recompute_status`.
    """

    milvus_status: str | None = None
    opensearch_status: str | None = None


async def write(records: list[ChunkRecord], target: IndexTarget) -> IndexResult:
    """Writes to Milvus and/or OpenSearch CONCURRENTLY via `asyncio.gather`
    when `target=BOTH` (LLD §6.2, matching the query-side concurrency
    pattern, HLD §18) — never sequentially, so one slow store doesn't delay
    the other's write. Each target's failure is caught independently: one
    store raising must not prevent the other's status from being recorded,
    since that's exactly what makes `PARTIALLY_INDEXED` (HLD §19/§20)
    representable.
    """
    result = IndexResult()

    async def _write_milvus() -> None:
        try:
            await MilvusRepository().upsert_chunks(records)
            result.milvus_status = StageStatus.COMPLETE.value
        except Exception:
            result.milvus_status = StageStatus.FAILED.value

    async def _write_opensearch() -> None:
        try:
            await OpenSearchRepository().upsert_chunks(records)
            result.opensearch_status = StageStatus.COMPLETE.value
        except Exception:
            result.opensearch_status = StageStatus.FAILED.value

    tasks = []
    if target in (IndexTarget.MILVUS, IndexTarget.BOTH):
        tasks.append(_write_milvus())
    if target in (IndexTarget.OPENSEARCH, IndexTarget.BOTH):
        tasks.append(_write_opensearch())

    await asyncio.gather(*tasks)
    return result
