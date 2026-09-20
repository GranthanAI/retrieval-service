from app.stores.milvus_repository import MilvusRepository
from app.stores.types import DenseSearchHit


async def search(
    milvus_repository: MilvusRepository, query_vector: list[float], top_n: int, current_versions: dict[str, int]
) -> list[DenseSearchHit]:
    """Thin wrapper over `milvus_repository.search` (HLD §13.3) — the seam
    `pipeline.py` depends on, kept independent of `MilvusRepository`'s own
    interface so the pipeline's tests inject a fake here without touching
    the store layer at all.
    """
    return await milvus_repository.search(query_vector=query_vector, top_n=top_n, current_versions=current_versions)
