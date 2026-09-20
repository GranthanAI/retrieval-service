from app.stores.opensearch_repository import OpenSearchRepository
from app.stores.types import BM25SearchHit


async def search(
    opensearch_repository: OpenSearchRepository, query_text: str, top_n: int, current_versions: dict[str, int]
) -> list[BM25SearchHit]:
    """Thin wrapper over `opensearch_repository.search` (HLD §13.3) — same
    rationale as `dense_search.search`."""
    return await opensearch_repository.search(query_text=query_text, top_n=top_n, current_versions=current_versions)
