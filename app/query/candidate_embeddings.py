from app.stores.milvus_repository import MilvusRepository


async def fetch(milvus_repository: MilvusRepository, chunk_ids: list[str]) -> dict[str, list[float]]:
    """HLD §18.1 — a Milvus primary-key `get` by the candidate chunk_ids
    already in the fused set, NOT a fresh ANN search and NOT a call back to
    embedding-service — every chunk_id here is already known to exist in
    Milvus, since dense search is one of the two sources that produced the
    fused candidate set in the first place.
    """
    if not chunk_ids:
        return {}
    return await milvus_repository.get_by_chunk_ids(chunk_ids)
