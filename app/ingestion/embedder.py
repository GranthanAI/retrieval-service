from app.clients.embedding_client import EmbeddingClient
from app.ingestion.chunker import Chunk


async def embed_batch(client: EmbeddingClient, chunks: list[Chunk]) -> list[list[float]]:
    """Thin wrapper over `embedding_client.embed_chunks` — batches the
    chunked output through embedding-service's `GenerateEmbeddings` (HLD
    §12), returning one vector per chunk, same order as `chunks`.
    """
    return await client.embed_chunks([c.content for c in chunks])
