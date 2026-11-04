"""The single code path from a parsed document to the records that are written to both stores, for ANY index generation.

    parsed document --chunk_parsed--> chunks --(embed chunk.content)--> vectors --make_records--> ChunkRecords

`chunk.content` is the one string that is embedded AND indexed (for chunker v2: heading path + body), so the vector and the keyword
index can never describe different text. The orchestrator (live ingestion), the reindex tool (`scripts/generation.py`) and the
benchmark all go through here."""
from dataclasses import dataclass

from app.clients.embedding_client import EmbeddingClient
from app.core.logging import get_logger
from app.ingestion import chunk_identity, chunker, chunker_v2
from app.ingestion.chunker import Chunk
from app.ingestion.parsed import ParsedDocument
from app.ingestion.tokenization import ModelProfile, TokenCounter, load_counter, validate_profile
from app.models.chunk_record import ChunkRecord
from app.stores.generations import GenerationInfo

logger = get_logger(__name__)


@dataclass
class Runtime:
    """Process-wide facts about the embedding model that chunker v2 needs. Filled once at worker start by `ensure_model_profile`."""

    profile: ModelProfile | None = None
    counter: TokenCounter | None = None


runtime = Runtime()


def set_runtime(profile: ModelProfile, counter: TokenCounter) -> None:
    validate_profile(profile)
    runtime.profile, runtime.counter = profile, counter


async def ensure_model_profile(client: EmbeddingClient) -> ModelProfile:
    """Asks embedding-service for its model (GetModelInfo), validates the name and dimension against the configured expectation and
    loads that model's tokenizer locally. Raises EmbeddingModelMismatchError on a mismatch."""
    profile = await client.get_model_info()
    validate_profile(profile)
    runtime.profile, runtime.counter = profile, load_counter(profile.tokenizer_id)
    logger.info("build.model_profile", model=profile.model, max_seq_length=profile.max_seq_length, counter=runtime.counter.name)
    return profile


def chunk_parsed(parsed: ParsedDocument, filename: str, info: GenerationInfo) -> list[Chunk]:
    if info.chunker_version == "1":
        return chunker.chunk(parsed)
    if runtime.profile is None or runtime.counter is None:
        raise RuntimeError(
            f"generation {info.generation} uses chunker v{info.chunker_version}, which needs the embedding model profile; "
            "call build.ensure_model_profile() at startup"
        )
    return chunker_v2.chunk_v2(parsed, filename, runtime.counter, runtime.profile.max_seq_length)


def make_records(
    chunks: list[Chunk], vectors: list[list[float]], *, document_id: str, content_version: int, owner_id: str,
    conversation_id: str | None, filename: str, info: GenerationInfo,
) -> list[ChunkRecord]:
    return [
        ChunkRecord(
            chunk_id=chunk_identity.derive_chunk_id(document_id, content_version, c.chunk_index),
            document_id=document_id, content_version=content_version, chunk_index=c.chunk_index, owner_id=owner_id,
            conversation_id=conversation_id, filename=filename, page_number=c.page_number, section=c.section, content=c.content,
            embedding=vector,
            heading_path=chunker_v2.PATH_SEP.join(c.heading_path) or None, page_start=c.page_start, page_end=c.page_end,
            block_type=c.block_type, location=c.location, generation=info.generation,
        )
        for c, vector in zip(chunks, vectors)
    ]
