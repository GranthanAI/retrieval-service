from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.index_generation import GenerationDocument, IndexGeneration
from app.stores import generations
from app.stores.generations import ACTIVE, BUILDING, RETIRED, GenerationInfo


def to_info(row: IndexGeneration) -> GenerationInfo:
    return GenerationInfo(
        generation=row.generation, milvus_collection=row.milvus_collection, opensearch_index=row.opensearch_index,
        parser_version=row.parser_version, chunker_version=row.chunker_version, analyzer_version=row.analyzer_version,
        embedding_model=row.embedding_model, embedding_dimension=row.embedding_dimension, status=row.status,
        opensearch_alias=row.opensearch_alias if row.status == ACTIVE else None,
    )


class GenerationRepository:
    """Postgres access for the generation registry (app/stores/generations.py is the in-process view of it)."""

    @staticmethod
    async def list_not_dropped(db: AsyncSession) -> list[IndexGeneration]:
        result = await db.execute(select(IndexGeneration).where(IndexGeneration.dropped_at.is_(None)).order_by(IndexGeneration.generation))
        return list(result.scalars().all())

    @staticmethod
    async def get(db: AsyncSession, generation: int) -> IndexGeneration | None:
        return await db.get(IndexGeneration, generation)

    @staticmethod
    async def active(db: AsyncSession) -> IndexGeneration | None:
        result = await db.execute(select(IndexGeneration).where(IndexGeneration.status == ACTIVE))
        return result.scalar_one_or_none()

    @staticmethod
    async def register(db: AsyncSession, info: GenerationInfo, notes: str | None = None) -> IndexGeneration:
        row = IndexGeneration(
            generation=info.generation, status=info.status, parser_version=info.parser_version, chunker_version=info.chunker_version,
            embedding_model=info.embedding_model, embedding_dimension=info.embedding_dimension, analyzer_version=info.analyzer_version,
            milvus_collection=info.milvus_collection, opensearch_index=info.opensearch_index, opensearch_alias=info.opensearch_alias,
            notes=notes, activated_at=datetime.now(timezone.utc) if info.status == ACTIVE else None)
        db.add(row)
        await db.flush()
        return row

    @staticmethod
    async def activate(db: AsyncSession, generation: int, alias: str | None) -> None:
        """One transaction: the current active generation becomes `retired` (its data is kept), `generation` becomes `active`."""
        await db.execute(update(IndexGeneration).where(IndexGeneration.status == ACTIVE).values(status=RETIRED, opensearch_alias=None))
        await db.execute(update(IndexGeneration).where(IndexGeneration.generation == generation)
                         .values(status=ACTIVE, opensearch_alias=alias, activated_at=datetime.now(timezone.utc)))

    @staticmethod
    async def mark_dropped(db: AsyncSession, generation: int) -> None:
        await db.execute(update(IndexGeneration).where(IndexGeneration.generation == generation)
                         .values(status=RETIRED, dropped_at=datetime.now(timezone.utc)))

    @staticmethod
    async def record_document(db: AsyncSession, generation: int, document_id: UUID, content_version: int, chunk_count: int) -> None:
        stmt = pg_insert(GenerationDocument).values(generation=generation, document_id=document_id, content_version=content_version,
                                                    chunk_count=chunk_count)
        await db.execute(stmt.on_conflict_do_update(
            index_elements=["generation", "document_id", "content_version"], set_={"chunk_count": chunk_count, "indexed_at": func.now()}))

    @staticmethod
    async def covered(db: AsyncSession, generation: int) -> set[tuple[str, int]]:
        result = await db.execute(select(GenerationDocument.document_id, GenerationDocument.content_version)
                                  .where(GenerationDocument.generation == generation))
        return {(str(d), v) for d, v in result.all()}


async def refresh(session_factory) -> list[GenerationInfo]:
    """Loads the registry into the in-process cache (app/stores/generations.py). Called at startup and then periodically; an empty
    registry (or an unreachable database) leaves the legacy generation 1 in effect."""
    async with session_factory() as db:
        rows = await GenerationRepository.list_not_dropped(db)
    infos = [to_info(r) for r in rows]
    generations.set_registered(infos)
    return infos
