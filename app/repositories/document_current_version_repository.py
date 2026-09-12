from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.document_current_version import DocumentCurrentVersion


class DocumentCurrentVersionRepository:
    @staticmethod
    async def advance_if_newer(db: AsyncSession, document_id: UUID, content_version: int) -> None:
        """LLD §3.4 — the monotonic guard is load-bearing, not defensive
        boilerplate: File Service's Kafka producer doesn't key by
        document_id, so v3 can arrive before v2 on a multi-partition topic.
        The `WHERE current_version < EXCLUDED.current_version` clause makes
        this correct regardless of delivery order — a lower/equal version
        arriving later is a no-op, never a regression (HLD §13.3).
        """
        stmt = (
            pg_insert(DocumentCurrentVersion)
            .values(document_id=document_id, current_version=content_version)
            .on_conflict_do_update(
                index_elements=["document_id"],
                set_={"current_version": content_version},
                where=DocumentCurrentVersion.current_version < content_version,
            )
        )
        await db.execute(stmt)
        await db.flush()

    @staticmethod
    async def get_many(db: AsyncSession, document_ids: list[UUID]) -> dict[str, int]:
        """HLD §13.3 — query-time resolution of `current_version` per
        `document_id`, a local indexed Postgres lookup, never a File Service
        round trip. A document_id with no row here is simply absent from the
        returned dict — callers drop it from search scope entirely (LLD §7.2
        step 3a), not an error.
        """
        if not document_ids:
            return {}
        result = await db.execute(
            select(DocumentCurrentVersion).where(DocumentCurrentVersion.document_id.in_(document_ids))
        )
        return {str(row.document_id): row.current_version for row in result.scalars().all()}
