from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Integer, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DocumentCurrentVersion(Base):
    """LLD §3.4 / HLD §13.3 — Retrieval-owned projection of which
    content_version is current per document_id, updated off file.uploaded
    events already being consumed (never a synchronous File Service call).

    Advanced only monotonically (`current_version < :new`, enforced by
    document_current_version_repository.advance_if_newer) — File Service's
    Kafka producer doesn't key by document_id, so v3 can arrive before v2;
    a lower/equal content_version arriving later must be a no-op, never a
    regression (HLD §13.3).
    """

    __tablename__ = "document_current_versions"

    document_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    current_version: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
