from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, DateTime, Index, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OutboxEvent(Base):
    """LLD §3.3 — poll-and-publish outbox for the three produced events
    (file.processing.started/completed/failed). Written in the SAME DB
    transaction as the ingestion_jobs status transition that causes it
    (e.g. job -> READY and the file.processing.completed row are one atomic
    commit) — enforced by callers (LLD §6.2), not by this model. This is
    what makes "never publish .completed for a PARTIALLY_INDEXED job"
    (HLD §20) enforceable at the transaction level.
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index("ix_outbox_events_unpublished", "created_at", postgresql_where=text("published = false")),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, unique=True)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
