from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ProcessedEvent(Base):
    """LLD §3.2 — inbox pattern, composite-keyed (event_id, consumer) so the
    same event_id redelivered to a different consumer group is tracked
    independently.

    A row here means "this event was durably accepted into an
    ingestion_jobs row," not "all processing caused by this event has
    finished" — see processed_event_repository.mark_processed's docstring
    for the same-transaction guarantee that makes this fact reliable, and
    LLD §3.2's redelivery dispatch table for what a consumer must do when
    this row already exists.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    consumer: Mapped[str] = mapped_column(String(100), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
