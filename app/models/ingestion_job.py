from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Index, Integer, String, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import IngestionStatus, StageStatus
from app.db.base import Base


class IngestionJob(Base):
    """LLD §3.1 — one row per (document_id, content_version), tracking HLD
    §19's explicit state machine. `status` is a coarser bucket, always
    recomputed from embedding_status/milvus_status/opensearch_status by
    ingestion_job_repository.recompute_status() — never written directly by
    anything else, so it can never drift out of sync with the three
    per-stage columns (LLD §3.1's closing note).

    `storage_bucket`/`storage_key` are retained for the job's full lifetime,
    not just at creation — LLD §6.3's partial-index recovery path
    re-downloads the original object from these exact fields, since this
    table stores no chunk/embedding payload itself.
    """

    __tablename__ = "ingestion_jobs"
    __table_args__ = (
        UniqueConstraint("document_id", "content_version", name="uq_ingestion_jobs_doc_version"),
        Index("ix_ingestion_jobs_status", "status"),
        Index("ix_ingestion_jobs_document_id", "document_id"),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    document_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    content_version: Mapped[int] = mapped_column(Integer, nullable=False)
    owner_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    conversation_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    storage_bucket: Mapped[str] = mapped_column(Text, nullable=False)
    storage_key: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=IngestionStatus.NOT_STARTED.value,
        server_default=IngestionStatus.NOT_STARTED.value,
    )
    embedding_status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=StageStatus.PENDING.value, server_default=StageStatus.PENDING.value
    )
    milvus_status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=StageStatus.PENDING.value, server_default=StageStatus.PENDING.value
    )
    opensearch_status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=StageStatus.PENDING.value, server_default=StageStatus.PENDING.value
    )
    chunk_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
