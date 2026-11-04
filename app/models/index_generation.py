from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class IndexGeneration(Base):
    """One complete index build (Milvus collection + OpenSearch index) and how it was made. Exactly one row is `active`; queries and new
    ingestion use it. `building` rows are filled by `make reindex`; `retired` rows are kept until an operator drops them. See
    app/stores/generations.py."""

    __tablename__ = "index_generations"

    generation: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)               # building | active | retired
    parser_version: Mapped[str] = mapped_column(String(16), nullable=False)
    chunker_version: Mapped[str] = mapped_column(String(16), nullable=False)
    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    embedding_dimension: Mapped[int] = mapped_column(Integer, nullable=False)
    analyzer_version: Mapped[str] = mapped_column(String(16), nullable=False)
    milvus_collection: Mapped[str] = mapped_column(Text, nullable=False)
    opensearch_index: Mapped[str] = mapped_column(Text, nullable=False)
    opensearch_alias: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dropped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)   # physical indexes deleted by an operator


class GenerationDocument(Base):
    """Which (document, content_version) pairs a generation holds. `make activate` uses it to refuse a switch while any READY current
    document version is missing from the new generation (documents ingested during a build go to the active generation only)."""

    __tablename__ = "generation_documents"

    generation: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    document_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    content_version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False)
    indexed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
