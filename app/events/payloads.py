from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class StorageRef(BaseModel):
    provider: str
    bucket: str
    key: str
    etag: str


class FileUploadedEvent(BaseModel):
    """HLD §8.1 — `storage.key` is the only place `content_version` is
    recoverable from (HLD §8.3 Phase 1); this model carries the wire shape
    verbatim, version parsing happens separately in the consumer."""

    event_id: UUID
    event_type: Literal["file.uploaded"] = "file.uploaded"
    version: int = 1
    document_id: UUID
    conversation_id: UUID | None = None
    owner_id: UUID
    filename: str
    content_type: str
    storage: StorageRef
    timestamp: datetime


class FileDeletedEvent(BaseModel):
    """HLD §8.2 — carries no content_version; delete is unconditional
    across all versions (HLD §21)."""

    event_id: UUID
    event_type: Literal["file.deleted"] = "file.deleted"
    version: int = 1
    document_id: UUID
    owner_id: UUID
    timestamp: datetime


class FileProcessingRetryEvent(BaseModel):
    """HLD §1.3 #1 / §8.5 — `content_version` is a REQUIRED field (no
    default). A payload missing it fails Pydantic validation and must be
    rejected/dead-lettered by the consumer, never guessed or defaulted to
    the document's current version.
    """

    event_id: UUID
    event_type: Literal["file.processing_retry"] = "file.processing_retry"
    version: int = 1
    document_id: UUID
    content_version: int
    retry_count: int
    timestamp: datetime


class ProcessingStartedEvent(BaseModel):
    """HLD §8.6 — produced. Shape: {event_id, document_id, timestamp} only."""

    event_id: UUID
    document_id: UUID
    timestamp: datetime


class ProcessingCompletedEvent(BaseModel):
    """HLD §8.6 — produced."""

    event_id: UUID
    document_id: UUID
    chunk_count: int
    timestamp: datetime


class ProcessingFailedEvent(BaseModel):
    """HLD §8.6 — produced."""

    event_id: UUID
    document_id: UUID
    error_code: str
    error_message: str
    timestamp: datetime
