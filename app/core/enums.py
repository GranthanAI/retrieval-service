from enum import Enum


class IngestionStatus(str, Enum):
    """HLD §19's explicit state machine — a coarser bucket than the three
    per-stage StageStatus columns on ingestion_jobs, always recomputed by
    ingestion_job_repository.recompute_status(), never written independently
    (LLD §3.1's closing note)."""

    NOT_STARTED = "NOT_STARTED"
    PROCESSING = "PROCESSING"
    PARTIALLY_INDEXED = "PARTIALLY_INDEXED"
    READY = "READY"
    FAILED = "FAILED"
    DELETING = "DELETING"
    DELETED = "DELETED"


class StageStatus(str, Enum):
    """embedding_status/milvus_status/opensearch_status on ingestion_jobs
    (LLD §3.1, HLD §20) — tracked independently so a partial indexing
    failure is distinguishable from a wholesale one."""

    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
