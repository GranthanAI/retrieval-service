import asyncio

from minio import Minio

from app.core.config import settings

_client: Minio | None = None


def _get_client() -> Minio:
    """Process-scoped reusable MinIO client, read-only (HLD §5) — this
    service only ever GETs objects File Service already wrote; it never
    creates buckets or writes objects (that stays File Service's job,
    LLD §15).
    """
    global _client
    if _client is None:
        _client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
    return _client


class DownloadError(Exception):
    """Wraps any failure fetching the original object from MinIO — the
    orchestrator maps this to `status=FAILED, error_code=DOWNLOAD_ERROR`
    (LLD §6.2)."""


async def fetch(bucket: str, key: str) -> bytes:
    """MinIO S3 GET (HLD §5), offloaded via `asyncio.to_thread` since the
    `minio` SDK is synchronous — same pattern as file-service's own MinIO
    usage and this repo's store-layer repositories.
    """

    def _get() -> bytes:
        # `asyncio.wait_for` below is what actually enforces
        # MINIO_DOWNLOAD_TIMEOUT_SECONDS — minio-py's get_object has no
        # first-class per-call timeout kwarg to pass through.
        response = _get_client().get_object(bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    try:
        return await asyncio.wait_for(asyncio.to_thread(_get), timeout=settings.MINIO_DOWNLOAD_TIMEOUT_SECONDS)
    except Exception as exc:
        raise DownloadError(f"failed to download {bucket}/{key}: {exc}") from exc


async def ping() -> None:
    """Reachability probe for /health/ready (worker mode) — raises if MinIO
    is unreachable. `bucket_exists` returning False is not a failure here:
    the bucket is File Service's to create, not this service's."""
    await asyncio.to_thread(_get_client().bucket_exists, settings.MINIO_BUCKET_FILES)
