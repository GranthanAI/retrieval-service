import httpx

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class FileServiceClient:
    """HLD §16.2 — batch permission-check client: "does `user_id` have
    >= VIEWER on these `document_id`s?" Authenticates via a service-to-
    service credential (`FILE_SERVICE_SERVICE_CREDENTIAL`), never a bare
    `user_id` claim — the caller must prove it IS Retrieval Service, with
    `user_id` treated as data being asked about, not something the caller
    is trusted to assert unchecked.

    **Fail-closed is the whole point of this client.** Every failure mode
    — timeout, connection refused, malformed response, 5xx — returns `[]`,
    never the input `document_ids` unfiltered and never "everything
    authorized." This is the one client in the codebase where "return
    less" is not good enough; it must be "return nothing" (LLD §8.2).

    The real File Service endpoint does not exist yet (Implementation
    Plan's own header note) — this client is built and tested against the
    documented contract and a local fake (Phase 7); `base_url`/`transport`
    are constructor-injectable so tests can point this at an in-process
    fake ASGI app with no real socket involved.
    """

    def __init__(self, base_url: str | None = None, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._base_url = base_url or settings.FILE_SERVICE_BASE_URL
        self._transport = transport

    async def batch_check_permissions(self, user_id: str, document_ids: list[str]) -> list[str]:
        if not document_ids:
            return []

        try:
            async with httpx.AsyncClient(
                base_url=self._base_url,
                timeout=settings.FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS,
                transport=self._transport,
            ) as client:
                response = await client.post(
                    "/internal/files/permissions/check",
                    headers={"Authorization": f"Bearer {settings.FILE_SERVICE_SERVICE_CREDENTIAL}"},
                    json={"user_id": user_id, "document_ids": document_ids},
                )
                response.raise_for_status()
                body = response.json()
                authorized = body["authorized_document_ids"]
                if not isinstance(authorized, list) or not all(isinstance(doc_id, str) for doc_id in authorized):
                    raise ValueError("malformed authorized_document_ids in response")
                return authorized
        except Exception as exc:
            logger.warning("file_service_client.permission_check_failed_closed", error=str(exc))
            return []
