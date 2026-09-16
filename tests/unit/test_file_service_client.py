"""Phase 7 exit criteria. The real File Service endpoint (HLD §16.2) does
not exist yet — this suite runs against a local fake ASGI app, wired in
via `httpx.ASGITransport` so no real socket/port is involved.
"""

import asyncio

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from app.clients.file_service_client import FileServiceClient

AUTHORIZED_DOCS = {"doc-a", "doc-c"}


async def _check_permissions(request: Request) -> JSONResponse:
    body = await request.json()
    document_ids = body.get("document_ids", [])
    authorized = [d for d in document_ids if d in AUTHORIZED_DOCS]
    return JSONResponse({"authorized_document_ids": authorized})


async def _malformed_response(_request: Request) -> JSONResponse:
    return JSONResponse({"authorized_document_ids": "not-a-list"})


async def _missing_key_response(_request: Request) -> JSONResponse:
    return JSONResponse({"unexpected_field": []})


async def _server_error(_request: Request) -> Response:
    return Response(status_code=500, content="internal error")


def _client_for(handler) -> FileServiceClient:
    app = Starlette(routes=[Route("/internal/files/permissions/check", handler, methods=["POST"])])
    transport = httpx.ASGITransport(app=app)
    return FileServiceClient(base_url="http://fileservice.test", transport=transport)


class ConnectErrorTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)


class SlowTransport(httpx.AsyncBaseTransport):
    """`httpx.ASGITransport` calls the ASGI app directly with no I/O
    deadline of its own — httpx's configured timeout is only enforced by
    its real network transport, not a generic wrapper around every
    transport. To genuinely exercise the client's own configured timeout
    (rather than an outer test-level safety net), this transport reads the
    same `request.extensions["timeout"]` a real socket-based transport
    would receive and raises `httpx.ReadTimeout` once it elapses —
    simulating exactly what a slow real server would produce.
    """

    def __init__(self, delay_seconds: float) -> None:
        self._delay_seconds = delay_seconds

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        read_timeout = request.extensions.get("timeout", {}).get("read")
        if read_timeout is not None and read_timeout < self._delay_seconds:
            await asyncio.sleep(read_timeout)
            raise httpx.ReadTimeout("simulated slow server", request=request)
        await asyncio.sleep(self._delay_seconds)
        return httpx.Response(200, json={"authorized_document_ids": []})


class TestAuthorizedSubset:
    @pytest.mark.asyncio
    async def test_returns_authorized_subset_for_mixed_request(self) -> None:
        client = _client_for(_check_permissions)
        result = await client.batch_check_permissions("user-1", ["doc-a", "doc-b", "doc-c"])
        assert sorted(result) == ["doc-a", "doc-c"]

    @pytest.mark.asyncio
    async def test_empty_document_ids_short_circuits_without_a_call(self) -> None:
        client = _client_for(_check_permissions)
        result = await client.batch_check_permissions("user-1", [])
        assert result == []


class TestFailClosed:
    """Every failure mode returns [] — never a partial/best-effort result,
    never the input unfiltered (LLD §8.2's explicit callout)."""

    @pytest.mark.asyncio
    async def test_timeout_returns_empty(self) -> None:
        from app.core.config import settings

        original_timeout = settings.FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS
        settings.FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS = 0.2
        try:
            client = FileServiceClient(base_url="http://fileservice.test", transport=SlowTransport(delay_seconds=5))
            result = await client.batch_check_permissions("user-1", ["doc-a"])
            assert result == []
        finally:
            settings.FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS = original_timeout

    @pytest.mark.asyncio
    async def test_connection_error_returns_empty(self) -> None:
        client = FileServiceClient(base_url="http://fileservice.test", transport=ConnectErrorTransport())
        result = await client.batch_check_permissions("user-1", ["doc-a"])
        assert result == []

    @pytest.mark.asyncio
    async def test_malformed_response_type_returns_empty(self) -> None:
        client = _client_for(_malformed_response)
        result = await client.batch_check_permissions("user-1", ["doc-a"])
        assert result == []

    @pytest.mark.asyncio
    async def test_missing_key_in_response_returns_empty(self) -> None:
        client = _client_for(_missing_key_response)
        result = await client.batch_check_permissions("user-1", ["doc-a"])
        assert result == []

    @pytest.mark.asyncio
    async def test_5xx_returns_empty(self) -> None:
        client = _client_for(_server_error)
        result = await client.batch_check_permissions("user-1", ["doc-a"])
        assert result == []

    @pytest.mark.asyncio
    async def test_never_returns_input_unfiltered_on_any_failure(self) -> None:
        """The specific failure mode doesn't matter — what matters is that
        NONE of them ever return the caller's own input back as if it were
        all authorized."""
        document_ids = ["doc-a", "doc-b", "doc-c", "doc-d"]
        for handler in (_malformed_response, _server_error, _missing_key_response):
            client = _client_for(handler)
            result = await client.batch_check_permissions("user-1", document_ids)
            assert result != document_ids
            assert result == []


class TestLatency:
    @pytest.mark.asyncio
    async def test_slow_server_triggers_clients_own_timeout_not_a_hang(self) -> None:
        from app.core.config import settings

        original_timeout = settings.FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS
        settings.FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS = 0.3
        try:
            client = FileServiceClient(base_url="http://fileservice.test", transport=SlowTransport(delay_seconds=5))

            start = asyncio.get_event_loop().time()
            result = await asyncio.wait_for(client.batch_check_permissions("user-1", ["doc-a"]), timeout=2.0)
            elapsed = asyncio.get_event_loop().time() - start

            assert result == []
            # The client's OWN configured timeout (0.3s) fired — not the
            # test's outer 2s safety net.
            assert elapsed < 1.0
        finally:
            settings.FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS = original_timeout
