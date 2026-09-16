# Task Update 7 — Phase 7: File Service Permission-Check Client

This document records the completion of Phase 7 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — the batch permission-check client (HLD §16.2), built and tested with fail-closed behavior proven under every failure mode, even though the real File Service endpoint doesn't exist yet.

---

## 1. A Config Correction: REST, Not gRPC

Phase 0's `Settings` had a `FILE_SERVICE_GRPC_TARGET` field, following HLD's own hedge ("gRPC or REST, new"). Building this client required actually deciding — and File Service is a FastAPI/HTTP service with no gRPC server anywhere in it (confirmed against its own `app/main.py`), and HLD §16.2's own worked example is explicitly a REST route: `POST /internal/files/permissions/check`. Renamed the setting to `FILE_SERVICE_BASE_URL` (`http://localhost:8006`, matching File Service's own local-dev port from its Makefile) in `config.py` and `.env.example`, and moved `httpx` from a dev-only dependency to a main one, since it's now used by production code, not just tests.

## 2. Fail-Closed by Construction (HLD §16.2, LLD §8.2)

```python
class FileServiceClient:
    async def batch_check_permissions(self, user_id: str, document_ids: list[str]) -> list[str]:
        if not document_ids:
            return []
        try:
            async with httpx.AsyncClient(base_url=..., timeout=..., transport=self._transport) as client:
                response = await client.post("/internal/files/permissions/check", ...)
                response.raise_for_status()
                authorized = response.json()["authorized_document_ids"]
                if not isinstance(authorized, list) or not all(isinstance(d, str) for d in authorized):
                    raise ValueError("malformed authorized_document_ids in response")
                return authorized
        except Exception:
            return []
```

One `try/except Exception` wraps everything — timeout, connection refusal, a 5xx (`raise_for_status()`), and a malformed/missing response key (the explicit `isinstance` check) all fall through to the same `return []`. This is deliberate: this is the one client in the codebase where "return less" isn't good enough, it has to be "return nothing" — a partial or best-effort result here would mean silently authorizing access to documents nobody actually checked.

`base_url`/`transport` are constructor-injectable specifically so tests can point the client at an in-process fake with no real socket involved — the real endpoint doesn't exist on File Service yet (flagged in the Implementation Plan's own header as a cross-repo prerequisite, not something this repo blocks on).

## 3. Testing Against a Local Fake — and a Real Gotcha in `httpx.ASGITransport`

Built the fake as a small Starlette app wired in via `httpx.ASGITransport`, avoiding a real port entirely:

```python
def _client_for(handler) -> FileServiceClient:
    app = Starlette(routes=[Route("/internal/files/permissions/check", handler, methods=["POST"])])
    return FileServiceClient(base_url="http://fileservice.test", transport=httpx.ASGITransport(app=app))
```

**The latency test initially failed** — `ASGITransport` calls the ASGI app directly with no I/O deadline of its own; httpx's configured `timeout` is only enforced by its real network transport, not as a generic wrapper around every transport. A `handle_async_request` that just does `await asyncio.sleep(5)` runs the full 5 seconds regardless of the client's configured 0.2–0.3s timeout — over `ASGITransport`, that setting is simply never consulted.

Fixed with a purpose-built transport that reads the same `request.extensions["timeout"]` dict a real socket-based transport receives and raises `httpx.ReadTimeout` once it elapses — genuinely simulating what a slow real server would produce, rather than relying on an outer test-level `asyncio.wait_for` to paper over the gap:

```python
class SlowTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        read_timeout = request.extensions.get("timeout", {}).get("read")
        if read_timeout is not None and read_timeout < self._delay_seconds:
            await asyncio.sleep(read_timeout)
            raise httpx.ReadTimeout("simulated slow server", request=request)
        ...
```

## 4. Exit Criteria Verified

- **Authorized subset**: a mixed request (`doc-a`, `doc-b`, `doc-c`) against a fake authorizing only `doc-a`/`doc-c` returns exactly that subset.
- **Fail-closed, one test per mode**: timeout, connection error (a transport that always raises `httpx.ConnectError`), malformed response type (`"authorized_document_ids": "not-a-list"`), missing response key, and a 5xx — every single one returns `[]`, and a dedicated test confirms none of them ever return the caller's own input back unfiltered.
- **Latency**: the client's own configured timeout (0.3s) fires well before the test's outer 2s safety net — proven by asserting elapsed time stays under 1s, not just that the call eventually returns.

9/9 tests passing.
