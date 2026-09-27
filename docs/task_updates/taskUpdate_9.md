# Task Update 9 — Phase 9: gRPC Server + Servicer Wiring

This document records the completion of Phase 9 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — `GetRelevantChunks` is now callable over a real gRPC connection, with every Phase 3/7/2 client wired in for real.

---

## 1. `app/grpc/server.py`, `interceptors/`, `retrieval_servicer.py` — Built to LLD §7.1/HLD §6

`RetrievalServicer.GetRelevantChunks` wraps `pipeline.run()` in `asyncio.wait_for(..., timeout=QUERY_PIPELINE_DEADLINE_SECONDS)` exactly per LLD §7.1, degrading to `response_mapper.empty_response()` on a timeout rather than raising. Every real client (`EmbeddingClient`, `FileServiceClient`, `MilvusRepository`, `OpenSearchRepository`, plus `session_factory` for `document_current_version_repository`) is constructed once in `lifespan.py`'s `api` branch and passed into the servicer's constructor — no more fakes at this layer, matching Phase 8's own injected-dependency shape with real objects instead.

`GRPC_MAX_WORKERS` (LLD §2) turned out to have no real meaning for `grpc.aio.server()` — that setting is a thread-pool-executor size for a *synchronous* `grpc.server()`; the async server runs the servicer directly on the event loop. Documented in `server.py` rather than silently repurposed or dropped.

## 2. A Real Bug: `grpc.aio` Has No `unary_unary_rpc_method_handler`

Both interceptors (`logging_interceptor.py`, `metrics_interceptor.py`) initially called `grpc.aio.unary_unary_rpc_method_handler(...)` to rebuild the wrapped handler — this function doesn't exist on `grpc.aio` in the installed grpcio version; it lives on the base `grpc` module (`grpc.unary_unary_rpc_method_handler`), and works identically for async handler functions. Every single `GetRelevantChunks` call failed with `AttributeError` inside the interceptor chain until this was caught by the very first real gRPC round-trip test — a case where "call it over a real connection" (this phase's whole point) caught something no unit test with mocked interceptors would have.

## 3. A Second Real Bug, Found by the Same Round-Trip Test: The Empty-Credential Header

Once the interceptor bug was fixed, the round-trip test still returned zero chunks. The actual cause: `FileServiceClient` builds its Authorization header as `f"Bearer {settings.FILE_SERVICE_SERVICE_CREDENTIAL}"` — with the shipped local-dev default (`FILE_SERVICE_SERVICE_CREDENTIAL=""`), that's the literal string `"Bearer "` with a trailing space, which httpx/h11 now reject as an illegal header value. The client's own broad `except Exception: return []` (its correct, deliberate fail-closed behavior from Phase 7) swallowed this silently — authorization always came back empty, indistinguishable from a genuinely-unauthorized request.

**This bug has existed since Phase 7** but went undetected there because every one of Phase 7's fail-closed tests asserts the same outcome (`[]`) whether the failure is a real simulated network error or an unrelated header-construction crash — they're indistinguishable from that vantage point. Only a happy-path test that expects actual authorized results through a real HTTP call could have caught it, and Phase 7 tested against `httpx.ASGITransport` where a dependency version difference apparently didn't yet enforce this strictly. Fixed by omitting the `Authorization` header entirely when no credential is configured, rather than sending a malformed one — an unset credential is a real "not configured" state, not a token that happens to be the empty string.

## 4. Exit Criteria Verified — Real Postgres/Milvus/OpenSearch/embedding-service, Real gRPC, a Real (Locally-Run) Fake File Service

- **Round trip**: a real `RetrievalServiceStub` call against a real running `grpc.aio` server returns actual `DocumentChunk`s for a document ingested through the real Phase 4 orchestrator — correct `source_file_id`, non-empty `content`. A companion test confirms an unauthorized document (fake File Service authorizes nothing) returns empty over the same real network path, not just in `pipeline.py` unit tests.
- **Deadline**: `OpenSearchRepository.search` patched to sleep well beyond `QUERY_PIPELINE_DEADLINE_SECONDS` — the RPC still returns an empty response within the deadline plus a small margin, never hanging or erroring, proving the servicer's `asyncio.wait_for` actually bounds a real slow downstream call end-to-end.
- **Load at low concurrency**: 20 concurrent `GetRelevantChunks` calls against fast fakes for every downstream client complete well under 5 seconds — `GRPC_MAX_WORKERS`'s irrelevance to `grpc.aio` (see §1) is exactly why this isn't a bottleneck; the full perf pass remains Phase 12's job.

A real bug surfaced along the way while re-testing: `_ingest_real_document` (this phase's own test helper) initially didn't seed `document_current_versions` — only `file_uploaded_consumer.handle_message` does that in the real system, and this helper bypasses Kafka entirely to ingest directly. Without it, the round-trip test would have silently returned empty for the *right* reason (HLD §13.3's version resolution correctly finding nothing) while masking whether the rest of the pipeline actually worked. Fixed by seeding it explicitly in the test helper, with a comment explaining why it's needed there and nowhere else in the ingestion path itself.

13 new tests (4 gRPC servicer + re-verified 9 file-service-client); **70/70 across the whole suite** (Phases 1–9).
