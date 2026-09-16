# Task Update 3 — Phase 4: Ingestion Pipeline Core (No Kafka Yet)

This document records the completion of Phase 4 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — `orchestrator.run(...)` working end to end when invoked directly, proving download→parse→chunk→embed→index and its `READY`/`PARTIALLY_INDEXED`/`FAILED` branching before any Kafka wiring exists.

---

## 1. Chunk Identity (HLD §8.3 Phase 1, §11)

`app/ingestion/chunk_identity.py` — strict version parsing and deterministic chunk IDs, both load-bearing for idempotency:

```python
_CONTENT_VERSION_PATTERN = re.compile(r"/v(\d+)/")

def parse_content_version(storage_key: str) -> int:
    match = _CONTENT_VERSION_PATTERN.search(storage_key)
    if match is None:
        raise ContentVersionParseError(...)  # reject, never default to 1
    return int(match.group(1))

def derive_chunk_id(document_id: str, content_version: int, chunk_index: int) -> str:
    name = f"{document_id}:{content_version}:{chunk_index}"
    return str(uuid.uuid5(RETRIEVAL_CHUNK_NAMESPACE, name))
```

Verified: deriving the same `(document_id, content_version, chunk_index)` twice (simulating a redelivered event) produces identical ids.

## 2. Parser — Real Docling, Not a Stub (HLD §9)

`app/ingestion/parser.py` wraps `docling.DocumentConverter`, walking the parsed document in reading order (`iterate_items()`) and starting a new `ParsedSection` at every heading/title, accumulating body text (paragraphs, list items, tables rendered to markdown) underneath it.

**OCR explicitly disabled.** Docling's default PDF pipeline runs OCR (RapidOCR, downloading ~30MB of models on first use) even for text-layer PDFs. File Service's uploads are digitally-authored, not scans — OCR only adds cost with nothing to recover:

```python
_PDF_PIPELINE_OPTIONS = PdfPipelineOptions(do_ocr=False, do_table_structure=True)
```

The layout model itself (`docling-layout-heron`, ~164MB) is a one-time download on first parse, cached under `~/.cache/huggingface/hub` afterward — subsequent parses in this same environment are fast.

Verified against a real (hand-built, no external PDF library dependency) fixture, `tests/fixtures/sample.pdf`: the parser correctly extracted `"Retrieval Service Sample Document"` as a heading on page 1, with the following paragraph text merged into that section.

## 3. Chunker (HLD §10)

`app/ingestion/chunker.py` — structure-aware recursive chunking, sized against `CHUNK_TARGET_TOKENS`/`CHUNK_MAX_TOKENS`/`CHUNK_OVERLAP_TOKENS`. Chunks never cross a `ParsedSection` boundary — long sections are further split into overlapping word windows; short ones pass through as a single chunk.

## 4. Indexer — Concurrent Dual-Store Write (LLD §6.2)

`app/ingestion/indexer.py`'s `write()` calls Milvus and OpenSearch **concurrently** via `asyncio.gather`, catching each store's failure independently so one store raising never prevents the other's status from being recorded — this is what makes `PARTIALLY_INDEXED` representable at all:

```python
async def _write_milvus() -> None:
    try:
        await MilvusRepository().upsert_chunks(records)
        result.milvus_status = StageStatus.COMPLETE.value
    except Exception:
        result.milvus_status = StageStatus.FAILED.value
# ... same shape for OpenSearch, run via asyncio.gather(*tasks)
```

## 5. Orchestrator — The Full State Machine (LLD §6.2)

`app/ingestion/orchestrator.py`'s `run(session_factory, document_id, content_version, embedding_client)`:

1. `status → PROCESSING` + `file.processing.started` outbox row, committed immediately (its own early transaction, before download begins).
2. Download (MinIO) → on failure: `FAILED`/`DOWNLOAD_ERROR`, `.failed` outbox row, return.
3. Parse (Docling) → on failure: `FAILED`/`PARSER_ERROR`, `.failed` outbox row, return.
4. Chunk + derive chunk ids.
5. Embed (batched) → on failure: `embedding_status=FAILED`, `status=FAILED`/`EMBEDDING_ERROR`, `.failed` outbox row, return — no partial index writes are possible without embeddings.
6. Index both stores concurrently.
7. Final transaction: set per-stage statuses, `chunk_count`, call `IngestionJobRepository.recompute_status` (reused directly from Phase 1 — no new derivation logic needed), then branch on the *result*:
   - `READY` → `.completed` outbox row with `chunk_count`, same transaction as the status write (LLD §3.3).
   - `FAILED` (both stores failed) → `error_code=INDEXING_ERROR`, `.failed` outbox row.
   - `PARTIALLY_INDEXED` → **no outbox row** — the document must not be treated as fully searchable until both indexes agree (HLD §20).

Every stage re-fetches its own copy of the job row in a fresh, short-lived transaction rather than holding one connection/object across the whole pipeline — the object a caller passes in would be detached the moment its own session closes, and download/parse/embed can each take long enough that holding a single DB transaction open across all of them would be its own problem.

## 6. Exit Criteria Verified — Against Real Infra, Not Mocks

Built `tests/fixtures/sample.pdf` (`scripts/_make_sample_pdf.py`, a hand-built minimal PDF, no external library dependency) and ran the full suite against real Postgres (testcontainers + real Alembic chain), real MinIO, real Milvus/OpenSearch, and the real embedding-service from Task Update 2:

- **Happy path**: `orchestrator.run(...)` → `status == READY`, `chunk_count > 0`, exactly one `file.processing.completed` outbox row with the matching `chunk_count`, and the chunk is genuinely queryable in both Milvus (`get_by_chunk_ids`) and OpenSearch (`search`).
- **Forced download failure** (nonexistent storage key): `status == FAILED`, `error_code == 'DOWNLOAD_ERROR'`, a `.failed` outbox row exists, **no** `.completed` row for that document.
- **Forced partial failure** (`OpenSearchRepository.upsert_chunks` mocked to raise): `status == PARTIALLY_INDEXED`, `milvus_status == COMPLETE`, `opensearch_status == FAILED`, **no** `.completed` outbox row — directly proving LLD §3.3's same-transaction guarantee.
- **Idempotency**: `derive_chunk_id` on the same inputs twice → identical ids.

5/5 new tests passing; 20/20 across the whole suite (Phases 1–4 combined).
