# Task Update 2 — Phase 3: embedding-service Client

This document records the completion of Phase 3 ([`docs/IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md)) — the gRPC client to embedding-service, built and tested against the real, running service rather than a mock.

---

## 1. Proto Ownership — Copied, Not Redesigned

`embedding-service` already owns a frozen `protos/embedding.proto` (`GenerateEmbedding`/`GenerateEmbeddings`, `EmbeddingResponse{model, dimension, embedding}`). Copied verbatim into `retrieval-service/protos/embedding.proto`, the same relationship this repo already has with `llm-service`'s `retrieval.proto` (HLD §1.1 #2) — retrieval-service is a gRPC *client* of embedding-service, not its proto's owner.

The `proto` Makefile target now generates both:

```makefile
proto:
	uv run python -m grpc_tools.protoc -Iprotos --python_out=app/grpc --grpc_python_out=app/grpc protos/retrieval.proto protos/embedding.proto
	uv run python scripts/fix_proto_grpc_imports.py
```

## 2. A Real Bug Found in the Proto Fixup Step (Latent Since Phase 0)

The original one-liner fixing up generated imports:

```python
open(p, 'w', encoding='utf-8').write(re.sub(..., open(p, 'r', encoding='utf-8').read()))
```

silently truncates every `_pb2_grpc.py` file it "fixes." Python evaluates the receiver of `.write(...)` — `open(p, 'w', ...)`, which truncates the file immediately — *before* evaluating the argument, which reads the now-empty file. This had been broken since Phase 0 and went undetected because Phase 0's only proto test imports `GetRelevantChunksRequest` from `retrieval_pb2`, never touching `retrieval_pb2_grpc`.

Replaced with a real script, `scripts/fix_proto_grpc_imports.py`, that reads fully before writing:

```python
def main() -> None:
    for path in glob.glob("app/grpc/*_pb2_grpc.py"):
        with open(path, encoding="utf-8") as f:
            content = f.read()
        fixed = PATTERN.sub(REPLACEMENT, content)
        with open(path, "w", encoding="utf-8") as f:
            f.write(fixed)
```

Verified both `retrieval_pb2_grpc.py` (98 lines) and `embedding_pb2_grpc.py` (140 lines) now generate with real content, and both stubs (`RetrievalServiceStub`, `EmbeddingServiceStub`) import successfully.

## 3. Getting a Real embedding-service Running Locally

Per instruction, built and tested the client against the actual embedding-service repo rather than mocking it. Its own `Makefile` runs `uv run uvicorn`, but its `pyproject.toml` uses legacy Poetry-style `[tool.poetry.dependencies]`, which `uv sync` doesn't parse — `uv sync` silently produced an empty venv (no `uvicorn`, no `torch`). Installed its real dependencies from its own `requirements.txt` into its own `.venv` instead (no changes to retrieval-service's own dependencies) and started it directly:

```bash
cd embedding-service
uv venv .venv --python 3.12
uv pip install -r requirements.txt --python .venv
.venv/Scripts/uvicorn.exe app.main:app --host 0.0.0.0 --port 8040
```

Confirmed live: REST `POST /v1/embed` and gRPC (port 50054) both serving real 384-dim vectors from the actual `all-MiniLM-L6-v2` model already present at `C:\Users\hp\Desktop\models\models\all-MiniLM-L6-v2`. This is a pre-existing tooling mismatch in embedding-service's own repo — not fixed there, just worked around to get a real server running for this repo's own testing.

## 4. `app/clients/embedding_client.py`

Both call shapes from LLD §8.1 (Phase 3): single call for query embedding, batched for ingestion.

```python
async def embed_query(self, text: str) -> list[float]:
    """Single call, ON the query path — EMBEDDING_QUERY_TIMEOUT_SECONDS,
    retried up to EMBEDDING_RETRY_ATTEMPTS times."""
    ...

async def embed_chunks(self, texts: list[str]) -> list[list[float]]:
    """Batched per EMBEDDING_BATCH_SIZE, off the query path —
    EMBEDDING_INGESTION_TIMEOUT_SECONDS per batch call."""
    ...
```

### 4.1 Model/Dimension Mismatch Guard (HLD §12)

```python
def _assert_model_matches(model: str, dimension: int) -> None:
    if model != settings.EMBEDDING_MODEL_EXPECTED or dimension != settings.EMBEDDING_DIMENSION_EXPECTED:
        raise EmbeddingModelMismatchError(...)
```

Verified live: setting `EMBEDDING_MODEL_EXPECTED = "wrong-model"` against the real (correctly-behaving) embedding-service correctly raises `EmbeddingModelMismatchError` rather than returning a vector.

## 5. Exit Criteria Verified (Real embedding-service, Not Mocked)

- `embed_query("hello world")` → 384-dim vector.
- `embed_chunks([...])` → one 384-dim vector per input, same order, batched.
- Model/dimension mismatch → raises, never returns a vector.

No dedicated unit test file was added in this phase alone — Phase 4's `test_orchestrator.py` exercises `embed_chunks` for real as part of the full ingestion pipeline, which is a stronger guarantee than an isolated mock-based test would have been.
