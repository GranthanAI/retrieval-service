# GraphGPT — Retrieval Service HLD

## 0. Document Status & How This Differs From the Draft

This is a **corrected and detailed** HLD, reviewed against the actual code/contracts already frozen in four sibling repos in this monorepo — not just their own HLDs, which in at least one case (`llm-service`) had already drifted from what the code does. The corrections below are not stylistic; each one either closes a real integration gap or fixes an assumption the draft got wrong. If you only read one section, read §1.1.

### 1.1 What changed from the draft, and why

| # | Draft assumed | Actually true (verified against real code) | Why it matters |
|---|---|---|---|
| 1 | Retrieval Service runs `all-MiniLM-L6-v2` **internally** | A **separate, already-built `embedding-service`** owns the model exclusively (`app/`, gRPC `GenerateEmbedding`/`GenerateEmbeddings` on port 50051, REST `/v1/embed`/`/v1/embed/batch` on port 8040 — see its `README.md`/`api_endpoints.md`) | Retrieval Service must **call out** to embedding-service via gRPC for both chunk embedding (batch, at ingestion) and query embedding (single, at query time). Duplicating the model inside Retrieval Service would mean two processes independently loading the same model, doubling memory and creating exactly the "model drift between indexing and querying" risk the draft's §11 was trying to avoid. |
| 2 | The gRPC contract (`Retrieve()`, `RetrieveRequest{query, conversation_id, user_id, top_k}`) was a placeholder to be designed | **`llm-service` already has a frozen `retrieval.proto`** (`llm-service/proto/retrieval.proto`, mirrored at `llm-service/docs/hld2.md` §25.4) defining `GetRelevantChunks`, with `scope` (enum), `file_ids` (repeated string), `min_relevance_score`, `max_tokens`, `trace_id` on the request, and a `DocumentChunk{chunk_id, content, source_file_id, source_name, relevance_score, metadata}` + `total_tokens`/`request_id` on the response | Retrieval Service does not get to design this contract — it must **implement the existing proto exactly**, copied verbatim into this repo's own `protos/`, not re-derived. |
| 3 | Retrieval is called for RAG/"Ask Files" queries | Per `llm-service/docs/hld2.md` §14.1/§18 (mode table), **Retrieval is a "baseline context provider" fetched via `asyncio.gather()` on every single chat turn, in every mode** (Default, Ask Files, Smart, etc.) — never optional, never gated behind a tool decision | This is a **hard, unconditional traffic-volume and latency commitment**, not a RAG-specific nice-to-have. See §17 for the exact SLA this implies (2-attempt retry, 100ms backoff, circuit breaker at 5 failures/30s, 2000ms gRPC deadline, contributing to a <1500ms p95 time-to-first-token budget shared with Memory + Graph). |
| 4 | (not addressed) | **No service in the current chain verifies that a `user_id` is authorized to read the `file_ids` they're requesting.** Conversation Service accepts `file_ids` as opaque UUIDs with zero File Service validation (by design — see `conversation-service/docs/updates/task_update_13.md` §4: "no File Service call required"); LLM Service forwards them unchanged into `GetRelevantChunksRequest.file_ids`. If Retrieval Service also trusts them blindly, **User A can read User B's private document by pasting its `document_id` into a message**, since nothing between the two ever checks File Service's `file_permissions` table. | This is a real, current security gap in the monorepo, not a hypothetical. §16 proposes the fix: Retrieval Service is the correct — and only remaining — place to close it. |
| 5 | Milvus is a resource Retrieval Service "has" | **Milvus is already shared cluster infrastructure** — `memory-service` already runs its own collections on the same Milvus deployment (`memory-service/docs/lld.md` — `milvus_repository.py`, `pymilvus` `Collection`/`connections`) | Retrieval Service must use a **distinctly namespaced collection** (`retrieval_document_chunks`, not just `chunks`) on the shared cluster, not assume it owns Milvus outright. Collection-name collision with memory-service is a real risk if this isn't explicit. |
| 6 | `file.uploaded`'s payload shape was approximated | File Service's actual emitted schema (`file-service/app/events/file_events.py`) is byte-verified in §8 below, including a real gap: **the event carries no explicit document-version field** — version is only recoverable by parsing `v{n}/` out of `storage.key` | §8.3 documents this parsing dependency explicitly, since it's implicit/fragile as currently emitted. |
| 7 | No mention of `file.scan_failed` | File Service has a mandatory AV-scan quarantine flow; an infected file **never emits `file.uploaded` at all** for that `document_id` | Retrieval Service needs no special handling for infected files — it simply never sees them. Documented in §8.4 so this isn't mistaken for a missed event. |

The rest of this document is the full HLD with these corrections folded in throughout, not bolted on at the end.

### 1.2 Round 2 — Reviewed and Refined

This HLD was reviewed a second time against the draft above. The core architecture was confirmed sound (embedding-service, Milvus+OpenSearch+RRF+MMR, no reranker, File Service event contracts) and **kept as-is**. Six further refinements came out of that review and are folded into the relevant sections below, not bolted on:

1. Permission-check endpoint (§16.2) — added service-to-service authentication requirement (don't trust a bare `user_id` field from an arbitrary internal caller) and a defense-in-depth note.
2. `KNOWLEDGE_BASE`/`ALL` scope (§16.3) — **severity raised**: since Retrieval is called on *every* chat turn (§1.1 #3), undefined scope semantics are a baseline-retrieval blocker, not just an Ask-Files one. V1 recommendation: implement `FILES` only; no scope/no `file_ids` → empty context, never an implicit full-corpus search (§16.4).
3. `content_version` (§8.3) — added a concrete two-phase migration path (parse from `storage.key` now; File Service adds an explicit field later) and a terminology fix (`event_schema_version` vs `content_version`, so the two stop getting casually conflated as "version").
4. Milvus collection (§13) — added explicit per-collection ownership table and a rule against dynamic collection creation at pod startup (infra provisioning creates the collection once; pods only connect).
5. **MMR's dependency on candidate embeddings** (§18) — the single most important technical gap in the first draft: MMR needs pairwise similarity between candidates, which needs their vectors. Fetch them from Milvus by the candidate `chunk_id`s already returned by the dense search — never re-call embedding-service per candidate.
6. `relevance_score` precision (§18) — defined exactly what's tracked internally (`dense_score`, `bm25_score`, `dense_rank`, `bm25_rank`, `rrf_score`, `mmr_score`) versus what's actually placed in the proto's `relevance_score` field: **`rrf_score`**, not `mmr_score` — MMR decides which chunks survive and their diversity-aware selection, but the relevance number itself must not depend on selection order, so the response is separately sorted by `rrf_score` descending before being returned.

Two smaller fixes also landed: OpenSearch's record gains an explicit `filename` field (§14), and internal processing state gained explicit lifecycle values instead of a bare arrow-chain (§19).

### 1.3 Round 3 — Versioning Was Still Incomplete

A third review, run jointly against the HLD *and* the LLD together, found that §8.3's version-handling fix (Round 1) was necessary but not sufficient — it solved "how does Retrieval learn a version number," not "which version should retrieval-time search actually return," and left `file.processing_retry` unable to say which version to retry when a document has more than one. Two corrections, both folded into §8.3/§13.3 below rather than left as open items, because — unlike `content_version` on the wire, which is genuinely a File Service prerequisite — the fix here only requires information File Service **already has and already exposes** (`files.current_version`, verified directly against `file-service/app/models/file.py:46` and `app/services/version_service.py`):

1. **`file.processing_retry` must carry `content_version`.** File Service's real, current payload (`file-service/app/services/retry_service.py::_build_file_processing_retry_payload`) is `{document_id, retry_count, timestamp}` — no version at all. Since a document can have multiple `ingestion_jobs` rows (one per `content_version`, HLD §11), a retry event with no version is structurally ambiguous the moment a second version exists. Fixed in §8.5.
2. **Retrieval must search only the current version, and must learn "current" from Kafka, not from a synchronous File Service call.** §13 never constrained the Milvus/OpenSearch filter to a single version — `document_id IN authorized_file_ids` alone means both `v1` and `v2` chunks are retrievable simultaneously once a document has been re-uploaded, which is a correctness bug (a query can return a chunk from a superseded version) rather than a mere gap. Fixed in new §13.3, using a lightweight Retrieval-owned projection (`document_id → current_version`) updated off events already being consumed — not a new File Service dependency, and not a per-query File Service round trip.

---

## 2. Overview

The **Retrieval Service** transforms uploaded documents into searchable representations and serves the top-K most relevant chunks for a given query. It is one of three **baseline context providers** in the LLM Service's request pipeline (alongside Memory Service and Graph Service) — called unconditionally on every chat turn, not just when a user explicitly attaches files.

It owns two pipelines:

### Ingestion Pipeline

```text
File Service --file.uploaded--> Kafka --> Retrieval Service
                                              │
                                              ▼ (S3 API)
                                            MinIO
                                              │
                                              ▼
                                            Docling (parse)
                                              │
                                              ▼
                                  Structure-aware chunking
                                              │
                                              ▼ (gRPC, batch)
                                     embedding-service
                                              │
                                  ┌───────────┴───────────┐
                                  ▼                        ▼
                             Milvus                   OpenSearch
                        (dense, shared cluster,     (BM25, dedicated
                     collection: retrieval_          index: retrieval_
                     document_chunks)                document_chunks)
```

### Query Pipeline

```text
LLM Service --gRPC: GetRelevantChunks--> Retrieval Service
                                              │
                                    Permission filter (§16)
                                              │
                                    Query embed (gRPC, embedding-service)
                                              │
                                  ┌───────────┴───────────┐
                                  ▼                        ▼
                             Dense Search              BM25 Search
                             (Milvus)                  (OpenSearch)
                                  │                        │
                                  └───────────┬────────────┘
                                              ▼
                                      Reciprocal Rank Fusion
                                              │
                                              ▼
                                             MMR
                                              │
                                              ▼
                                     Top-K DocumentChunks
                                              │
                                              ▼ (gRPC response)
                                        LLM Service
```

---

## 3. Goals

* Support baseline retrieval context on **every** chat turn (not RAG-gated), per LLM Service's Context Collector contract.
* Support "Ask Files" mode — retrieval scoped to specific, user-attached `document_id`s.
* Process documents asynchronously off the File Service's `file.uploaded` event, idempotently under Kafka's at-least-once delivery.
* Combine dense semantic search (via embedding-service, not a local model) with BM25 lexical search.
* Deduplicate/diversify results via MMR — no cross-encoder reranker (justified by the real latency budget in §17, not just an assumption).
* Enforce document-level authorization before any chunk is returned (§16) — the one gap nothing upstream currently closes.
* Preserve chunk provenance (filename, page, section, chunk position) through to the gRPC response's `metadata` map.
* Be horizontally scalable, with ingestion and query workloads scaled independently.
* Never become the source of truth for file ownership, permissions, or lifecycle — those remain File Service's.

---

## 4. Non-Goals

Retrieval Service does **not**:

* Own uploaded files, original binaries, or file lifecycle state (`UPLOADING`/`READY`/etc. — File Service owns this; Retrieval owns only its own internal processing state, §19).
* Enforce conversation or message ownership (Conversation Service's job).
* Generate the final LLM answer, manage prompts, or stream tokens (LLM Service's job).
* Maintain conversation memory (Memory Service's job) or graph relationships (Graph Service's job).
* Run its own embedding model in-process (embedding-service's job — see §1.1 correction #1).
* Grant or revoke file access — it only **checks** File Service's existing grants (§16), never writes to `file_permissions`.

---

## 5. Position in GraphGPT

```text
                              ┌────────────────────┐
                              │  Conversation Svc   │
                              └──────────┬──────────┘
                                         │ Kafka: chat.message.created
                                         │ (carries file_ids, per
                                         │  conversation-service task_update_13)
                                         ▼
                              ┌────────────────────┐
                              │     LLM Service     │
                              │  (Context Collector) │
                              └──────────┬──────────┘
                          asyncio.gather (parallel, always)
                   ┌──────────────────────┼──────────────────────┐
                   ▼                      ▼                      ▼
           ┌───────────────┐    ┌────────────────┐    ┌───────────────────┐
           │ Memory Service │    │  Graph Service  │    │ RETRIEVAL SERVICE │
           └───────────────┘    └────────────────┘    └─────────┬─────────┘
                                                                  │
                                              ┌───────────────────┼───────────────────┐
                                              ▼                   ▼                   ▼
                                         embedding-service      Milvus            OpenSearch
                                         (gRPC, MiniLM)     (shared cluster,     (dedicated index)
                                                              dedicated
                                                              collection)
```

For document ingestion:

```text
File Service ──Kafka: file.uploaded──▶ Retrieval Service ──S3 API──▶ MinIO
                                              │
                                              ▼
                                      File Service permission
                                      check NOT needed here —
                                      ingestion trusts owner_id
                                      from the event (File Service
                                      already verified it at
                                      upload time). Only QUERY-time
                                      access needs re-checking (§16),
                                      since the requester at query
                                      time may differ from the owner.
```

---

## 6. Service Dependencies

| Component | Direction | Protocol | Purpose |
|---|---|---|---|
| File Service | → Retrieval | Kafka (`file.uploaded`, `file.deleted`, `file.processing_retry`) | Ingestion triggers |
| Retrieval | → File Service | Kafka (`file.processing.started/completed/failed`) | Processing status (File Service is the consumer — see `file-service/app/events/processing_status_consumer.py`, already built and consuming exactly these three topics) |
| Retrieval | → File Service | **gRPC or REST, new** (§16) | Batch permission check: "does `user_id` have ≥VIEWER on these `document_id`s?" — **not yet implemented on File Service's side; a dependency this HLD imposes, not something already there** |
| Retrieval | → MinIO | S3 API | Download original object for parsing |
| Retrieval | → embedding-service | gRPC (`GenerateEmbedding`, `GenerateEmbeddings`) | Chunk + query embeddings |
| Retrieval | ↔ Milvus | pymilvus SDK | Dense vector search (shared cluster, dedicated collection) |
| Retrieval | ↔ OpenSearch | HTTP | BM25 search (dedicated index) |
| LLM Service | → Retrieval | gRPC (`GetRelevantChunks`, frozen proto) | Baseline context fetch, every turn |

---

## 7. Why Two Search Mechanisms? (unchanged from draft — sound reasoning)

Dense search catches conceptual/paraphrased matches ("how do we recover from failures" ↔ "retried with exponential backoff"); BM25 catches exact identifiers and jargon ("`file.processing.failed`") that embeddings can dilute. Combining them outperforms either alone — this part of the draft was correct and is kept as-is.

---

## 8. Kafka Event Contracts — Verified Against File Service's Actual Code

The draft approximated these payloads. Below is the **exact current schema** (`file-service/app/events/file_events.py`), not a paraphrase.

### 8.1 Consumed: `file.uploaded`

```json
{
  "event_id": "uuid",
  "event_type": "file.uploaded",
  "version": 1,
  "document_id": "uuid",
  "conversation_id": "uuid | null",
  "owner_id": "uuid",
  "filename": "string",
  "content_type": "string",
  "storage": {
    "provider": "minio",
    "bucket": "graphgpt-files",
    "key": "{owner_id}/{document_id}/v{n}/original.{ext}",
    "etag": "string"
  },
  "timestamp": "ISO-8601"
}
```

`version` here is the **event schema version** (always `1` today) — call it `event_schema_version` in Retrieval Service's own code and docs even though the wire field is literally named `version`, so nobody confuses it with the document's **content version**, a completely different number that this payload doesn't carry at all. **This is the gap flagged in §1.1 #6** — see §8.3.

### 8.2 Consumed: `file.deleted`

```json
{
  "event_id": "uuid",
  "event_type": "file.deleted",
  "version": 1,
  "document_id": "uuid",
  "owner_id": "uuid",
  "timestamp": "ISO-8601"
}
```

On receipt: delete all Milvus vectors and OpenSearch documents where `document_id` matches, across all chunk indices and all document-content-versions (§8.3 — since content version isn't in this event either, delete-all-for-document_id is the only safe interpretation).

### 8.3 Document-version handling — a real gap, with a two-phase fix (not a permanent workaround)

File Service supports multiple content versions per `document_id` (`file-service/app/services/version_service.py` — each new version gets a new object key `v{n}/original.{ext}` and a fresh `file.uploaded`-shaped event on completion, per that module's own docstring). **Neither `file.uploaded` nor `file.deleted` carries an explicit top-level version integer.** The only place the version number is recoverable is by parsing the `v(\d+)/` segment out of `storage.key`.

**Terminology, made explicit so the two numbers never get conflated:**

```text
event_schema_version   = the event envelope's own schema version (the "version" field
                          on the wire today — always 1, unrelated to document content)
content_version        = which upload attempt/edit of the document this event refers to
                          (currently NOT on the wire at all — recovered only by parsing
                          storage.key's "v{n}/" segment)
```

**Phase 1 (do this now, don't block on File Service):** parse `content_version` from `storage.key` with **strict validation** — reject (log + route to a dead-letter/manual-review path, don't silently default to `1`) any event whose key doesn't match the expected `{owner_id}/{document_id}/v(\d+)/` pattern, since a parse failure here means chunk identity (§11) cannot be computed safely.

**Phase 2 (file as a follow-up to File Service, not a hard prerequisite):** request File Service add an explicit `content_version: int` field to `FileUploadedEvent`/`FileDeletedEvent`, alongside the existing `event_schema_version` (`version`) field — so the two are unambiguous on the wire, not just in Retrieval's own internal naming. Once that ships, Retrieval reads `content_version` directly when present and **falls back to the Phase 1 key-parsing logic only for older/unmigrated events** — this is backward compatible and requires no coordinated cutover.

Retrieval Service should not treat Phase 1 as temporary scaffolding to be ripped out later — keep the parser and its strict validation permanently as a defensive fallback, even after Phase 2 ships.

### 8.4 `file.scan_failed` — why Retrieval never needs to handle it

File Service's quarantine/AV-scan flow (`file-service/app/services/quarantine_service.py`) is the **sole gate** before `file.uploaded` is ever emitted when `AV_SCAN_ENABLED=true`. An infected file gets `file.scan_failed` instead, and `file.uploaded` is **never** emitted for that `document_id` — confirmed directly in that module's own docstring ("file.uploaded is NEVER emitted for this document_id"). Retrieval Service does not subscribe to `file.scan_failed` at all; it simply never receives an ingestion trigger for a rejected file. No code needed here — documenting this so a future engineer doesn't add a defensive `file.scan_failed` consumer under the mistaken belief something is missing.

### 8.5 Consumed: `file.processing_retry`

**Current real payload** (`file-service/app/services/retry_service.py::_build_file_processing_retry_payload`, verified against the actual code, not assumed):

```json
{
  "event_id": "uuid",
  "event_type": "file.processing_retry",
  "version": 1,
  "document_id": "uuid",
  "retry_count": 1,
  "timestamp": "ISO-8601"
}
```

**Required change (Round 3, §1.3 #1) — this must carry `content_version`:**

```json
{
  "event_id": "uuid",
  "event_type": "file.processing_retry",
  "version": 1,
  "document_id": "uuid",
  "content_version": 3,
  "retry_count": 1,
  "timestamp": "ISO-8601"
}
```

**Why this is a real gap, not a hypothetical.** Retrieval's own job table is keyed `(document_id, content_version)` (HLD §11, §19) — a document with two uploaded versions has two independent `ingestion_jobs` rows, each potentially in a different state (e.g. `v1 = READY`, `v2 = FAILED`). File Service's retry route (`retry_service.retry_upload`) only ever checks `file.status == FAILED` against the file's **current** row and republishes against `file.current_version` implicitly (it never reads or forwards a version at all) — but nothing about the *event* tells Retrieval which `ingestion_jobs` row to act on. Today this happens to work only because File Service's own state machine makes `FAILED` unreachable except on the current version's row (an older, already-`READY` version can't be retried through this endpoint) — but that is an accident of File Service's current business rules, not a guarantee Retrieval's event contract should depend on silently. Making the field explicit removes the ambiguity structurally instead of relying on that coincidence continuing to hold.

File Service should populate `content_version` from `file.current_version` (the same field already used to build `storage_key`, `file-service/app/models/file.py:46`) — a one-line addition to `_build_file_processing_retry_payload`, not a new capability.

Emitted by File Service's `POST /files/{document_id}/retry` — re-signals Retrieval to reprocess the *existing* object (the bytes in MinIO are untouched; this is not a new version). Retrieval looks up the existing `ingestion_jobs` row by `(document_id, content_version)` and treats this identically to a `file.uploaded` retry — re-run the ingestion pipeline for that exact version, never inferred.

### 8.6 Produced: `file.processing.started` / `.completed` / `.failed`

These are consumed by File Service's already-built `processing_status_consumer.py` and drive its own `files.status` state machine (`UPLOADED → PROCESSING → READY`, or `→ FAILED`). Exact expected shapes (from `file-service/app/events/processing_status_consumer.py`'s handler, which reads `event_id`, `document_id`, and — for `.failed` only — `error_code`/`error_message`):

```json
// file.processing.started
{ "event_id": "uuid", "document_id": "uuid", "timestamp": "ISO-8601" }

// file.processing.completed
{ "event_id": "uuid", "document_id": "uuid", "chunk_count": 127, "timestamp": "ISO-8601" }

// file.processing.failed
{ "event_id": "uuid", "document_id": "uuid", "error_code": "PARSER_ERROR", "error_message": "string", "timestamp": "ISO-8601" }
```

File Service's consumer **dedupes on `event_id`** (its own `processed_event_ids` table, composite-keyed on `(event_id, consumer)`) — Retrieval must generate a fresh, unique `event_id` per publish, never reuse one across retries of the *same* publish attempt (a genuine retry of a failed Kafka send should reuse the id; a logically new attempt should not).

---

## 9. Parsing

Docling, as the draft specified — this matches the rest of GraphGPT's stack and is kept unchanged. Extract and preserve: page number, heading hierarchy, paragraph boundaries, table structure, section boundaries.

---

## 10. Chunking Strategy

Structure-aware recursive chunking, as the draft specified (target 400–500 tokens, ~600 max, 50–75 overlap, configurable) — kept unchanged. This part of the draft was sound.

---

## 11. Chunk Identity

Corrected per §8.3 — content version is not in the event, so it must be parsed from `storage.key`:

```text
content_version = parse_int(storage.key, pattern=r"/v(\d+)/")

chunk_id = uuid5(
    namespace=RETRIEVAL_CHUNK_NAMESPACE,
    name=f"{document_id}:{content_version}:{chunk_index}"
)
```

Deterministic on `(document_id, content_version, chunk_index)` — a redelivered `file.uploaded` for the same version produces the same `chunk_id`s, making both the Milvus upsert and the OpenSearch index write naturally idempotent (upsert-by-id, not insert). This directly satisfies Kafka's at-least-once guarantee without needing separate dedup bookkeeping for the ingestion path itself (though the `event_id`-level inbox pattern below is still needed to avoid redundant *work*, not just redundant *data*).

**Recommendation:** adopt the same inbox-dedup pattern already used by both `file-service` (`processed_event_ids`) and `conversation-service` (`inbox_events`) for consistency across the monorepo — a `processed_events(event_id, consumer, processed_at)` table, checked via `INSERT ... ON CONFLICT DO NOTHING` before doing any ingestion work, not just relying on chunk-id determinism (which prevents *bad data* but not *wasted embedding-service calls* on a redelivered event).

---

## 12. Embedding — via embedding-service (corrected, §1.1 #1)

```text
Ingestion (batch):
  chunks[] --gRPC GenerateEmbeddings--> embedding-service --> vectors[]

Query (single):
  query_text --gRPC GenerateEmbedding--> embedding-service --> query_vector
```

Both paths call the **same external service**, which is what actually guarantees index/query embedding consistency — not, as the draft implied, both paths sharing an in-process model instance. `embedding-service` already publishes its model identity in every response (`model: "all-MiniLM-L6-v2"`, `dimension: 384`) — Retrieval Service should **assert this matches its own configured expectation** on every response and refuse to index/query on a mismatch, as a cheap runtime guard against a silent embedding-service model upgrade breaking the index.

Practical implication: Retrieval Service's ingestion pipeline should batch chunk embedding calls (`GenerateEmbeddings`, plural) rather than one `GenerateEmbedding` call per chunk — embedding-service's own config (`BATCH_SIZE=64`, `MAX_BATCH_SIZE=256`) signals it's built for this.

---

## 13. Dense Index — Milvus (corrected, §1.1 #5)

**Shared Milvus cluster** (already running for `memory-service`) — Retrieval Service gets a **dedicated collection**, not a dedicated deployment:

```text
Collection: retrieval_document_chunks
```

Record schema:

```text
{
  chunk_id:         uuid5 (§11) — primary key, enables upsert-idempotent writes
  document_id:      uuid
  content_version:  int          (parsed per §8.3)
  chunk_index:      int
  owner_id:         uuid         (denormalized from file.uploaded — used for
                                   coarse pre-filtering; NOT the authorization
                                   check itself, see §16)
  conversation_id:  uuid | null
  filename:         string        (denormalized from file.uploaded, §14)
  page_number:      int | null
  section:          string | null
  content:          string
  embedding:        float[384]
}
```

Responsible for approximate nearest-neighbor semantic search only. Metadata pre-filtering (Milvus supports scalar filtering alongside ANN search) should apply `document_id IN (authorized_file_ids)` **after** §16's permission check has resolved the authorized set — never filter on raw `owner_id == user_id`, since that misses legitimately-shared files (File Service's `file_permissions` grants). **This filter alone is not sufficient** — see §13.3, added in Round 3: without an additional `content_version` constraint, a re-uploaded document's old and new versions are both retrievable simultaneously, which is a correctness bug, not a missing optimization.

### 13.1 Collection ownership on the shared cluster

Make ownership explicit and documented somewhere both teams can see it (this table, or wherever the Milvus cluster itself is provisioned/tracked) — not just implied by naming convention:

| Collection | Owner |
|---|---|
| `memory_*` | Memory Service |
| `retrieval_document_chunks` | Retrieval Service |

Confirm this with whoever owns the shared Milvus deployment before freezing the name (tracked as Open Item #4, §25) — but the name itself, and the ownership-table convention, are not blocked on that confirmation.

### 13.2 Collection/index creation is an infrastructure step, not application startup code

Retrieval Service pods must **not** create the Milvus collection (or its indexes) dynamically on their own startup. Two pods starting simultaneously against a not-yet-provisioned collection race on creation — unnecessary, avoidable coordination complexity for something that only needs to happen once, ever, per environment. Sequence instead:

```text
Infrastructure provisioning (migration/bootstrap script, run once per environment)
        ↓
Create collection `retrieval_document_chunks`
        ↓
Create its indexes (HNSW/IVF on the embedding field, scalar indexes on document_id/owner_id)
        ↓
Retrieval Service pods start and connect — never create
```

Note this is a deliberate *difference* from File Service's own `bucket_bootstrap.py`, which re-applies its desired-state bucket config on every pod boot (safe there because MinIO's bucket operations are naturally idempotent/race-tolerant — "create if not exists" on an already-existing bucket is a no-op regardless of how many pods attempt it concurrently). Milvus collection/index creation does not have the same guarantee across client versions, so treating it as a separate, single-run provisioning step (not per-pod-boot logic) is the safer default here, not a copy of File Service's pattern.

### 13.3 Current-version-only retrieval (Round 3, §1.3 #2 — a correctness fix, not an optimization)

**The gap.** File Service supports re-uploading a document to a new content version (HLD §8.3, §11). Each version gets its own `file.uploaded` event and its own set of chunks in Milvus/OpenSearch — by design, so that a `PARTIALLY_INDEXED`/`FAILED` version can be retried without disturbing an already-`READY` older version. But nothing before Round 3 stopped **both** versions from being searchable at once. Left unfixed, a user who re-uploads a corrected PDF can still get search results quoting the superseded version — indistinguishable, from the response alone, from a correct hit.

**The fix — only the current version participates in query-time search:**

```text
Milvus search filter   (§13, updated):
    document_id IN (authorized_file_ids) AND content_version = current_version[document_id]

OpenSearch search filter (§14, updated):
    same two-clause filter, same source for current_version
```

Older versions' chunks are **not deleted** when a new version supersedes them (deletion only happens on `file.deleted`, §21) — they simply stop being returned. This keeps the retry/reconciliation story simple: a `v2` that's still `PARTIALLY_INDEXED` doesn't need `v1` yanked out from under in-flight queries first, and a rolled-back or re-retried version doesn't need re-indexing from scratch.

**How Retrieval learns which version is current — without calling File Service per query (Round 3, §1.3 #3).** File Service already has this answer today: `files.current_version` (`file-service/app/models/file.py:46`), the same field `version_service.py` uses to build every new `storage_key`. The question this HLD has to answer is only *how that value reaches Retrieval* — and the answer is: **it already does, implicitly, in the event Retrieval already consumes.**

```text
Every file.uploaded event IS, by construction, a statement that
storage.key's v{n} is now the document's current version — File
Service only ever emits file.uploaded AFTER repointing
files.current_version at that same n (version_service.py's
complete_new_version, HLD §8.3's own storage.key parsing already
recovers n from this exact event)
```

So Retrieval maintains a small, Retrieval-owned projection table (`document_current_version(document_id, current_version)`, part of `ingestion_jobs`'s schema in the LLD, not a separate service call) updated on every successfully-consumed `file.uploaded`:

```text
Consume file.uploaded, parse content_version = n (§8.3)
        │
        ▼
UPDATE document_current_version
   SET current_version = n
 WHERE document_id = :document_id
   AND current_version < n        ← monotonic guard, not an unconditional overwrite
        │
        ▼
(then proceed with the normal ingestion job as before)
```

**The monotonic guard is load-bearing, not defensive boilerplate.** File Service's Kafka producer (`file-service/app/events/outbox_publisher.py`) does not key messages by `document_id` when publishing — `producer.send_and_wait(row.event_type, message)` takes no partition key — so on a multi-partition topic, two `file.uploaded` events for the same document (`v2` then `v3`) are **not guaranteed to arrive in order**. An unconditional "last event wins" projection would be wrong under reordering; the `current_version < n` guard makes the projection correct regardless of delivery order, without requiring File Service to change its partitioning strategy first. (Keying by `document_id` on File Service's side would still be a good independent improvement — reducing reordering *at the source* rather than only correcting for it at the consumer — but it is not a prerequisite for this fix, and is not something this HLD blocks on.)

Query-time, `dense_search.py`/`bm25_search.py` resolve `current_version` for each `document_id` in `authorized_file_ids` from this projection (a fast local Postgres lookup, not a File Service round trip — keeping this inside §17's latency budget) before building the two search filters above.

---

## 14. BM25 Index — OpenSearch

Dedicated index, same record shape as Milvus minus the embedding vector, plus `content_version`:

```text
Index: retrieval_document_chunks
```

```json
{
  "chunk_id": "...", "document_id": "...", "content_version": 1,
  "chunk_index": 17, "owner_id": "...", "conversation_id": "...",
  "filename": "architecture.pdf",
  "page_number": 8, "section": "Kafka Retry Strategy", "content": "..."
}
```

`filename` is denormalized from the originating `file.uploaded` event's own `filename` field (§8.1) — stored explicitly on every chunk record, not left implicit. This is what makes filename a first-class, independently-boostable BM25 field (distinct relevance weight from `content`/`section`), and it's also where `DocumentChunk.source_name` in the gRPC response (§18) is sourced from without a separate File Service lookup at query time.

BM25 operates over `content`, `section`, and `filename`, each independently boostable. Indexed by `chunk_id` (not auto-generated), so a redelivered ingestion event overwrites rather than duplicates — same idempotency property as §11/§13.

---

## 15. Database Ownership Boundary (unchanged in spirit from draft, restated precisely)

| Store | Owns | Owned by |
|---|---|---|
| File Service PostgreSQL | filename, owner, permissions, lifecycle status, storage pointer | File Service |
| MinIO | original bytes | File Service (Retrieval only ever reads) |
| Retrieval PostgreSQL | ingestion job state, retry counts, `processed_events` inbox table | Retrieval Service |
| Milvus (`retrieval_document_chunks`) | dense vector representation | Retrieval Service |
| OpenSearch (`retrieval_document_chunks`) | lexical index | Retrieval Service |

Neither Milvus nor OpenSearch is ever a source of truth for file ownership or access — that remains File Service's, and every query-time request re-derives authorization from it (§16), never from denormalized fields cached in the index.

---

## 16. Authorization — A Real Gap, and the Fix (§1.1 #4, new section)

### 16.1 The gap, stated precisely

1. A user attaches `file_ids: [doc_a, doc_b]` to a message in Conversation Service. Conversation Service stores and forwards these as **opaque UUIDs** — by explicit design decision (`conversation-service/docs/updates/task_update_13.md`), it never calls File Service to check whether the sending user actually owns or has been granted access to `doc_a`/`doc_b`.
2. This flows through `chat.message.created` → LLM Service's Context Collector → `GetRelevantChunksRequest.file_ids = [doc_a, doc_b]`, alongside `user_id` from the same request.
3. **If Retrieval Service also trusts `file_ids` at face value**, it will happily return chunks from `doc_b` even if `doc_b` belongs to a different user who never shared it — the requesting `user_id` is present in the request, but nothing has ever cross-checked it against `doc_b`'s actual `file_permissions` grants.

This is exploitable today by simply pasting a `document_id` UUID (observed, guessed, or leaked another way) into a chat message.

### 16.2 The fix

Retrieval Service is the **last and only remaining point** in this chain that can close this gap without re-architecting Conversation Service or LLM Service (both of which have made deliberate, documented decisions not to own file authorization). Before running any search:

```text
GetRelevantChunksRequest{user_id, file_ids, scope, ...}
        │
        ▼
Retrieval Service calls File Service:
   "for user_id, which of file_ids does it have >= VIEWER on?"
        │
        ▼
authorized_file_ids = <subset, possibly empty>
        │
        ▼
Proceed with search filtered to authorized_file_ids only —
unauthorized ids are silently dropped from scope, NOT an error
(matching the rest of this pipeline's degrade-don't-fail posture,
§17) — a chat message referencing a file the user can't see should
just retrieve nothing from it, not surface an authorization error
mid-conversation.
```

**This requires a new, small endpoint on File Service that does not exist yet** — e.g. `POST /internal/files/permissions/check` (service-to-service, not the public API), request `{user_id, document_ids: []}`, response `{authorized_document_ids: []}`. File Service already has every piece needed to build this trivially — `permission_service.assert_access` already does exactly this check, just currently only exposed one document at a time, over the public authenticated API. This HLD flags the dependency; implementing it is File Service work, tracked as a prerequisite for Retrieval Service's "Ask Files" mode to be safe to ship.

**This endpoint must not trust a bare `user_id` field as its only input.** An internal endpoint that takes `{user_id, document_ids}` at face value is itself a new authorization gap if *anything* can call it and claim to be checking on behalf of any `user_id` — that just moves the trust problem one hop over instead of closing it. The caller (Retrieval Service) must authenticate to File Service via **service-to-service credentials** (a service account token, or mTLS if the cluster already supports it for internal traffic) that identify *Retrieval Service itself* as the caller; `user_id` in the request body is then treated as data being asked about, not as something the caller is allowed to assert unchecked:

```text
Retrieval Service ──[service credential / mTLS]──▶ File Service
                        (proves: "this call really is
                         Retrieval Service, not an
                         arbitrary internal client")
                    ──{"user_id": "...", "document_ids": [...]}──▶
                        (data: "check access for this user,
                         who Retrieval learned about from a
                         separately-authenticated gRPC request")
```

**Defense in depth, not a shift in ownership.** Conversation Service's decision not to validate `file_ids` against File Service (`task_update_13.md`) is defensible as a local decision — it keeps Conversation Service simple and avoids a synchronous cross-service call on every message send. That decision does not need to be reversed. But it does mean Retrieval Service's check is not merely *a* layer of defense, it is currently **the only one** — so treat it as the authoritative check, not an optimization to skip under load. If Conversation Service later adds a cheap, non-blocking existence/ownership check of its own (e.g. at message-send time, async, not gating the send), that's additional defense in depth on top, not a replacement for Retrieval's own check.

### 16.3 `scope=KNOWLEDGE_BASE` and `scope=ALL` — a blocker for *baseline* retrieval, not just "Ask Files"

The frozen proto's `RetrievalScope` enum (`FILES | KNOWLEDGE_BASE | ALL`) implies retrieval can span more than just explicitly-attached files — e.g. "search everything I've ever uploaded to GraphGPT," not just this conversation's attachments. **This HLD does not invent a definition for `KNOWLEDGE_BASE`** — the proto exists in `llm-service` but neither its HLD nor LLD defines what a "knowledge base" scope actually enumerates (all of a user's files? an org-wide corpus? something graph-service-adjacent?).

**This is a bigger blocker than it first looks.** §1.1 #3 already established that Retrieval is called on **every** chat turn, in every mode — Default, Smart, Ask Files, all of it — via `asyncio.gather`, unconditionally. Since scope is a request-level field on every one of those calls, an undefined `KNOWLEDGE_BASE`/`ALL` scope isn't just a gap in one feature ("Ask Files") — it's undefined behavior in the **baseline retrieval path that every single message goes through**. Proceeding with a guessed definition risks building the wrong authorization boundary for whatever scope actually ends up meaning, and guessing wrong here is expensive to unwind once documents are indexed and queries depend on it.

**Recommendation for V1 — implement `FILES` scope only:**

```text
scope = FILES, file_ids = [...]
   → search restricted to those file_ids, filtered through §16.2's
     authorization check (the only scope with an unambiguous
     authorization boundary today)

scope = KNOWLEDGE_BASE or ALL
   → V1 behavior is FROZEN, not left as an implementation choice:
     treat identically to "no scope" (§16.4) — return an empty
     GetRelevantChunksResponse, plus a metric/log tag marking the
     request as "unsupported scope requested" for product visibility
     into real demand. NOT a gRPC error — LLM Service's Context
     Collector already has a degrade-to-empty path (§17) for a
     failed/circuit-broken call; reusing "empty" for "unsupported"
     means no new error-handling branch is needed on LLM Service's
     side, and a turn never fails just because a scope isn't built yet
```

`KNOWLEDGE_BASE`/`ALL` remain a tracked open item (§25) requiring a product decision before *implementing* them — but their V1 non-behavior (empty response, not an error) is decided now, not deferred.

### 16.4 No implicit global search — an explicit default, not an oversight

A direct consequence of §16.3: **"Retrieval is called every turn" must not be read as "Retrieval searches every document every turn."** The two are independent decisions. This HLD fixes the default explicitly:

```text
No file_ids supplied (or scope not yet implemented, per §16.3)
        │
        ▼
Return an empty GetRelevantChunksResponse — chunks: [], total_tokens: 0
        │
        ▼
NOT: "search across all of the user's authorized documents by default"
```

A user chatting normally, with no files attached, gets empty Retrieval context — not a silent implicit search across their entire upload history. This matters for reasons beyond just "scope is undefined":

* **Privacy** — a user's mental model of "I didn't attach anything" should match what actually happens; an implicit global search over everything they've ever uploaded is a surprising, unrequested behavior.
* **Latency** — every chat turn already pays Memory + Graph + Retrieval in parallel against a shared <1500ms budget (§17); an unscoped Milvus/OpenSearch search over a user's entire corpus is strictly more expensive than a `file_ids`-scoped one, for zero benefit when the user didn't ask for document grounding at all.
* **Load** — multiplying "every chat turn" by "full-corpus search" is a materially different capacity-planning problem for Milvus/OpenSearch than "every chat turn, but usually zero-or-few-file-scoped."
* **Authorization complexity** — a `file_ids`-scoped check (§16.2) is a bounded, cheap batch lookup; an implicit "everything this user can see" search would need the same check expanded to the user's *entire* grant set on every single turn, for no clear product benefit until `KNOWLEDGE_BASE` is actually defined.

If Product later wants "search everything I've uploaded" as a real, intentional feature, that is exactly what defining `KNOWLEDGE_BASE` (§16.3) is for — implemented deliberately, not backed into as the default behavior for an unspecified enum value.

---

## 17. Query-Time SLA — Verified Against LLM Service's Real Retry/Circuit-Breaker Policy (§1.1 #3)

The draft's "graceful degradation" language (§37 of the original) was correct in spirit but had no real numbers. These are not invented — they're `llm-service`'s actual, already-implemented policy (`llm-service/docs/hld2.md` §20–21) that Retrieval Service must design to survive being subjected to:

| Parameter | Value | Source |
|---|---|---|
| Per-call gRPC deadline | 2000ms default | hld2.md §14 |
| Retry attempts | 2, exponential backoff + jitter, 100ms base | hld2.md §20.2 |
| Circuit breaker trip | 5 consecutive failures within 30s | hld2.md §21.2 |
| Behavior when circuit open | Context Collector proceeds with Retrieval context = empty; **request is never blocked** | hld2.md §21.2 |
| Downstream latency budget | p95 time-to-first-token < 1500ms, **shared** across Memory + Graph + Retrieval fetched in parallel (`asyncio.gather`) — bounded by the *slowest* of the three, not their sum | hld2.md §37.1, §2993 |

**Implications for Retrieval Service's own design:**

* Retrieval's `GetRelevantChunks` p95 should target well under 2000ms — realistically a few hundred ms, since it's competing with Memory/Graph for the same shared 1500ms TTFT budget and any of the three being slow drags the whole turn.
* This is the load-bearing justification for **no cross-encoder reranker** (§1.1, §18): a cross-encoder pass over 30–50 candidates is exactly the kind of stage that would blow this budget. The draft's "no reranker" decision is *correct*, and now provably so against a real number, not a preference.
* Retrieval Service must **never let a Milvus or OpenSearch failure block the whole request** — implement the same fallback the draft described (dense-only or BM25-only degradation) internally, so that even a *partial* internal failure still returns *something* within budget, rather than relying solely on LLM Service's outer circuit breaker (which only degrades to fully-empty, losing more signal than necessary).
* Given `user_id`-scoped authorization now sits in the request path (§16), that File Service permission-check call must **also** fit inside this budget — it should be a fast, indexed lookup (File Service's `file_permissions` table is already indexed on `principal_id`), not a chain of individual per-file calls. Batch it in one round trip.

**On caching the permission check — don't, yet.** It's tempting to cache `(user_id, document_id) → authorized` in Redis with a short TTL to shave latency off the hot path. For V1, **don't** — batch-check File Service on every single request, unconditionally:

```text
V1:  GetRelevantChunks → File Service batch permission check (every request, no cache) → search
```

Reasons to hold off rather than build the cache preemptively:

* A revoked grant (owner removes a `VIEWER` from `file_permissions`) should take effect on the *next* request, not after some TTL expires — correctness of an authorization check is a poor place to trade for latency before there's evidence the latency actually needs trading.
* This is a new dependency (Redis in the critical authorization path) added on a guess, not on measurement — exactly the kind of premature complexity worth avoiding until the batch-check-every-request approach is actually shown to be a bottleneck.
* If it does turn out to matter, the natural place is a cache scoped to the **lifetime of a single request's context collection**, not a cross-request Redis cache with its own invalidation problem — since within one `GetRelevantChunks` call the same `(user_id, document_id)` pairs aren't re-checked twice anyway (the batch call already resolves the whole set once per request).

**Sequence: batch-check every request first, measure, then decide** — not the other way around.

---

## 18. Query Pipeline — Fusion & Diversification (kept from draft, now grounded — with one critical gap closed)

```text
                          GetRelevantChunksRequest
                                    │
                                    ▼
                    Scope check (§16.3/§16.4) — not FILES, or no
                    file_ids → EMPTY response, return immediately
                                    │
                                    ▼ (scope == FILES, file_ids non-empty)
                    Authorization (§16.2 — batch File Service check) —
                    zero authorized ids → EMPTY response, return immediately
                                    │
                                    ▼
                    Resolve current_version per document_id (§13.3 —
                    local projection lookup, NOT a File Service call)
                                    │
                                    ▼
                    Query Embedding (gRPC, embedding-service, §12)
                                    │
                       ┌────────────┴────────────┐
                       ▼                         ▼
                  Dense Search               BM25 Search
                  (Milvus, Top-N)          (OpenSearch, Top-N)
                       │                         │
                       └────────────┬────────────┘
                                    ▼
                                 Dedupe
                                    │
                                    ▼
                    Reciprocal Rank Fusion (RRF)
                                    │
                                    ▼
                Fetch candidate embeddings (§18.1 — from Milvus,
                by the chunk_ids already in the fused candidate set;
                never re-call embedding-service per candidate)
                                    │
                                    ▼
                                   MMR
                                    │
                                    ▼
                              Top-K (5–8)
                                    │
                                    ▼
                        GetRelevantChunksResponse
```

Dense and BM25 search run **concurrently** (`asyncio.gather` or equivalent), not sequentially — this was implicit in the draft's diagram but is made explicit here since it's a real latency requirement given §17's shared budget, not just a nice-to-have.

**RRF over weighted-score fusion** (draft §22's stated preference) — kept, and reinforced: RRF avoids assuming Milvus cosine-similarity scores and OpenSearch BM25 scores are on comparable distributions, which they are not, and avoids needing per-deployment tuning of a fusion weight `α` that would silently go stale as the corpus grows.

**MMR over a cross-encoder reranker** — kept, now justified against the real latency budget in §17 rather than a general preference. Suggested starting parameters unchanged from the draft: candidate pool 30–50, final K 5–8, `λ = 0.7` — these should be benchmarked against GraphGPT's actual corpus once there's real query traffic, not treated as final.

### 18.1 MMR needs candidate-to-candidate similarity — the single most important technical gap in the original draft

MMR's whole mechanism is:

```text
MMR(candidate) = λ · relevance(candidate) − (1 − λ) · max_similarity(candidate, already_selected)
```

That `max_similarity` term requires comparing the **embedding vectors** of the RRF-fused candidates against each other — not just their relevance scores. The original draft described MMR conceptually but never stated where these candidate vectors actually come from, which would have been a real implementation blocker the first time someone sat down to write the MMR step: query embedding alone is not enough; MMR is fundamentally a candidate-vs-candidate operation.

**The fix is simple and cheap, precisely because of two facts already true elsewhere in this HLD:** every chunk's embedding is already sitting in Milvus (§13's record schema includes `embedding: float[384]`), and the candidate pool at this stage is small (30–50 chunks, 384 dimensions each). So:

```text
RRF-fused candidate set (chunk_ids, up to ~50)
        │
        ▼
Milvus: fetch by chunk_id (a primary-key lookup, not a fresh ANN search —
        cheap and fast; every chunk_id in this set is already known to
        exist in Milvus, since dense search is one of the two sources
        that produced this candidate set in the first place)
        │
        ▼
{ chunk_id → embedding } for the whole candidate set
        │
        ▼
MMR computes candidate-vs-candidate cosine similarity locally, in-process
```

**Do not** call embedding-service again for this — the candidates' embeddings already exist from ingestion time; re-embedding them at query time would be redundant work, and at 30–50 calls per query it would also meaningfully eat into the §17 latency budget for no reason. The only chunks that legitimately need a *fresh* embedding-service call in the query path are none — only the query text itself does (§12), once, per request.

### 18.2 What `relevance_score` actually means — precise, not just "post-fusion, post-MMR"

The draft's "post-fusion, post-MMR score" was directionally right but underspecified. Track these explicitly, internally, per candidate, and be precise about which one ends up on the wire:

```text
Per candidate, tracked internally through the pipeline:
  dense_score    — raw Milvus cosine similarity (or distance, however Milvus reports it)
  dense_rank     — this candidate's rank position in the dense Top-N (1 = best)
  bm25_score     — raw OpenSearch BM25 score
  bm25_rank      — this candidate's rank position in the BM25 Top-N (1 = best)
  rrf_score      — computed from dense_rank + bm25_rank (§18's RRF formula) —
                   a candidate present in only one of the two lists still gets
                   an rrf_score, just without a contribution from the list it's
                   absent from
  mmr_score      — the MMR selection score (relevance term minus the
                   diversity penalty against already-selected chunks) —
                   only computed for candidates MMR actually considers/
                   selects, in selection order

DocumentChunk.relevance_score (on the wire) = rrf_score
                                               — NOT dense_score, NOT bm25_score,
                                                 NOT mmr_score
```

**Why `rrf_score`, not `mmr_score`, reaches the wire.** `mmr_score` is *selection-order-dependent* — it's computed relative to whichever chunks MMR has already picked, so the same chunk can carry a different `mmr_score` depending on what got selected before it. That makes it a good internal signal for *which* chunks to keep and diversify away from, but a poor fit for a field LLM Service treats as "how relevant is this chunk to the query" in isolation (§18.2 below — it trims context by this field). `rrf_score` has no such dependency: it's a pure function of the chunk's own dense/BM25 rank, independent of anything else in the candidate set. So the split is:

```text
MMR   → decides WHICH chunks survive and in what order the response lists them
rrf_score → decides HOW RELEVANT each surviving chunk is reported as being
```

Both still ship: the response list order is MMR's selection order (diversity-aware), but each chunk's `relevance_score` field is its own `rrf_score`, not a function of its position in that order.

Keeping all five internally (even though only one reaches the proto) matters for two concrete reasons: it makes the pipeline debuggable (a chunk that scored well on BM25 but got excluded by MMR's diversity penalty is a very different situation from one that never made the RRF-fused candidate set at all, and only having the final number can't distinguish them), and it's what any future tuning/benchmarking of `α`-free RRF or MMR's `λ` will need to evaluate against.

**Response mapping** — the fused/diversified chunks must map onto the *actual* frozen response type, not the draft's looser "conceptual record":

```text
internal chunk { chunk_id, content, document_id, filename,
                  rrf_score, mmr_score, page_number, section, chunk_index }
        │
        ▼
MMR selects the final Top-K set (membership + diversity only —
does NOT determine final response order, see below)
        │
        ▼
Sort the selected set by rrf_score, descending
        │
        ▼
proto DocumentChunk {
  chunk_id         = internal.chunk_id
  content          = internal.content
  source_file_id   = internal.document_id
  source_name      = internal.filename
  relevance_score  = internal.rrf_score          // §18.2 — NOT mmr_score
  metadata         = {                           // protobuf map<string,string> —
    "page_number": str(internal.page_number),    // everything must be stringified
    "section": internal.section,
    "chunk_index": str(internal.chunk_index),
  }
}
```

MMR's selection process is order-agnostic for the response: it decides *which* 5–8 chunks survive (diversity-aware), not the order they're returned in. The response list itself is explicitly re-sorted by `rrf_score` descending as a separate, final step — never returned in raw MMR-selection order, since that order reflects diversity trade-offs, not relevance.

`GetRelevantChunksResponse.total_tokens` must be computed against the **same tokenizer** the target LLM provider uses (or a reasonable proxy) — this budget is what LLM Service's Context Window Manager trims against (`hld2.md` §16.2, trimming "lowest-relevance-scored chunks first" — meaning Retrieval's chunks must already arrive **sorted by `relevance_score` (i.e. `rrf_score`) descending**, since LLM Service trims from the tail without re-sorting).

---

## 19. Internal Processing State (renamed/clarified from draft §30)

The draft's bare arrow-chain implied a single linear progression, which doesn't have anywhere to represent §20's partial-index-failure case cleanly. Explicit states instead:

```text
NOT_STARTED         — row exists (or about to), no work has begun
PROCESSING          — actively downloading/parsing/chunking/embedding/indexing
                       (a coarser bucket is fine for the job's own status column;
                       §20's per-stage embedding/milvus/opensearch tracking is
                       separate, finer-grained bookkeeping within this state)
PARTIALLY_INDEXED   — §20's case: one of {Milvus, OpenSearch} succeeded, the
                       other failed — deliberately distinct from both COMPLETED
                       (would overstate readiness) and FAILED (would understate
                       it and trigger an unnecessary full re-parse on retry)
READY               — both Milvus and OpenSearch writes confirmed complete
                       (renamed from the draft's "COMPLETED" to read consistently
                       with File Service's own terminal-state naming, e.g. its
                       files.status READY — not the same state, just the same word)
FAILED              — unrecoverable without manual intervention or a fresh retry
                       signal (file.processing_retry, §8.5)
DELETING            — file.deleted received, purge from Milvus/OpenSearch in flight
DELETED             — purge confirmed complete
```

`PARTIALLY_INDEXED` is what §20's retry-only-the-failed-stage behavior actually needs a named state for — without it, "Milvus succeeded, OpenSearch failed" has nowhere honest to live between `PROCESSING` and either terminal state.

This state machine is independent of, and never written back into, File Service's own `files.status` — the only coupling between the two is the three events in §8.6 (Retrieval's `READY`/`FAILED` transitions are what *cause* File Service to publish its own `PROCESSING → READY`/`FAILED` transition on File Service's side, via those events — but the state values themselves are two separate, uncoupled state machines that happen to use similar names for similar concepts).

---

## 20. Partial Index Failure (kept from draft, sound — now maps onto §19's explicit state)

Track dense/lexical indexing completion independently:

```text
embedding: COMPLETE | milvus: COMPLETE | opensearch: FAILED → overall: PARTIALLY_INDEXED (§19)
                                                              → retry only opensearch, not a full re-parse/re-embed
```

`PARTIALLY_INDEXED` is a legitimate, load-bearing terminal-ish state, not just an implementation detail — it's what the job repository's status column actually stores while the retry-only-the-failed-stage logic is pending. Only once *both* Milvus and OpenSearch confirm complete does the job move to `READY`, and only then does Retrieval publish `file.processing.completed` (§8.6) — a `PARTIALLY_INDEXED` document must never trigger that event, since File Service (and everything downstream of it, including query-time retrieval itself) should not treat the document as fully searchable until both indexes genuinely agree it is.

---

## 21. Delete Consistency

On `file.deleted` (§8.2): delete from both Milvus and OpenSearch by `document_id` (all `content_version`s and `chunk_index`es — no version filter, since the event carries none). Idempotent — a redelivered `file.deleted` for an already-purged `document_id` is a no-op, not an error.

---

## 22. Horizontal Scaling (kept from draft, sound)

Ingestion workers (Kafka consumer group, scaled on lag) and query servers (gRPC, scaled on request rate) scale independently — a burst of uploads must never degrade query latency for unrelated in-flight chat turns, and vice versa.

---

## 23. Internal Architecture

```text
retrieval-service/
├── protos/
│   └── retrieval.proto            # copied verbatim from llm-service, not re-derived
├── app/
│   ├── grpc/
│   │   └── retrieval_servicer.py  # implements GetRelevantChunks
│   ├── events/
│   │   ├── consumers.py           # file.uploaded / file.deleted / file.processing_retry
│   │   └── producers.py           # file.processing.started/completed/failed (outbox pattern,
│   │                               #  matching file-service's/conversation-service's own convention)
│   ├── ingestion/
│   │   ├── downloader.py          # MinIO S3 client
│   │   ├── parser.py              # Docling wrapper
│   │   ├── chunker.py             # structure-aware recursive
│   │   └── indexer.py             # writes Milvus + OpenSearch, idempotent by chunk_id
│   ├── query/
│   │   ├── dense_search.py
│   │   ├── bm25_search.py
│   │   ├── fusion.py              # RRF
│   │   └── mmr.py
│   ├── clients/
│   │   ├── embedding_client.py    # gRPC to embedding-service
│   │   └── file_service_client.py # gRPC/HTTP permission-check (§16, new dependency)
│   ├── repositories/
│   │   ├── job_repository.py      # ingestion state (§19)
│   │   └── processed_event_repository.py  # inbox pattern (§11), matching
│   │                               #  file-service's processed_event_ids /
│   │                               #  conversation-service's inbox_events
│   ├── models/
│   ├── core/                      # config, logging, exceptions
│   └── main.py / lifespan.py
```

---

## 24. Technology Stack

| Layer | Technology | Note |
|---|---|---|
| Service | FastAPI + Python | matches the rest of the monorepo |
| Query API | gRPC | contract owned by `llm-service`'s frozen proto, not this service |
| Event bus | Kafka | outbox/inbox pattern, matching file-service/conversation-service convention |
| Object storage | MinIO | read-only from Retrieval's side |
| Parser | Docling | |
| Chunking | Structure-aware recursive | |
| Embedding | `all-MiniLM-L6-v2`, via **embedding-service** (gRPC) | **not local** — §1.1 #1 |
| Dense vector DB | Milvus, **shared cluster**, dedicated collection | §1.1 #5 |
| Lexical search | OpenSearch (BM25) | dedicated index |
| Fusion | Reciprocal Rank Fusion | |
| Diversification | MMR | no reranker — justified in §17 |
| Processing state | PostgreSQL | Retrieval-owned, separate from File Service's |
| Permission check | File Service (new endpoint, §16) | **not yet implemented — a dependency this HLD creates** |
| Observability | Prometheus metrics, structured logs, tracing | matching monorepo convention (`trace_id` already threaded through the gRPC request) |

---

## 25. Open Items — Final Status (Round 3, do not silently resolve — confirm before implementation)

| # | Item | Status |
|---|---|---|
| 1 | File Service permission-check endpoint (§16.2) | 🔴 **Must build** — blocks safe shipping of baseline retrieval with any `file_ids` scope, not just "Ask Files" (§16.3 raised this from a feature-level to a baseline-path blocker) |
| 2 | `RetrievalScope.KNOWLEDGE_BASE` / `ALL` semantics (§16.3) | 🔴 **Product decision required** before implementing either — V1 ships `FILES` scope only (§16.3), with an explicit empty-context default for anything else (§16.4) |
| 3 | `content_version` not in `file.uploaded`/`file.deleted` (§8.3) | 🟠 **Two-phase fix**: parse from `storage.key` now (with strict validation, kept permanently as a fallback), request File Service add an explicit `content_version` field as a follow-up — not a hard prerequisite for starting implementation |
| 4 | Milvus collection provisioning (§13.1) | 🟢 **Get infrastructure sign-off** on the `retrieval_document_chunks` name and the "provisioned once by infra, never created by application pods" rule (§13.2) — not a design blocker |
| 5 | MMR needs candidate embeddings, fetched from Milvus (§18.1) | 🟠 **Resolved in Round 2** — fetch by `chunk_id` from Milvus, never re-call embedding-service per candidate |
| 6 | `relevance_score` precisely defined (§18.2) | 🟠 **Resolved in Round 2** — internal pipeline tracks `dense_score`/`bm25_score`/`dense_rank`/`bm25_rank`/`rrf_score`/`mmr_score` separately; `rrf_score` (not `mmr_score`) reaches the wire, since `mmr_score` is selection-order-dependent — MMR decides Top-K membership/diversity only, the response is then sorted by `rrf_score` descending |
| 7 | `file.processing_retry` doesn't identify which `content_version` to retry (§1.3 #1, §8.5) | 🔴 **File Service must add `content_version` to the payload** — a one-line change (`file.current_version` is already read elsewhere in the same module), tracked as a File Service prerequisite the same way item 1 is; Retrieval cannot safely retry a specific `ingestion_jobs` row without it |
| 8 | Old content versions remained searchable alongside the current one (§1.3 #2, §13.3) | 🟢 **Resolved this round** — query-time search filters on `content_version = current_version[document_id]`, resolved from a Retrieval-owned projection kept current off `file.uploaded` (§13.3), not a File Service call |

**Bottom line:** the core architecture (embedding-service for MiniLM, the frozen `GetRelevantChunks` proto, Milvus + OpenSearch + RRF + MMR with no reranker, File Service's event contracts) is frozen as of this round. Items 1, 2, and 7 are genuine blockers — 1 and 7 because File Service must ship a small, concrete change before Retrieval can be fully correct (a live security gap and a versioning-retry ambiguity, respectively), 2 because it leaves baseline retrieval's own request contract partially undefined. Item 8 is resolved without any cross-service dependency. Items 3–6 are refinements to implement alongside the initial build, not prerequisites to starting it.

---

## 26. Frozen Architecture — Final Reference Diagrams

These two diagrams are the implementation reference — everything above explains the *why* behind each box; these are the *what* to build against.

### 26.1 Ingestion

```text
                FILE INGESTION

File Service
     │
     │ file.uploaded
     ▼
    Kafka
     │
     ▼
 Retrieval Service
     │
     ▼
   MinIO  (download original)
     │
     ▼
  Docling  (parse)
     │
     ▼
Structure-Aware
Recursive Chunking
     │
     ▼
embedding-service
 (gRPC, batch, MiniLM-L6-v2)
     │
     ├──────────────┐
     ▼              ▼
  Milvus        OpenSearch
  Dense            BM25
(retrieval_    (retrieval_
 document_      document_
 chunks)        chunks)
     │              │
     └──────┬───────┘
            ▼
     READY  (§19 — both confirmed,
             only then publish
             file.processing.completed)
```

### 26.2 Query

```text
                QUERY

LLM Service
     │
     │ GetRelevantChunks (frozen proto)
     ▼
 Retrieval Service
     │
     ▼
Scope check (§16.3) — scope != FILES, or file_ids empty?
     │
     ├── yes ──▶ EMPTY response (chunks: [], total_tokens: 0) — return
     │           immediately, before any embedding/search/permission call
     │
     ▼ no
File Service
Permission Check (§16.2 — batch, every request, no cache in V1)
     │
     ├── zero authorized_file_ids ──▶ EMPTY response — return immediately
     │
     ▼
Resolve current_version per document_id
(§13.3 — local projection, not a File Service call)
     │
     ▼
Query Embedding (gRPC, embedding-service)
     │
     ├──────────────┐
     ▼              ▼
  Milvus        OpenSearch
  Top-N           Top-N
  (filtered: document_id IN authorized_file_ids
   AND content_version = current_version, §13.3)
     │              │
     └──────┬───────┘
            ▼
        Dedupe → RRF
            ▼
  Fetch candidate embeddings
     from Milvus (§18.1)
            ▼
           MMR (selects Top 5–8, diversity-aware)
            ▼
  Sort selected set by rrf_score, descending
     (relevance_score = rrf_score, §18.2 —
      NOT mmr_score, which is selection-order-dependent)
            │
            ▼
       LLM Service
```

**Short-circuit before any expensive work.** The scope check and the permission check are both cheap, and both can end the request with an empty response before Retrieval ever calls embedding-service or touches Milvus/OpenSearch. This matters because §16.4 established that "no `file_ids`" is the *common* case, not an edge case — most chat turns won't have attachments, so the hot path for the majority of requests should be two fast checks and a return, not a full embed-search-fuse-diversify pipeline that's discarded anyway.
