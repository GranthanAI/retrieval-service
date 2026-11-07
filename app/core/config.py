import json
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- Service identity ---
    SERVICE_NAME: str = "retrieval-service"
    ENVIRONMENT: str = "development"
    DEPLOYMENT_MODE: str = "api"  # "api" | "worker" | "all" — see LLD §9, drives lifespan.py's branch

    # --- gRPC server (DEPLOYMENT_MODE in {api, all}) ---
    GRPC_PORT: int = 50060
    GRPC_MAX_WORKERS: int = 10
    GRPC_MAX_MESSAGE_LENGTH_BYTES: int = 4 * 1024 * 1024

    # --- FastAPI (health/metrics — runs in every mode) ---
    HTTP_PORT: int = 8060

    # --- PostgreSQL (job + inbox + outbox state) ---
    DATABASE_URL: str = "postgresql+asyncpg://postgres:1234@localhost:5432/retrieval_service_db"
    DB_POOL_SIZE: int = 10
    DB_POOL_MAX_OVERFLOW: int = 5
    DB_ECHO: bool = False

    # --- Kafka (DEPLOYMENT_MODE in {worker, all}) ---
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP: str = "retrieval-service"
    KAFKA_CONSUMER_POLL_TIMEOUT_MS: int = 1000
    KAFKA_TOPIC_FILE_UPLOADED: str = "file.uploaded"
    KAFKA_TOPIC_FILE_DELETED: str = "file.deleted"
    KAFKA_TOPIC_FILE_PROCESSING_RETRY: str = "file.processing_retry"
    KAFKA_TOPIC_PROCESSING_STARTED: str = "file.processing.started"
    KAFKA_TOPIC_PROCESSING_COMPLETED: str = "file.processing.completed"
    KAFKA_TOPIC_PROCESSING_FAILED: str = "file.processing.failed"
    OUTBOX_BATCH_SIZE: int = 50
    OUTBOX_POLL_INTERVAL_SECONDS: float = 1.0

    # --- Ingestion concurrency (worker only) ---
    INGESTION_MAX_CONCURRENCY: int = 4

    # --- MinIO (read-only source — reuses file-service's bucket) ---
    MINIO_ENDPOINT: str = "localhost:9010"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin123"
    MINIO_SECURE: bool = False
    MINIO_BUCKET_FILES: str = "graphgpt-files"
    MINIO_DOWNLOAD_TIMEOUT_SECONDS: float = 30.0

    # --- embedding-service ---
    EMBEDDING_SERVICE_GRPC_TARGET: str = "localhost:50054"
    EMBEDDING_MODEL_EXPECTED: str = "all-MiniLM-L6-v2"
    EMBEDDING_DIMENSION_EXPECTED: int = 384
    EMBEDDING_BATCH_SIZE: int = 64
    EMBEDDING_INGESTION_TIMEOUT_SECONDS: float = 20.0
    EMBEDDING_QUERY_TIMEOUT_SECONDS: float = 0.4
    EMBEDDING_RETRY_ATTEMPTS: int = 2

    # --- File Service permission-check client (HLD §16.2) ---
    # REST, not gRPC — File Service is a FastAPI/HTTP service (no gRPC
    # server), and HLD §16.2's own documented contract is
    # `POST /internal/files/permissions/check`. Port 8006 matches
    # file-service's own local-dev convention (Makefile's `make run`).
    FILE_SERVICE_BASE_URL: str = "http://localhost:8006"
    FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS: float = 0.3
    FILE_SERVICE_SERVICE_CREDENTIAL: str = ""

    # --- Milvus (shared cluster, dedicated collection) ---
    # --- Docling: PDF / Word / PowerPoint / Excel / HTML parsing (app/ingestion/docling_config.py) ---
    # DOCLING_ENABLED=false refuses those file types with a clear message and never imports Docling (text, Markdown, CSV and code still work).
    # DOCLING_TABLE_STRUCTURE / DOCLING_TABLE_MODE change what is extracted: apply them with a NEW index generation (parser_version records them).
    DOCLING_ENABLED: bool = True
    DOCLING_TABLE_STRUCTURE: bool = True
    DOCLING_TABLE_MODE: Literal["", "fast", "accurate"] = ""      # "" = Docling's own default
    DOCLING_DEVICE: Literal["auto", "cpu", "cuda", "mps", "xpu"] = "auto"
    DOCLING_NUM_THREADS: int = 0                                  # 0 = Docling's default
    DOCLING_DOCUMENT_TIMEOUT_SECONDS: float = 0.0                 # 0 = no limit
    DOCLING_REUSE_CONVERTER: bool = False                         # one shared converter (parses serialised) instead of one per file

    # --- Phase 6: selective OCR (off by default; see app/ingestion/ocr.py) ---
    OCR_ENABLED: bool = False
    OCR_MIN_CHARS_PER_PAGE: int = 20          # a PDF page with fewer extracted characters is treated as having no text layer
    OCR_MAX_PAGES_PER_DOCUMENT: int = 50      # cost cap per document

    # --- Phase 6: contextual chunk text (off by default; see app/ingestion/contextual.py) ---
    CONTEXTUAL_CHUNKS_ENABLED: bool = False
    CONTEXT_MAX_TOKENS: int = 60              # room reserved per chunk for the 1-2 sentence description
    CONTEXT_LLM_URL: str = ""                 # OpenAI-compatible chat completions endpoint of a cheap model; empty = not configured
    CONTEXT_LLM_MODEL: str = ""
    CONTEXT_LLM_API_KEY: str = ""
    CONTEXT_LLM_TIMEOUT_SECONDS: float = 20.0
    CONTEXT_LLM_CONCURRENCY: int = 4

    # --- Index generations (app/stores/generations.py) ---
    GENERATION_REFRESH_SECONDS: float = 30.0       # how quickly a running process notices `make activate`
    OPENSEARCH_ACTIVE_ALIAS: str = "retrieval_document_chunks_active"   # reads go through this alias once a generation is activated

    MILVUS_HOST: str = "localhost"
    MILVUS_PORT: int = 19530
    MILVUS_COLLECTION_NAME: str = "retrieval_document_chunks"
    MILVUS_SEARCH_TOP_N: int = 40
    # HNSW search breadth. Sent as `ef` and never below the requested limit (Milvus rejects ef < limit). Replaces the IVF-only
    # MILVUS_SEARCH_NPROBE, which an HNSW index ignores.
    MILVUS_HNSW_EF: int = 64
    MILVUS_INDEX_METRIC_TYPE: str = "COSINE"
    MILVUS_CONNECT_TIMEOUT_SECONDS: float = 5.0
    MILVUS_QUERY_TIMEOUT_SECONDS: float = 0.5

    # --- OpenSearch ---
    OPENSEARCH_HOSTS: str = "localhost:9200"
    OPENSEARCH_INDEX_NAME: str = "retrieval_document_chunks"
    OPENSEARCH_SEARCH_TOP_N: int = 40
    OPENSEARCH_QUERY_TIMEOUT_SECONDS: float = 0.5

    # --- Query pipeline tuning ---
    FUSION_RRF_K: int = 60
    MMR_CANDIDATE_POOL_SIZE: int = 40
    MMR_FINAL_TOP_K: int = 6
    MMR_LAMBDA: float = 0.7
    # Relevance (RRF score) is min-max normalised to 0-1 inside the candidate pool before MMR, and only candidates at or above
    # this normalised relevance take part in diversification. Set to 0 to diversify over the whole pool.
    MMR_RELEVANCE_FLOOR: float = 0.5
    # A candidate whose section heading contains a word pair from the query verbatim ("phase 6") gets one extra rank-1 RRF vote
    # (1 / (FUSION_RRF_K + 1)) times this factor. 0 disables it.
    HEADING_MATCH_BOOST: float = 1.0
    # --- Phase 3: reranker (embedding-service Rerank RPC) ---
    # Order when on: dense + BM25 (RERANK_LEG_TOP_N each) -> dedupe -> RRF -> top RERANK_TOP_N -> rerank -> per-file balance -> MMR
    # (reranker scores as relevance) -> top_k -> max_tokens. On timeout or error the Phase 1 path runs and a metric is emitted.
    RERANK_ENABLED: bool = False
    # Which reranker: a Hugging Face id sent to embedding-service with every Rerank call. It must be enabled there (RERANKER_MODEL or
    # RERANKER_ALLOWED_MODELS), else the call fails and the query falls back to the non-reranked path. "" = embedding-service's default.
    RERANK_MODEL: str = ""
    # Coverage thresholds PER reranker, because score scales differ by model (cross-encoder/ms-marco-MiniLM-L-6-v2 returns raw logits, about -11
    # to +4 here). JSON: {"<RERANK_MODEL>": {"weak_min": x, "strong_min": y}}; the key "default" is used when RERANK_MODEL is empty. A score
    # >= strong_min is STRONG, >= weak_min is WEAK, otherwise NONE. A reranker with no entry produces NO coverage signal (never a guess).
    # EMPTY BY DEFAULT on purpose. Thresholds picked on the main set for ms-marco (weak_min -9, strong_min -4.5) caught 7/8 unanswerable questions
    # there but flagged 12/41 ANSWERABLE held-out questions as weak or none (7 of them "none"), so they were not shipped. See docs/retrieval-quality.md.
    RERANK_COVERAGE_THRESHOLDS: str = '{}'
    RERANK_TIMEOUT_SECONDS: float = 0.8
    RERANK_TOP_N: int = 30
    RERANK_LEG_TOP_N: int = 50
    # Neighbour expansion: attach the previous/next chunk of the same section to each selected chunk while max_tokens allows.
    EXPAND_NEIGHBOURS: bool = False
    # Weak-retrieval signal (response.top_score / response.coverage). With reranking: thresholds come from RERANK_COVERAGE_THRESHOLDS (per model). Without it: thresholds on a rank-derived score (the best candidate's RRF score divided by the maximum possible,
    # i.e. rank 1 in both legs = 1.0). Calibrated on the main set's unanswerable questions: see docs/retrieval-quality.md.
    COVERAGE_ENABLED: bool = False   # flipped on after calibration on the benchmark (see docs); uncalibrated thresholds must not reach the prompt
    # Rank-derived coverage (no reranker) is OFF: on the benchmark it cannot tell answerable from unanswerable questions (the best candidate
    # is rank 1 in at least one engine either way: answerable top scores 0.90-1.00, unanswerable 0.87-1.00), so it would label everything
    # STRONG. Without a reranker the response carries no signal (COVERAGE_UNSPECIFIED, no top_score). See docs/retrieval-quality.md.
    COVERAGE_RANK_SIGNAL_ENABLED: bool = False
    COVERAGE_RANK_STRONG_MIN: float = 0.55
    COVERAGE_RANK_WEAK_MIN: float = 0.30
    # --- Phase 5 (retrieval side) ---
    # Chunk ids from the previous assistant turn become extra candidates (after the authorisation and current-version checks).
    PREVIOUS_CHUNKS_ENABLED: bool = True
    PREVIOUS_CHUNKS_MAX: int = 10
    PREVIOUS_CHUNKS_WEIGHT: float = 1.0       # their RRF vote relative to a normal rank vote
    # One `retrieval.feedback` log line per query. The query text is included only when this is true (off by default); a hash always is.
    LOG_QUERY_TEXT: bool = False
    # GetDocumentChunks: safety cap on chunks returned per call.
    DOCUMENT_CHUNKS_MAX: int = 5000
    # Candidates whose text overlaps an already-kept, higher-ranked candidate by more than this (Jaccard over word 3-shingles)
    # are dropped before MMR. 1.0 disables the filter.
    DEDUPE_JACCARD_THRESHOLD: float = 0.9
    # With two or more file_ids, every file's best candidate gets a slot in the final set when its normalised relevance is at
    # or above MMR_RELEVANCE_FLOOR.
    BALANCE_ACROSS_FILES: bool = True
    # GetRelevantChunksRequest.top_k is honoured up to this cap; 0 in the request means MMR_FINAL_TOP_K.
    RETRIEVAL_MAX_TOP_K: int = 20
    # Phrase boosts on the keyword query (section x5, content x2) so a pasted heading or phrase ranks its section first.
    OPENSEARCH_PHRASE_BOOST_ENABLED: bool = True
    OPENSEARCH_SECTION_PHRASE_BOOST: float = 5.0
    OPENSEARCH_CONTENT_PHRASE_BOOST: float = 2.0
    # If the query embedding fails, answer from the keyword leg alone instead of returning UNAVAILABLE.
    EMBEDDING_FAILURE_FALLBACK_BM25: bool = True
    # Tokenizer used to enforce max_tokens and to fill total_tokens. A proxy for the LLM's own tokenizer (which this service
    # does not know); falls back to a words/0.75 estimate, with a warning, if it cannot be loaded.
    TOKEN_COUNT_TOKENIZER_ID: str = "sentence-transformers/all-MiniLM-L6-v2"
    QUERY_PIPELINE_DEADLINE_SECONDS: float = 1.8

    # --- Ingestion pipeline tuning ---
    # --- Parsing: which files are read and how much work one file may cause ---
    # Zip archives are opened and every readable member is indexed (see app/ingestion/archive.py). These bound what a hostile or
    # simply huge archive can cost.
    ARCHIVE_MAX_ENTRIES: int = 2_000
    ARCHIVE_MAX_MEMBER_BYTES: int = 50 * 1024 * 1024
    ARCHIVE_MAX_TOTAL_BYTES: int = 300 * 1024 * 1024
    ARCHIVE_MAX_COMPRESSION_RATIO: int = 200
    ARCHIVE_MAX_DEPTH: int = 2               # a zip inside a zip is opened; a third level is not
    ARCHIVE_MAX_DOCUMENT_MEMBERS: int = 60   # PDF/Word/PowerPoint/Excel/HTML members (slow to parse); text and code are not limited
    PARSE_MAX_TEXT_CHARS: int = 8_000_000    # text beyond this in one file is not indexed
    LEGACY_DOC_COMMAND: str = "antiword"     # converter for binary .doc files; absent = a clear "save as .docx" error
    LEGACY_DOC_TIMEOUT_SECONDS: float = 30.0

    CHUNK_TARGET_TOKENS: int = 450
    CHUNK_MAX_TOKENS: int = 600
    CHUNK_OVERLAP_TOKENS: int = 60
    # Chunker v2 (index generation 2+), in tokens of the embedding model. Maximum = model max_seq_length - CHUNK_MAX_MARGIN_V2.
    CHUNK_TARGET_TOKENS_V2: int = 200
    CHUNK_OVERLAP_TOKENS_V2: int = 30
    CHUNK_MAX_MARGIN_V2: int = 16
    RECONCILIATION_ENABLED: bool = True
    RECONCILIATION_INTERVAL_SECONDS: int = 300

    # --- Observability ---
    LOG_LEVEL: str = "INFO"
    METRICS_ENABLED: bool = True
    HEALTH_CHECK_TIMEOUT_SECONDS: float = 3.0

    @field_validator("RERANK_COVERAGE_THRESHOLDS")
    @classmethod
    def _thresholds_are_valid_json(cls, value: str) -> str:
        """Fail at startup, not at the first query, on a malformed threshold table."""
        try:
            table = json.loads(value or "{}")
            assert isinstance(table, dict)
            for model, th in table.items():
                assert isinstance(th, dict) and float(th["weak_min"]) <= float(th["strong_min"]), model
        except (ValueError, TypeError, KeyError, AssertionError) as exc:
            raise ValueError(f'RERANK_COVERAGE_THRESHOLDS must be JSON like {{"model": {{"weak_min": -9, "strong_min": -4}}}} with weak_min <= strong_min ({exc})') from exc
        return value

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
