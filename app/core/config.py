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
    RERANK_TIMEOUT_SECONDS: float = 0.8
    RERANK_TOP_N: int = 30
    RERANK_LEG_TOP_N: int = 50
    # Neighbour expansion: attach the previous/next chunk of the same section to each selected chunk while max_tokens allows.
    EXPAND_NEIGHBOURS: bool = False
    # Weak-retrieval signal (response.top_score / response.coverage). With reranking: thresholds on the reranker's own score (a 0-1
    # probability for the default model; per-model). Without it: thresholds on a rank-derived score (the best candidate's RRF score divided by the maximum possible,
    # i.e. rank 1 in both legs = 1.0). Calibrated on the main set's unanswerable questions: see docs/retrieval-quality.md.
    COVERAGE_ENABLED: bool = False   # flipped on after calibration on the benchmark (see docs); uncalibrated thresholds must not reach the prompt
    COVERAGE_RANK_STRONG_MIN: float = 0.55
    COVERAGE_RANK_WEAK_MIN: float = 0.30
    COVERAGE_RERANK_STRONG_MIN: float = 0.50
    COVERAGE_RERANK_WEAK_MIN: float = 0.10
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

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
