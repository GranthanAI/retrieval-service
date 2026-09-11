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

    # --- File Service permission-check client ---
    FILE_SERVICE_GRPC_TARGET: str = "localhost:50052"
    FILE_SERVICE_PERMISSION_CHECK_TIMEOUT_SECONDS: float = 0.3
    FILE_SERVICE_SERVICE_CREDENTIAL: str = ""

    # --- Milvus (shared cluster, dedicated collection) ---
    MILVUS_HOST: str = "localhost"
    MILVUS_PORT: int = 19530
    MILVUS_COLLECTION_NAME: str = "retrieval_document_chunks"
    MILVUS_SEARCH_TOP_N: int = 40
    MILVUS_SEARCH_NPROBE: int = 16
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
    QUERY_PIPELINE_DEADLINE_SECONDS: float = 1.8

    # --- Ingestion pipeline tuning ---
    CHUNK_TARGET_TOKENS: int = 450
    CHUNK_MAX_TOKENS: int = 600
    CHUNK_OVERLAP_TOKENS: int = 60
    RECONCILIATION_ENABLED: bool = True
    RECONCILIATION_INTERVAL_SECONDS: int = 300

    # --- Observability ---
    LOG_LEVEL: str = "INFO"
    METRICS_ENABLED: bool = True
    HEALTH_CHECK_TIMEOUT_SECONDS: float = 3.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
