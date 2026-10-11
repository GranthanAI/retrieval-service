from prometheus_client import Counter, Histogram

# LLD §10 (Observability) lists the full metric set; Phase 11 wires every
# call site into real Prometheus instruments and adds the /metrics endpoint.
# The two counters below are needed by Phase 8's query pipeline itself
# (scope_guard's product-visibility signal, HLD §16.3; the degradation
# matrix's own counter, LLD §7.2) — defined now rather than left as a stub,
# matching the Implementation Plan's own framing of earlier phases (3/5/8/9)
# leaving metric call sites that Phase 11 "wires into real Prometheus
# instruments," not "creates for the first time."

unsupported_scope_counter = Counter(
    "retrieval_unsupported_scope_total",
    "GetRelevantChunks requests with an unimplemented RetrievalScope (HLD §16.3)",
    ["scope"],
)

search_degraded_total = Counter(
    "retrieval_search_degraded_total",
    "Query pipeline runs that degraded to a single search engine (LLD §7.2)",
    ["engine"],
)

query_latency_histogram = Histogram(
    "retrieval_query_latency_seconds",
    "End-to-end GetRelevantChunks latency, servicer's own timer (HLD §24)",
)

query_timeout_counter = Counter(
    "retrieval_query_timeout_total",
    "GetRelevantChunks calls that hit QUERY_PIPELINE_DEADLINE_SECONDS (LLD §7.1)",
)

query_short_circuit_total = Counter(
    "retrieval_query_short_circuit_total",
    "GetRelevantChunks requests that returned empty via a short-circuit (LLD §7.2)",
    ["reason"],
)

permission_check_latency_histogram = Histogram(
    "retrieval_permission_check_latency_seconds",
    "File Service batch permission-check call latency only (HLD §24)",
)

embedding_model_mismatch_total = Counter(
    "retrieval_embedding_model_mismatch_total",
    "embedding-service responses that failed the model/dimension guard (HLD §12) — should stay 0",
)

ingestion_jobs_total = Counter(
    "retrieval_ingestion_jobs_total",
    "Ingestion jobs by terminal/settled status (LLD §10)",
    ["status"],
)

ingestion_stage_duration_seconds = Histogram(
    "retrieval_ingestion_stage_duration_seconds",
    "Ingestion pipeline stage duration (LLD §10)",
    ["stage"],
)
