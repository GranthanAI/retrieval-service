"""Versioned definitions of the two indexes, and what the application REQUIRES of them. Pure data and comparison, no index creation
(creation lives in scripts/provision_*.py and scripts/generation.py; tests/unit/test_no_collection_index_creation_in_app.py enforces
that nothing under app/ creates a collection or index).

Schema version "1" is the original index (standard analyzer only). Schema version "2" belongs to index generation 2+: heading path,
page range, block type and location fields, an `english` and an `exact` sub-field on content/section, and a keyword sub-field on
filename. A generation's `analyzer_version` selects its schema, its keyword query fields and its Milvus output fields.

A dynamically created index (first write into a missing index) maps `document_id` and `conversation_id` as `text`, so the exact `term`
filters that scope every query silently match nothing. `mapping_problems` detects that."""

SCHEMA_V1 = "1"
SCHEMA_V2 = "2"

# ---- OpenSearch -------------------------------------------------------------------------------------------------------------
_V1_PROPERTIES: dict = {
    "chunk_id": {"type": "keyword"},
    "document_id": {"type": "keyword"},
    "content_version": {"type": "integer"},
    "chunk_index": {"type": "integer"},
    "owner_id": {"type": "keyword"},
    "conversation_id": {"type": "keyword"},
    "filename": {"type": "text", "analyzer": "standard", "boost": 2.0},
    "page_number": {"type": "integer"},
    "section": {"type": "text", "analyzer": "standard", "boost": 1.5},
    "content": {"type": "text", "analyzer": "standard"},
}

_ANALYSIS_V2 = {
    "char_filter": {"strip_trailing_punctuation": {"type": "pattern_replace", "pattern": "[.,]+(?=\\s|$)", "replacement": ""}},
    # Identifiers (MAX_RETRY_ATTEMPTS, TXN-102394, /api/v1/subscriptions) stay whole: only whitespace and brackets/quotes separate tokens.
    "tokenizer": {"identifier_tokenizer": {"type": "pattern", "pattern": "[\\s;:()\\[\\]\"'<>!?|]+"}},
    "filter": {"identifier_parts": {
        "type": "word_delimiter_graph", "preserve_original": True, "split_on_case_change": True, "split_on_numerics": False,
        "generate_word_parts": True, "generate_number_parts": True, "catenate_all": False}},
    "analyzer": {
        # index time: the whole identifier AND its parts (snake_case, camelCase, hyphens, slashes)
        "identifier_index": {"type": "custom", "char_filter": ["strip_trailing_punctuation"], "tokenizer": "identifier_tokenizer",
                             "filter": ["identifier_parts", "flatten_graph", "lowercase"]},
        # query time: the identifier as typed, so "MAX_RETRY_ATTEMPTS" matches that token and not every document containing "retry"
        "identifier_search": {"type": "custom", "char_filter": ["strip_trailing_punctuation"], "tokenizer": "identifier_tokenizer",
                              "filter": ["lowercase"]},
    },
}


def _v2_text(base_boost: float | None = None) -> dict:
    out = {"type": "text", "analyzer": "standard", "fields": {
        "english": {"type": "text", "analyzer": "english"},
        "exact": {"type": "text", "analyzer": "identifier_index", "search_analyzer": "identifier_search"}}}
    return out


def opensearch_body(schema_version: str) -> dict:
    """The full create-index body for a schema version."""
    if schema_version == SCHEMA_V1:
        return {"mappings": {"properties": dict(_V1_PROPERTIES)}}
    props = dict(_V1_PROPERTIES)
    props.update({
        "content": _v2_text(),
        "section": _v2_text(),
        "filename": {"type": "text", "analyzer": "standard", "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
        "heading_path": {"type": "text", "analyzer": "standard"},
        "block_type": {"type": "keyword"},
        "page_start": {"type": "integer"},
        "page_end": {"type": "integer"},
        "location": {"type": "keyword"},
        "generation": {"type": "integer"},
    })
    return {"settings": {"number_of_shards": 1, "analysis": _ANALYSIS_V2}, "mappings": {"properties": props}}


def keyword_query_fields(schema_version: str) -> list[str]:
    """Fields of the `multi_match` (the `must` clause) for a schema version."""
    if schema_version == SCHEMA_V1:
        return ["content", "section^1.5", "filename^2.0"]
    return ["content", "content.english", "content.exact^2", "section^1.5", "section.english", "filename^2"]


REQUIRED_OPENSEARCH_TYPES: dict[str, str] = {
    "chunk_id": "keyword",
    "document_id": "keyword",
    "content_version": "integer",
    "chunk_index": "integer",
    "owner_id": "keyword",
    "conversation_id": "keyword",
    "page_number": "integer",
    "filename": "text",
    "section": "text",
    "content": "text",
}
_REQUIRED_V2_TYPES = {**REQUIRED_OPENSEARCH_TYPES, "heading_path": "text", "block_type": "keyword", "page_start": "integer",
                      "page_end": "integer", "location": "keyword", "generation": "integer"}
_REQUIRED_V2_SUBFIELDS = {"content": ["english", "exact"], "section": ["english", "exact"], "filename": ["keyword"]}


def required_types(schema_version: str) -> dict[str, str]:
    return REQUIRED_OPENSEARCH_TYPES if schema_version == SCHEMA_V1 else _REQUIRED_V2_TYPES


def mapping_problems(mapping_response: dict, index: str, required: dict[str, str] | None = None, schema_version: str = SCHEMA_V1) -> list[str]:
    """Human-readable differences between the live mapping of `index` and what `schema_version` requires. `mapping_response` is the
    body of `GET /<index>/_mapping`, which is keyed by the concrete index name (an alias resolves to it)."""
    required = required_types(schema_version) if required is None else required
    if not mapping_response:
        return [f"index {index!r} returned no mapping"]
    properties = next(iter(mapping_response.values())).get("mappings", {}).get("properties", {})
    problems = []
    for name, expected in required.items():
        actual = properties.get(name, {}).get("type")
        if actual != expected:
            problems.append(f"field {name!r} is {actual or 'missing'}, expected {expected}")
    if schema_version != SCHEMA_V1:
        for name, subs in _REQUIRED_V2_SUBFIELDS.items():
            have = properties.get(name, {}).get("fields", {})
            problems += [f"field {name!r} has no .{sub} sub-field" for sub in subs if sub not in have]
    return problems


# ---- Milvus -----------------------------------------------------------------------------------------------------------------
# (name, datatype name, kwargs). Absent values are sentinels because nullable fields misbehave in the deployed version
# (see scripts/provision_milvus_collection.py): "" for VARCHAR, -1 for page numbers.
_MILVUS_V1: list[tuple[str, str, dict]] = [
    ("chunk_id", "VARCHAR", {"max_length": 64, "is_primary": True}),
    ("document_id", "VARCHAR", {"max_length": 36}),
    ("content_version", "INT32", {}),
    ("chunk_index", "INT32", {}),
    ("owner_id", "VARCHAR", {"max_length": 36}),
    ("conversation_id", "VARCHAR", {"max_length": 36}),
    ("filename", "VARCHAR", {"max_length": 512}),
    ("page_number", "INT32", {}),
    ("section", "VARCHAR", {"max_length": 512}),
    ("content", "VARCHAR", {"max_length": 8192}),
]
_MILVUS_V2_EXTRA: list[tuple[str, str, dict]] = [
    ("heading_path", "VARCHAR", {"max_length": 2048}),
    ("page_start", "INT32", {}),
    ("page_end", "INT32", {}),
    ("block_type", "VARCHAR", {"max_length": 16}),
    ("location", "VARCHAR", {"max_length": 128}),
    ("generation", "INT32", {}),
]


def milvus_fields(schema_version: str, dimension: int) -> list[tuple[str, str, dict]]:
    fields = list(_MILVUS_V1) + (list(_MILVUS_V2_EXTRA) if schema_version != SCHEMA_V1 else [])
    return fields + [("embedding", "FLOAT_VECTOR", {"dim": dimension})]


def milvus_output_fields(schema_version: str) -> list[str]:
    names = [n for n, _, _ in milvus_fields(schema_version, 1) if n not in ("embedding", "owner_id", "conversation_id", "content_version")]
    return names
