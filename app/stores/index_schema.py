"""What the application REQUIRES of the OpenSearch index it reads and writes. Pure data and comparison, no index creation
(creation lives in scripts/provision_opensearch_index.py; tests/unit/test_no_collection_index_creation_in_app.py enforces that).

A dynamically created index (first write into a missing index) maps `document_id` and `conversation_id` as `text`, so the exact
`term` filters that scope every query silently match nothing. `mapping_problems` detects that."""

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


def mapping_problems(mapping_response: dict, index: str, required: dict[str, str] | None = None) -> list[str]:
    """Human-readable differences between the live mapping of `index` and `required`. `mapping_response` is the body of
    `GET /<index>/_mapping`, which is keyed by the concrete index name (an alias resolves to it)."""
    required = REQUIRED_OPENSEARCH_TYPES if required is None else required
    if not mapping_response:
        return [f"index {index!r} returned no mapping"]
    properties = next(iter(mapping_response.values())).get("mappings", {}).get("properties", {})
    problems = []
    for name, expected in required.items():
        actual = properties.get(name, {}).get("type")
        if actual != expected:
            problems.append(f"field {name!r} is {actual or 'missing'}, expected {expected}")
    return problems
