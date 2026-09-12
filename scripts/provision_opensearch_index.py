"""One-time infra script — creates the `retrieval_document_chunks`
OpenSearch index and its mapping (LLD §5, HLD §13.2's reasoning applied
identically to OpenSearch). Run manually, once per environment:

    uv run python scripts/provision_opensearch_index.py

Never imported by `app/`. Idempotent: skips creation if the index already
exists.
"""

from opensearchpy import OpenSearch

from app.core.config import settings

MAPPING = {
    "mappings": {
        "properties": {
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
    }
}


def main() -> None:
    client = OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","))

    if client.indices.exists(index=settings.OPENSEARCH_INDEX_NAME):
        print(f"Index '{settings.OPENSEARCH_INDEX_NAME}' already exists — nothing to do.")
        return

    client.indices.create(index=settings.OPENSEARCH_INDEX_NAME, body=MAPPING)
    print(f"Index '{settings.OPENSEARCH_INDEX_NAME}' created with LLD §5's mapping.")


if __name__ == "__main__":
    main()
