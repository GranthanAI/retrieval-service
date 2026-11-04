class EmbeddingModelMismatchError(Exception):
    """embedding-service's response claims a model/dimension that doesn't
    match this service's own configured expectation
    (EMBEDDING_MODEL_EXPECTED/EMBEDDING_DIMENSION_EXPECTED) — HLD §12's
    cheap runtime guard against a silent embedding-service model upgrade
    breaking the index. Raised instead of returning a vector; callers must
    never index or query with a mismatched embedding.
    """


class IndexMissingError(Exception):
    """The OpenSearch index a write or read targets does not exist. Writes must never create it implicitly: a first write would
    auto-create it with a dynamic mapping (see app/stores/index_schema.py). Provision it with scripts/provision_opensearch_index.py."""


class IndexMappingError(Exception):
    """The OpenSearch index exists but its mapping differs from what the application requires (e.g. created by a first write)."""
