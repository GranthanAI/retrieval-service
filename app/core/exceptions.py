class EmbeddingModelMismatchError(Exception):
    """embedding-service's response claims a model/dimension that doesn't
    match this service's own configured expectation
    (EMBEDDING_MODEL_EXPECTED/EMBEDDING_DIMENSION_EXPECTED) — HLD §12's
    cheap runtime guard against a silent embedding-service model upgrade
    breaking the index. Raised instead of returning a vector; callers must
    never index or query with a mismatched embedding.
    """
