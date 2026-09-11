"""One-time infra script — creates the `retrieval_document_chunks` OpenSearch
index and its mapping (LLD §5, HLD §13.2's reasoning applied identically to
OpenSearch). Run manually, once per environment. Never imported by `app/`.

Mapping lands in Phase 2 (LLD §5). This is a structural stub for Phase 0.
"""


def main() -> None:
    raise NotImplementedError("OpenSearch index mapping lands in Phase 2 (LLD §5)")


if __name__ == "__main__":
    main()
