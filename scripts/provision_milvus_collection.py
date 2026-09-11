"""One-time infra script — creates the `retrieval_document_chunks` Milvus
collection and its indexes (LLD §4, HLD §13.2). Run manually, once per
environment. Never imported by `app/` — pods only ever connect and
read/write, never create (HLD §13.2).

Schema lands in Phase 2 (LLD §4). This is a structural stub for Phase 0,
whose only job is to make provisioning live outside `app/` from day one.
"""


def main() -> None:
    raise NotImplementedError("Milvus collection schema lands in Phase 2 (LLD §4)")


if __name__ == "__main__":
    main()
