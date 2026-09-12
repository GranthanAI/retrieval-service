"""One-time infra script — creates the `retrieval_document_chunks` Milvus
collection and its indexes (LLD §4, HLD §13.2). Run manually, once per
environment:

    uv run python scripts/provision_milvus_collection.py

Never imported by `app/` — pods only ever connect and read/write, never
create (HLD §13.2: two pods racing to create the same collection on
startup is unnecessary, avoidable coordination complexity for something
that only needs to happen once, ever, per environment). Idempotent: skips
creation if the collection already exists, so re-running against an
already-provisioned environment is a no-op, not an error.

Uses `MilvusClient` (pymilvus's current API, not the deprecated ORM-style
`Collection`/`connections`/`utility`).

**Deviation from LLD §4's literal schema, confirmed empirically, not
assumed:** LLD §4 declares `conversation_id`/`page_number`/`section` as
`nullable=True`. Neither the deployed server (tested against both
milvus-standalone v2.3.0 and v2.4.15) nor this pymilvus client actually
supports a NULL insert for these field types — v2.3.0 silently drops the
`nullable` kwarg client-side (no such concept existed yet), and v2.4.15's
server rejects a `None` value at insert time with `MilvusException
code=1100: varchar field '<name>' is illegal ... expected=need string
array, actual=got nil`, even though `nullable=True` is accepted without
error at collection-creation time. Given that, these three fields are
created as plain non-nullable VARCHAR/INT32, and `milvus_repository.py`
encodes an absent value as a sentinel before writing (`""` for the two
VARCHAR fields, `-1` for `page_number`) rather than `None` — see that
module's own docstring for the encoding contract.
"""

from pymilvus import DataType, MilvusClient

from app.core.config import settings


def _build_schema(client: MilvusClient):
    """LLD §4's exact field list, except `conversation_id`/`page_number`/
    `section` are NOT declared nullable — see this module's docstring for
    why that's a deliberate, verified deviation, not an oversight.

    `chunk_id` is VARCHAR, not Milvus's auto-increment int64 — it stores the
    string form of the deterministic uuid5 chunk identity (HLD §11), which
    is what makes both the Milvus write and the MMR candidate-embedding
    fetch (HLD §18.1, a primary-key `get` by this same id) work without a
    secondary lookup index.
    """
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name="chunk_id", datatype=DataType.VARCHAR, max_length=64, is_primary=True)
    schema.add_field(field_name="document_id", datatype=DataType.VARCHAR, max_length=36)
    schema.add_field(field_name="content_version", datatype=DataType.INT32)
    schema.add_field(field_name="chunk_index", datatype=DataType.INT32)
    schema.add_field(field_name="owner_id", datatype=DataType.VARCHAR, max_length=36)
    schema.add_field(field_name="conversation_id", datatype=DataType.VARCHAR, max_length=36)
    schema.add_field(field_name="filename", datatype=DataType.VARCHAR, max_length=512)
    schema.add_field(field_name="page_number", datatype=DataType.INT32)
    schema.add_field(field_name="section", datatype=DataType.VARCHAR, max_length=512)
    schema.add_field(field_name="content", datatype=DataType.VARCHAR, max_length=8192)
    schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=settings.EMBEDDING_DIMENSION_EXPECTED)
    return schema


def main() -> None:
    client = MilvusClient(uri=f"http://{settings.MILVUS_HOST}:{settings.MILVUS_PORT}")

    if client.has_collection(settings.MILVUS_COLLECTION_NAME):
        print(f"Collection '{settings.MILVUS_COLLECTION_NAME}' already exists — nothing to do.")
        return

    schema = _build_schema(client)

    index_params = client.prepare_index_params()
    # HNSW on `embedding`, metric_type per settings (HLD §13, LLD §4).
    index_params.add_index(
        field_name="embedding",
        index_type="HNSW",
        metric_type=settings.MILVUS_INDEX_METRIC_TYPE,
        params={"M": 16, "efConstruction": 200},
    )
    # Scalar indexes on document_id AND content_version — both fields
    # participate in the SAME query-time filter clause (HLD §13.3's
    # current-version-only fix), not just document_id alone.
    index_params.add_index(field_name="document_id")
    index_params.add_index(field_name="content_version")

    client.create_collection(
        collection_name=settings.MILVUS_COLLECTION_NAME,
        schema=schema,
        index_params=index_params,
    )
    client.load_collection(settings.MILVUS_COLLECTION_NAME)

    print(
        f"Collection '{settings.MILVUS_COLLECTION_NAME}' created with "
        f"HNSW/{settings.MILVUS_INDEX_METRIC_TYPE} index on `embedding`, "
        "scalar indexes on `document_id` and `content_version`."
    )


if __name__ == "__main__":
    main()
