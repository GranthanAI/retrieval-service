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

import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))  # so `uv run python scripts/<this>.py` finds `app`

from pymilvus import DataType, MilvusClient

from app.core.config import settings
from app.stores import generations
from app.stores.index_schema import SCHEMA_V1, milvus_fields


def _build_schema(client: MilvusClient, schema_version: str = SCHEMA_V1, dimension: int | None = None):
    """LLD §4's exact field list (schema 1), except `conversation_id`/`page_number`/`section` are NOT declared nullable (see this module's
    docstring). Schema 2 (index generation 2+) adds heading_path, page_start, page_end, block_type, location and generation. The field
    lists live in app/stores/index_schema.py so the application and this script cannot disagree.

    `chunk_id` is VARCHAR, not Milvus's auto-increment int64 — it stores the string form of the deterministic uuid5 chunk identity
    (HLD §11), which is what makes both the Milvus write and the MMR candidate-embedding fetch (HLD §18.1, a primary-key `get` by this
    same id) work without a secondary lookup index.
    """
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    for name, dtype, kwargs in milvus_fields(schema_version, dimension or settings.EMBEDDING_DIMENSION_EXPECTED):
        schema.add_field(field_name=name, datatype=getattr(DataType, dtype), **kwargs)
    return schema


def create_collection(client: MilvusClient, name: str, schema_version: str = SCHEMA_V1, dimension: int | None = None) -> bool:
    """Creates collection `name` explicitly (HNSW index on `embedding`, scalar indexes on document_id and content_version) and loads it.
    Returns False if it already exists."""
    if client.has_collection(name):
        return False
    index_params = client.prepare_index_params()
    # HNSW on `embedding`, metric_type per settings (HLD §13, LLD §4).
    index_params.add_index(field_name="embedding", index_type="HNSW", metric_type=settings.MILVUS_INDEX_METRIC_TYPE,
                           params={"M": 16, "efConstruction": 200})
    # Scalar indexes on document_id AND content_version — both fields participate in the SAME query-time filter clause
    # (HLD §13.3's current-version-only fix), not just document_id alone.
    index_params.add_index(field_name="document_id")
    index_params.add_index(field_name="content_version")
    client.create_collection(collection_name=name, schema=_build_schema(client, schema_version, dimension), index_params=index_params)
    client.load_collection(name)
    return True


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Create a Milvus collection explicitly (never by a first write).")
    ap.add_argument("--generation", type=int, help="index generation N: creates <MILVUS_COLLECTION_NAME>_gN (schema 2)")
    args = ap.parse_args(argv)
    client = MilvusClient(uri=f"http://{settings.MILVUS_HOST}:{settings.MILVUS_PORT}")
    if args.generation and args.generation > 1:
        name, version = generations.names_for(args.generation)[0], "2"
    else:
        name, version = settings.MILVUS_COLLECTION_NAME, SCHEMA_V1
    if create_collection(client, name, version):
        print(f"Collection '{name}' created (schema {version}) with HNSW/{settings.MILVUS_INDEX_METRIC_TYPE} index on `embedding`, "
              "scalar indexes on `document_id` and `content_version`.")
    else:
        print(f"Collection '{name}' already exists — nothing to do.")


if __name__ == "__main__":
    main()
