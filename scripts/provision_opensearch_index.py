"""Infra script: creates an OpenSearch index EXPLICITLY from the versioned mapping in app/stores/index_schema.py. The application never
creates an index (a first write would auto-create one with a dynamic mapping that breaks every scoped query).

    uv run python scripts/provision_opensearch_index.py                 # the legacy un-suffixed index (schema 1), as before
    uv run python scripts/provision_opensearch_index.py --generation 2  # retrieval_document_chunks_g2 (schema 2)

Idempotent: skips creation if the index already exists (and reports a mapping mismatch instead of silently accepting it).
Never imported by `app/`; `scripts/generation.py` and the benchmark call `create_index`."""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))  # so `uv run python scripts/<this>.py` finds `app`

from opensearchpy import OpenSearch

from app.core.config import settings
from app.stores import generations
from app.stores.index_schema import SCHEMA_V1, mapping_problems, opensearch_body

MAPPING = opensearch_body(SCHEMA_V1)   # kept for callers/tests that import the v1 body from here


def create_index(client: OpenSearch, index: str, schema_version: str) -> bool:
    """Creates `index` with the mapping of `schema_version`. Returns False (and creates nothing) if it already exists; raises if the
    existing index does not match."""
    if client.indices.exists(index=index):
        problems = mapping_problems(client.indices.get_mapping(index=index), index, schema_version=schema_version)
        if problems:
            raise SystemExit(f"index {index!r} exists but its mapping is wrong ({'; '.join(problems)}); delete it and re-run")
        return False
    client.indices.create(index=index, body=opensearch_body(schema_version))
    return True


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--generation", type=int, help="index generation N: creates <OPENSEARCH_INDEX_NAME>_gN (schema 2)")
    args = ap.parse_args(argv)
    client = OpenSearch(hosts=settings.OPENSEARCH_HOSTS.split(","))
    if args.generation and args.generation > 1:
        index, version = generations.names_for(args.generation)[1], "2"
    else:
        index, version = settings.OPENSEARCH_INDEX_NAME, SCHEMA_V1
    if create_index(client, index, version):
        print(f"Index '{index}' created with schema {version}.")
    else:
        print(f"Index '{index}' already exists with the expected mapping; nothing to do.")


if __name__ == "__main__":
    main()
