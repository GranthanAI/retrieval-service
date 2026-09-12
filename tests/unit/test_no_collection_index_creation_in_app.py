"""LLD §13.2 — Milvus collection / OpenSearch index creation is an
infrastructure step, never application startup code. This is a CI gate for
that rule, not just a convention: no `create_collection`, Milvus
`create_index`, or an OpenSearch `indices.create` call may exist anywhere
under `app/`. The two provisioning scripts under `scripts/` are the only
place these calls are allowed to live.

Scoped to `app/stores/` (and the rest of `app/`, excluding
`app/db/migrations/`) rather than a bare substring match everywhere: Alembic
migrations legitimately call SQLAlchemy's own `op.create_index` to manage
Retrieval's *own* Postgres schema (LLD §3) — an entirely different concern
from LLD §13.2, which is about the two external search engines.
"""

import pathlib

FORBIDDEN_SUBSTRINGS = (
    "create_collection",
    ".create_index(",
    "indices.create",
)

APP_DIR = pathlib.Path(__file__).resolve().parents[2] / "app"
EXCLUDED_DIR = APP_DIR / "db" / "migrations"


def test_no_create_collection_or_create_index_under_app() -> None:
    offenders = []
    for path in APP_DIR.rglob("*.py"):
        if EXCLUDED_DIR in path.parents:
            continue
        text = path.read_text(encoding="utf-8")
        for needle in FORBIDDEN_SUBSTRINGS:
            if needle in text:
                offenders.append(f"{path}: contains '{needle}'")

    assert not offenders, "Collection/index creation found under app/ (LLD §13.2 violation):\n" + "\n".join(
        offenders
    )
