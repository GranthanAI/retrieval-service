"""index generations: index_generations, generation_documents

Revision ID: a7c3e91f2b64
Revises: d2b5abb1095e
Create Date: 2026-11-05 10:00:00.000000

Adds the registry of index generations (see app/stores/generations.py). No existing table is altered, and no row is inserted: until a
generation row exists the service uses the legacy un-suffixed index names, so applying this migration changes nothing at runtime.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a7c3e91f2b64"
down_revision: Union[str, None] = "d2b5abb1095e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "index_generations",
        sa.Column("generation", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("parser_version", sa.String(length=16), nullable=False),
        sa.Column("chunker_version", sa.String(length=16), nullable=False),
        sa.Column("embedding_model", sa.Text(), nullable=False),
        sa.Column("embedding_dimension", sa.Integer(), nullable=False),
        sa.Column("analyzer_version", sa.String(length=16), nullable=False),
        sa.Column("milvus_collection", sa.Text(), nullable=False),
        sa.Column("opensearch_index", sa.Text(), nullable=False),
        sa.Column("opensearch_alias", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dropped_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("generation"),
        sa.CheckConstraint("status IN ('building', 'active', 'retired')", name="ck_index_generations_status"),
    )
    # At most one active generation, enforced by the database.
    op.create_index("uq_index_generations_one_active", "index_generations", ["status"], unique=True, postgresql_where=sa.text("status = 'active'"))
    op.create_table(
        "generation_documents",
        sa.Column("generation", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("content_version", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("indexed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("generation", "document_id", "content_version"),
        sa.ForeignKeyConstraint(["generation"], ["index_generations.generation"], ondelete="CASCADE"),
    )


def downgrade() -> None:
    op.drop_table("generation_documents")
    op.drop_index("uq_index_generations_one_active", table_name="index_generations")
    op.drop_table("index_generations")
