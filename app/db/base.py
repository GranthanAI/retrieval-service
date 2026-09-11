from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for all SQLAlchemy models."""


# Imported here (not in app/models/__init__.py) so Alembic's env.py only
# needs `from app.db.base import Base` to see every table via Base.metadata.
# No models exist yet — Phase 1 adds ingestion_job/processed_event/
# outbox_event/document_current_version (LLD §3) and imports them here.
