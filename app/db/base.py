from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for all SQLAlchemy models."""


# Imported here (not in app/models/__init__.py) so Alembic's env.py only
# needs `from app.db.base import Base` to see every table via Base.metadata.
from app.models import document_current_version, ingestion_job, outbox_event, processed_event  # noqa: E402,F401
