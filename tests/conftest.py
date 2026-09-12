import os

# Ensure Settings() can construct during test collection even without a .env
# file present (CI, fresh clones) — every field already has a harmless
# default (app/core/config.py), this just keeps DATABASE_URL pointed at a
# dedicated test database name so nothing here touches the dev DB.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://postgres:1234@localhost:5433/retrieval_service_db_test"
)
