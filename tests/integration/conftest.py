import os
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def postgres_container():
    """Throwaway, ephemeral Postgres on a random free host port — avoids any
    conflict with a host-side Postgres on 5432/5433 (see README.md's
    troubleshooting note). testcontainers manages its full lifecycle;
    nothing persists after the test session."""
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def database_url(postgres_container: PostgresContainer) -> str:
    sync_url = postgres_container.get_connection_url()  # postgresql+psycopg2://...
    return sync_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")


@pytest.fixture(scope="session")
def migrated_database_url(database_url: str) -> str:
    """Runs the real Alembic migration chain (app/db/migrations) against the
    ephemeral container, so these tests exercise the same schema production
    uses — not a hand-rolled create_all()."""
    alembic_cfg = Config(f"{_project_root()}/alembic.ini")
    alembic_cfg.set_main_option("script_location", f"{_project_root()}/app/db/migrations")
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, "head")
    return database_url


@pytest_asyncio.fixture
async def db_session(migrated_database_url: str) -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(migrated_database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()
    await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(migrated_database_url: str) -> AsyncGenerator[async_sessionmaker, None]:
    """A real `async_sessionmaker` against the migrated ephemeral Postgres —
    for Phase 5 tests that need to pass a session factory (not a single
    session) to a consumer/orchestrator, the same way `lifespan.py` does.
    """
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest.fixture(scope="session")
def kafka_container():
    """Ephemeral Kafka on a random free host port — isolated, nothing
    persists after the test session. Session-scoped so its startup cost is
    paid once across every Phase 5 test, matching file-service's own
    convention.
    """
    with KafkaContainer() as container:
        yield container


@pytest_asyncio.fixture
async def kafka_producer(kafka_container: KafkaContainer) -> AsyncGenerator[AIOKafkaProducer, None]:
    producer = AIOKafkaProducer(bootstrap_servers=kafka_container.get_bootstrap_server())
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()
