.PHONY: help sync infra infra-down infra-status db-create db-migrate db-revision proto run run-worker run-all test lint clean

all: help

help:
	@echo "Available commands:"
	@echo "  make sync        - Install and sync virtual environment packages using uv"
	@echo "  make infra       - Start shared GraphGPT docker containers (Postgres, Kafka, Milvus, OpenSearch, file-minio, etc.)"
	@echo "  make infra-down  - Stop the shared GraphGPT docker containers"
	@echo "  make infra-status- Check status of the shared GraphGPT containers"
	@echo "  make db-create   - Idempotently create the retrieval_service_db database"
	@echo "  make db-migrate  - Apply Alembic migrations (uv run alembic upgrade head)"
	@echo "  make db-revision - Autogenerate a new Alembic revision (make db-revision MSG=\"description\")"
	@echo "  make proto       - Regenerate gRPC stubs from protos/retrieval.proto"
	@echo "  make run         - Launch DEPLOYMENT_MODE=api dev server with auto-reload"
	@echo "  make run-worker  - Launch DEPLOYMENT_MODE=worker locally (no reload — long-running consumers)"
	@echo "  make run-all     - Launch DEPLOYMENT_MODE=all locally (single-process dev convenience)"
	@echo "  make test        - Run unit tests using pytest"
	@echo "  make clean       - Remove virtual environment (.venv) and byte caches"

sync:
	uv sync

# Shared infrastructure lives in the root ../docker-compose.yml — reuse it,
# never spin up a second Postgres/Kafka/Milvus/MinIO from this service's own
# compose file (docker-compose.yml here is a commented-out deployment
# template only, matching file-service's/auth-service's convention).
infra:
	docker compose -f ../docker-compose.yml up -d

infra-down:
	docker compose -f ../docker-compose.yml down

infra-status:
	docker compose -f ../docker-compose.yml ps

# Idempotent regardless of whether docker-entrypoint-initdb.d already ran
# (i.e. works even on a pre-existing postgres_data volume).
db-create:
	docker exec graphgpt-postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'retrieval_service_db'" | grep -q 1 || \
	docker exec graphgpt-postgres psql -U postgres -c "CREATE DATABASE retrieval_service_db"

db-migrate:
	uv run alembic upgrade head

db-revision:
	uv run alembic revision --autogenerate -m "$(MSG)"

# Generated stubs are committed (matching llm-service/memory-service's
# convention, not embedding-service's gitignored one) — retrieval.proto is
# copied verbatim from llm-service (HLD §1.1 #2) and diffed against it in CI
# (Phase 12), so keeping the generated pair alongside it in git makes drift
# visible in the same PR that would introduce it. embedding.proto is copied
# verbatim from embedding-service (HLD §12) for the same reason — retrieval
# is a gRPC client of embedding-service, not its proto's owner.
proto:
	uv run python -m grpc_tools.protoc -Iprotos --python_out=app/grpc --grpc_python_out=app/grpc protos/retrieval.proto protos/embedding.proto
	uv run python scripts/fix_proto_grpc_imports.py

run:
	DEPLOYMENT_MODE=api uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8060

run-worker:
	DEPLOYMENT_MODE=worker uv run uvicorn app.main:app --host 127.0.0.1 --port 8061

run-all:
	DEPLOYMENT_MODE=all uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8060

test:
	uv run pytest

clean:
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
