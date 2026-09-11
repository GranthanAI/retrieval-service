# ==============================================================================
# STAGE 1: Builder
# ==============================================================================
FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Matches the runtime stage's WORKDIR (/app) deliberately: `uv sync` bakes an
# absolute shebang into installed console scripts like `uvicorn`. If this
# stage builds at a different path than where the venv is copied to below,
# those scripts fail at runtime with "no such file or directory".
WORKDIR /app

COPY pyproject.toml uv.lock* ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev || uv sync --no-install-project --no-dev

# ==============================================================================
# STAGE 2: Runtime
# ==============================================================================
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd -r appgroup && useradd -r -g appgroup -s /sbin/nologin appuser

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY app /app/app
COPY protos /app/protos
COPY alembic.ini /app/
COPY README.md /app/

RUN chown -R appuser:appgroup /app

USER appuser

# gRPC (api/all) and HTTP health/metrics (every mode) — DEPLOYMENT_MODE
# selects behavior at runtime (LLD §9), a single image for both deployments.
EXPOSE 50060
EXPOSE 8060

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8060"]
