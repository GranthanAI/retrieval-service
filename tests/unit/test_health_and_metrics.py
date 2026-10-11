"""Phase 11 exit criteria that don't need real infra: mode-aware readiness,
the /metrics surface, wired metric call sites, and log correlation."""

import grpc
import httpx
import pytest
import structlog
from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from app.api import dependency_checks
from app.api.routers import health
from app.clients.embedding_client import _assert_model_matches
from app.clients.file_service_client import FileServiceClient
from app.core.config import settings
from app.core.enums import RetrievalScope
from app.core.exceptions import EmbeddingModelMismatchError
from app.grpc import retrieval_pb2
from app.grpc.interceptors.logging_interceptor import LoggingInterceptor
from app.query import scope_guard


def _sample(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(health.router)
    return TestClient(app)


def _stub_checks(monkeypatch, *, down: set[str]) -> list[str]:
    called: list[str] = []

    def _make(name: str):
        async def _check() -> bool:
            called.append(name)
            return name not in down

        return _check

    for name in dependency_checks.CHECKS:
        monkeypatch.setitem(dependency_checks.CHECKS, name, _make(name))
    return called


class TestModeAwareReadiness:
    def test_api_mode_503_when_milvus_down(self, client, monkeypatch) -> None:
        monkeypatch.setattr(settings, "DEPLOYMENT_MODE", "api")
        _stub_checks(monkeypatch, down={"milvus"})
        assert client.get("/health/ready").status_code == 503

    def test_api_mode_503_when_opensearch_down(self, client, monkeypatch) -> None:
        monkeypatch.setattr(settings, "DEPLOYMENT_MODE", "api")
        _stub_checks(monkeypatch, down={"opensearch"})
        assert client.get("/health/ready").status_code == 503

    def test_api_mode_ignores_kafka_and_minio(self, client, monkeypatch) -> None:
        monkeypatch.setattr(settings, "DEPLOYMENT_MODE", "api")
        called = _stub_checks(monkeypatch, down={"kafka", "minio"})
        response = client.get("/health/ready")
        assert response.status_code == 200
        assert "kafka" not in called and "minio" not in called  # never even probed

    def test_worker_mode_503_when_kafka_down(self, client, monkeypatch) -> None:
        monkeypatch.setattr(settings, "DEPLOYMENT_MODE", "worker")
        _stub_checks(monkeypatch, down={"kafka"})
        assert client.get("/health/ready").status_code == 503

    def test_worker_mode_503_when_minio_down(self, client, monkeypatch) -> None:
        monkeypatch.setattr(settings, "DEPLOYMENT_MODE", "worker")
        _stub_checks(monkeypatch, down={"minio"})
        assert client.get("/health/ready").status_code == 503

    def test_worker_mode_ignores_milvus_and_opensearch(self, client, monkeypatch) -> None:
        monkeypatch.setattr(settings, "DEPLOYMENT_MODE", "worker")
        called = _stub_checks(monkeypatch, down={"milvus", "opensearch"})
        assert client.get("/health/ready").status_code == 200
        assert "milvus" not in called and "opensearch" not in called

    def test_postgres_down_is_not_ready_in_every_mode(self, client, monkeypatch) -> None:
        for mode in ("api", "worker", "all"):
            monkeypatch.setattr(settings, "DEPLOYMENT_MODE", mode)
            _stub_checks(monkeypatch, down={"postgres"})
            assert client.get("/health/ready").status_code == 503

    def test_liveness_never_calls_a_dependency(self, client, monkeypatch) -> None:
        called = _stub_checks(monkeypatch, down={"postgres", "milvus", "kafka"})
        assert client.get("/health/live").status_code == 200
        assert called == []


class TestMetricsEndpoint:
    def test_exposes_every_metric_in_the_llm_list(self, client) -> None:
        body = client.get("/metrics").text
        for name in (
            "retrieval_ingestion_jobs_total",
            "retrieval_ingestion_stage_duration_seconds",
            "retrieval_query_latency_seconds",
            "retrieval_query_short_circuit_total",
            "retrieval_unsupported_scope_total",
            "retrieval_search_degraded_total",
            "retrieval_permission_check_latency_seconds",
            "retrieval_embedding_model_mismatch_total",
        ):
            assert name in body, name


class TestWiredCallSites:
    def test_unsupported_scope_counter_increments(self) -> None:
        before = _sample("retrieval_unsupported_scope_total", {"scope": "KNOWLEDGE_BASE"})
        scope_guard.check(RetrievalScope.KNOWLEDGE_BASE, ["doc-a"])
        assert _sample("retrieval_unsupported_scope_total", {"scope": "KNOWLEDGE_BASE"}) == before + 1

    def test_embedding_model_mismatch_counter_increments(self) -> None:
        before = _sample("retrieval_embedding_model_mismatch_total")
        with pytest.raises(EmbeddingModelMismatchError):
            _assert_model_matches("some-other-model", settings.EMBEDDING_DIMENSION_EXPECTED)
        assert _sample("retrieval_embedding_model_mismatch_total") == before + 1

    @pytest.mark.asyncio
    async def test_permission_check_latency_is_observed(self) -> None:
        async def check(request: Request) -> JSONResponse:
            return JSONResponse({"authorized_document_ids": ["doc-a"]})

        app = Starlette(routes=[Route("/internal/files/permissions/check", check, methods=["POST"])])
        file_service = FileServiceClient(base_url="http://fs.test", transport=httpx.ASGITransport(app=app))

        before = _sample("retrieval_permission_check_latency_seconds_count")
        assert await file_service.batch_check_permissions("user-1", ["doc-a"]) == ["doc-a"]
        assert _sample("retrieval_permission_check_latency_seconds_count") == before + 1


class TestLogCorrelation:
    @pytest.mark.asyncio
    async def test_trace_id_bound_for_the_call_and_cleared_after(self) -> None:
        seen: dict = {}

        async def behavior(request, context):
            seen.update(structlog.contextvars.get_contextvars())
            return "response"

        handler = grpc.unary_unary_rpc_method_handler(behavior)

        async def continuation(_details):
            return handler

        wrapped = await LoggingInterceptor().intercept_service(continuation, None)
        request = retrieval_pb2.GetRelevantChunksRequest(trace_id="trace-abc")

        assert await wrapped.unary_unary(request, None) == "response"
        assert seen["trace_id"] == "trace-abc"
        assert "trace_id" not in structlog.contextvars.get_contextvars()
