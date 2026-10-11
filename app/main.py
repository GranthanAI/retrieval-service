from fastapi import FastAPI

from app.api.middleware import RequestIDMiddleware
from app.api.routers import health
from app.core.config import settings
from app.core.logging import configure_logging
from app.lifespan import lifespan


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title="Granthan Retrieval Service",
        description="Ingests uploaded documents into Milvus/OpenSearch and serves top-K relevant chunks over gRPC.",
        version="0.1.0",
        debug=settings.ENVIRONMENT == "development",
        lifespan=lifespan,
    )

    app.add_middleware(RequestIDMiddleware)
    app.include_router(health.router)

    return app


app = create_app()
