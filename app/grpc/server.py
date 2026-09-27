import grpc

from app.core.config import settings
from app.core.logging import get_logger
from app.grpc import retrieval_pb2_grpc
from app.grpc.interceptors.logging_interceptor import LoggingInterceptor
from app.grpc.interceptors.metrics_interceptor import MetricsInterceptor
from app.grpc.retrieval_servicer import RetrievalServicer

logger = get_logger(__name__)


def create_server(servicer: RetrievalServicer) -> grpc.aio.Server:
    """HLD §6 — `grpc.aio` server bootstrap.

    `GRPC_MAX_WORKERS` (LLD §2) has no direct equivalent here — it's a
    thread-pool-executor size, relevant to a synchronous `grpc.server()`;
    `grpc.aio.server()` runs the async servicer directly on the event loop
    instead. Kept in `Settings` for parity with LLD §2's table (and in case
    a future sync interop path needs it), but genuinely unused by this
    module — noted here rather than silently repurposed for something it
    doesn't mean.
    """
    server = grpc.aio.server(
        options=[
            ("grpc.max_send_message_length", settings.GRPC_MAX_MESSAGE_LENGTH_BYTES),
            ("grpc.max_receive_message_length", settings.GRPC_MAX_MESSAGE_LENGTH_BYTES),
        ],
        interceptors=[LoggingInterceptor(), MetricsInterceptor()],
    )
    retrieval_pb2_grpc.add_RetrievalServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{settings.GRPC_PORT}")
    return server


async def start_server(servicer: RetrievalServicer) -> grpc.aio.Server:
    server = create_server(servicer)
    await server.start()
    logger.info("grpc_server.started", port=settings.GRPC_PORT)
    return server


async def stop_server(server: grpc.aio.Server, grace: float = 5.0) -> None:
    await server.stop(grace)
    logger.info("grpc_server.stopped")
