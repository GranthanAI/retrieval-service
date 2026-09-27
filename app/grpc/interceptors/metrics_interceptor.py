import time

import grpc

from app.core.logging import get_logger

logger = get_logger(__name__)


class MetricsInterceptor(grpc.aio.ServerInterceptor):
    """Per-RPC latency/status hook point (HLD §24). The servicer's own
    `metrics.query_latency_histogram.time()` (LLD §7.1) is the real
    Prometheus instrument for `GetRelevantChunks`'s pipeline-internal
    timing; this interceptor exists at the transport boundary instead —
    logs latency/status for every unary RPC method, including ones that
    never reach `pipeline.run()` at all (e.g. a malformed request grpc
    itself rejects). Phase 11 wires this hook into its own dedicated
    Prometheus instrument rather than only structlog, per the
    Implementation Plan's own framing of this phase.
    """

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or handler.response_streaming or handler.request_streaming:
            return handler

        method = handler_call_details.method
        original_behavior = handler.unary_unary

        async def new_behavior(request, context):
            start = time.monotonic()
            status = "OK"
            try:
                return await original_behavior(request, context)
            except grpc.aio.AbortError:
                status = "ABORTED"
                raise
            except Exception:
                status = "ERROR"
                raise
            finally:
                elapsed = time.monotonic() - start
                logger.info("grpc.request", method=method, status=status, duration_seconds=elapsed)

        return grpc.unary_unary_rpc_method_handler(
            new_behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
