import grpc
import structlog


class LoggingInterceptor(grpc.aio.ServerInterceptor):
    """Binds `request.trace_id` (HLD §17's correlation field) into
    structlog contextvars for the duration of the call, so every log line
    inside `pipeline.run()` — and everything it calls — carries the same
    `trace_id` a caller can grep for, same pattern as file-service's own
    `RequestIDMiddleware` adapted for gRPC.
    """

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or handler.response_streaming or handler.request_streaming:
            return handler

        original_behavior = handler.unary_unary

        async def new_behavior(request, context):
            trace_id = getattr(request, "trace_id", None)
            structlog.contextvars.bind_contextvars(trace_id=trace_id)
            try:
                return await original_behavior(request, context)
            finally:
                structlog.contextvars.clear_contextvars()

        return grpc.unary_unary_rpc_method_handler(
            new_behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
