from app.core import metrics
from app.core.enums import RetrievalScope


def check(scope: RetrievalScope, file_ids: list[str]) -> bool:
    """HLD §16.3/§16.4 — returns True if the pipeline should proceed to
    authorization; False -> empty response.

    Deliberately the smallest possible module: HLD §16.3 froze this as a
    two-line decision (not FILES, or empty file_ids -> stop) and any
    complexity creeping in here is a sign scope semantics are being
    guessed at again, which the HLD explicitly deferred to a product
    decision (KNOWLEDGE_BASE/ALL remain undefined, tracked as HLD §25 Open
    Item #2).
    """
    if scope != RetrievalScope.FILES:
        metrics.unsupported_scope_counter.labels(scope=scope.name).inc()
        return False
    if not file_ids:
        return False
    return True
