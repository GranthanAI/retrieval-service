import re
import uuid

# HLD §11 — a fixed namespace UUID for this service's chunk_id derivation.
# Generated once (uuid.uuid5(uuid.NAMESPACE_URL, "retrieval-service.chunk"))
# and frozen here — changing this value would silently re-derive every
# chunk_id in the system, breaking the upsert-by-id idempotency HLD §11
# depends on.
RETRIEVAL_CHUNK_NAMESPACE = uuid.UUID("6f6a9b8e-6c0c-5b8a-9d7d-9a3f2f6b8a10")

# HLD §8.3 Phase 1 — storage.key is "{owner_id}/{document_id}/v{n}/original.{ext}".
_CONTENT_VERSION_PATTERN = re.compile(r"/v(\d+)/")


class ContentVersionParseError(ValueError):
    """storage.key does not match the expected `{owner_id}/{document_id}/
    v(\\d+)/` pattern — HLD §8.3 requires rejecting (dead-letter/manual
    review), never silently defaulting to version 1, since chunk identity
    cannot be computed safely without a real version number.
    """


def parse_content_version(storage_key: str) -> int:
    """HLD §8.3 Phase 1 — parses `content_version` from `storage.key`'s
    `v{n}/` segment with strict validation. Kept permanently as a defensive
    fallback even after File Service ships an explicit `content_version`
    field on the event (HLD §8.3 Phase 2), not temporary scaffolding.
    """
    match = _CONTENT_VERSION_PATTERN.search(storage_key)
    if match is None:
        raise ContentVersionParseError(
            f"storage_key {storage_key!r} does not match the expected "
            "'{owner_id}/{document_id}/v(\\d+)/...' pattern"
        )
    return int(match.group(1))


def derive_chunk_id(document_id: str, content_version: int, chunk_index: int) -> str:
    """HLD §11 — deterministic uuid5 on (document_id, content_version,
    chunk_index). A redelivered `file.uploaded` for the same version
    reproduces the same chunk_id, making both the Milvus upsert and the
    OpenSearch index write naturally idempotent (upsert-by-id, not insert).
    """
    name = f"{document_id}:{content_version}:{chunk_index}"
    return str(uuid.uuid5(RETRIEVAL_CHUNK_NAMESPACE, name))
