"""A YouTube transcript object goes through the SAME orchestrator as documents: chunks keep their time ranges, the document
parser is never involved, the job ends READY with a `completed` event, and an unreadable transcript FAILS instead of indexing."""

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app.ingestion import orchestrator
from app.ingestion import transcript as t
from app.ingestion.indexer import IndexResult
from app.models.outbox_event import OutboxEvent
from app.repositories.ingestion_job_repository import IngestionJobRepository

OWNER = uuid.uuid4()


def canonical(n=60):
    segs = [{"start": i * 5.0, "duration": 5.0, "text": f"Sentence number {i} about gradient descent."} for i in range(n)]
    return json.dumps({"schema": t.SCHEMA, "video_id": "dQw4w9WgXcQ", "title": "GD", "language": "en", "is_generated": True, "segments": segs}).encode()


async def make_job(session_factory, document_id, filename="GD (YouTube transcript).json"):
    async with session_factory() as db:
        await IngestionJobRepository.upsert_job(db, document_id=document_id, content_version=1, owner_id=OWNER, conversation_id=None,
                                                filename=filename, storage_bucket="b", storage_key="k")
        await db.commit()


async def _embed(client, chunks):
    return [[0.1] * 4 for _ in chunks]


@pytest.mark.asyncio
async def test_transcript_is_indexed_with_time_ranges_and_never_touches_the_document_parser(session_factory) -> None:
    document_id = uuid.uuid4()
    await make_job(session_factory, document_id)
    captured = {}

    async def fake_write(records, target):
        captured["records"] = records
        return IndexResult(milvus_status="COMPLETE", opensearch_status="COMPLETE")

    def boom(raw, name):
        raise AssertionError("the document parser must not run for a transcript")

    with (
        patch.object(orchestrator.downloader, "fetch", AsyncMock(return_value=canonical())),
        patch.object(orchestrator.parser_module, "parse", boom),
        patch.object(orchestrator, "embed_batch", _embed),
        patch.object(orchestrator, "index_write", fake_write),
    ):
        await orchestrator.run(session_factory, document_id, 1, AsyncMock())

    recs = captured["records"]
    assert len(recs) >= 1
    assert all(r.document_id == str(document_id) and r.content_version == 1 and r.owner_id == str(OWNER) for r in recs)
    assert all(r.filename == "GD (YouTube transcript).json" and r.page_number is None for r in recs)
    ranges = [t.parse_section(r.section) for r in recs]
    assert all(ranges) and ranges[0][0] == 0 and ranges[-1][1] >= 299
    assert [r.chunk_index for r in recs] == list(range(len(recs)))
    assert len({r.chunk_id for r in recs}) == len(recs)

    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
        events = [e for e in (await db.execute(select(OutboxEvent))).scalars().all() if e.payload.get("document_id") == str(document_id)]
    assert job.status == "READY" and job.chunk_count == len(recs)
    assert any("completed" in e.topic for e in events) and not any("failed" in e.topic for e in events)


@pytest.mark.asyncio
async def test_redelivery_reproduces_identical_chunk_ids(session_factory) -> None:
    document_id = uuid.uuid4()
    await make_job(session_factory, document_id)
    seen = []

    async def fake_write(records, target):
        seen.append([r.chunk_id for r in records])
        return IndexResult(milvus_status="COMPLETE", opensearch_status="COMPLETE")

    for _ in range(2):
        with (
            patch.object(orchestrator.downloader, "fetch", AsyncMock(return_value=canonical())),
            patch.object(orchestrator, "embed_batch", _embed),
            patch.object(orchestrator, "index_write", fake_write),
        ):
            await orchestrator.run(session_factory, document_id, 1, AsyncMock())
    assert len(seen) >= 1 and all(s == seen[0] for s in seen)


@pytest.mark.asyncio
async def test_a_malformed_transcript_fails_visibly_and_is_never_indexed(session_factory) -> None:
    document_id = uuid.uuid4()
    await make_job(session_factory, document_id)
    bad = json.dumps({"schema": t.SCHEMA, "segments": [{"start": "x", "duration": 1, "text": "a"}]}).encode()
    write = AsyncMock()
    with (
        patch.object(orchestrator.downloader, "fetch", AsyncMock(return_value=bad)),
        patch.object(orchestrator, "index_write", write),
    ):
        await orchestrator.run(session_factory, document_id, 1, AsyncMock())
    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
    assert job.status == "FAILED" and job.error_code == "PARSER_ERROR"
    write.assert_not_called()


@pytest.mark.asyncio
async def test_an_all_blank_transcript_fails_with_the_existing_no_text_rule(session_factory) -> None:
    document_id = uuid.uuid4()
    await make_job(session_factory, document_id)
    blank = json.dumps({"schema": t.SCHEMA, "segments": [{"start": 0, "duration": 1, "text": " "}]}).encode()
    with patch.object(orchestrator.downloader, "fetch", AsyncMock(return_value=blank)):
        await orchestrator.run(session_factory, document_id, 1, AsyncMock())
    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
    assert job.status == "FAILED" and job.error_code == "NO_EXTRACTABLE_TEXT"
