"""A document that parses to no text (scanned / image-only PDF) must FAIL visibly, never end READY
with zero chunks. Uses the real Postgres testcontainer + outbox; download/parse are stubbed
because the point is the orchestrator's decision, not Docling."""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app.ingestion import orchestrator
from app.ingestion.parser import ParsedDocument
from app.models.outbox_event import OutboxEvent
from app.repositories.ingestion_job_repository import IngestionJobRepository


async def _job(session_factory, document_id):
    async with session_factory() as db:
        await IngestionJobRepository.upsert_job(
            db,
            document_id=document_id,
            content_version=1,
            owner_id=uuid.uuid4(),
            conversation_id=None,
            filename="scan.pdf",
            storage_bucket="b",
            storage_key="k",
        )
        await db.commit()


@pytest.mark.asyncio
async def test_document_with_no_extractable_text_fails_instead_of_ready(session_factory) -> None:
    document_id = uuid.uuid4()
    await _job(session_factory, document_id)
    embed = AsyncMock()

    with (
        patch.object(orchestrator.downloader, "fetch", AsyncMock(return_value=b"%PDF-1.4 stub")),
        patch.object(orchestrator.parser_module, "parse", lambda raw, name: ParsedDocument(sections=[])),
    ):
        await orchestrator.run(session_factory, document_id, 1, embed)

    async with session_factory() as db:
        job = await IngestionJobRepository.get_by_document_and_version(db, document_id, 1)
        events = (await db.execute(select(OutboxEvent))).scalars().all()
    assert job.status == "FAILED"
    assert job.error_code == "NO_EXTRACTABLE_TEXT"
    embed.embed_batch.assert_not_called()  # nothing was embedded or indexed

    mine = [e for e in events if e.payload.get("document_id") == str(document_id)]
    topics = [e.topic for e in mine]
    assert any("failed" in t for t in topics)  # File Service is told: visible and retryable
    assert not any("completed" in t for t in topics)  # never announced as completed
    failed = next(e for e in mine if "failed" in e.topic)
    assert failed.payload["error_code"] == "NO_EXTRACTABLE_TEXT"
    assert "scanned" in failed.payload["error_message"]
