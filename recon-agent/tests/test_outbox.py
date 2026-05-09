"""
Unit tests for the outbox drainer (OutboxPublisher).

No live Kafka or DB required; we mock repo methods and the AIOKafkaProducer.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from recon.messaging import SCHEMA_RECON_REQUEST, TOPIC_RECON_REQUESTS, ReconRequest, encode


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_outbox_row(
    id: int = 1,
    topic: str = TOPIC_RECON_REQUESTS,
    partition_key: str = "f1",
    schema_id: int = SCHEMA_RECON_REQUEST,
) -> dict:
    msg = ReconRequest(file_id="f1", line_no=id, case_uid=f"uid-{id}")
    return {
        "id": id,
        "topic": topic,
        "partition_key": partition_key,
        "payload": encode(msg),
        "schema_id": schema_id,
    }


def _make_publisher(batch: list[dict]):
    """Create an OutboxPublisher with a mocked repo and Kafka producer."""
    from recon.ledger.outbox import OutboxPublisher

    mock_repo = MagicMock()
    mock_repo.fetch_outbox_batch = AsyncMock(return_value=batch)
    mock_repo.mark_outbox_published = AsyncMock(return_value=None)

    with patch("recon.ledger.outbox._AIOKAFKA_AVAILABLE", True):
        pub = OutboxPublisher.__new__(OutboxPublisher)
        pub._repo = mock_repo
        pub._brokers = "kafka:9092"
        pub._notify_event = MagicMock()
        pub._notify_event.wait = AsyncMock(side_effect=Exception("stop"))

    return pub, mock_repo


# ── _drain_once ───────────────────────────────────────────────────────────────


class TestDrainOnce:
    @pytest.mark.asyncio
    async def test_empty_batch_returns_zero(self):
        pub, repo = _make_publisher(batch=[])
        mock_producer = AsyncMock()
        pub._producer = mock_producer

        result = await pub._drain_once()

        assert result == 0
        mock_producer.send.assert_not_awaited()
        repo.mark_outbox_published.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_single_row_published(self):
        row = _make_outbox_row(id=1)
        pub, repo = _make_publisher(batch=[row])
        mock_producer = AsyncMock()
        pub._producer = mock_producer

        result = await pub._drain_once()

        assert result == 1
        mock_producer.send.assert_awaited_once()
        call_args = mock_producer.send.call_args
        assert call_args.kwargs["topic"] == TOPIC_RECON_REQUESTS
        assert call_args.kwargs["value"] == bytes(row["payload"])

    @pytest.mark.asyncio
    async def test_multiple_rows_all_published(self):
        rows = [_make_outbox_row(id=i) for i in range(1, 6)]
        pub, repo = _make_publisher(batch=rows)
        mock_producer = AsyncMock()
        pub._producer = mock_producer

        result = await pub._drain_once()

        assert result == 5
        assert mock_producer.send.await_count == 5
        published_ids = repo.mark_outbox_published.call_args[0][0]
        assert published_ids == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_failed_produce_leaves_row_unpublished(self):
        """If Kafka producer raises on one row, that row is NOT in published_ids."""
        rows = [_make_outbox_row(id=1), _make_outbox_row(id=2)]
        pub, repo = _make_publisher(batch=rows)

        mock_producer = AsyncMock()
        # First call succeeds, second raises
        mock_producer.send = AsyncMock(
            side_effect=[None, Exception("Kafka down")]
        )
        pub._producer = mock_producer

        result = await pub._drain_once()

        assert result == 1  # only id=1 succeeded
        published_ids = repo.mark_outbox_published.call_args[0][0]
        assert published_ids == [1]
        assert 2 not in published_ids

    @pytest.mark.asyncio
    async def test_partition_key_passed_to_producer(self):
        row = _make_outbox_row(id=1, partition_key="my_file")
        pub, repo = _make_publisher(batch=[row])
        mock_producer = AsyncMock()
        pub._producer = mock_producer

        await pub._drain_once()

        call_args = mock_producer.send.call_args
        assert call_args.kwargs["key"] == b"my_file"

    @pytest.mark.asyncio
    async def test_empty_partition_key_sends_none(self):
        row = _make_outbox_row(id=1, partition_key="")
        pub, repo = _make_publisher(batch=[row])
        mock_producer = AsyncMock()
        pub._producer = mock_producer

        await pub._drain_once()

        call_args = mock_producer.send.call_args
        # empty string → None
        assert call_args.kwargs["key"] is None


# ── mark_outbox_published (repo layer) ───────────────────────────────────────


class TestMarkPublished:
    @pytest.mark.asyncio
    async def test_empty_ids_does_not_call_db(self):
        """mark_outbox_published with empty list should short-circuit."""
        from unittest.mock import AsyncMock, MagicMock

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock()

        from recon.ledger.repo import LedgerRepo
        repo = LedgerRepo.__new__(LedgerRepo)
        repo._pool = mock_pool

        # Should not raise or call the pool
        await repo.mark_outbox_published([])
        mock_pool.acquire.assert_not_called()


# ── notify() ──────────────────────────────────────────────────────────────────


class TestNotify:
    def test_notify_sets_event(self):
        import asyncio
        from recon.ledger.outbox import OutboxPublisher

        with patch("recon.ledger.outbox._AIOKAFKA_AVAILABLE", True):
            pub = OutboxPublisher.__new__(OutboxPublisher)
            pub._notify_event = asyncio.Event()
            assert not pub._notify_event.is_set()
            pub.notify()
            assert pub._notify_event.is_set()
