"""
Phase 2 — Outbox Publisher / Drainer (LLD §6.8).

Polls `outbox WHERE published_at IS NULL` in batches of 200, publishes each
row to Kafka, then marks the batch as published.

Loop interval: 100 ms (or woken immediately via Postgres LISTEN 'outbox_new').

Failure handling:
  - Kafka producer error → leave rows unpublished; next pass retries.
  - Kafka producer is idempotent (enable.idempotence=True) and uses acks=all.

Usage::

    pub = OutboxPublisher(repo=repo, brokers="kafka:9092")
    await pub.run()   # blocks forever; run as asyncio.create_task
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_BATCH_SIZE = 200
_POLL_INTERVAL_S = 0.1   # 100 ms

try:
    from aiokafka import AIOKafkaProducer
    _AIOKAFKA_AVAILABLE = True
except ImportError:  # pragma: no cover
    _AIOKAFKA_AVAILABLE = False
    AIOKafkaProducer = None  # type: ignore[assignment,misc]


class OutboxPublisher:
    """
    Drains the transactional outbox to Kafka.

    Args:
        repo:    LedgerRepo instance (needs fetch_outbox_batch + mark_outbox_published).
        brokers: Comma-separated Kafka broker addresses, e.g. "kafka:9092".
    """

    def __init__(self, repo, brokers: str = "kafka:9092") -> None:
        if not _AIOKAFKA_AVAILABLE:
            raise RuntimeError(
                "aiokafka is not installed. Run: pip install 'recon-agent[kafka]'"
            )
        self._repo = repo
        self._brokers = brokers
        self._producer: Optional[AIOKafkaProducer] = None
        self._notify_event = asyncio.Event()

    async def run(self) -> None:
        """Start the Kafka producer and drain loop."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._brokers,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()
        logger.info("OutboxPublisher started; brokers=%s", self._brokers)
        try:
            await self._drain_loop()
        finally:
            await self._producer.stop()

    async def _drain_loop(self) -> None:
        while True:
            try:
                published = await self._drain_once()
                if published == 0:
                    # Nothing to do — sleep or wait for NOTIFY wake-up
                    try:
                        await asyncio.wait_for(
                            self._notify_event.wait(), timeout=_POLL_INTERVAL_S
                        )
                        self._notify_event.clear()
                    except asyncio.TimeoutError:
                        pass
            except Exception as exc:
                logger.error("OutboxPublisher drain error: %s", exc)
                await asyncio.sleep(1)

    async def _drain_once(self) -> int:
        """
        Fetch one batch, publish to Kafka, mark as published.
        Returns number of rows published.
        """
        batch = await self._repo.fetch_outbox_batch(batch_size=_BATCH_SIZE)
        if not batch:
            return 0

        published_ids: list[int] = []
        for row in batch:
            try:
                await self._producer.send(
                    topic=row["topic"],
                    value=bytes(row["payload"]),
                    key=(row["partition_key"] or "").encode("utf-8") or None,
                )
                published_ids.append(row["id"])
            except Exception as exc:
                # Leave this row unpublished; it will be retried on the next pass.
                logger.warning(
                    "Failed to publish outbox id=%d topic=%s: %s",
                    row["id"], row["topic"], exc,
                )

        await self._producer.flush()
        await self._repo.mark_outbox_published(published_ids)

        logger.debug("OutboxPublisher: published %d rows", len(published_ids))
        return len(published_ids)

    def notify(self) -> None:
        """
        Wake the drain loop immediately (call from a Postgres NOTIFY listener
        or any async context when new rows are known to have been committed).
        """
        self._notify_event.set()


# ── Postgres NOTIFY listener (optional integration) ───────────────────────────


async def listen_for_outbox_notify(pool, publisher: OutboxPublisher) -> None:
    """
    Listen on Postgres channel 'outbox_new' and wake the publisher whenever
    the trigger fires (migration 002_phase2.sql sets this up).

    Run as a background asyncio task alongside OutboxPublisher.run().
    """
    async with pool.acquire() as conn:
        await conn.add_listener("outbox_new", lambda *_: publisher.notify())
        logger.info("Listening for Postgres NOTIFY outbox_new")
        while True:
            await asyncio.sleep(3600)  # keep connection alive; listener is event-driven
