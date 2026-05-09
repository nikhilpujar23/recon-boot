"""
Phase 2 — Kafka consumer for topic `files.new` (LLD §6.1 / §8.1).

Consumes FileArrived messages and triggers UdirParser for each new file.

Consumer group: recon.ingest  (configurable via KAFKA_CONSUMER_GROUP_PREFIX)

Message flow:
  SftpWatcher → outbox → Kafka `files.new`
                              ↓
                    FileIngestConsumer.consume()
                              ↓
                    UdirParser.parse(file_id, path)
                              ↓
                    settlement_lines + outbox(ReconRequest)  → `recon.requests`

Usage (run as a long-lived service)::

    consumer = FileIngestConsumer(
        brokers="kafka:9092",
        watch_dir=Path("/data/sftp"),
        repo=repo,
        group_prefix="recon",
    )
    await consumer.run()
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    from aiokafka import AIOKafkaConsumer
    from aiokafka.errors import GroupCoordinatorNotAvailableError, KafkaConnectionError
    _AIOKAFKA_AVAILABLE = True
except ImportError:  # pragma: no cover
    _AIOKAFKA_AVAILABLE = False
    AIOKafkaConsumer = None  # type: ignore[assignment,misc]
    GroupCoordinatorNotAvailableError = Exception  # type: ignore[assignment,misc]
    KafkaConnectionError = Exception  # type: ignore[assignment,misc]


class FileIngestConsumer:
    """
    Consumes `files.new` → invokes UdirParser for each FileArrived message.

    Args:
        brokers:      Kafka bootstrap servers.
        watch_dir:    Local directory where settlement files are stored.
        repo:         LedgerRepo (for parser + outbox writes).
        group_prefix: KAFKA_CONSUMER_GROUP_PREFIX env var value.
    """

    TOPIC = "files.new"

    def __init__(
        self,
        brokers: str,
        watch_dir: Path,
        repo,
        group_prefix: str = "recon",
    ) -> None:
        if not _AIOKAFKA_AVAILABLE:
            raise RuntimeError("aiokafka not installed. Run: pip install 'recon-agent[kafka]'")
        self._brokers = brokers
        self._watch_dir = watch_dir
        self._repo = repo
        self._group = f"{group_prefix}.ingest"

    async def run(self) -> None:
        """Start consuming `files.new` — blocks indefinitely, retries on broker unavailability."""
        _TRANSIENT = (GroupCoordinatorNotAvailableError, KafkaConnectionError)
        delay = 2.0
        attempt = 0
        while True:
            consumer = AIOKafkaConsumer(
                self.TOPIC,
                bootstrap_servers=self._brokers,
                group_id=self._group,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                retry_backoff_ms=500,
            )
            try:
                await consumer.start()
            except _TRANSIENT as exc:
                attempt += 1
                logger.warning(
                    "FileIngestConsumer: broker not ready (attempt %d): %s — retrying in %.0fs",
                    attempt, exc, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue

            logger.info("FileIngestConsumer started; group=%s topic=%s", self._group, self.TOPIC)
            delay = 2.0
            attempt = 0
            try:
                async for msg in consumer:
                    await self._handle(msg.value, msg.key)
            except _TRANSIENT as exc:
                logger.warning("FileIngestConsumer: lost broker connection: %s — reconnecting", exc)
            finally:
                await consumer.stop()

    async def _handle(self, payload: bytes, key: bytes | None) -> None:
        from recon.ingest.parser import UdirParser
        from recon.messaging import SCHEMA_FILE_ARRIVED, decode

        try:
            event = decode(SCHEMA_FILE_ARRIVED, payload)
        except Exception as exc:
            logger.error("FileIngestConsumer: failed to decode message: %s", exc)
            return

        path = self._watch_dir / event.filename
        if not path.exists():
            logger.error(
                "FileIngestConsumer: file not found on disk: %s (file_id=%s)",
                path, event.file_id,
            )
            return

        parser = UdirParser(repo=self._repo, outbox_enabled=True)
        try:
            written = await parser.parse(file_id=event.file_id, path=path)
            logger.info(
                "FileIngestConsumer: parsed file_id=%s → %d lines written",
                event.file_id, written,
            )
        except Exception as exc:
            logger.error(
                "FileIngestConsumer: parse error for file_id=%s: %s",
                event.file_id, exc,
            )
