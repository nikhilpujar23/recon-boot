"""
Phase 2 — Kafka consumer for topic `recon.requests` (LLD §6.3 / §8.1).

Consumes ReconRequest messages, runs the rules engine, persists the resulting
ReconCase, and emits a ReconResult outbox event.

Consumer group: recon.rules  (configurable via KAFKA_CONSUMER_GROUP_PREFIX)

Message flow:
  Parser → outbox → Kafka `recon.requests`
                         ↓
               ReconRequestConsumer.consume()
                         ↓
               RulesEngine.run(line) → ReconCase
                         ↓
               repo.insert_recon_case(case)
               repo.insert_outbox_tx(ReconResult)
                         ↓
               Kafka `recon.results`
               (unresolved → Kafka `recon.investigate` for Agent, Phase 3)

Resilience:
  - DB or rules error → log and continue (no commit); Kafka auto-commit is
    disabled so the offset is not advanced on failure.
  - DLQ: after max_retries failures for the same offset, forward to `recon.dlq`.

Usage::

    consumer = ReconRequestConsumer(
        brokers="kafka:9092",
        repo=repo,
        group_prefix="recon",
    )
    await consumer.run()
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.errors import GroupCoordinatorNotAvailableError, KafkaConnectionError
    _AIOKAFKA_AVAILABLE = True
except ImportError:  # pragma: no cover
    _AIOKAFKA_AVAILABLE = False
    AIOKafkaConsumer = None  # type: ignore[assignment,misc]
    AIOKafkaProducer = None  # type: ignore[assignment,misc]
    GroupCoordinatorNotAvailableError = Exception  # type: ignore[assignment,misc]
    KafkaConnectionError = Exception  # type: ignore[assignment,misc]

_TRANSIENT = (GroupCoordinatorNotAvailableError, KafkaConnectionError)

# Topics that need agent investigation (Phase 3 will consume these)
_AGENT_MATCH_TYPES = frozenset({"AMOUNT_MISMATCH", "MISSING_LEG", "UNKNOWN"})

TOPIC_RECON_INVESTIGATE = "recon.investigate"
TOPIC_RECON_DLQ = "recon.dlq"


class ReconRequestConsumer:
    """
    Consumes `recon.requests` → runs rules engine → writes recon_case + outbox result.

    Args:
        brokers:      Kafka bootstrap servers.
        repo:         LedgerRepo instance.
        group_prefix: KAFKA_CONSUMER_GROUP_PREFIX env var value.
        max_retries:  Attempts before a message is forwarded to DLQ.
    """

    TOPIC = "recon.requests"

    def __init__(
        self,
        brokers: str,
        repo,
        group_prefix: str = "recon",
        max_retries: int = 3,
    ) -> None:
        if not _AIOKAFKA_AVAILABLE:
            raise RuntimeError("aiokafka not installed. Run: pip install 'recon-agent[kafka]'")
        self._brokers = brokers
        self._repo = repo
        self._group = f"{group_prefix}.rules"
        self._max_retries = max_retries
        # failure counter keyed by (partition, offset)
        self._failures: dict[tuple[int, int], int] = {}

    async def run(self) -> None:
        """Start consuming `recon.requests` — blocks indefinitely, retries on broker unavailability."""
        delay = 2.0
        attempt = 0
        while True:
            consumer = AIOKafkaConsumer(
                self.TOPIC,
                bootstrap_servers=self._brokers,
                group_id=self._group,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                retry_backoff_ms=500,
            )
            producer = AIOKafkaProducer(
                bootstrap_servers=self._brokers,
                enable_idempotence=True,
                acks="all",
            )
            try:
                await consumer.start()
                await producer.start()
            except _TRANSIENT as exc:
                attempt += 1
                logger.warning(
                    "ReconRequestConsumer: broker not ready (attempt %d): %s — retrying in %.0fs",
                    attempt, exc, delay,
                )
                await consumer.stop()
                await producer.stop()
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue

            logger.info(
                "ReconRequestConsumer started; group=%s topic=%s", self._group, self.TOPIC
            )
            delay = 2.0
            attempt = 0
            try:
                async for msg in consumer:
                    ok = await self._handle(msg.value, producer)
                    if ok:
                        await consumer.commit()
                    else:
                        key = (msg.partition, msg.offset)
                        self._failures[key] = self._failures.get(key, 0) + 1
                        if self._failures[key] >= self._max_retries:
                            await self._send_dlq(producer, msg)
                            await consumer.commit()
                            del self._failures[key]
            except _TRANSIENT as exc:
                logger.warning("ReconRequestConsumer: lost broker connection: %s — reconnecting", exc)
            finally:
                await consumer.stop()
                await producer.stop()

    async def _handle(self, payload: bytes, producer: "AIOKafkaProducer") -> bool:
        from recon.messaging import (
            SCHEMA_RECON_REQUEST,
            SCHEMA_RECON_RESULT,
            TOPIC_RECON_RESULTS,
            decode,
            encode,
            recon_result_from_case,
        )
        from recon.models import SettlementLine
        from recon.rules.engine import RulesEngine

        try:
            req = decode(SCHEMA_RECON_REQUEST, payload)
        except Exception as exc:
            logger.error("ReconRequestConsumer: decode error: %s", exc)
            return False

        # Fetch the settlement line from DB
        row = await self._repo.fetch_settlement_line_by_case(
            file_id=req.file_id, line_no=req.line_no
        )
        if row is None:
            logger.warning(
                "ReconRequestConsumer: settlement line not found: file_id=%s line_no=%d",
                req.file_id, req.line_no,
            )
            return False

        raw = json.loads(row["raw"]) if row["raw"] else {}
        line = SettlementLine(
            file_id=row["file_id"],
            line_no=row["line_no"],
            rrn=row["rrn"],
            utr=row["utr"],
            amount_paise=row["amount_paise"],
            fee_paise=row["fee_paise"],
            net_paise=row["net_paise"],
            status=row["status"],
            txn_date=None,
            payer_vpa=None,
            payee_vpa=None,
            txn_type=None,
            remarks=None,
            raw=raw,
            id=row["id"],
        )

        # Run rules engine
        engine = RulesEngine(self._repo)
        try:
            case = await engine.run(line)
        except Exception as exc:
            logger.error(
                "ReconRequestConsumer: rules error for case_uid=%s: %s", req.case_uid, exc
            )
            return False

        # Persist the recon case
        try:
            await self._repo.insert_recon_case(case)
        except Exception as exc:
            logger.error(
                "ReconRequestConsumer: DB write error for case_uid=%s: %s", req.case_uid, exc
            )
            return False

        # Emit ReconResult to Kafka
        result_msg = recon_result_from_case(case)
        result_payload = encode(result_msg)
        try:
            await producer.send(
                topic=TOPIC_RECON_RESULTS,
                value=result_payload,
                key=str(case.case_uid).encode("utf-8"),
            )
        except Exception as exc:
            logger.warning(
                "ReconRequestConsumer: failed to publish ReconResult for %s: %s",
                case.case_uid, exc,
            )
            # Non-fatal: the case is already in the DB; result will be recomputable.

        # If case needs agent investigation, forward to recon.investigate (Phase 3)
        if case.match_type in _AGENT_MATCH_TYPES:
            try:
                await producer.send(
                    topic=TOPIC_RECON_INVESTIGATE,
                    value=payload,  # forward the original ReconRequest
                    key=str(case.case_uid).encode("utf-8"),
                )
                logger.debug(
                    "Forwarded to recon.investigate: case_uid=%s match_type=%s",
                    case.case_uid, case.match_type,
                )
            except Exception as exc:
                logger.warning(
                    "ReconRequestConsumer: failed to forward to investigate: %s", exc
                )

        return True

    async def _send_dlq(self, producer: "AIOKafkaProducer", msg) -> None:
        """Forward a failed message to the DLQ topic with error metadata."""
        try:
            dlq_payload = json.dumps({
                "original_topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "payload_b64": msg.value.hex(),
                "attempts": self._max_retries,
            }).encode("utf-8")
            await producer.send(
                topic=TOPIC_RECON_DLQ,
                value=dlq_payload,
                key=msg.key,
                headers=[
                    ("error_class", b"max_retries_exceeded"),
                    ("original_topic", msg.topic.encode()),
                ],
            )
            logger.warning(
                "Sent to DLQ: topic=%s partition=%d offset=%d",
                msg.topic, msg.partition, msg.offset,
            )
        except Exception as exc:
            logger.error("Failed to send to DLQ: %s", exc)
