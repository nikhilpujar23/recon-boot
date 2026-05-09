"""
Phase 3 — Kafka consumer for topic `recon.investigate` (LLD §6.4 / §8.2).

Consumes ReconRequest messages forwarded by the rules worker for cases
that the rules engine could not deterministically resolve (AMOUNT_MISMATCH,
MISSING_LEG, UNKNOWN), then runs AgentOrchestrator.investigate().

Consumer group: recon.agent  (configurable via KAFKA_CONSUMER_GROUP_PREFIX)

Message flow:
  rules-worker → Kafka recon.investigate
                        ↓
               AgentConsumer.consume()
                        ↓
               AgentOrchestrator.investigate(case)
                        ↓
               UPDATE recon_cases (write guard)
               INSERT agent_traces
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

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

_TRANSIENT = (GroupCoordinatorNotAvailableError, KafkaConnectionError)

TOPIC_RECON_INVESTIGATE = "recon.investigate"


class AgentConsumer:
    """
    Consumes `recon.investigate` and runs the AgentOrchestrator for each case.

    Args:
        brokers:      Kafka bootstrap servers.
        repo:         LedgerRepo instance.
        group_prefix: KAFKA_CONSUMER_GROUP_PREFIX env var.
        mock_llm:     If True, use MockLLMClient (no Anthropic API calls).
    """

    def __init__(
        self,
        brokers: str,
        repo,
        group_prefix: str = "recon",
        mock_llm: bool = False,
    ) -> None:
        if not _AIOKAFKA_AVAILABLE:
            raise RuntimeError("aiokafka not installed. Run: pip install 'recon-agent[kafka]'")
        self._brokers = brokers
        self._repo = repo
        self._group = f"{group_prefix}.agent"
        self._mock_llm = mock_llm
        self._orchestrator = None  # built lazily in run()

    async def run(self) -> None:
        """Start consuming `recon.investigate` — blocks indefinitely, retries on broker unavailability."""
        self._orchestrator = await self._build_orchestrator()

        delay = 2.0
        attempt = 0
        while True:
            consumer = AIOKafkaConsumer(
                TOPIC_RECON_INVESTIGATE,
                bootstrap_servers=self._brokers,
                group_id=self._group,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                retry_backoff_ms=500,
            )
            try:
                await consumer.start()
            except _TRANSIENT as exc:
                attempt += 1
                logger.warning(
                    "AgentConsumer: broker not ready (attempt %d): %s — retrying in %.0fs",
                    attempt, exc, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue

            logger.info(
                "AgentConsumer started; group=%s topic=%s mock=%s",
                self._group, TOPIC_RECON_INVESTIGATE, self._mock_llm,
            )
            delay = 2.0
            attempt = 0
            try:
                async for msg in consumer:
                    await self._handle(msg.value)
                    await consumer.commit()
            except _TRANSIENT as exc:
                logger.warning("AgentConsumer: lost broker connection: %s — reconnecting", exc)
            finally:
                await consumer.stop()

    async def _handle(self, payload: bytes) -> None:
        from recon.messaging import SCHEMA_RECON_REQUEST, decode
        from recon.models import SettlementLine

        try:
            req = decode(SCHEMA_RECON_REQUEST, payload)
        except Exception as exc:
            logger.error("AgentConsumer: decode error: %s", exc)
            return

        # Fetch the ReconCase from DB (already created by rules-worker)
        case = await self._repo.fetch_recon_case_by_file_line(
            file_id=req.file_id, line_no=req.line_no
        )
        if case is None:
            logger.warning(
                "AgentConsumer: no recon_case found for file_id=%s line_no=%d",
                req.file_id, req.line_no,
            )
            return

        # Skip if already resolved by another worker
        if case.get("resolution") is not None:
            logger.debug(
                "AgentConsumer: case already resolved (%s), skipping",
                case["resolution"],
            )
            return

        # Build a ReconCase model object
        from recon.models import ReconCase
        import uuid as _uuid
        recon_case = ReconCase(
            case_uid=_uuid.UUID(str(case["case_uid"])),
            settlement_line_id=case["settlement_line"],
            pg_txn_id=case["pg_transaction"],
            match_type=case["match_type"],
            confidence=float(case["confidence"]) if case["confidence"] is not None else None,
            resolution=case["resolution"],
            resolved_by=case["resolved_by"],
            notes=case["notes"] if isinstance(case["notes"], dict) else {},
            id=case["id"],
        )

        logger.info(
            "AgentConsumer: investigating case_uid=%s match_type=%s",
            recon_case.case_uid, recon_case.match_type,
        )

        decision = await self._orchestrator.investigate(recon_case)

        logger.info(
            "AgentConsumer: case_uid=%s → %s (conf=%.2f, cost=$%.4f, model=%s)",
            decision.case_uid,
            decision.resolution_type,
            decision.confidence,
            decision.cost_usd,
            decision.model_used,
        )

    async def _build_orchestrator(self):
        from recon.agent.orchestrator import AgentOrchestrator
        from recon.agent.tools import ToolRegistry

        tools = ToolRegistry(repo=self._repo)

        if self._mock_llm:
            client = _build_mock_client()
        else:
            from recon.config import settings
            try:
                from anthropic import AsyncAnthropic
                client = AsyncAnthropic(api_key=settings.anthropic_api_key)
            except ImportError:
                raise RuntimeError(
                    "anthropic package not installed. Run: pip install 'recon-agent[agent]'"
                )

        from recon.config import settings as s
        return AgentOrchestrator(
            client=client,
            tools=tools,
            repo=self._repo,
            model_triage=s.model_triage,
            model_invest=s.model_invest,
            max_steps=s.agent_max_steps,
            timeout_s=s.agent_timeout_s,
        )


# ── Mock LLM client (used when MOCK_LLM=true) ─────────────────────────────────


def _build_mock_client():
    """
    Returns a mock Anthropic client that always proposes ESCALATE.
    Useful for testing the pipeline without consuming API credits.
    Replace with richer scripted responses in eval tests.
    """
    import uuid

    class _MockMessage:
        stop_reason = "tool_use"
        usage = type("U", (), {
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        })()

        def __init__(self, case_uid: str):
            self.content = [
                type("Block", (), {
                    "type": "tool_use",
                    "id": f"mock_{uuid.uuid4().hex[:8]}",
                    "name": "propose_resolution",
                    "input": {
                        "case_uid": case_uid,
                        "resolution_type": "ESCALATE",
                        "confidence": 0.0,
                        "rationale": "Mock LLM: escalating for human review.",
                    },
                })(),
            ]

    class _MockMessages:
        async def create(self, messages, **kwargs):
            # Extract case_uid from first user message
            first = messages[0]["content"] if messages else ""
            try:
                import re
                m = re.search(r'"case_uid":\s*"([^"]+)"', str(first))
                uid = m.group(1) if m else str(uuid.uuid4())
            except Exception:
                uid = str(uuid.uuid4())
            return _MockMessage(uid)

    class _MockClient:
        messages = _MockMessages()

    return _MockClient()
