"""
Unit tests for the Phase 3 LLM Agent.

Covers:
  - Two-tier state machine (Haiku triage → Sonnet investigation)
  - Timeout and max-steps escalation paths
  - Tool dispatch and ToolRegistry
  - Cost calculation
  - can_auto_approve logic
  - Stale-write guard
  - AgentConsumer._handle (mock DB + mock orchestrator)
  - NullRedactor passthrough
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from recon.agent.orchestrator import (
    AgentDecision,
    AgentOrchestrator,
    _calculate_cost,
    can_auto_approve,
)
from recon.agent.tools import MUTATING_TOOL, NullRedactor, ToolRegistry
from recon.models import ReconCase


# ── Test helpers ──────────────────────────────────────────────────────────────


def _make_case(
    match_type: str = "AMOUNT_MISMATCH",
    confidence: float = 0.5,
) -> ReconCase:
    return ReconCase(
        case_uid=uuid.uuid4(),
        settlement_line_id=1,
        pg_txn_id=None,
        match_type=match_type,
        confidence=confidence,
        resolution=None,
        resolved_by=None,
        notes={},
    )


def _propose_block(
    case_uid: str,
    resolution_type: str = "MATCH",
    confidence: float = 0.95,
    pg_txn_id: Optional[int] = 42,
) -> Any:
    """Return a mock tool_use block for propose_resolution."""
    block = MagicMock()
    block.type = "tool_use"
    block.name = "propose_resolution"
    block.id = f"tu_{uuid.uuid4().hex[:8]}"
    block.input = {
        "case_uid": case_uid,
        "resolution_type": resolution_type,
        "confidence": confidence,
        "rationale": "Test rationale.",
        "pg_txn_id": pg_txn_id,
    }
    return block


def _tool_use_block(name: str, input_: dict) -> Any:
    """Return a mock tool_use block for a regular (read-only) tool."""
    block = MagicMock()
    block.type = "tool_use"
    block.name = name
    block.id = f"tu_{uuid.uuid4().hex[:8]}"
    block.input = input_
    return block


def _mock_response(
    content: list,
    stop_reason: str = "tool_use",
    input_tokens: int = 100,
    output_tokens: int = 50,
) -> MagicMock:
    resp = MagicMock()
    resp.content = content
    resp.stop_reason = stop_reason
    resp.usage = MagicMock()
    resp.usage.input_tokens = input_tokens
    resp.usage.output_tokens = output_tokens
    resp.usage.cache_read_input_tokens = 0
    resp.usage.cache_creation_input_tokens = 0
    return resp


def _make_repo() -> MagicMock:
    repo = MagicMock()
    repo.update_recon_case_proposed = AsyncMock(return_value=True)
    repo.insert_agent_trace = AsyncMock()
    repo.search_pg_transactions = AsyncMock(return_value=[])
    repo.fetch_settlement_history_by_rrn = AsyncMock(return_value=[])
    repo.lookup_pg_txns = AsyncMock(return_value=[])
    return repo


def _make_orchestrator(
    client,
    repo=None,
    max_steps: int = 6,
    timeout_s: int = 30,
) -> AgentOrchestrator:
    if repo is None:
        repo = _make_repo()
    tools = ToolRegistry(repo=repo)
    return AgentOrchestrator(
        client=client,
        tools=tools,
        repo=repo,
        model_triage="claude-haiku-4-5",
        model_invest="claude-sonnet-4-6",
        max_steps=max_steps,
        timeout_s=timeout_s,
    )


# ── Cost calculation ──────────────────────────────────────────────────────────


class TestCalculateCost:
    def test_haiku_input_pricing(self):
        cost = _calculate_cost(
            model="claude-haiku-4-5",
            input_tokens=1_000_000,
            output_tokens=0,
        )
        assert cost == pytest.approx(0.80, rel=1e-4)

    def test_haiku_output_pricing(self):
        cost = _calculate_cost(
            model="claude-haiku-4-5",
            input_tokens=0,
            output_tokens=1_000_000,
        )
        assert cost == pytest.approx(4.00, rel=1e-4)

    def test_sonnet_input_pricing(self):
        cost = _calculate_cost(
            model="claude-sonnet-4-6",
            input_tokens=1_000_000,
            output_tokens=0,
        )
        assert cost == pytest.approx(3.00, rel=1e-4)

    def test_sonnet_output_pricing(self):
        cost = _calculate_cost(
            model="claude-sonnet-4-6",
            input_tokens=0,
            output_tokens=1_000_000,
        )
        assert cost == pytest.approx(15.00, rel=1e-4)

    def test_cache_read_cheaper_than_input(self):
        cost_read = _calculate_cost(
            model="claude-haiku-4-5",
            input_tokens=0,
            output_tokens=0,
            cache_read_tokens=1_000_000,
        )
        cost_input = _calculate_cost(
            model="claude-haiku-4-5",
            input_tokens=1_000_000,
            output_tokens=0,
        )
        assert cost_read < cost_input

    def test_unknown_model_uses_default_pricing(self):
        cost = _calculate_cost(
            model="claude-unknown-99",
            input_tokens=1_000_000,
            output_tokens=0,
        )
        # Default input pricing is $3.00/M (same as Sonnet)
        assert cost == pytest.approx(3.00, rel=1e-4)

    def test_zero_tokens_zero_cost(self):
        cost = _calculate_cost(model="claude-haiku-4-5", input_tokens=0, output_tokens=0)
        assert cost == 0.0

    def test_mixed_tokens_sums_correctly(self):
        cost = _calculate_cost(
            model="claude-haiku-4-5",
            input_tokens=100_000,
            output_tokens=50_000,
            cache_read_tokens=200_000,
        )
        expected = (
            100_000 * 0.80 / 1_000_000
            + 50_000 * 4.00 / 1_000_000
            + 200_000 * 0.08 / 1_000_000
        )
        assert cost == pytest.approx(expected, rel=1e-4)


# ── can_auto_approve ──────────────────────────────────────────────────────────


class TestCanAutoApprove:
    def _decision(
        self, resolution_type: str = "MATCH", confidence: float = 0.99
    ) -> AgentDecision:
        d = AgentDecision(
            case_uid=uuid.uuid4(),
            resolution_type=resolution_type,
            resolution="PROPOSED",
            confidence=confidence,
            rationale="ok",
            pg_txn_id=1,
            model_used="claude-haiku-4-5",
            tools_called=[],
            total_ms=100,
            cost_usd=0.0,
            steps=[],
        )
        return d

    def test_approves_high_confidence_match(self):
        assert can_auto_approve(self._decision()) is True

    def test_rejects_write_off(self):
        d = self._decision(resolution_type="WRITE_OFF")
        assert can_auto_approve(d) is False

    def test_rejects_confidence_below_threshold(self):
        assert can_auto_approve(self._decision(confidence=0.98)) is False

    def test_rejects_when_disabled_flag_set(self):
        assert can_auto_approve(self._decision(), disable_auto_approve=True) is False

    def test_rejects_escalate(self):
        d = self._decision(resolution_type="ESCALATE")
        d.resolution = "ESCALATE"
        assert can_auto_approve(d) is False

    def test_rejects_amount_mismatch(self):
        d = self._decision(resolution_type="AMOUNT_MISMATCH")
        assert can_auto_approve(d) is False


# ── NullRedactor ──────────────────────────────────────────────────────────────


class TestNullRedactor:
    def test_redact_is_passthrough(self):
        r = NullRedactor()
        d = {"vpa": "alice@okhdfc", "amount": 100}
        assert r.redact_dict(d) == d

    def test_unredact_is_passthrough(self):
        r = NullRedactor()
        d = {"token": "VPA_a3b2c1", "amount": 200}
        assert r.unredact_dict(d) == d


# ── ToolRegistry ──────────────────────────────────────────────────────────────


class TestToolRegistry:
    def setup_method(self):
        self.repo = _make_repo()
        self.registry = ToolRegistry(repo=self.repo)

    @pytest.mark.asyncio
    async def test_propose_resolution_raises(self):
        with pytest.raises(ValueError, match="intercepted"):
            await self.registry.dispatch(MUTATING_TOOL, {}, step_no=1)

    @pytest.mark.asyncio
    async def test_unknown_tool_returns_error_json(self):
        result = await self.registry.dispatch("nonexistent_tool", {}, step_no=1)
        data = json.loads(result)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_search_pg_transactions_routes_to_repo(self):
        self.repo.search_pg_transactions.return_value = [
            {"id": 1, "amount_paise": 100_000, "status": "SUCCESS"}
        ]
        result = await self.registry.dispatch(
            "search_pg_transactions",
            {"rrn": "412345678901"},
            step_no=1,
        )
        data = json.loads(result)
        assert data["count"] == 1
        assert data["transactions"][0]["id"] == 1
        self.repo.search_pg_transactions.assert_called_once_with(
            rrn="412345678901",
            utr=None,
            amount_min=None,
            amount_max=None,
            date_from=None,
            date_to=None,
        )

    @pytest.mark.asyncio
    async def test_get_settlement_history_routes_to_repo(self):
        self.repo.fetch_settlement_history_by_rrn.return_value = [
            {"file_id": "f1", "line_no": 10},
            {"file_id": "f1", "line_no": 847},
        ]
        result = await self.registry.dispatch(
            "get_settlement_history",
            {"rrn": "412345678901"},
            step_no=1,
        )
        data = json.loads(result)
        assert data["count"] == 2
        self.repo.fetch_settlement_history_by_rrn.assert_called_once_with("412345678901")

    @pytest.mark.asyncio
    async def test_compute_fee_breakdown_p2m(self):
        result = await self.registry.dispatch(
            "compute_fee_breakdown",
            {"amount_paise": 100_000, "txn_type": "P2M"},
            step_no=1,
        )
        data = json.loads(result)
        # MDR = 0.9% of 100000 = 900; GST = 18% of 900 = 162
        assert data["mdr_paise"] == 900
        assert data["gst_paise"] == 162
        assert data["net_paise"] == 100_000 - 900 - 162

    @pytest.mark.asyncio
    async def test_compute_fee_breakdown_p2p_zero_fees(self):
        result = await self.registry.dispatch(
            "compute_fee_breakdown",
            {"amount_paise": 100_000, "txn_type": "P2P"},
            step_no=1,
        )
        data = json.loads(result)
        assert data["mdr_paise"] == 0
        assert data["gst_paise"] == 0
        assert data["net_paise"] == 100_000

    @pytest.mark.asyncio
    async def test_get_chargeback_status_no_reversal(self):
        self.repo.lookup_pg_txns.return_value = []
        result = await self.registry.dispatch(
            "get_chargeback_status",
            {"rrn": "412345678901"},
            step_no=1,
        )
        data = json.loads(result)
        assert data["chargeback"] is False

    @pytest.mark.asyncio
    async def test_steps_recorded_after_dispatch(self):
        await self.registry.dispatch(
            "compute_fee_breakdown",
            {"amount_paise": 50_000, "txn_type": "P2M"},
            step_no=1,
        )
        assert len(self.registry.steps) == 1
        step = self.registry.steps[0]
        assert step.tool_name == "compute_fee_breakdown"
        assert step.step == 1
        assert step.latency_ms >= 0

    @pytest.mark.asyncio
    async def test_reset_steps_clears_history(self):
        await self.registry.dispatch(
            "compute_fee_breakdown",
            {"amount_paise": 50_000, "txn_type": "P2M"},
            step_no=1,
        )
        self.registry.reset_steps()
        assert len(self.registry.steps) == 0

    @pytest.mark.asyncio
    async def test_tool_error_recorded_in_step(self):
        self.repo.search_pg_transactions.side_effect = RuntimeError("DB down")
        result = await self.registry.dispatch(
            "search_pg_transactions",
            {"rrn": "x"},
            step_no=1,
        )
        data = json.loads(result)
        assert "error" in data
        assert self.registry.steps[0].error is not None


# ── Orchestrator — happy path ─────────────────────────────────────────────────


class TestOrchestratorHappyPath:
    @pytest.mark.asyncio
    async def test_haiku_high_confidence_returns_directly(self):
        """Haiku conf=0.95 ≥ 0.9 → Sonnet never called."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()

        propose = _propose_block(str(case.case_uid), confidence=0.95)
        resp = _mock_response([propose])
        client.messages.create = AsyncMock(return_value=resp)

        orchestrator = _make_orchestrator(client)
        decision = await orchestrator.investigate(case)

        assert decision.resolution_type == "MATCH"
        assert decision.confidence == pytest.approx(0.95)
        assert decision.resolution == "PROPOSED"
        assert decision.model_used == "claude-haiku-4-5"
        assert not decision.escalated
        # Exactly one messages.create call (Haiku only, no Sonnet)
        assert client.messages.create.call_count == 1

    @pytest.mark.asyncio
    async def test_haiku_low_confidence_escalates_to_sonnet(self):
        """Haiku conf=0.5 < 0.9 → Sonnet is called; final decision from Sonnet."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()

        haiku_propose = _propose_block(str(case.case_uid), confidence=0.5)
        haiku_resp = _mock_response([haiku_propose])

        sonnet_propose = _propose_block(
            str(case.case_uid), resolution_type="MISSING_LEG", confidence=0.93
        )
        sonnet_resp = _mock_response([sonnet_propose])

        client.messages.create = AsyncMock(
            side_effect=[haiku_resp, sonnet_resp]
        )

        orchestrator = _make_orchestrator(client)
        decision = await orchestrator.investigate(case)

        assert client.messages.create.call_count == 2
        assert decision.model_used == "claude-sonnet-4-6"
        assert decision.resolution_type == "MISSING_LEG"
        assert decision.confidence == pytest.approx(0.93)

    @pytest.mark.asyncio
    async def test_escalated_haiku_skips_sonnet(self):
        """Haiku proposes ESCALATE → Sonnet is NOT called even though conf=0.0."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()

        haiku_propose = _propose_block(
            str(case.case_uid), resolution_type="ESCALATE", confidence=0.0
        )
        haiku_resp = _mock_response([haiku_propose])
        client.messages.create = AsyncMock(return_value=haiku_resp)

        orchestrator = _make_orchestrator(client)
        decision = await orchestrator.investigate(case)

        assert client.messages.create.call_count == 1
        assert decision.escalated is True
        assert decision.resolution == "ESCALATE"

    @pytest.mark.asyncio
    async def test_tool_call_before_propose(self):
        """Agent makes a tool call then proposes; two API responses expected."""
        case = _make_case()
        repo = _make_repo()
        repo.search_pg_transactions.return_value = [
            {"id": 99, "amount_paise": 150_000, "status": "SUCCESS"}
        ]
        client = MagicMock()
        client.messages = MagicMock()

        # First response: search_pg_transactions tool call
        search_block = _tool_use_block("search_pg_transactions", {"rrn": "412345678901"})
        first_resp = _mock_response([search_block], stop_reason="tool_use")

        # Second response: propose_resolution
        propose = _propose_block(str(case.case_uid), confidence=1.0, pg_txn_id=99)
        second_resp = _mock_response([propose], stop_reason="tool_use")

        client.messages.create = AsyncMock(side_effect=[first_resp, second_resp])

        orchestrator = _make_orchestrator(client, repo=repo)
        decision = await orchestrator.investigate(case)

        assert decision.confidence == pytest.approx(1.0)
        assert decision.pg_txn_id == 99
        assert "search_pg_transactions" in decision.tools_called

    @pytest.mark.asyncio
    async def test_decision_maps_write_off_to_proposed(self):
        """WRITE_OFF resolution_type → DB resolution = PROPOSED."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()

        propose = _propose_block(
            str(case.case_uid),
            resolution_type="WRITE_OFF",
            confidence=0.98,
        )
        resp = _mock_response([propose])
        client.messages.create = AsyncMock(return_value=resp)

        orchestrator = _make_orchestrator(client)
        decision = await orchestrator.investigate(case)

        assert decision.resolution_type == "WRITE_OFF"
        assert decision.resolution == "PROPOSED"

    @pytest.mark.asyncio
    async def test_cost_and_model_recorded_on_decision(self):
        """Decision carries non-zero cost and correct model name."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()

        propose = _propose_block(str(case.case_uid), confidence=0.99)
        resp = _mock_response([propose], input_tokens=500, output_tokens=200)
        client.messages.create = AsyncMock(return_value=resp)

        orchestrator = _make_orchestrator(client)
        decision = await orchestrator.investigate(case)

        assert decision.cost_usd > 0
        assert decision.model_used == "claude-haiku-4-5"
        assert decision.total_ms >= 0


# ── Orchestrator — escalation paths ──────────────────────────────────────────


class TestOrchestratorEscalation:
    @pytest.mark.asyncio
    async def test_timeout_escalates(self):
        """asyncio.wait_for timeout triggers ESCALATE decision."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()

        async def _slow_create(*args, **kwargs):
            await asyncio.sleep(5)  # longer than timeout_s=1

        client.messages.create = _slow_create

        orchestrator = _make_orchestrator(client, timeout_s=1)
        decision = await orchestrator.investigate(case)

        assert decision.escalated is True
        assert "timeout" in decision.rationale.lower()

    @pytest.mark.asyncio
    async def test_max_steps_exceeded_escalates(self):
        """Hitting step_budget without propose_resolution → ESCALATE."""
        case = _make_case()
        repo = _make_repo()
        repo.search_pg_transactions.return_value = []
        client = MagicMock()
        client.messages = MagicMock()

        # Always return a regular tool call — never propose_resolution
        call_count = 0

        async def _always_search(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            block = _tool_use_block("search_pg_transactions", {"rrn": "x"})
            return _mock_response([block], stop_reason="tool_use")

        client.messages.create = _always_search

        # max_steps=2: after 2 tool calls, budget exhausted → ESCALATE
        orchestrator = _make_orchestrator(client, repo=repo, max_steps=2)
        decision = await orchestrator.investigate(case)

        assert decision.escalated is True
        assert "max_steps" in decision.rationale

    @pytest.mark.asyncio
    async def test_end_turn_without_propose_escalates(self):
        """Model sends end_turn with no tool calls → ESCALATE."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()

        resp = _mock_response([], stop_reason="end_turn")
        client.messages.create = AsyncMock(return_value=resp)

        orchestrator = _make_orchestrator(client)
        decision = await orchestrator.investigate(case)

        assert decision.escalated is True

    @pytest.mark.asyncio
    async def test_unexpected_exception_escalates(self):
        """Unhandled exception in messages.create → ESCALATE (never raises)."""
        case = _make_case()
        client = MagicMock()
        client.messages = MagicMock()
        client.messages.create = AsyncMock(side_effect=RuntimeError("API down"))

        orchestrator = _make_orchestrator(client)
        decision = await orchestrator.investigate(case)

        assert decision.escalated is True
        assert "unexpected_error" in decision.rationale


# ── Orchestrator — write guard (stale write) ──────────────────────────────────


class TestOrchestratorWriteGuard:
    @pytest.mark.asyncio
    async def test_stale_write_does_not_raise(self):
        """update_recon_case_proposed returning False must not raise."""
        case = _make_case()
        repo = _make_repo()
        repo.update_recon_case_proposed.return_value = False  # stale write

        client = MagicMock()
        client.messages = MagicMock()
        propose = _propose_block(str(case.case_uid), confidence=0.97)
        resp = _mock_response([propose])
        client.messages.create = AsyncMock(return_value=resp)

        orchestrator = _make_orchestrator(client, repo=repo)
        decision = await orchestrator.investigate(case)

        # Investigation still returns a valid decision
        assert decision.confidence == pytest.approx(0.97)
        assert not decision.escalated

    @pytest.mark.asyncio
    async def test_agent_trace_inserted_even_on_stale_write(self):
        """insert_agent_trace is always called, regardless of write guard outcome."""
        case = _make_case()
        repo = _make_repo()
        repo.update_recon_case_proposed.return_value = False  # stale write

        client = MagicMock()
        client.messages = MagicMock()
        propose = _propose_block(str(case.case_uid), confidence=0.97)
        resp = _mock_response([propose])
        client.messages.create = AsyncMock(return_value=resp)

        orchestrator = _make_orchestrator(client, repo=repo)
        await orchestrator.investigate(case)

        repo.insert_agent_trace.assert_called_once()

    @pytest.mark.asyncio
    async def test_agent_trace_args(self):
        """insert_agent_trace receives the expected keyword arguments."""
        case = _make_case()
        repo = _make_repo()
        client = MagicMock()
        client.messages = MagicMock()

        propose = _propose_block(str(case.case_uid), confidence=0.99)
        resp = _mock_response([propose], input_tokens=300, output_tokens=100)
        client.messages.create = AsyncMock(return_value=resp)

        orchestrator = _make_orchestrator(client, repo=repo)
        decision = await orchestrator.investigate(case)

        call_kwargs = repo.insert_agent_trace.call_args.kwargs
        assert call_kwargs["model"] == "claude-haiku-4-5"
        assert call_kwargs["total_ms"] >= 0
        assert call_kwargs["cost_usd"] >= 0
        assert isinstance(call_kwargs["tools_used"], list)
        assert isinstance(call_kwargs["steps"], list)


# ── AgentConsumer._handle ─────────────────────────────────────────────────────


class TestAgentConsumerHandle:
    @pytest.mark.asyncio
    async def test_handle_calls_orchestrator(self):
        """_handle decodes payload, fetches case, calls orchestrator.investigate."""
        from recon.agent.kafka_consumer import AgentConsumer
        from recon.messaging import ReconRequest, encode

        case_uid = uuid.uuid4()
        req = ReconRequest(
            file_id="test_file",
            line_no=1,
            case_uid=str(case_uid),
        )
        payload = encode(req)

        repo = _make_repo()
        repo.fetch_recon_case_by_file_line = AsyncMock(
            return_value={
                "id": 1,
                "case_uid": str(case_uid),
                "settlement_line": 1,
                "pg_transaction": None,
                "match_type": "AMOUNT_MISMATCH",
                "confidence": 0.5,
                "resolution": None,
                "resolved_by": None,
                "notes": {},
            }
        )

        # Build the consumer without going through __init__ (avoids aiokafka import)
        consumer = AgentConsumer.__new__(AgentConsumer)
        consumer._repo = repo
        consumer._mock_llm = False

        mock_decision = AgentDecision(
            case_uid=case_uid,
            resolution_type="MATCH",
            resolution="PROPOSED",
            confidence=0.98,
            rationale="Matched.",
            pg_txn_id=42,
            model_used="claude-haiku-4-5",
            tools_called=[],
            total_ms=200,
            cost_usd=0.001,
            steps=[],
        )
        consumer._orchestrator = MagicMock()
        consumer._orchestrator.investigate = AsyncMock(return_value=mock_decision)

        await consumer._handle(payload)

        consumer._orchestrator.investigate.assert_called_once()
        # Verify the ReconCase passed to investigate has the right case_uid
        call_args = consumer._orchestrator.investigate.call_args
        passed_case = call_args[0][0]
        assert str(passed_case.case_uid) == str(case_uid)

    @pytest.mark.asyncio
    async def test_handle_skips_already_resolved_case(self):
        """_handle skips the case if resolution is already set (not None)."""
        from recon.agent.kafka_consumer import AgentConsumer
        from recon.messaging import ReconRequest, encode

        req = ReconRequest(
            file_id="test_file",
            line_no=2,
            case_uid=str(uuid.uuid4()),
        )
        payload = encode(req)

        repo = _make_repo()
        repo.fetch_recon_case_by_file_line = AsyncMock(
            return_value={
                "id": 2,
                "case_uid": str(uuid.uuid4()),
                "settlement_line": 2,
                "pg_transaction": None,
                "match_type": "AMOUNT_MISMATCH",
                "confidence": 0.9,
                "resolution": "PROPOSED",  # already resolved — must skip
                "resolved_by": "rules",
                "notes": {},
            }
        )

        consumer = AgentConsumer.__new__(AgentConsumer)
        consumer._repo = repo
        consumer._mock_llm = False
        consumer._orchestrator = MagicMock()
        consumer._orchestrator.investigate = AsyncMock()

        await consumer._handle(payload)

        consumer._orchestrator.investigate.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_skips_missing_case(self):
        """_handle gracefully skips when recon_case is not found in DB."""
        from recon.agent.kafka_consumer import AgentConsumer
        from recon.messaging import ReconRequest, encode

        req = ReconRequest(
            file_id="test_file",
            line_no=3,
            case_uid=str(uuid.uuid4()),
        )
        payload = encode(req)

        repo = _make_repo()
        repo.fetch_recon_case_by_file_line = AsyncMock(return_value=None)

        consumer = AgentConsumer.__new__(AgentConsumer)
        consumer._repo = repo
        consumer._mock_llm = False
        consumer._orchestrator = MagicMock()
        consumer._orchestrator.investigate = AsyncMock()

        await consumer._handle(payload)

        consumer._orchestrator.investigate.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_decode_error_is_swallowed(self):
        """_handle does not raise on a malformed payload."""
        from recon.agent.kafka_consumer import AgentConsumer

        consumer = AgentConsumer.__new__(AgentConsumer)
        consumer._repo = _make_repo()
        consumer._mock_llm = False
        consumer._orchestrator = MagicMock()
        consumer._orchestrator.investigate = AsyncMock()

        # Completely invalid payload
        await consumer._handle(b"not valid json {{{{")

        consumer._orchestrator.investigate.assert_not_called()
