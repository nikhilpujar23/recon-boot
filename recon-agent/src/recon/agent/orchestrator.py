"""
Phase 3 — Agent Orchestrator (LLD §6.4).

State machine:

  NEW → TRIAGE_HAIKU
     ├── conf >= 0.9 ──▶ DECISION
     └── conf <  0.9 ──▶ INVESTIGATE_SONNET ──▶ DECISION
  DECISION → PROPOSED   (conditional UPDATE where resolution IS NULL)
           → ESCALATE   (tool failure / timeout / max steps exceeded)

Hard limits (enforced in code, not via prompt):
  - Max 6 tool calls per case (AGENT_MAX_STEPS)
  - 30 s wall-clock (AGENT_TIMEOUT_S) via asyncio.wait_for
  - Temperature = 0, top_p = 1
  - Exactly one propose_resolution call; multiple → ESCALATE

Prompt cache layout:
  [cache-anchor 1]  system_prompt (~3.5k tokens, cache_control=ephemeral)
  [cache-anchor 2]  tool_schemas + exemplars (~2k tokens, cache_control=ephemeral)
  [per-case]        user message with redacted ReconCase JSON

Write guard:
  UPDATE recon_cases SET ... WHERE case_uid=$uid AND resolution IS NULL
  Returns 0 rows → stale write; log metric and exit without retry.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Pricing table (USD per 1M tokens) ────────────────────────────────────────
# Update if Anthropic changes pricing.
_PRICING: dict[str, dict[str, float]] = {
    "claude-haiku-4-5": {
        "input": 0.80,
        "output": 4.00,
        "cache_read": 0.08,
        "cache_write": 1.00,
    },
    "claude-sonnet-4-6": {
        "input": 3.00,
        "output": 15.00,
        "cache_read": 0.30,
        "cache_write": 3.75,
    },
}
_DEFAULT_PRICING = {"input": 3.00, "output": 15.00, "cache_read": 0.30, "cache_write": 3.75}

# Resolution types that map to DB resolution values
_RESOLUTION_MAP = {
    "MATCH":            "PROPOSED",
    "WRITE_OFF":        "PROPOSED",
    "AMOUNT_MISMATCH":  "PROPOSED",
    "MISSING_LEG":      "PROPOSED",
    "DUPLICATE":        "PROPOSED",
    "ESCALATE":         "ESCALATE",
}


# ── AgentDecision ─────────────────────────────────────────────────────────────


@dataclass
class AgentDecision:
    case_uid: uuid.UUID
    resolution_type: str          # propose_resolution.resolution_type
    resolution: str               # DB resolution value (PROPOSED / ESCALATE)
    confidence: float
    rationale: str
    pg_txn_id: Optional[int]
    model_used: str
    tools_called: list[str]
    total_ms: int
    cost_usd: float
    steps: list[dict]
    escalated: bool = False
    prompt_hash: str = ""


# ── AgentOrchestrator ─────────────────────────────────────────────────────────


class AgentOrchestrator:
    """
    Investigates residual ReconCases using Claude as the reasoning engine.

    Usage::

        orchestrator = AgentOrchestrator(
            client=AsyncAnthropic(api_key=...),
            tools=ToolRegistry(repo),
            repo=repo,
        )
        decision = await orchestrator.investigate(case)
    """

    def __init__(
        self,
        client,                           # anthropic.AsyncAnthropic
        tools,                            # ToolRegistry
        repo,                             # LedgerRepo
        model_triage: str = "claude-haiku-4-5",
        model_invest: str = "claude-sonnet-4-6",
        max_steps: int = 6,
        timeout_s: int = 30,
    ) -> None:
        self._client = client
        self._tools = tools
        self._repo = repo
        self._model_triage = model_triage
        self._model_invest = model_invest
        self._max_steps = max_steps
        self._timeout_s = timeout_s

    async def investigate(self, case) -> AgentDecision:  # case: ReconCase
        """
        Main entry point. Returns an AgentDecision; never raises.

        Side effects:
          - Updates recon_cases (conditional write guard)
          - Inserts agent_traces row
        """
        start_ms = time.monotonic()
        self._tools.reset_steps()

        try:
            decision = await asyncio.wait_for(
                self._run_investigation(case, start_ms),
                timeout=self._timeout_s,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Agent timeout for case_uid=%s after %ds", case.case_uid, self._timeout_s
            )
            elapsed = int((time.monotonic() - start_ms) * 1000)
            decision = _escalate_decision(
                case=case,
                reason="timeout",
                model=self._model_invest,
                tools_called=[s.tool_name for s in self._tools.steps],
                steps=[s.to_dict() for s in self._tools.steps],
                total_ms=elapsed,
            )
        except Exception as exc:
            logger.error("Agent error for case_uid=%s: %s", case.case_uid, exc)
            elapsed = int((time.monotonic() - start_ms) * 1000)
            decision = _escalate_decision(
                case=case,
                reason=f"unexpected_error: {type(exc).__name__}",
                model=self._model_invest,
                tools_called=[s.tool_name for s in self._tools.steps],
                steps=[s.to_dict() for s in self._tools.steps],
                total_ms=elapsed,
            )

        await self._persist_result(case, decision)
        return decision

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _run_investigation(self, case, start_ms: float) -> AgentDecision:
        """Two-tier investigation: Haiku triage → Sonnet if confidence < 0.9."""
        from recon.agent.prompts import SYSTEM_PROMPT, TOOL_EXEMPLARS
        from recon.agent.tools import TOOL_SCHEMAS, MUTATING_TOOL

        # Build the per-case user message
        case_json = _case_to_json(case)
        prompt_hash = hashlib.sha256(case_json.encode()).hexdigest()[:16]
        user_message = {
            "role": "user",
            "content": (
                f"Investigate the following reconciliation case and propose a resolution.\n\n"
                f"```json\n{case_json}\n```"
            ),
        }

        # Build system content with cache anchors
        system_content = [
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            },
            {
                "type": "text",
                "text": TOOL_EXEMPLARS,
                "cache_control": {"type": "ephemeral"},
            },
        ]

        # Tool schemas with cache control on the last one
        tools_with_cache = []
        for i, schema in enumerate(TOOL_SCHEMAS):
            entry = dict(schema)
            if i == len(TOOL_SCHEMAS) - 1:
                entry["cache_control"] = {"type": "ephemeral"}
            tools_with_cache.append(entry)

        # ── Stage 1: Haiku triage ─────────────────────────────────────────────
        haiku_decision = await self._run_tool_loop(
            case=case,
            model=self._model_triage,
            system=system_content,
            tools=tools_with_cache,
            messages=[user_message],
            start_ms=start_ms,
            step_budget=self._max_steps,
            prompt_hash=prompt_hash,
        )

        if haiku_decision.confidence >= 0.9 or haiku_decision.escalated:
            return haiku_decision

        logger.info(
            "Haiku triage confidence=%.2f < 0.9 for case_uid=%s — escalating to Sonnet",
            haiku_decision.confidence, case.case_uid,
        )

        # ── Stage 2: Sonnet investigation ─────────────────────────────────────
        # Reset tool steps; Sonnet gets the full step budget fresh.
        self._tools.reset_steps()
        sonnet_decision = await self._run_tool_loop(
            case=case,
            model=self._model_invest,
            system=system_content,
            tools=tools_with_cache,
            messages=[user_message],
            start_ms=start_ms,
            step_budget=self._max_steps,
            prompt_hash=prompt_hash,
        )
        return sonnet_decision

    async def _run_tool_loop(
        self,
        case,
        model: str,
        system: list[dict],
        tools: list[dict],
        messages: list[dict],
        start_ms: float,
        step_budget: int,
        prompt_hash: str,
    ) -> AgentDecision:
        """Core tool-call loop. Returns an AgentDecision when propose_resolution is called."""
        from recon.agent.tools import MUTATING_TOOL

        messages = list(messages)  # local copy
        step_count = 0
        propose_calls = 0
        total_input_tokens = 0
        total_output_tokens = 0
        total_cache_read_tokens = 0
        total_cache_write_tokens = 0

        while step_count < step_budget:
            response = await self._client.messages.create(
                model=model,
                max_tokens=1024,
                temperature=0,
                top_p=1,
                system=system,
                tools=tools,
                messages=messages,
            )

            # Accumulate token counts across turns
            if hasattr(response, "usage"):
                u = response.usage
                total_input_tokens += getattr(u, "input_tokens", 0)
                total_output_tokens += getattr(u, "output_tokens", 0)
                total_cache_read_tokens += getattr(u, "cache_read_input_tokens", 0)
                total_cache_write_tokens += getattr(u, "cache_creation_input_tokens", 0)

            tool_results = []

            for block in response.content:
                if block.type == "tool_use":
                    if block.name == MUTATING_TOOL:
                        # Intercept propose_resolution — extract decision, return ack
                        propose_calls += 1
                        if propose_calls > 1:
                            logger.warning(
                                "Multiple propose_resolution calls for case_uid=%s — ESCALATE",
                                case.case_uid,
                            )
                            # Append ack and break out
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": json.dumps({"error": "propose_resolution already called"}),
                            })
                            break

                        args = block.input if isinstance(block.input, dict) else {}
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps({"status": "accepted"}),
                        })

                        elapsed = int((time.monotonic() - start_ms) * 1000)
                        cost = _calculate_cost(
                            model=model,
                            input_tokens=total_input_tokens,
                            output_tokens=total_output_tokens,
                            cache_read_tokens=total_cache_read_tokens,
                            cache_write_tokens=total_cache_write_tokens,
                        )
                        tools_called = [s.tool_name for s in self._tools.steps]
                        steps = [s.to_dict() for s in self._tools.steps]

                        resolution_type = args.get("resolution_type", "ESCALATE")
                        db_resolution = _RESOLUTION_MAP.get(resolution_type, "ESCALATE")
                        escalated = resolution_type == "ESCALATE"

                        return AgentDecision(
                            case_uid=case.case_uid,
                            resolution_type=resolution_type,
                            resolution=db_resolution,
                            confidence=float(args.get("confidence", 0.0)),
                            rationale=str(args.get("rationale", "")),
                            pg_txn_id=args.get("pg_txn_id"),
                            model_used=model,
                            tools_called=tools_called,
                            total_ms=elapsed,
                            cost_usd=cost,
                            steps=steps,
                            escalated=escalated,
                            prompt_hash=prompt_hash,
                        )

                    else:
                        # Regular tool call
                        step_count += 1
                        result_json = await self._tools.dispatch(
                            name=block.name,
                            raw_input=block.input if isinstance(block.input, dict) else {},
                            step_no=step_count,
                        )
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_json,
                        })

            if response.stop_reason == "end_turn" and not tool_results:
                # Model finished without calling propose_resolution
                break

            if tool_results:
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})

            if propose_calls > 1:
                break

            if response.stop_reason == "end_turn":
                break

        # Fell through without propose_resolution → ESCALATE
        elapsed = int((time.monotonic() - start_ms) * 1000)
        cost = _calculate_cost(
            model=model,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            cache_read_tokens=total_cache_read_tokens,
            cache_write_tokens=total_cache_write_tokens,
        )
        reason = "max_steps_exceeded" if step_count >= step_budget else "no_proposal"
        if propose_calls > 1:
            reason = "multiple_propose_calls"

        return _escalate_decision(
            case=case,
            reason=reason,
            model=model,
            tools_called=[s.tool_name for s in self._tools.steps],
            steps=[s.to_dict() for s in self._tools.steps],
            total_ms=elapsed,
            cost_usd=cost,
            prompt_hash=prompt_hash,
        )

    async def _persist_result(self, case, decision: AgentDecision) -> None:
        """
        1. Conditional UPDATE recon_cases WHERE resolution IS NULL (write guard).
        2. INSERT agent_traces.
        """
        notes = {
            "resolution_type": decision.resolution_type,
            "rationale": decision.rationale,
            "source": "agent",
            "model": decision.model_used,
        }

        written = await self._repo.update_recon_case_proposed(
            uid=decision.case_uid,
            fields={
                "resolution": decision.resolution,
                "resolved_by": f"agent:{decision.model_used}",
                "confidence": decision.confidence,
                "notes": notes,
                "pg_txn_id": decision.pg_txn_id,
            },
        )

        if not written:
            logger.warning(
                "Stale write for case_uid=%s — another worker resolved it first",
                decision.case_uid,
                extra={"metric": "agent_stale_write_total"},
            )

        await self._repo.insert_agent_trace(
            case_uid=decision.case_uid,
            prompt_hash=decision.prompt_hash,
            model=decision.model_used,
            tools_used=decision.tools_called,
            steps=decision.steps,
            total_ms=decision.total_ms,
            cost_usd=decision.cost_usd,
        )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _case_to_json(case) -> str:  # case: ReconCase
    """Serialize a ReconCase to a JSON string for the agent prompt."""
    return json.dumps({
        "case_uid": str(case.case_uid),
        "match_type": case.match_type,
        "settlement_line_id": case.settlement_line_id,
        "pg_txn_id": case.pg_txn_id,
        "confidence": case.confidence,
        "notes": case.notes,
    }, indent=2)


def _escalate_decision(
    case,
    reason: str,
    model: str,
    tools_called: list[str],
    steps: list[dict],
    total_ms: int,
    cost_usd: float = 0.0,
    prompt_hash: str = "",
) -> AgentDecision:
    return AgentDecision(
        case_uid=case.case_uid,
        resolution_type="ESCALATE",
        resolution="ESCALATE",
        confidence=0.0,
        rationale=f"Agent escalated: {reason}",
        pg_txn_id=None,
        model_used=model,
        tools_called=tools_called,
        total_ms=total_ms,
        cost_usd=cost_usd,
        steps=steps,
        escalated=True,
        prompt_hash=prompt_hash,
    )


def _calculate_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0,
) -> float:
    pricing = _PRICING.get(model, _DEFAULT_PRICING)
    cost = (
        input_tokens        * pricing["input"]        / 1_000_000
        + output_tokens     * pricing["output"]       / 1_000_000
        + cache_read_tokens * pricing["cache_read"]   / 1_000_000
        + cache_write_tokens * pricing["cache_write"] / 1_000_000
    )
    return round(cost, 6)


def can_auto_approve(decision: AgentDecision, disable_auto_approve: bool = False) -> bool:
    """
    LLD §9.4 — auto-approve threshold.
    WRITE_OFF is never auto-approved regardless of confidence.
    """
    if disable_auto_approve:
        return False
    if decision.resolution_type == "WRITE_OFF":
        return False
    return (
        decision.resolution == "PROPOSED"
        and decision.resolution_type == "MATCH"
        and decision.confidence >= 0.99
    )
