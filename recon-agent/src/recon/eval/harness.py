"""
Eval harness — LLD §6.12.

Loads YAML golden cases from tests/eval/cases/, runs each through either:
  - RulesEngine   (mode: "rules")
  - AgentOrchestrator with a MockLLMClient (mode: "agent")

Asserts resolution_type, must_call_tools ⊆ tools_called, cost_usd ≤ max_cost_usd.
Reports rules-only and rules+agent accuracy, plus a regression gate vs. baseline.json.

YAML case schema
----------------
id: "001_write_off"
description: "..."
mode: rules | agent

# agent mode only — the ReconCase fed to the orchestrator
case:
  match_type: AMOUNT_MISMATCH
  settlement_line_id: 1
  pg_txn_id: null
  confidence: null
  notes: {reason: amount_mismatch}

# rules mode only — the SettlementLine fed to the engine
settlement_line:
  file_id: udir_2026_04_30
  line_no: 1
  rrn: "412345678901"
  utr: "HDFC0001234"
  amount_paise: 150000
  status: SUCCESS

# RRNs already seen before evaluating this line (duplicate detection)
seen_rrns: []

# Stub return values for repo methods called during the run
stubs:
  lookup_pg_txns:           [...]  # list of PgTxn field dicts
  search_pg_transactions:   [...]  # list of result dicts
  fetch_settlement_history_by_rrn: [...]

# agent mode only — the script the MockLLM will execute step by step
llm_script:
  - call: search_pg_transactions
    args: {rrn: "412345678901"}
  - propose:
      resolution_type: WRITE_OFF
      confidence: 0.98
      rationale: "..."
      pg_txn_id: 7711

expected:
  resolution_type: WRITE_OFF     # agent mode
  match_type: EXACT              # rules mode
  confidence: 1.0                # rules mode (optional)
  must_call_tools: [search_pg_transactions]
  max_cost_usd: 0.01
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

from recon.models import PgTxn, ReconCase, SettlementLine


# ── Result types ──────────────────────────────────────────────────────────────


@dataclass
class EvalCase:
    id: str
    description: str
    mode: str                      # "rules" or "agent"
    case_data: dict                # ReconCase fields (agent mode)
    settlement_line_data: dict     # SettlementLine fields (rules mode)
    seen_rrns: list[str]
    stubs: dict
    llm_script: list[dict]
    expected: dict


@dataclass
class EvalResult:
    case_id: str
    mode: str
    passed: bool
    actual: str                    # resolution_type (agent) or match_type (rules)
    expected_val: str
    tools_called: list[str]
    cost_usd: float
    errors: list[str] = field(default_factory=list)


@dataclass
class EvalReport:
    total: int
    passed: int
    rules_total: int
    rules_passed: int
    agent_total: int
    agent_passed: int
    total_cost_usd: float
    failed_ids: list[str]
    results: list[EvalResult]

    @property
    def accuracy(self) -> float:
        return self.passed / self.total if self.total else 1.0

    @property
    def rules_accuracy(self) -> float:
        return self.rules_passed / self.rules_total if self.rules_total else 1.0

    @property
    def agent_accuracy(self) -> float:
        return self.agent_passed / self.agent_total if self.agent_total else 1.0

    @property
    def avg_cost_usd(self) -> float:
        agent_count = self.agent_total or 1
        return self.total_cost_usd / agent_count


# ── Mock LLM client ───────────────────────────────────────────────────────────


class _MockUsage:
    def __init__(self, input_tokens: int = 100, output_tokens: int = 50) -> None:
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cache_read_input_tokens = 0
        self.cache_creation_input_tokens = 0


class _MockBlock:
    def __init__(self, name: str, block_id: str, input_: dict) -> None:
        self.type = "tool_use"
        self.name = name
        self.id = block_id
        self.input = input_


class _MockResponse:
    def __init__(
        self,
        content: list,
        stop_reason: str = "tool_use",
        input_tokens: int = 100,
        output_tokens: int = 50,
    ) -> None:
        self.content = content
        self.stop_reason = stop_reason
        self.usage = _MockUsage(input_tokens, output_tokens)


class _MockMessages:
    """Replays a fixed script as Claude API responses, one step per create() call."""

    def __init__(self, script: list[dict]) -> None:
        self._script = list(script)
        self._cursor = 0

    async def create(self, **kwargs: Any) -> _MockResponse:
        if self._cursor >= len(self._script):
            return _MockResponse(content=[], stop_reason="end_turn")

        step = self._script[self._cursor]
        self._cursor += 1
        block_id = f"tu_{self._cursor}"

        if "propose" in step:
            block = _MockBlock("propose_resolution", block_id, dict(step["propose"]))
            return _MockResponse([block], stop_reason="tool_use")

        if "call" in step:
            block = _MockBlock(step["call"], block_id, dict(step.get("args") or {}))
            return _MockResponse([block], stop_reason="tool_use")

        return _MockResponse(content=[], stop_reason="end_turn")


class MockLLMClient:
    """Drop-in replacement for anthropic.AsyncAnthropic scoped to one eval case."""

    def __init__(self, script: list[dict]) -> None:
        self.messages = _MockMessages(script)


# ── Mock repo ─────────────────────────────────────────────────────────────────


class MockRepo:
    """Stub LedgerRepo that returns pre-loaded data from the eval case stubs."""

    def __init__(self, stubs: dict) -> None:
        self._stubs = stubs

    async def lookup_pg_txns(
        self, rrn: Optional[str] = None, utr: Optional[str] = None
    ) -> list[PgTxn]:
        return [_dict_to_pg_txn(r) for r in self._stubs.get("lookup_pg_txns", [])]

    async def search_pg_transactions(self, **_kwargs: Any) -> list[dict]:
        return list(self._stubs.get("search_pg_transactions", []))

    async def fetch_settlement_history_by_rrn(self, rrn: str) -> list[dict]:
        return list(self._stubs.get("fetch_settlement_history_by_rrn", []))

    async def update_recon_case_proposed(self, uid: uuid.UUID, fields: dict) -> bool:
        return True

    async def insert_agent_trace(self, **_kwargs: Any) -> None:
        pass


# ── EvalHarness ───────────────────────────────────────────────────────────────


class EvalHarness:
    def load_cases(self, cases_dir: Path) -> list[EvalCase]:
        cases: list[EvalCase] = []
        for path in sorted(cases_dir.glob("*.yaml")):
            with path.open() as f:
                data = yaml.safe_load(f)
            cases.append(EvalCase(
                id=str(data["id"]),
                description=data.get("description", ""),
                mode=data.get("mode", "agent"),
                case_data=data.get("case") or {},
                settlement_line_data=data.get("settlement_line") or {},
                seen_rrns=list(data.get("seen_rrns") or []),
                stubs=data.get("stubs") or {},
                llm_script=list(data.get("llm_script") or []),
                expected=data.get("expected") or {},
            ))
        return cases

    async def run_case(self, case: EvalCase) -> EvalResult:
        if case.mode == "rules":
            return await self._run_rules_case(case)
        return await self._run_agent_case(case)

    async def run_all(self, cases: list[EvalCase]) -> EvalReport:
        results: list[EvalResult] = []
        for case in cases:
            results.append(await self.run_case(case))

        rules_results = [r for r in results if r.mode == "rules"]
        agent_results = [r for r in results if r.mode == "agent"]

        return EvalReport(
            total=len(results),
            passed=sum(1 for r in results if r.passed),
            rules_total=len(rules_results),
            rules_passed=sum(1 for r in rules_results if r.passed),
            agent_total=len(agent_results),
            agent_passed=sum(1 for r in agent_results if r.passed),
            total_cost_usd=sum(r.cost_usd for r in agent_results),
            failed_ids=[r.case_id for r in results if not r.passed],
            results=results,
        )

    def check_regression(self, report: EvalReport, baseline_path: Path) -> None:
        """
        Compare report against baseline. Writes baseline if it doesn't exist.
        Raises AssertionError if accuracy drops by >0.02 or avg cost grows by >20%.
        """
        snapshot = {
            "accuracy": report.accuracy,
            "rules_accuracy": report.rules_accuracy,
            "agent_accuracy": report.agent_accuracy,
            "avg_cost_usd": report.avg_cost_usd,
        }

        if not baseline_path.exists():
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            baseline_path.write_text(json.dumps(snapshot, indent=2))
            return

        baseline = json.loads(baseline_path.read_text())
        errors: list[str] = []

        prev_acc = baseline.get("accuracy", 1.0)
        if report.accuracy < prev_acc - 0.02:
            errors.append(
                f"accuracy {report.accuracy:.3f} < baseline {prev_acc:.3f} - 0.02"
            )

        prev_cost = baseline.get("avg_cost_usd", 0.0)
        if prev_cost > 0 and report.avg_cost_usd > prev_cost * 1.20:
            errors.append(
                f"avg_cost_usd {report.avg_cost_usd:.6f} > baseline {prev_cost:.6f} * 1.20"
            )

        if errors:
            raise AssertionError("Eval regression:\n" + "\n".join(errors))

    # ── Private ───────────────────────────────────────────────────────────────

    async def _run_rules_case(self, case: EvalCase) -> EvalResult:
        from recon.rules.engine import RulesEngine

        repo = MockRepo(case.stubs)
        engine = RulesEngine(repo=repo)
        for rrn in case.seen_rrns:
            engine._seen_rrns.add(rrn)

        line = _build_settlement_line(case.settlement_line_data)
        recon_case = await engine.run(line)

        errors: list[str] = []
        expected_match = case.expected.get("match_type", "")
        actual_match = recon_case.match_type

        if expected_match and actual_match != expected_match:
            errors.append(
                f"match_type: got {actual_match!r}, expected {expected_match!r}"
            )

        expected_conf = case.expected.get("confidence")
        if expected_conf is not None and recon_case.confidence != expected_conf:
            errors.append(
                f"confidence: got {recon_case.confidence}, expected {expected_conf}"
            )

        return EvalResult(
            case_id=case.id,
            mode="rules",
            passed=len(errors) == 0,
            actual=actual_match,
            expected_val=expected_match,
            tools_called=[],
            cost_usd=0.0,
            errors=errors,
        )

    async def _run_agent_case(self, case: EvalCase) -> EvalResult:
        from recon.agent.orchestrator import AgentOrchestrator
        from recon.agent.tools import ToolRegistry

        recon_case = _build_recon_case(case.case_data)
        repo = MockRepo(case.stubs)
        tools = ToolRegistry(repo=repo)
        client = MockLLMClient(case.llm_script)
        orchestrator = AgentOrchestrator(
            client=client,
            tools=tools,
            repo=repo,
            model_triage="claude-haiku-4-5",
            model_invest="claude-sonnet-4-6",
        )

        decision = await orchestrator.investigate(recon_case)

        errors: list[str] = []
        expected_res = case.expected.get("resolution_type", "")
        actual_res = decision.resolution_type

        if expected_res and actual_res != expected_res:
            errors.append(
                f"resolution_type: got {actual_res!r}, expected {expected_res!r}"
            )

        must_call = set(case.expected.get("must_call_tools") or [])
        missing = must_call - set(decision.tools_called)
        if missing:
            errors.append(f"tools not called: {sorted(missing)}")

        max_cost = case.expected.get("max_cost_usd")
        if max_cost is not None and decision.cost_usd > max_cost:
            errors.append(
                f"cost_usd {decision.cost_usd:.6f} > max {max_cost}"
            )

        return EvalResult(
            case_id=case.id,
            mode="agent",
            passed=len(errors) == 0,
            actual=actual_res,
            expected_val=expected_res,
            tools_called=decision.tools_called,
            cost_usd=decision.cost_usd,
            errors=errors,
        )


# ── Builders ──────────────────────────────────────────────────────────────────


def _build_recon_case(data: dict) -> ReconCase:
    return ReconCase(
        case_uid=uuid.uuid4(),
        settlement_line_id=data.get("settlement_line_id"),
        pg_txn_id=data.get("pg_txn_id"),
        match_type=data.get("match_type", "UNKNOWN"),
        confidence=data.get("confidence"),
        resolution=data.get("resolution"),
        resolved_by=data.get("resolved_by"),
        notes=data.get("notes") or {},
    )


def _build_settlement_line(data: dict) -> SettlementLine:
    txn_date = None
    if data.get("txn_date"):
        from datetime import date
        txn_date = date.fromisoformat(data["txn_date"])
    return SettlementLine(
        file_id=data.get("file_id", "eval_file"),
        line_no=int(data.get("line_no", 1)),
        rrn=data.get("rrn"),
        utr=data.get("utr"),
        amount_paise=data.get("amount_paise"),
        fee_paise=data.get("fee_paise"),
        net_paise=data.get("net_paise"),
        status=data.get("status"),
        txn_date=txn_date,
        payer_vpa=data.get("payer_vpa"),
        payee_vpa=data.get("payee_vpa"),
        txn_type=data.get("txn_type"),
        remarks=data.get("remarks"),
        raw=data,
        id=data.get("id"),
    )


def _dict_to_pg_txn(d: dict) -> PgTxn:
    def _dt(val: Any) -> datetime:
        if isinstance(val, datetime):
            return val
        if val is None:
            return datetime(2026, 1, 1, tzinfo=timezone.utc)
        return datetime.fromisoformat(str(val)).replace(tzinfo=timezone.utc)

    return PgTxn(
        id=int(d["id"]),
        txn_id=str(d.get("txn_id", "")),
        rrn=d.get("rrn"),
        utr=d.get("utr"),
        payer_vpa=d.get("payer_vpa"),
        payee_vpa=d.get("payee_vpa"),
        amount_paise=int(d["amount_paise"]),
        status=str(d.get("status", "SUCCESS")),
        created_at=_dt(d.get("created_at")),
        updated_at=_dt(d.get("updated_at")),
    )
