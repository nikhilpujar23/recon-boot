"""
Eval harness tests — LLD §6.12.

Marked with @pytest.mark.eval so they can be excluded from the fast unit suite:
    pytest -m "not eval"   # skip eval
    pytest -m eval         # eval only
    pytest                 # everything

Test structure:
  - test_golden_case  : one parametrized test per YAML file — fast, isolated failure output
  - test_regression_gate : runs all cases, checks accuracy & cost against baseline.json
"""
from __future__ import annotations

from pathlib import Path

import pytest

from recon.eval.harness import EvalCase, EvalHarness

CASES_DIR = Path(__file__).parent / "eval" / "cases"
BASELINE_PATH = Path(__file__).parent / "eval" / "baseline.json"

_harness = EvalHarness()
_cases: list[EvalCase] = _harness.load_cases(CASES_DIR) if CASES_DIR.exists() else []


# ── Per-case golden tests ─────────────────────────────────────────────────────


@pytest.mark.eval
@pytest.mark.parametrize("case", _cases, ids=lambda c: c.id)
@pytest.mark.asyncio
async def test_golden_case(case: EvalCase) -> None:
    result = await _harness.run_case(case)
    assert result.passed, (
        f"Case {case.id!r} ({case.description})\n"
        + "\n".join(f"  • {e}" for e in result.errors)
    )


# ── Regression gate ───────────────────────────────────────────────────────────


@pytest.mark.eval
@pytest.mark.asyncio
async def test_regression_gate() -> None:
    """Fails if accuracy drops >2% or avg cost grows >20% vs. baseline.json."""
    report = await _harness.run_all(_cases)

    # Surface individual failures before the regression gate
    failed = [r for r in report.results if not r.passed]
    assert not failed, (
        f"{len(failed)}/{report.total} cases failed:\n"
        + "\n".join(
            f"  [{r.case_id}] {'; '.join(r.errors)}" for r in failed
        )
    )

    _harness.check_regression(report, BASELINE_PATH)


# ── Harness unit tests (no mark — always run) ─────────────────────────────────


class TestEvalHarnessUnit:
    """Tests for harness internals: MockLLMClient, MockRepo, builders, loader."""

    def test_load_cases_finds_yaml_files(self):
        cases = _harness.load_cases(CASES_DIR)
        assert len(cases) >= 6
        ids = [c.id for c in cases]
        assert "001_write_off" in ids
        assert "004_exact_rules" in ids

    def test_cases_have_correct_modes(self):
        cases = {c.id: c for c in _harness.load_cases(CASES_DIR)}
        assert cases["001_write_off"].mode == "agent"
        assert cases["004_exact_rules"].mode == "rules"
        assert cases["005_tolerance_rules"].mode == "rules"

    def test_agent_cases_have_llm_script(self):
        cases = _harness.load_cases(CASES_DIR)
        for c in cases:
            if c.mode == "agent":
                assert len(c.llm_script) > 0, f"{c.id} has empty llm_script"

    def test_rules_cases_have_settlement_line(self):
        cases = _harness.load_cases(CASES_DIR)
        for c in cases:
            if c.mode == "rules":
                assert c.settlement_line_data, f"{c.id} missing settlement_line"

    @pytest.mark.asyncio
    async def test_mock_llm_replays_script_in_order(self):
        from recon.eval.harness import MockLLMClient

        script = [
            {"call": "search_pg_transactions", "args": {"rrn": "x"}},
            {"propose": {"resolution_type": "MATCH", "confidence": 0.99, "rationale": "ok"}},
        ]
        client = MockLLMClient(script)
        r1 = await client.messages.create()
        assert r1.content[0].name == "search_pg_transactions"
        assert r1.content[0].input == {"rrn": "x"}

        r2 = await client.messages.create()
        assert r2.content[0].name == "propose_resolution"
        assert r2.content[0].input["resolution_type"] == "MATCH"

    @pytest.mark.asyncio
    async def test_mock_llm_returns_end_turn_when_exhausted(self):
        from recon.eval.harness import MockLLMClient

        client = MockLLMClient([])
        r = await client.messages.create()
        assert r.stop_reason == "end_turn"
        assert r.content == []

    @pytest.mark.asyncio
    async def test_mock_repo_lookup_pg_txns_converts_to_pg_txn(self):
        from recon.eval.harness import MockRepo

        repo = MockRepo({"lookup_pg_txns": [
            {"id": 1, "txn_id": "t1", "amount_paise": 100, "status": "SUCCESS"}
        ]})
        txns = await repo.lookup_pg_txns(rrn="x")
        assert len(txns) == 1
        assert txns[0].id == 1
        assert txns[0].amount_paise == 100

    @pytest.mark.asyncio
    async def test_mock_repo_returns_empty_when_stub_absent(self):
        from recon.eval.harness import MockRepo

        repo = MockRepo({})
        txns = await repo.lookup_pg_txns()
        assert txns == []
        rows = await repo.search_pg_transactions()
        assert rows == []

    @pytest.mark.asyncio
    async def test_mock_repo_update_always_returns_true(self):
        import uuid
        from recon.eval.harness import MockRepo

        repo = MockRepo({})
        ok = await repo.update_recon_case_proposed(uuid.uuid4(), {})
        assert ok is True

    def test_build_settlement_line_maps_fields(self):
        from recon.eval.harness import _build_settlement_line

        line = _build_settlement_line({
            "file_id": "f1", "line_no": 5, "rrn": "123",
            "amount_paise": 9999, "status": "SUCCESS",
        })
        assert line.file_id == "f1"
        assert line.line_no == 5
        assert line.rrn == "123"
        assert line.amount_paise == 9999

    def test_build_recon_case_gets_new_uuid(self):
        import uuid
        from recon.eval.harness import _build_recon_case

        c1 = _build_recon_case({"match_type": "UNKNOWN"})
        c2 = _build_recon_case({"match_type": "UNKNOWN"})
        assert isinstance(c1.case_uid, uuid.UUID)
        assert c1.case_uid != c2.case_uid

    def test_dict_to_pg_txn_uses_utc_fallback(self):
        from recon.eval.harness import _dict_to_pg_txn

        txn = _dict_to_pg_txn({"id": 1, "amount_paise": 500, "created_at": None})
        assert txn.created_at.tzinfo is not None

    @pytest.mark.asyncio
    async def test_run_all_returns_report_with_correct_counts(self):
        cases = _harness.load_cases(CASES_DIR)
        report = await _harness.run_all(cases)
        assert report.total == len(cases)
        assert report.rules_total + report.agent_total == report.total
        assert report.passed <= report.total

    @pytest.mark.asyncio
    async def test_report_accuracy_properties(self):
        cases = _harness.load_cases(CASES_DIR)
        report = await _harness.run_all(cases)
        assert 0.0 <= report.accuracy <= 1.0
        assert 0.0 <= report.rules_accuracy <= 1.0
        assert 0.0 <= report.agent_accuracy <= 1.0

    def test_check_regression_writes_baseline_if_missing(self, tmp_path):
        from recon.eval.harness import EvalReport

        report = EvalReport(
            total=6, passed=6,
            rules_total=2, rules_passed=2,
            agent_total=4, agent_passed=4,
            total_cost_usd=0.006,
            failed_ids=[],
            results=[],
        )
        baseline = tmp_path / "baseline.json"
        _harness.check_regression(report, baseline)
        assert baseline.exists()
        data = __import__("json").loads(baseline.read_text())
        assert data["accuracy"] == 1.0

    def test_check_regression_passes_when_within_tolerance(self, tmp_path):
        import json
        from recon.eval.harness import EvalReport

        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps({"accuracy": 1.0, "rules_accuracy": 1.0,
                                         "agent_accuracy": 1.0, "avg_cost_usd": 0.001}))
        report = EvalReport(
            total=6, passed=5,  # 5/6 = 0.833 — drop of 0.167, more than 0.02
            rules_total=2, rules_passed=2,
            agent_total=4, agent_passed=3,
            total_cost_usd=0.005,
            failed_ids=["x"],
            results=[],
        )
        # This should raise because accuracy dropped > 0.02
        with pytest.raises(AssertionError, match="regression"):
            _harness.check_regression(report, baseline)

    def test_check_regression_raises_on_cost_spike(self, tmp_path):
        import json
        from recon.eval.harness import EvalReport

        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps({"accuracy": 1.0, "rules_accuracy": 1.0,
                                         "agent_accuracy": 1.0, "avg_cost_usd": 0.001}))
        report = EvalReport(
            total=6, passed=6,
            rules_total=2, rules_passed=2,
            agent_total=4, agent_passed=4,
            total_cost_usd=0.010,  # avg = 0.0025 = 2.5× baseline
            failed_ids=[],
            results=[],
        )
        with pytest.raises(AssertionError, match="regression"):
            _harness.check_regression(report, baseline)
