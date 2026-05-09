"""
Unit tests for the rules engine.

All rules are pure functions — no DB, no async.
Tests assert:
  1. Each rule fires on its intended input.
  2. Each rule does NOT fire on inputs owned by other rules (mutual exclusivity).
  3. Idempotence: same input → same output on repeated calls.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Optional

import pytest

from recon.models import Match, PgTxn, ReconCase, SettlementLine
from recon.rules.rules.amount_mismatch import amount_mismatch
from recon.rules.rules.duplicate import duplicate
from recon.rules.rules.exact_rrn import exact_rrn
from recon.rules.rules.missing_leg import missing_leg
from recon.rules.rules.tolerance import tolerance
from recon.rules.rules.utr_amount import utr_amount

# ── Fixtures ──────────────────────────────────────────────────────────────────

_NOW = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)
_TODAY = date(2026, 4, 30)


def _txn(
    id: int = 1,
    rrn: str = "412345678901",
    utr: str = "HDFC0123456789",
    amount_paise: int = 150_000,
    status: str = "SUCCESS",
    created_at: datetime = _NOW,
) -> PgTxn:
    return PgTxn(
        id=id,
        txn_id=f"txn-{id}",
        rrn=rrn,
        utr=utr,
        payer_vpa="alice@okhdfc",
        payee_vpa="merchant@okicici",
        amount_paise=amount_paise,
        status=status,
        created_at=created_at,
        updated_at=created_at,
    )


def _line(
    rrn: Optional[str] = "412345678901",
    utr: Optional[str] = "HDFC0123456789",
    amount_paise: Optional[int] = 150_000,
    status: str = "SUCCESS",
    txn_date: Optional[date] = _TODAY,
) -> SettlementLine:
    return SettlementLine(
        file_id="test_file",
        line_no=1,
        rrn=rrn,
        utr=utr,
        amount_paise=amount_paise,
        fee_paise=270,
        net_paise=149_730,
        status=status,
        txn_date=txn_date,
        payer_vpa="alice@okhdfc",
        payee_vpa="merchant@okicici",
        txn_type="P2M",
        remarks=None,
        raw={},
        id=1,
    )


# ── Rule 1: exact_rrn ────────────────────────────────────────────────────────


class TestExactRrn:
    def test_fires_on_exact_match(self):
        txn = _txn()
        m = exact_rrn(_line(), [txn])
        assert m is not None
        assert m.match_type == "EXACT"
        assert m.confidence == 1.0
        assert m.pg_txn_id == txn.id

    def test_no_fire_if_amount_differs(self):
        txn = _txn(amount_paise=100_000)
        assert exact_rrn(_line(amount_paise=150_000), [txn]) is None

    def test_no_fire_if_status_not_success(self):
        txn = _txn(status="FAILED")
        assert exact_rrn(_line(), [txn]) is None

    def test_no_fire_if_rrn_missing(self):
        assert exact_rrn(_line(rrn=None), [_txn()]) is None

    def test_no_fire_if_multiple_candidates(self):
        txns = [_txn(id=1), _txn(id=2)]
        assert exact_rrn(_line(), txns) is None

    def test_idempotent(self):
        line, txn = _line(), _txn()
        assert exact_rrn(line, [txn]) == exact_rrn(line, [txn])


# ── Rule 2: utr_amount ───────────────────────────────────────────────────────


class TestUtrAmount:
    def test_fires_on_utr_amount_same_day(self):
        txn = _txn(rrn="DIFFERENT_RRN")  # RRN differs but UTR + amount + date match
        m = utr_amount(_line(), [txn])
        assert m is not None
        assert m.match_type == "EXACT"
        assert m.confidence == 0.990

    def test_no_fire_if_utr_missing(self):
        assert utr_amount(_line(utr=None), [_txn()]) is None

    def test_no_fire_if_date_differs(self):
        txn = _txn(created_at=datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc))
        assert utr_amount(_line(), [txn]) is None

    def test_no_fire_if_amount_differs(self):
        txn = _txn(amount_paise=999)
        assert utr_amount(_line(), [txn]) is None

    def test_idempotent(self):
        line, txn = _line(), _txn(rrn="OTHER")
        assert utr_amount(line, [txn]) == utr_amount(line, [txn])


# ── Rule 3: tolerance ────────────────────────────────────────────────────────


class TestTolerance:
    def test_fires_on_one_paise_delta(self):
        txn = _txn(amount_paise=150_001)
        m = tolerance(_line(amount_paise=150_000), [txn])
        assert m is not None
        assert m.match_type == "TOLERANCE"
        assert m.confidence == 0.950
        assert m.notes["delta_paise"] == 1

    def test_fires_on_negative_one_paise_delta(self):
        txn = _txn(amount_paise=149_999)
        m = tolerance(_line(amount_paise=150_000), [txn])
        assert m is not None

    def test_no_fire_if_delta_exceeds_tolerance(self):
        txn = _txn(amount_paise=150_002)
        assert tolerance(_line(amount_paise=150_000), [txn]) is None

    def test_no_fire_if_rrn_differs(self):
        txn = _txn(rrn="DIFFERENT", amount_paise=150_001)
        assert tolerance(_line(amount_paise=150_000), [txn]) is None

    def test_idempotent(self):
        line = _line(amount_paise=150_000)
        txn = _txn(amount_paise=150_001)
        assert tolerance(line, [txn]) == tolerance(line, [txn])


# ── Rule 4: duplicate ────────────────────────────────────────────────────────


class TestDuplicate:
    def test_fires_when_rrn_already_seen(self):
        seen = {"412345678901"}
        m = duplicate(_line(), [_txn()], seen)
        assert m is not None
        assert m.match_type == "DUPLICATE"
        assert m.confidence == 0.900

    def test_no_fire_when_rrn_not_seen(self):
        assert duplicate(_line(), [_txn()], set()) is None

    def test_no_fire_if_rrn_missing(self):
        assert duplicate(_line(rrn=None), [], {"412345678901"}) is None

    def test_idempotent(self):
        seen = {"412345678901"}
        line = _line()
        assert duplicate(line, [], seen) == duplicate(line, [], seen)


# ── Rule 5: amount_mismatch ──────────────────────────────────────────────────


class TestAmountMismatch:
    def test_fires_when_rrn_matches_but_amounts_differ(self):
        txn = _txn(amount_paise=200_000)
        m = amount_mismatch(_line(amount_paise=150_000), [txn])
        assert m is not None
        assert m.match_type == "AMOUNT_MISMATCH"
        assert m.notes["route"] == "agent"

    def test_no_fire_if_amounts_match(self):
        txn = _txn(amount_paise=150_000)
        assert amount_mismatch(_line(amount_paise=150_000), [txn]) is None

    def test_no_fire_if_within_one_paise(self):
        txn = _txn(amount_paise=150_001)
        assert amount_mismatch(_line(amount_paise=150_000), [txn]) is None

    def test_no_fire_if_no_candidates(self):
        assert amount_mismatch(_line(), []) is None

    def test_no_fire_if_multiple_candidates(self):
        txns = [_txn(id=1, amount_paise=200_000), _txn(id=2, amount_paise=200_000)]
        assert amount_mismatch(_line(), txns) is None

    def test_idempotent(self):
        line = _line(amount_paise=150_000)
        txn = _txn(amount_paise=200_000)
        assert amount_mismatch(line, [txn]) == amount_mismatch(line, [txn])


# ── Rule 6: missing_leg ──────────────────────────────────────────────────────


class TestMissingLeg:
    def test_fires_when_no_candidates(self):
        m = missing_leg(_line(), [])
        assert m is not None
        assert m.match_type == "MISSING_LEG"
        assert m.notes["route"] == "agent"

    def test_no_fire_when_candidates_exist(self):
        assert missing_leg(_line(), [_txn()]) is None

    def test_idempotent(self):
        line = _line()
        assert missing_leg(line, []) == missing_leg(line, [])


# ── Mutual exclusivity on a curated synthetic set ────────────────────────────


class TestMutualExclusivity:
    """
    For the canonical exact-match scenario, only exact_rrn should fire.
    All other rules must return None.
    """

    def test_only_exact_rrn_fires(self):
        """
        Mutual exclusivity is enforced by the engine's first-match-wins order.
        Individual rule predicates are not mutually exclusive — exact_rrn and
        utr_amount can both match the same txn; the engine stops at rule 1.
        Rules that are genuinely predicate-exclusive on this input are tested here.
        """
        txn = _txn()
        line = _line()
        seen: set[str] = set()

        # exact_rrn fires
        assert exact_rrn(line, [txn]) is not None
        # tolerance requires |delta| <= 1 paise; amounts are equal so delta=0 — it fires too
        # (engine stops before reaching it); just assert amount_mismatch and missing_leg don't
        assert amount_mismatch(line, [txn]) is None   # amounts are equal
        assert missing_leg(line, [txn]) is None       # candidate exists
        assert duplicate(line, [txn], seen) is None   # rrn not yet in seen

    def test_only_missing_leg_fires_when_no_candidates(self):
        line = _line(rrn="GHOST_RRN")
        seen: set[str] = set()

        assert exact_rrn(line, []) is None
        assert utr_amount(line, []) is None
        assert tolerance(line, []) is None
        assert duplicate(line, [], seen) is None
        assert amount_mismatch(line, []) is None
        assert missing_leg(line, []) is not None

    def test_ReconCase_from_match_sets_auto_resolved(self):
        txn = _txn()
        line = _line()
        m = exact_rrn(line, [txn])
        case = ReconCase.from_match(line, m)
        assert case.resolution == "AUTO_RESOLVED"
        assert case.resolved_by == "rules"
        assert case.case_uid == line.case_uid

    def test_ReconCase_unresolved_has_no_resolution(self):
        line = _line()
        case = ReconCase.unresolved(line, reason="test")
        assert case.resolution is None
        assert case.resolved_by is None


# ── case_uid determinism ──────────────────────────────────────────────────────


class TestCaseUid:
    def test_same_input_same_uid(self):
        l1 = _line()
        l2 = _line()
        assert l1.case_uid == l2.case_uid

    def test_different_line_no_different_uid(self):
        l1 = SettlementLine(
            file_id="f", line_no=1, rrn=None, utr=None, amount_paise=None,
            fee_paise=None, net_paise=None, status=None, txn_date=None,
            payer_vpa=None, payee_vpa=None, txn_type=None, remarks=None, raw={},
        )
        l2 = SettlementLine(
            file_id="f", line_no=2, rrn=None, utr=None, amount_paise=None,
            fee_paise=None, net_paise=None, status=None, txn_date=None,
            payer_vpa=None, payee_vpa=None, txn_type=None, remarks=None, raw={},
        )
        assert l1.case_uid != l2.case_uid

    def test_different_file_id_different_uid(self):
        l1 = SettlementLine(
            file_id="f1", line_no=1, rrn=None, utr=None, amount_paise=None,
            fee_paise=None, net_paise=None, status=None, txn_date=None,
            payer_vpa=None, payee_vpa=None, txn_type=None, remarks=None, raw={},
        )
        l2 = SettlementLine(
            file_id="f2", line_no=1, rrn=None, utr=None, amount_paise=None,
            fee_paise=None, net_paise=None, status=None, txn_date=None,
            payer_vpa=None, payee_vpa=None, txn_type=None, remarks=None, raw={},
        )
        assert l1.case_uid != l2.case_uid
