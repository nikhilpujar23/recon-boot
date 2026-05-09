from __future__ import annotations

from recon.ledger.repo import LedgerRepo
from recon.models import ReconCase, SettlementLine
from recon.rules.rules.amount_mismatch import amount_mismatch
from recon.rules.rules.duplicate import duplicate
from recon.rules.rules.exact_rrn import exact_rrn
from recon.rules.rules.missing_leg import missing_leg
from recon.rules.rules.tolerance import tolerance
from recon.rules.rules.utr_amount import utr_amount


class RulesEngine:
    """
    Deterministic first-match rules pipeline.

    All rules are pure functions; the engine is the only component allowed
    to call repo.lookup_pg_txns during reconciliation. No rule writes to the DB.
    """

    def __init__(self, repo: LedgerRepo) -> None:
        self._repo = repo
        # Set of RRNs seen so far in the current file pass — used by duplicate rule.
        # Reset between files by the caller via reset_seen().
        self._seen_rrns: set[str] = set()

    def reset_seen(self) -> None:
        self._seen_rrns = set()

    async def run(self, line: SettlementLine) -> ReconCase:
        """
        Evaluate the ordered rule pipeline for one settlement line.
        Returns a ReconCase; never raises.
        """
        # MALFORMED lines get their own case without a DB lookup
        if line.status == "MALFORMED":
            return ReconCase.unresolved(line, reason="malformed_line", match_type="UNKNOWN")

        candidates = await self._repo.lookup_pg_txns(rrn=line.rrn, utr=line.utr)

        # Ambiguous: multiple distinct PG txns (not a duplicate settlement line)
        unique_ids = {t.id for t in candidates}
        if len(unique_ids) > 1:
            return ReconCase.unresolved(line, reason="ambiguous_candidates", match_type="UNKNOWN")

        # ── Rule 1: exact RRN ─────────────────────────────────────────────────
        m = exact_rrn(line, candidates)
        if m:
            self._mark_seen(line)
            return ReconCase.from_match(line, m)

        # ── Rule 2: UTR + amount + same day ──────────────────────────────────
        m = utr_amount(line, candidates)
        if m:
            self._mark_seen(line)
            return ReconCase.from_match(line, m)

        # ── Rule 3: RRN match within tolerance ───────────────────────────────
        m = tolerance(line, candidates)
        if m:
            self._mark_seen(line)
            return ReconCase.from_match(line, m)

        # ── Rule 4: duplicate settlement line ────────────────────────────────
        m = duplicate(line, candidates, self._seen_rrns)
        if m:
            # Do NOT add to seen — the original occurrence is already there
            return ReconCase.from_match(line, m)

        # ── Rule 5: amount mismatch (routes to agent) ─────────────────────────
        m = amount_mismatch(line, candidates)
        if m:
            self._mark_seen(line)
            return ReconCase.from_match(line, m)

        # ── Rule 6: missing leg (routes to agent) ─────────────────────────────
        m = missing_leg(line, candidates)
        if m:
            return ReconCase.from_match(line, m)

        # Should not reach here; missing_leg is a catch-all when candidates==[]
        return ReconCase.unresolved(line, reason="no_rule_fired")

    def _mark_seen(self, line: SettlementLine) -> None:
        if line.rrn:
            self._seen_rrns.add(line.rrn)
