from typing import Optional

from recon.models import Match, PgTxn, SettlementLine


def amount_mismatch(line: SettlementLine, candidates: list[PgTxn]) -> Optional[Match]:
    """
    Rule 5 — RRN matches exactly one PG txn but amounts differ by >1 paise.
    Routes to the agent (returns AMOUNT_MISMATCH with no pg_txn_id locked in).
    """
    if not line.rrn or line.amount_paise is None:
        return None

    rrn_hits = [t for t in candidates if t.rrn == line.rrn]
    if len(rrn_hits) != 1:
        return None

    txn = rrn_hits[0]
    if abs(txn.amount_paise - line.amount_paise) <= 1:
        return None  # tolerance rule should have caught this

    return Match(
        pg_txn_id=txn.id,
        match_type="AMOUNT_MISMATCH",
        confidence=0.0,
        notes={
            "rule": "amount_mismatch",
            "settlement_paise": line.amount_paise,
            "pg_paise": txn.amount_paise,
            "delta_paise": txn.amount_paise - line.amount_paise,
            "route": "agent",
        },
    )
