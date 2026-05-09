from typing import Optional

from recon.models import Match, PgTxn, SettlementLine


def duplicate(
    line: SettlementLine,
    candidates: list[PgTxn],
    all_settlement_rrns: set[str],
) -> Optional[Match]:
    """
    Rule 4 — duplicate settlement line.
    Fired when the same RRN already appears in a *previously processed* line
    (tracked by the caller via all_settlement_rrns).

    The first occurrence is resolved normally by earlier rules; every subsequent
    occurrence is marked DUPLICATE pointing to no PG txn (the first case carries
    the real link).
    """
    if not line.rrn:
        return None

    if line.rrn not in all_settlement_rrns:
        return None

    return Match(
        pg_txn_id=None,
        match_type="DUPLICATE",
        confidence=0.900,
        notes={"rule": "duplicate", "rrn": line.rrn},
    )
