from typing import Optional

from recon.models import Match, PgTxn, SettlementLine


def exact_rrn(line: SettlementLine, candidates: list[PgTxn]) -> Optional[Match]:
    """
    Rule 1 — exact RRN match.
    Exactly one SUCCESS PG txn with same RRN and amount.
    """
    if not line.rrn or line.amount_paise is None:
        return None

    hits = [
        t for t in candidates
        if t.rrn == line.rrn
        and t.amount_paise == line.amount_paise
        and t.status == "SUCCESS"
    ]
    if len(hits) != 1:
        return None

    return Match(
        pg_txn_id=hits[0].id,
        match_type="EXACT",
        confidence=1.000,
        notes={"rule": "exact_rrn"},
    )
