from typing import Optional

from recon.models import Match, PgTxn, SettlementLine


def utr_amount(line: SettlementLine, candidates: list[PgTxn]) -> Optional[Match]:
    """
    Rule 2 — UTR + amount + same calendar day.
    Exactly one PG txn with matching UTR, amount, and date(created_at) == txn_date.
    """
    if not line.utr or line.amount_paise is None or line.txn_date is None:
        return None

    hits = [
        t for t in candidates
        if t.utr == line.utr
        and t.amount_paise == line.amount_paise
        and t.created_at.date() == line.txn_date
    ]
    if len(hits) != 1:
        return None

    return Match(
        pg_txn_id=hits[0].id,
        match_type="EXACT",
        confidence=0.990,
        notes={"rule": "utr_amount"},
    )
