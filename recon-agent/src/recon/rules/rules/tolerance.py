from typing import Optional

from recon.config import settings
from recon.models import Match, PgTxn, SettlementLine


def tolerance(line: SettlementLine, candidates: list[PgTxn]) -> Optional[Match]:
    """
    Rule 3 — RRN matches, amount within ±RULES_TOLERANCE_PAISE (default 1 paise).
    Captures rounding artefacts in fee calculations.
    """
    if not line.rrn or line.amount_paise is None:
        return None

    tol = settings.rules_tolerance_paise

    hits = [
        t for t in candidates
        if t.rrn == line.rrn
        and abs(t.amount_paise - line.amount_paise) <= tol
        and t.status == "SUCCESS"
    ]
    if len(hits) != 1:
        return None

    delta = hits[0].amount_paise - line.amount_paise
    return Match(
        pg_txn_id=hits[0].id,
        match_type="TOLERANCE",
        confidence=0.950,
        notes={"rule": "tolerance", "delta_paise": delta},
    )
