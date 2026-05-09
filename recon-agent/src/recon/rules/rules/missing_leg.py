from typing import Optional

from recon.models import Match, PgTxn, SettlementLine


def missing_leg(line: SettlementLine, candidates: list[PgTxn]) -> Optional[Match]:
    """
    Rule 6 — no PG txn found for the settlement line's RRN/UTR.
    Always fires as a catch-all when no earlier rule matched and candidates is empty.
    Routes to the agent.
    """
    if candidates:
        return None  # some candidate exists; earlier rules should have handled it

    return Match(
        pg_txn_id=None,
        match_type="MISSING_LEG",
        confidence=0.0,
        notes={"rule": "missing_leg", "route": "agent"},
    )
