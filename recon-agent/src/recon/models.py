from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal, Optional

# ── Type aliases ──────────────────────────────────────────────────────────────

MatchType = Literal[
    "EXACT", "TOLERANCE", "MISSING_LEG", "AMOUNT_MISMATCH", "DUPLICATE", "UNKNOWN"
]
Resolution = Literal[
    "PROPOSED", "APPROVED", "REJECTED", "AUTO_RESOLVED", "ESCALATE"
]

_NAMESPACE_OID = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


# ── Domain objects ────────────────────────────────────────────────────────────


@dataclass
class SettlementLine:
    file_id: str
    line_no: int
    rrn: Optional[str]
    utr: Optional[str]
    amount_paise: Optional[int]
    fee_paise: Optional[int]
    net_paise: Optional[int]
    status: Optional[str]
    txn_date: Optional[date]
    payer_vpa: Optional[str]
    payee_vpa: Optional[str]
    txn_type: Optional[str]
    remarks: Optional[str]
    raw: dict
    # Set after DB insert
    id: Optional[int] = None

    @property
    def case_uid(self) -> uuid.UUID:
        return uuid.uuid5(_NAMESPACE_OID, f"{self.file_id}:{self.line_no}")


@dataclass
class PgTxn:
    id: int
    txn_id: str
    rrn: Optional[str]
    utr: Optional[str]
    payer_vpa: Optional[str]
    payee_vpa: Optional[str]
    amount_paise: int
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass
class Match:
    pg_txn_id: Optional[int]
    match_type: MatchType
    confidence: float
    notes: dict = field(default_factory=dict)


@dataclass
class ReconCase:
    case_uid: uuid.UUID
    settlement_line_id: Optional[int]
    pg_txn_id: Optional[int]
    match_type: MatchType
    confidence: Optional[float]
    resolution: Optional[Resolution]
    resolved_by: Optional[str]
    notes: dict = field(default_factory=dict)
    id: Optional[int] = None

    @classmethod
    def from_match(cls, line: SettlementLine, m: Match) -> "ReconCase":
        resolved_by: Optional[str] = None
        resolution: Optional[Resolution] = None

        if m.match_type in ("EXACT", "TOLERANCE", "DUPLICATE"):
            resolution = "AUTO_RESOLVED"
            resolved_by = "rules"

        return cls(
            case_uid=line.case_uid,
            settlement_line_id=line.id,
            pg_txn_id=m.pg_txn_id,
            match_type=m.match_type,
            confidence=m.confidence,
            resolution=resolution,
            resolved_by=resolved_by,
            notes=m.notes,
        )

    @classmethod
    def unresolved(cls, line: SettlementLine, reason: str, match_type: MatchType = "UNKNOWN") -> "ReconCase":
        return cls(
            case_uid=line.case_uid,
            settlement_line_id=line.id,
            pg_txn_id=None,
            match_type=match_type,
            confidence=None,
            resolution=None,
            resolved_by=None,
            notes={"reason": reason},
        )
