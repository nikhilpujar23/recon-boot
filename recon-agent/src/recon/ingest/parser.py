"""
Phase 1 + Phase 2 — UDIR settlement file parser.

Phase 1:  Stream-parse, persist settlement_lines.
Phase 2:  Each line insert is coupled with an outbox(ReconRequest) write
          inside the SAME transaction, so the Kafka event is never emitted
          without the corresponding DB row (LLD §9.2).

The `outbox_enabled` flag (default True when repo has insert_outbox_tx)
lets the Phase-1 CLI path keep working without a Kafka broker.
"""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

from recon.ledger.repo import LedgerRepo
from recon.messaging import (
    SCHEMA_RECON_REQUEST,
    TOPIC_RECON_REQUESTS,
    encode,
    recon_request_from_line,
)
from recon.models import SettlementLine


class UdirParser:
    """
    Stream-parse a pipe-separated UDIR settlement file line-by-line.

    Each physical row produces one SettlementLine persisted to the DB.
    Amounts arrive as decimal rupees; we convert to paise via Decimal (never float).

    Phase 2: wrap each (settlement_line + outbox_event) in a single transaction.
    """

    HEADER = [
        "TXN_DATE", "RRN", "UTR", "PAYER_VPA", "PAYEE_VPA",
        "AMOUNT", "FEE", "GST", "NET", "STATUS", "TXN_TYPE", "REMARKS",
    ]
    _EXPECTED_COLS = len(HEADER)

    def __init__(self, repo: LedgerRepo, outbox_enabled: bool = True) -> None:
        self._repo = repo
        self._outbox_enabled = outbox_enabled

    async def parse(self, file_id: str, path: Path) -> int:
        """
        Parse the file at `path` and persist each row.

        Each new line is written in its own transaction together with a
        ReconRequest outbox event (Phase 2). Duplicate lines (ON CONFLICT)
        produce no outbox event.

        Returns the number of rows written (duplicates not counted).
        """
        written = 0
        with path.open(encoding="utf-8") as fh:
            for raw_line_no, raw_text in enumerate(fh, start=1):
                text = raw_text.rstrip("\n")
                if not text or text.startswith("#"):
                    continue

                # Skip header if present
                if raw_line_no == 1 and text.startswith("TXN_DATE"):
                    continue

                line = self._parse_line(file_id, raw_line_no, text)
                inserted = await self._persist_with_outbox(line)
                if inserted is not None:
                    line.id = inserted
                    written += 1

        return written

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _persist_with_outbox(self, line: SettlementLine) -> Optional[int]:
        """
        In a single transaction:
          1. INSERT INTO settlement_lines … ON CONFLICT DO NOTHING RETURNING id
          2. If inserted: INSERT INTO outbox (ReconRequest payload)

        Returns the new row id, or None for duplicates.
        """
        raw_json = json.dumps(line.raw)
        async with self._repo._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO settlement_lines
                        (file_id, line_no, rrn, utr, amount_paise, fee_paise,
                         net_paise, status, raw)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                    ON CONFLICT (file_id, line_no) DO NOTHING
                    RETURNING id
                    """,
                    line.file_id,
                    line.line_no,
                    line.rrn,
                    line.utr,
                    line.amount_paise,
                    line.fee_paise,
                    line.net_paise,
                    line.status,
                    raw_json,
                )
                if row is None:
                    return None  # duplicate; no outbox event

                line_id: int = row["id"]
                line.id = line_id

                if self._outbox_enabled:
                    msg = recon_request_from_line(line)
                    payload = encode(msg)
                    await self._repo.insert_outbox_tx(
                        conn,
                        topic=TOPIC_RECON_REQUESTS,
                        payload=payload,
                        schema_id=SCHEMA_RECON_REQUEST,
                        partition_key=line.file_id,
                    )

                return line_id

    def _parse_line(self, file_id: str, line_no: int, text: str) -> SettlementLine:
        parts = text.split("|")
        raw: dict = dict(zip(self.HEADER, parts)) if len(parts) == self._EXPECTED_COLS else {"_raw": text}

        if len(parts) != self._EXPECTED_COLS:
            return SettlementLine(
                file_id=file_id,
                line_no=line_no,
                rrn=None,
                utr=None,
                amount_paise=None,
                fee_paise=None,
                net_paise=None,
                status="MALFORMED",
                txn_date=None,
                payer_vpa=None,
                payee_vpa=None,
                txn_type=None,
                remarks=None,
                raw=raw,
            )

        return SettlementLine(
            file_id=file_id,
            line_no=line_no,
            rrn=_nonempty(parts[1]),
            utr=_nonempty(parts[2]),
            amount_paise=_rupees_to_paise(parts[5]),
            fee_paise=_rupees_to_paise(parts[6]),
            net_paise=_rupees_to_paise(parts[8]),
            status=_nonempty(parts[9]),
            txn_date=_parse_date(parts[0]),
            payer_vpa=_nonempty(parts[3]),
            payee_vpa=_nonempty(parts[4]),
            txn_type=_nonempty(parts[10]),
            remarks=_nonempty(parts[11]),
            raw=raw,
        )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _nonempty(s: str) -> Optional[str]:
    s = s.strip()
    return s if s else None


def _rupees_to_paise(s: str) -> Optional[int]:
    s = s.strip()
    if not s:
        return None
    try:
        return int(Decimal(s) * 100)
    except InvalidOperation:
        return None


def _parse_date(s: str) -> Optional[date]:
    s = s.strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None
