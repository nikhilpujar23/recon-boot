#!/usr/bin/env python3
"""
Generate a synthetic UDIR settlement file at /tmp/udir.txt.

Distribution (1 000 lines):
  - 850 exact matches      → exact_rrn fires  (confidence 1.0)
  -  52 UTR+amount matches → utr_amount fires  (RRN intentionally wrong)
  -  10 tolerance matches  → tolerance fires   (±1 paise rounding)
  -  10 duplicates         → duplicate rule fires
  -  38 amount mismatches  → routes to agent
  -  40 missing legs       → routes to agent (no PG txn for that RRN)
  ─────────────────────────────────────────────────────────────────────
  Total: 1 000 lines, ~922 auto-resolved (≥90%), ~78 to agent

Must be run AFTER seed_pg_txns.py (reads pg_transactions from DB).

Run: python scripts/gen_settlement_file.py
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import asyncpg
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://recon:recon@localhost:5432/recon")
OUTPUT_FILE = Path(os.environ.get("UDIR_FILE", "/tmp/udir.txt"))
RNG_SEED = 99
TXN_DATE = date(2026, 4, 30)

HEADER = "TXN_DATE|RRN|UTR|PAYER_VPA|PAYEE_VPA|AMOUNT|FEE|GST|NET|STATUS|TXN_TYPE|REMARKS"


def paise_to_rupees(p: int) -> str:
    return f"{Decimal(p) / 100:.2f}"


def fee_from_amount(amount_paise: int) -> int:
    """Simulate 0.18% MDR."""
    return max(1, round(amount_paise * 18 / 10_000))


async def main() -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    try:
        rows = await pool.fetch(
            "SELECT id, rrn, utr, payer_vpa, payee_vpa, amount_paise, status, created_at "
            "FROM pg_transactions ORDER BY id ASC LIMIT 1000"
        )
    finally:
        await pool.close()

    if not rows:
        print("No pg_transactions found. Run seed_pg_txns.py first.")
        sys.exit(1)

    rng = random.Random(RNG_SEED)
    lines: list[str] = [HEADER]

    rows = list(rows)
    rng.shuffle(rows)

    exact_rows       = rows[:850]
    utr_rows         = rows[850:902]
    tolerance_rows   = rows[902:912]
    duplicate_rows   = rows[912:922]   # will be emitted twice
    mismatch_rows    = rows[922:960]
    # missing-leg rows are synthetic (RRNs not in DB)
    # We use a different RRN prefix so the DB lookup returns nothing
    n_missing = 40

    def make_line(rrn, utr, payer, payee, amount_paise, txn_date=TXN_DATE, status="SUCCESS") -> str:
        fee = fee_from_amount(amount_paise)
        gst = round(fee * 18 / 100)
        net = amount_paise - fee - gst
        return (
            f"{txn_date}|{rrn}|{utr}|{payer}|{payee}"
            f"|{paise_to_rupees(amount_paise)}"
            f"|{paise_to_rupees(fee)}"
            f"|{paise_to_rupees(gst)}"
            f"|{paise_to_rupees(net)}"
            f"|{status}|P2M|"
        )

    # ── 850 exact matches ────────────────────────────────────────────────────
    for r in exact_rows:
        lines.append(make_line(r["rrn"], r["utr"], r["payer_vpa"], r["payee_vpa"], r["amount_paise"]))

    # ── 52 UTR+amount matches (wrong RRN in file) ────────────────────────────
    for r in utr_rows:
        bad_rrn = f"999{rng.randint(100000000, 999999999):09d}"
        lines.append(make_line(bad_rrn, r["utr"], r["payer_vpa"], r["payee_vpa"], r["amount_paise"]))

    # ── 10 tolerance matches (±1 paise) ─────────────────────────────────────
    for r in tolerance_rows:
        delta = rng.choice([-1, 1])
        lines.append(make_line(r["rrn"], r["utr"], r["payer_vpa"], r["payee_vpa"], r["amount_paise"] + delta))

    # ── 10 duplicates (same RRN emitted twice) ───────────────────────────────
    # First occurrence (will match via exact_rrn)
    for r in duplicate_rows:
        lines.append(make_line(r["rrn"], r["utr"], r["payer_vpa"], r["payee_vpa"], r["amount_paise"]))
    # Second occurrence (will be caught by duplicate rule)
    for r in duplicate_rows:
        lines.append(make_line(r["rrn"], r["utr"], r["payer_vpa"], r["payee_vpa"], r["amount_paise"]))

    # ── 38 amount mismatches (RRN exists but amount differs by >1 paise) ─────
    for r in mismatch_rows:
        delta = rng.randint(500, 50_000)  # ₹5 – ₹500 difference
        lines.append(make_line(r["rrn"], r["utr"], r["payer_vpa"], r["payee_vpa"], r["amount_paise"] + delta))

    # ── 40 missing legs (RRN not in DB) ─────────────────────────────────────
    for i in range(n_missing):
        fake_rrn = f"888{700000000 + i:09d}"
        fake_utr = f"FAKE{i:010d}"
        amount = rng.randint(100, 200_000)
        lines.append(make_line(fake_rrn, fake_utr, "ghost@okhdfcbank", "merchant@okicici", amount))

    rng.shuffle(lines[1:])  # shuffle data rows; keep header first

    OUTPUT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    total = len(lines) - 1  # exclude header
    print(f"Generated {total} settlement lines → {OUTPUT_FILE}")
    print(f"  Expected auto-resolved : ~{850 + 52 + 10 + 10 + 10} ({(850+52+10+10+10)/total*100:.1f}%)")
    print(f"  Expected agent/unknown : ~{38 + 40} ({(38+40)/total*100:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())
