#!/usr/bin/env python3
"""
Seed ~1 000 synthetic pg_transactions into the DB.

Generates a deterministic dataset (fixed random seed) so that
gen_settlement_file.py can produce a file where ~90% of lines match
by exact_rrn and the remaining ~10% are intentional mismatches.

Run: python scripts/seed_pg_txns.py
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow importing src/recon without an editable install
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import asyncpg
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://recon:recon@localhost:5432/recon")

N_TXNS = 1_000
RNG_SEED = 42
TXN_DATE = datetime(2026, 4, 30, tzinfo=timezone.utc)

VPA_PAYERS = [f"user{i:04d}@okhdfc" for i in range(200)]
VPA_PAYEES = [f"merchant{i:03d}@okicici" for i in range(50)]
STATUSES = ["SUCCESS"] * 88 + ["FAILED"] * 6 + ["TIMEOUT"] * 4 + ["PARTIAL_REVERSED"] * 2


def _rrn(n: int) -> str:
    return f"{400000000000 + n:012d}"


def _utr(n: int) -> str:
    return f"HDFC{2000000000 + n:010d}"


def _gen_rows(rng: random.Random) -> list[dict]:
    rows = []
    for i in range(N_TXNS):
        amount_paise = rng.randint(100, 500_000)  # ₹1 – ₹5 000
        created = TXN_DATE + timedelta(seconds=rng.randint(0, 86_399))
        rows.append(
            {
                "txn_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "rrn": _rrn(i),
                "utr": _utr(i),
                "payer_vpa": rng.choice(VPA_PAYERS),
                "payee_vpa": rng.choice(VPA_PAYEES),
                "amount_paise": amount_paise,
                "status": rng.choice(STATUSES),
                "created_at": created,
                "updated_at": created,
            }
        )
    return rows


async def main() -> None:
    rng = random.Random(RNG_SEED)
    rows = _gen_rows(rng)

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    try:
        inserted = await pool.executemany(
            """
            INSERT INTO pg_transactions
                (txn_id, rrn, utr, payer_vpa, payee_vpa,
                 amount_paise, status, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (txn_id) DO NOTHING
            """,
            [
                (
                    r["txn_id"], r["rrn"], r["utr"], r["payer_vpa"], r["payee_vpa"],
                    r["amount_paise"], r["status"], r["created_at"], r["updated_at"],
                )
                for r in rows
            ],
        )
        print(f"Seeded {N_TXNS} pg_transactions (conflicts skipped)")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
