#!/usr/bin/env python3
"""
Generate a sample UDIR pipe-delimited settlement file for local testing.

The SftpWatcher picks up *.txt files from the SFTP drop directory.
UdirParser expects rows in the format:
  HDR|<file_id>|<date>|<total_records>
  <rrn>|<utr>|<amount_paise>|<fee_paise>|<net_paise>|<status>
  TRL|<count>

Usage:
  python3 scripts/gen_settlement_file.py           # writes to /tmp/udir_<ts>.txt
  python3 scripts/gen_settlement_file.py --out /tmp/my.txt
"""

import argparse
import datetime
import random
import string
import sys

def rand_rrn():
    return "RRN" + "".join(random.choices(string.digits, k=12))

def rand_utr():
    return "UTR" + "".join(random.choices(string.digits, k=12))

def gen_file(n_exact=4, n_tolerance=2, n_mismatch=2, n_missing=2):
    """Return (file_id, lines[]) covering all match-type scenarios."""
    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    fid  = f"UDIR_{ts}"
    rows = []

    # Scenarios we can seed pg_transactions for:
    # EXACT — amounts match perfectly
    for _ in range(n_exact):
        rrn    = rand_rrn()
        utr    = rand_utr()
        amount = random.randint(1000, 500000)
        fee    = int(amount * 0.009)
        net    = amount - fee
        rows.append((rrn, utr, amount, fee, net, "SUCCESS", "exact"))

    # TOLERANCE — settlement amount differs by 1 paise from PG
    for _ in range(n_tolerance):
        rrn    = rand_rrn()
        utr    = rand_utr()
        amount = random.randint(1000, 500000)
        fee    = int(amount * 0.009)
        net    = amount - fee
        rows.append((rrn, utr, amount + 1, fee, net - 1, "SUCCESS", "tolerance"))

    # AMOUNT_MISMATCH — settlement amount is 30%+ different
    for _ in range(n_mismatch):
        rrn    = rand_rrn()
        utr    = rand_utr()
        amount = random.randint(10000, 500000)
        fee    = int(amount * 0.009)
        net    = amount - fee
        # settlement reports a much higher amount
        s_amt  = int(amount * 1.35)
        rows.append((rrn, utr, s_amt, fee, s_amt - fee, "SUCCESS", "mismatch"))

    # MISSING_LEG — RRN/UTR that don't exist in pg_transactions
    for _ in range(n_missing):
        rrn    = rand_rrn()
        utr    = rand_utr()
        amount = random.randint(1000, 500000)
        fee    = int(amount * 0.009)
        net    = amount - fee
        rows.append((rrn, utr, amount, fee, net, "SUCCESS", "missing"))

    return fid, rows


def write_file(path, fid, rows):
    date = datetime.date.today().isoformat()
    with open(path, "w") as f:
        f.write(f"HDR|{fid}|{date}|{len(rows)}\n")
        for rrn, utr, amount, fee, net, status, _ in rows:
            f.write(f"{rrn}|{utr}|{amount}|{fee}|{net}|{status}\n")
        f.write(f"TRL|{len(rows)}\n")

    print(f"Written {len(rows)} rows to {path}")
    print(f"  file_id : {fid}")
    print()
    print("  Scenarios included:")
    counts = {}
    for *_, scenario in rows:
        counts[scenario] = counts.get(scenario, 0) + 1
    for s, n in counts.items():
        print(f"    {s:12s}: {n}")
    print()
    print("  To upload to SFTP:")
    print(f"    sshpass -p recon sftp -o StrictHostKeyChecking=no -P 2222 recon@localhost <<< $'put {path}'")
    print()
    print("  Or use the full test script:")
    print("    ./scripts/test_pipeline.sh")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out",      default=None,  help="Output file path")
    ap.add_argument("--exact",    type=int, default=4)
    ap.add_argument("--tolerance",type=int, default=2)
    ap.add_argument("--mismatch", type=int, default=2)
    ap.add_argument("--missing",  type=int, default=2)
    args = ap.parse_args()

    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = args.out or f"/tmp/udir_{ts}.txt"
    fid, rows = gen_file(args.exact, args.tolerance, args.mismatch, args.missing)
    write_file(path, fid, rows)
    print(path)   # last line = path, so callers can capture it
