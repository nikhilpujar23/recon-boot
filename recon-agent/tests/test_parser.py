"""
Unit tests for UdirParser.

All tests are synchronous (parser internals are sync; only DB calls are async).
We test _parse_line directly to avoid needing a DB connection.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from recon.ingest.parser import UdirParser, _nonempty, _parse_date, _rupees_to_paise


# ── Helper: instantiate parser without a repo ─────────────────────────────────


def make_parser() -> UdirParser:
    # Repo is only needed for async DB writes; pure parsing tests don't touch it.
    return UdirParser(repo=None)  # type: ignore[arg-type]


VALID_LINE = "2026-04-30|412345678901|HDFC0123456789|alice@okhdfc|merchant@okicici|1500.00|2.00|0.36|1497.64|SUCCESS|P2M|"


# ── Happy path ────────────────────────────────────────────────────────────────


class TestParseLineHappyPath:
    def test_parses_rrn(self):
        p = make_parser()
        line = p._parse_line("f1", 1, VALID_LINE)
        assert line.rrn == "412345678901"

    def test_parses_utr(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        assert line.utr == "HDFC0123456789"

    def test_parses_amount_in_paise(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        # ₹1500.00 → 150 000 paise
        assert line.amount_paise == 150_000

    def test_parses_fee_in_paise(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        # ₹2.00 → 200 paise
        assert line.fee_paise == 200

    def test_parses_net_in_paise(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        # ₹1497.64 → 149764 paise
        assert line.net_paise == 149_764

    def test_parses_status(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        assert line.status == "SUCCESS"

    def test_parses_txn_date(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        assert line.txn_date == date(2026, 4, 30)

    def test_parses_payer_vpa(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        assert line.payer_vpa == "alice@okhdfc"

    def test_stores_raw_dict(self):
        line = make_parser()._parse_line("f1", 1, VALID_LINE)
        assert line.raw["RRN"] == "412345678901"
        assert line.raw["STATUS"] == "SUCCESS"

    def test_file_id_and_line_no(self):
        line = make_parser()._parse_line("udir_2026_04_30", 42, VALID_LINE)
        assert line.file_id == "udir_2026_04_30"
        assert line.line_no == 42


# ── Malformed lines ───────────────────────────────────────────────────────────


class TestMalformedLines:
    def test_wrong_column_count_sets_malformed(self):
        bad = "2026-04-30|only|three|fields"
        line = make_parser()._parse_line("f1", 1, bad)
        assert line.status == "MALFORMED"
        assert line.rrn is None
        assert line.amount_paise is None

    def test_malformed_preserves_raw_text(self):
        bad = "garbage|line"
        line = make_parser()._parse_line("f1", 1, bad)
        assert "_raw" in line.raw

    def test_empty_status_stored_as_none(self):
        # Replace SUCCESS with empty string
        no_status = VALID_LINE.replace("|SUCCESS|", "||")
        line = make_parser()._parse_line("f1", 1, no_status)
        assert line.status is None

    def test_empty_rrn_stored_as_none(self):
        no_rrn = VALID_LINE.replace("|412345678901|", "||")
        line = make_parser()._parse_line("f1", 1, no_rrn)
        assert line.rrn is None


# ── Amount conversion ─────────────────────────────────────────────────────────


class TestAmountConversion:
    def test_rupees_to_paise_whole_number(self):
        assert _rupees_to_paise("1500") == 150_000

    def test_rupees_to_paise_two_decimals(self):
        assert _rupees_to_paise("1500.00") == 150_000

    def test_rupees_to_paise_one_decimal(self):
        assert _rupees_to_paise("1500.5") == 150_050

    def test_rupees_to_paise_zero(self):
        assert _rupees_to_paise("0.00") == 0

    def test_rupees_to_paise_empty_string(self):
        assert _rupees_to_paise("") is None

    def test_rupees_to_paise_whitespace(self):
        assert _rupees_to_paise("  ") is None

    def test_rupees_to_paise_invalid(self):
        assert _rupees_to_paise("not_a_number") is None

    def test_no_float_precision_loss(self):
        # Decimal must be used — float("1497.64") * 100 = 149763.99999... → wrong
        assert _rupees_to_paise("1497.64") == 149_764


# ── Helper functions ──────────────────────────────────────────────────────────


class TestHelpers:
    def test_nonempty_returns_string(self):
        assert _nonempty("hello") == "hello"

    def test_nonempty_strips_whitespace(self):
        assert _nonempty("  hi  ") == "hi"

    def test_nonempty_returns_none_for_empty(self):
        assert _nonempty("") is None

    def test_nonempty_returns_none_for_whitespace(self):
        assert _nonempty("   ") is None

    def test_parse_date_valid(self):
        assert _parse_date("2026-04-30") == date(2026, 4, 30)

    def test_parse_date_empty(self):
        assert _parse_date("") is None

    def test_parse_date_invalid(self):
        assert _parse_date("not-a-date") is None


# ── Header recognition ────────────────────────────────────────────────────────


class TestHeaderSkip:
    """The parser should skip the header row silently."""

    def test_header_columns_match_expected(self):
        p = make_parser()
        assert p.HEADER[0] == "TXN_DATE"
        assert p.HEADER[1] == "RRN"
        assert len(p.HEADER) == 12
