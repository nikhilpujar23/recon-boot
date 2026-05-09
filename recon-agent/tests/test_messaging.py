"""
Unit tests for recon.messaging — codec round-trips and topic/schema mappings.

No DB, no Kafka required.
"""
from __future__ import annotations

import time
import uuid

import pytest

from recon.messaging import (
    SCHEMA_FILE_ARRIVED,
    SCHEMA_RECON_REQUEST,
    SCHEMA_RECON_RESULT,
    SCHEMA_TO_TOPIC,
    TOPIC_FILES_NEW,
    TOPIC_RECON_REQUESTS,
    TOPIC_RECON_RESULTS,
    FileArrived,
    ReconRequest,
    ReconResult,
    decode,
    encode,
    recon_request_from_line,
    recon_result_from_case,
)
from recon.models import ReconCase, SettlementLine


# ── FileArrived ───────────────────────────────────────────────────────────────


class TestFileArrivedCodec:
    def _make(self) -> FileArrived:
        return FileArrived(
            file_id="udir_2026_04_30",
            filename="udir_2026_04_30.txt",
            sha256="a" * 64,
            bytes=102400,
            detected_at=1_700_000_000_000,
        )

    def test_encode_returns_bytes(self):
        msg = self._make()
        assert isinstance(encode(msg), bytes)

    def test_round_trip(self):
        msg = self._make()
        decoded = decode(SCHEMA_FILE_ARRIVED, encode(msg))
        assert decoded == msg

    def test_schema_id_attribute(self):
        msg = self._make()
        assert msg.schema_id == SCHEMA_FILE_ARRIVED

    def test_partition_key_is_file_id(self):
        msg = self._make()
        assert msg.partition_key == "udir_2026_04_30"

    def test_topic_mapping(self):
        assert SCHEMA_TO_TOPIC[SCHEMA_FILE_ARRIVED] == TOPIC_FILES_NEW


# ── ReconRequest ──────────────────────────────────────────────────────────────


class TestReconRequestCodec:
    def _make(self) -> ReconRequest:
        return ReconRequest(
            file_id="udir_2026_04_30",
            line_no=42,
            case_uid=str(uuid.uuid4()),
        )

    def test_round_trip(self):
        msg = self._make()
        decoded = decode(SCHEMA_RECON_REQUEST, encode(msg))
        assert decoded == msg

    def test_schema_id_attribute(self):
        msg = self._make()
        assert msg.schema_id == SCHEMA_RECON_REQUEST

    def test_partition_key_is_file_id(self):
        msg = self._make()
        assert msg.partition_key == "udir_2026_04_30"

    def test_topic_mapping(self):
        assert SCHEMA_TO_TOPIC[SCHEMA_RECON_REQUEST] == TOPIC_RECON_REQUESTS


# ── ReconResult ───────────────────────────────────────────────────────────────


class TestReconResultCodec:
    def _make(self) -> ReconResult:
        return ReconResult(
            case_uid=str(uuid.uuid4()),
            match_type="EXACT",
            resolution="AUTO_RESOLVED",
            confidence=1.0,
            resolved_by="rules",
            resolved_at_ms=1_700_000_000_000,
        )

    def test_round_trip(self):
        msg = self._make()
        decoded = decode(SCHEMA_RECON_RESULT, encode(msg))
        assert decoded == msg

    def test_schema_id_attribute(self):
        msg = self._make()
        assert msg.schema_id == SCHEMA_RECON_RESULT

    def test_partition_key_is_case_uid(self):
        msg = self._make()
        assert msg.partition_key == msg.case_uid

    def test_topic_mapping(self):
        assert SCHEMA_TO_TOPIC[SCHEMA_RECON_RESULT] == TOPIC_RECON_RESULTS


# ── Unknown schema_id ─────────────────────────────────────────────────────────


class TestUnknownSchema:
    def test_decode_unknown_schema_raises(self):
        with pytest.raises(ValueError, match="Unknown schema_id"):
            decode(999, b'{"foo": "bar"}')


# ── Convenience builders ──────────────────────────────────────────────────────


class TestConvenienceBuilders:
    def _make_line(self) -> SettlementLine:
        return SettlementLine(
            file_id="f1",
            line_no=5,
            rrn="RRN001",
            utr="UTR001",
            amount_paise=100_000,
            fee_paise=200,
            net_paise=99_800,
            status="SUCCESS",
            txn_date=None,
            payer_vpa="a@okhdfc",
            payee_vpa="b@okicici",
            txn_type="P2M",
            remarks=None,
            raw={},
            id=1,
        )

    def test_recon_request_from_line(self):
        line = self._make_line()
        req = recon_request_from_line(line)
        assert req.file_id == "f1"
        assert req.line_no == 5
        assert req.case_uid == str(line.case_uid)
        assert req.schema_id == SCHEMA_RECON_REQUEST

    def test_recon_result_from_case(self):
        from recon.models import Match
        line = self._make_line()
        match = Match(pg_txn_id=7, match_type="EXACT", confidence=1.0)
        case = ReconCase.from_match(line, match)
        result = recon_result_from_case(case)
        assert result.match_type == "EXACT"
        assert result.resolution == "AUTO_RESOLVED"
        assert result.confidence == 1.0
        assert result.resolved_by == "rules"
        assert result.schema_id == SCHEMA_RECON_RESULT

    def test_recon_result_defaults_for_unresolved(self):
        line = self._make_line()
        case = ReconCase.unresolved(line, reason="no_match")
        result = recon_result_from_case(case)
        assert result.resolution == "PENDING"
        assert result.confidence == 0.0


# ── Idempotence / determinism ─────────────────────────────────────────────────


class TestDeterminism:
    def test_encode_same_message_same_bytes(self):
        msg = ReconRequest(file_id="f", line_no=1, case_uid="uid")
        assert encode(msg) == encode(msg)

    def test_decode_is_inverse_of_encode(self):
        original = ReconResult(
            case_uid="abc",
            match_type="MISSING_LEG",
            resolution="PROPOSED",
            confidence=0.72,
            resolved_by="agent",
            resolved_at_ms=999,
        )
        assert decode(SCHEMA_RECON_RESULT, encode(original)) == original
