"""
Phase 2 — Message schemas and codec.

Three message types mirror proto/recon.proto:
  FileArrived   — emitted by SftpWatcher when a new settlement file lands
  ReconRequest  — one per settlement line; drives rules + agent workers
  ReconResult   — written after rules/agent resolution

Payload encoding: JSON → UTF-8 bytes.
This is a drop-in replacement target for betterproto/protobuf when the
proto/recon.proto codegen is wired in CI; callers use encode()/decode() only.

Schema IDs (stored in outbox.schema_id):
  1 → FileArrived
  2 → ReconRequest
  3 → ReconResult
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any


# ── Schema ID registry ────────────────────────────────────────────────────────

SCHEMA_FILE_ARRIVED  = 1
SCHEMA_RECON_REQUEST = 2
SCHEMA_RECON_RESULT  = 3

_SCHEMA_MAP: dict[int, type] = {}


def _register(schema_id: int):
    def _decorator(cls):
        _SCHEMA_MAP[schema_id] = cls
        cls._schema_id = schema_id
        return cls
    return _decorator


# ── Message dataclasses ───────────────────────────────────────────────────────


@_register(SCHEMA_FILE_ARRIVED)
@dataclass(frozen=True)
class FileArrived:
    """Emitted by SftpWatcher; consumed by the ingest Kafka consumer."""
    file_id: str
    filename: str
    sha256: str
    bytes: int
    detected_at: int = field(default_factory=lambda: int(time.time() * 1000))  # epoch ms

    @property
    def schema_id(self) -> int:
        return SCHEMA_FILE_ARRIVED

    @property
    def partition_key(self) -> str:
        return self.file_id


@_register(SCHEMA_RECON_REQUEST)
@dataclass(frozen=True)
class ReconRequest:
    """One message per settlement line; drives rules + agent workers."""
    file_id: str
    line_no: int
    case_uid: str  # hex UUID-5(file_id:line_no)

    @property
    def schema_id(self) -> int:
        return SCHEMA_RECON_REQUEST

    @property
    def partition_key(self) -> str:
        return self.file_id


@_register(SCHEMA_RECON_RESULT)
@dataclass(frozen=True)
class ReconResult:
    """Written to Kafka after rules/agent resolution."""
    case_uid: str
    match_type: str
    resolution: str
    confidence: float
    resolved_by: str
    resolved_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    @property
    def schema_id(self) -> int:
        return SCHEMA_RECON_RESULT

    @property
    def partition_key(self) -> str:
        return self.case_uid


# ── Kafka topic names ─────────────────────────────────────────────────────────

TOPIC_FILES_NEW       = "files.new"
TOPIC_RECON_REQUESTS  = "recon.requests"
TOPIC_RECON_RESULTS   = "recon.results"
TOPIC_RECON_DLQ       = "recon.dlq"

SCHEMA_TO_TOPIC: dict[int, str] = {
    SCHEMA_FILE_ARRIVED:  TOPIC_FILES_NEW,
    SCHEMA_RECON_REQUEST: TOPIC_RECON_REQUESTS,
    SCHEMA_RECON_RESULT:  TOPIC_RECON_RESULTS,
}


# ── Codec (JSON bytes) ────────────────────────────────────────────────────────


def encode(msg: FileArrived | ReconRequest | ReconResult) -> bytes:
    """Serialize a message to UTF-8 JSON bytes."""
    return json.dumps(asdict(msg)).encode("utf-8")


def decode(schema_id: int, payload: bytes) -> FileArrived | ReconRequest | ReconResult:
    """Deserialize payload bytes into the appropriate message dataclass."""
    cls = _SCHEMA_MAP.get(schema_id)
    if cls is None:
        raise ValueError(f"Unknown schema_id {schema_id}")
    data: dict[str, Any] = json.loads(payload.decode("utf-8"))
    return cls(**data)


# ── Convenience: build a ReconResult from a ReconCase ─────────────────────────


def recon_result_from_case(case) -> ReconResult:  # case: ReconCase
    return ReconResult(
        case_uid=str(case.case_uid),
        match_type=case.match_type,
        resolution=case.resolution or "PENDING",
        confidence=float(case.confidence) if case.confidence is not None else 0.0,
        resolved_by=case.resolved_by or "rules",
    )


# ── Convenience: build a ReconRequest from a SettlementLine ──────────────────


def recon_request_from_line(line) -> ReconRequest:  # line: SettlementLine
    return ReconRequest(
        file_id=line.file_id,
        line_no=line.line_no,
        case_uid=str(line.case_uid),
    )
