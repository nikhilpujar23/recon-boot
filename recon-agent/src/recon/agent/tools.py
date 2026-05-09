"""
Phase 3 — Agent tool schemas and ToolRegistry (LLD §6.5).

Five tools:
  search_pg_transactions  — query internal ledger (read-only)
  get_settlement_history  — find all settlement lines for an RRN (read-only)
  get_chargeback_status   — check chargeback flag (read-only)
  compute_fee_breakdown   — pure MDR/GST calculation (no DB)
  propose_resolution      — signals the orchestrator to finalise (write, intercepted)

Only propose_resolution mutates state; the orchestrator intercepts it and
applies the write itself rather than letting the tool touch the DB directly.

PII contract:
  Inputs and outputs are passed through a Redactor before being handed to
  the LLM (Phase 4).  Until Phase 4 is wired, NullRedactor is the default —
  it is a transparent passthrough that satisfies the same interface.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional, Protocol

logger = logging.getLogger(__name__)

# ── MDR / GST rate table (simplified) ─────────────────────────────────────────
# In production these would come from a config table per acquirer.
_MDR_RATES: dict[str, Decimal] = {
    "P2M": Decimal("0.009"),   # 0.90 %
    "P2P": Decimal("0.000"),   # 0 %
    "DEFAULT": Decimal("0.009"),
}
_GST_RATE = Decimal("0.18")   # 18 % on MDR

# ── Redactor protocol (swapped in Phase 4) ────────────────────────────────────


class RedactorProtocol(Protocol):
    def redact_dict(self, d: dict) -> dict:
        """Replace raw PII values with tokens."""
        ...

    def unredact_dict(self, d: dict) -> dict:
        """Resolve tokens back to raw values for DB queries."""
        ...


class NullRedactor:
    """Passthrough redactor — used until Phase 4 PII module is implemented."""

    def redact_dict(self, d: dict) -> dict:
        return d

    def unredact_dict(self, d: dict) -> dict:
        return d


# ── Tool step record (stored in agent_traces.steps) ──────────────────────────


@dataclass
class ToolStep:
    step: int
    tool_name: str
    input_hash: str       # SHA-256 of redacted input JSON
    output_hash: str      # SHA-256 of redacted output JSON
    latency_ms: int
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "step": self.step,
            "tool_name": self.tool_name,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "latency_ms": self.latency_ms,
            "error": self.error,
        }


# ── Anthropic tool schema definitions ─────────────────────────────────────────

TOOL_SCHEMAS: list[dict] = [
    {
        "name": "search_pg_transactions",
        "description": (
            "Search the internal payment ledger for PG transactions. "
            "Filter by RRN, UTR, amount range, and/or date range. "
            "Returns a list of matching transactions with amounts in paise."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "rrn": {
                    "type": "string",
                    "description": "Retrieval Reference Number (12 digits)",
                },
                "utr": {
                    "type": "string",
                    "description": "Unique Transaction Reference (bank-issued)",
                },
                "amount_min_paise": {
                    "type": "integer",
                    "description": "Minimum amount in paise (inclusive)",
                },
                "amount_max_paise": {
                    "type": "integer",
                    "description": "Maximum amount in paise (inclusive)",
                },
                "date_from": {
                    "type": "string",
                    "description": "ISO date YYYY-MM-DD (created_at >= date_from)",
                },
                "date_to": {
                    "type": "string",
                    "description": "ISO date YYYY-MM-DD (created_at <= date_to)",
                },
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "get_settlement_history",
        "description": (
            "Get all settlement file lines that share the same RRN. "
            "Useful for detecting duplicate settlement entries across files."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "rrn": {
                    "type": "string",
                    "description": "RRN to look up across all settlement files",
                },
            },
            "required": ["rrn"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_chargeback_status",
        "description": (
            "Check whether a transaction has an associated chargeback or reversal. "
            "Returns chargeback flag, amount, and processed timestamp."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "rrn": {
                    "type": "string",
                    "description": "RRN of the transaction to check",
                },
            },
            "required": ["rrn"],
            "additionalProperties": False,
        },
    },
    {
        "name": "compute_fee_breakdown",
        "description": (
            "Compute the MDR fee and GST for a transaction amount and type. "
            "Use this to verify whether an amount difference is explained by fee rounding."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "amount_paise": {
                    "type": "integer",
                    "description": "Gross transaction amount in paise",
                },
                "txn_type": {
                    "type": "string",
                    "description": "Transaction type: P2M (Peer-to-Merchant) or P2P (Peer-to-Peer)",
                },
            },
            "required": ["amount_paise", "txn_type"],
            "additionalProperties": False,
        },
    },
    {
        "name": "propose_resolution",
        "description": (
            "Submit your final resolution for this reconciliation case. "
            "Call EXACTLY ONCE when your investigation is complete. "
            "The system will persist your resolution and close the case."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "case_uid": {
                    "type": "string",
                    "description": "UUID of the recon case (from the input)",
                },
                "resolution_type": {
                    "type": "string",
                    "enum": ["MATCH", "AMOUNT_MISMATCH", "MISSING_LEG",
                             "DUPLICATE", "WRITE_OFF", "ESCALATE"],
                    "description": "Resolution category",
                },
                "confidence": {
                    "type": "number",
                    "description": "Confidence score between 0.0 and 1.0",
                },
                "rationale": {
                    "type": "string",
                    "description": "1–3 sentence explanation. No raw PII.",
                },
                "pg_txn_id": {
                    "type": "integer",
                    "description": "Matched internal PG transaction ID (omit if none found)",
                },
            },
            "required": ["case_uid", "resolution_type", "confidence", "rationale"],
            "additionalProperties": False,
        },
    },
]

# Fast lookup by name
TOOL_SCHEMA_MAP: dict[str, dict] = {t["name"]: t for t in TOOL_SCHEMAS}

# The only tool that mutates state — orchestrator intercepts this one
MUTATING_TOOL = "propose_resolution"


# ── ToolRegistry ──────────────────────────────────────────────────────────────


class ToolRegistry:
    """
    Implements each tool as an async method.

    The orchestrator calls dispatch(name, raw_input) for every tool_use block.
    propose_resolution is intercepted by the orchestrator BEFORE reaching here;
    dispatch() will raise if it arrives (defensive programming).

    All inputs are unredacted before the DB query;
    all outputs are redacted before returning to the LLM.
    """

    def __init__(self, repo, redactor: Optional[RedactorProtocol] = None) -> None:
        self._repo = repo
        self._redactor: RedactorProtocol = redactor or NullRedactor()
        self._steps: list[ToolStep] = []

    def reset_steps(self) -> None:
        self._steps = []

    @property
    def steps(self) -> list[ToolStep]:
        return list(self._steps)

    async def dispatch(self, name: str, raw_input: dict, step_no: int) -> str:
        """
        Route a tool call to the appropriate implementation.
        Returns a JSON string to be sent back as the tool_result.
        """
        if name == MUTATING_TOOL:
            raise ValueError("propose_resolution must be intercepted by the orchestrator")

        handler = {
            "search_pg_transactions": self._search_pg_transactions,
            "get_settlement_history": self._get_settlement_history,
            "get_chargeback_status": self._get_chargeback_status,
            "compute_fee_breakdown": self._compute_fee_breakdown,
        }.get(name)

        if handler is None:
            return json.dumps({"error": f"Unknown tool: {name}"})

        inputs = self._redactor.unredact_dict(raw_input)
        input_hash = _hash_dict(raw_input)
        t0 = time.monotonic()
        error: Optional[str] = None

        try:
            result = await handler(inputs)
        except Exception as exc:
            logger.warning("Tool %s failed: %s", name, exc)
            result = {"error": str(exc)}
            error = str(exc)

        latency_ms = int((time.monotonic() - t0) * 1000)
        redacted_result = self._redactor.redact_dict(result)
        output_json = json.dumps(redacted_result)
        output_hash = _hash_str(output_json)

        self._steps.append(ToolStep(
            step=step_no,
            tool_name=name,
            input_hash=input_hash,
            output_hash=output_hash,
            latency_ms=latency_ms,
            error=error,
        ))

        return output_json

    # ── Tool implementations ──────────────────────────────────────────────────

    async def _search_pg_transactions(self, args: dict) -> dict:
        rrn = args.get("rrn")
        utr = args.get("utr")
        amount_min = args.get("amount_min_paise")
        amount_max = args.get("amount_max_paise")
        date_from = args.get("date_from")
        date_to = args.get("date_to")

        txns = await self._repo.search_pg_transactions(
            rrn=rrn,
            utr=utr,
            amount_min=amount_min,
            amount_max=amount_max,
            date_from=date_from,
            date_to=date_to,
        )
        return {"transactions": txns, "count": len(txns)}

    async def _get_settlement_history(self, args: dict) -> dict:
        rrn = args["rrn"]
        rows = await self._repo.fetch_settlement_history_by_rrn(rrn)
        return {"settlement_lines": rows, "count": len(rows)}

    async def _get_chargeback_status(self, args: dict) -> dict:
        rrn = args["rrn"]
        # Phase 3: stub — no chargeback table yet.
        # Returns false unless a PARTIAL_REVERSED PG txn exists for this RRN.
        txns = await self._repo.lookup_pg_txns(rrn=rrn, utr=None)
        reversed_txn = next(
            (t for t in txns if t.status == "PARTIAL_REVERSED"), None
        )
        if reversed_txn:
            return {
                "chargeback": True,
                "amount_paise": reversed_txn.amount_paise,
                "processed_at": reversed_txn.updated_at.isoformat(),
            }
        return {"chargeback": False, "amount_paise": None, "processed_at": None}

    async def _compute_fee_breakdown(self, args: dict) -> dict:
        amount_paise = int(args["amount_paise"])
        txn_type = str(args.get("txn_type", "DEFAULT")).upper()

        mdr_rate = _MDR_RATES.get(txn_type, _MDR_RATES["DEFAULT"])
        amount = Decimal(amount_paise)
        mdr = int((amount * mdr_rate).to_integral_value())
        gst = int((Decimal(mdr) * _GST_RATE).to_integral_value())
        net = amount_paise - mdr - gst

        return {
            "amount_paise": amount_paise,
            "txn_type": txn_type,
            "mdr_rate_pct": float(mdr_rate * 100),
            "mdr_paise": mdr,
            "gst_paise": gst,
            "net_paise": net,
        }


# ── Helpers ───────────────────────────────────────────────────────────────────


def _hash_dict(d: dict) -> str:
    return hashlib.sha256(
        json.dumps(d, sort_keys=True).encode()
    ).hexdigest()[:16]


def _hash_str(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:16]
