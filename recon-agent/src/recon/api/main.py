"""
FastAPI service — LLD §6.9 / §7.

Endpoints
  GET  /api/v1/cases                   — paginated case list
  GET  /api/v1/cases/{case_uid}        — single case + last agent trace
  POST /api/v1/cases/{case_uid}/approve
  POST /api/v1/cases/{case_uid}/reject
  GET  /healthz                        — public health check
  GET  /metrics                        — Prometheus text (populated by obs module)

Cross-cutting
  • X-Request-ID propagation
  • Bearer token auth (all routes except /healthz)
  • In-memory rate limiter: 60 rpm per token
"""
from __future__ import annotations

import base64
import json
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr

from recon.config import settings
from recon.ledger.repo import LedgerRepo, create_pool

# ── Rate limiter state ────────────────────────────────────────────────────────

_rate_hits: dict[str, list[float]] = defaultdict(list)
_RATE_WINDOW_S = 60
_RATE_LIMIT = 60


# ── Lifespan ──────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await create_pool(settings.database_url)
    app.state.pool = pool
    app.state.repo = LedgerRepo(pool)
    yield
    await pool.close()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="Recon Agent API", version="1.0", lifespan=lifespan)


# ── Middleware: X-Request-ID ──────────────────────────────────────────────────


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    response = await call_next(request)
    response.headers["X-Request-ID"] = req_id
    return response


# ── Exception handler: standard error envelope ───────────────────────────────


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    req_id = request.headers.get("X-Request-ID", "")
    detail = exc.detail
    if isinstance(detail, dict):
        body = {"error": {**detail, "request_id": req_id}}
    else:
        body = {"error": {"code": _status_code(exc.status_code), "message": str(detail), "request_id": req_id}}
    return JSONResponse(status_code=exc.status_code, content=body)


def _status_code(http_status: int) -> str:
    return {
        400: "BAD_REQUEST",
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        409: "CONFLICT",
        422: "UNPROCESSABLE",
        429: "RATE_LIMITED",
        500: "INTERNAL_ERROR",
        503: "SERVICE_UNAVAILABLE",
    }.get(http_status, "ERROR")


# ── Dependencies ──────────────────────────────────────────────────────────────


def get_repo(request: Request) -> LedgerRepo:
    return request.app.state.repo


def require_auth(request: Request) -> None:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != settings.api_bearer_token:
        raise HTTPException(status_code=401, detail="Unauthorized")


def rate_limit(request: Request) -> None:
    token = request.headers.get("Authorization", "anonymous")
    now = time.time()
    hits = [t for t in _rate_hits[token] if now - t < _RATE_WINDOW_S]
    if len(hits) >= _RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    hits.append(now)
    _rate_hits[token] = hits


# ── Request / response schemas ────────────────────────────────────────────────


class ApprovalBody(BaseModel):
    reviewer_email: str
    comment: str = ""


# ── Formatters ────────────────────────────────────────────────────────────────


def _fmt_case(row: dict) -> dict:
    return {
        "case_uid": str(row["case_uid"]),
        "match_type": row["match_type"],
        "confidence": float(row["confidence"]) if row.get("confidence") is not None else None,
        "resolution": row.get("resolution"),
        "settlement_line_id": row.get("settlement_line"),
        "pg_transaction_id": row.get("pg_transaction"),
        "notes": row.get("notes") or {},
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "resolved_at": row["resolved_at"].isoformat() if row.get("resolved_at") else None,
    }


def _fmt_trace(row: dict) -> dict:
    return {
        "model": row.get("model"),
        "tools_used": list(row.get("tools_used") or []),
        "total_ms": row.get("total_ms"),
        "cost_usd": float(row["cost_usd"]) if row.get("cost_usd") is not None else None,
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
    }


def _decode_cursor(cursor: str) -> int:
    try:
        data = json.loads(base64.b64decode(cursor).decode())
        return int(data["id"])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


def _encode_cursor(last_id: int) -> str:
    return base64.b64encode(json.dumps({"id": last_id}).encode()).decode()


def _parse_uid(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid case_uid format")


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/api/v1/cases")
async def list_cases(
    request: Request,
    status: Optional[str] = None,
    match_type: Optional[str] = None,
    cursor: Optional[str] = None,
    limit: int = 50,
    repo: LedgerRepo = Depends(get_repo),
    _auth: None = Depends(require_auth),
    _rl: None = Depends(rate_limit),
):
    limit = min(limit, 200)
    after_id = _decode_cursor(cursor) if cursor else None
    rows = await repo.fetch_recon_cases(status, match_type, after_id, limit)
    next_cursor = _encode_cursor(rows[-1]["id"]) if len(rows) == limit else None
    return {"cases": [_fmt_case(r) for r in rows], "next_cursor": next_cursor}


@app.get("/api/v1/cases/{case_uid}")
async def get_case(
    case_uid: str,
    repo: LedgerRepo = Depends(get_repo),
    _auth: None = Depends(require_auth),
    _rl: None = Depends(rate_limit),
):
    uid = _parse_uid(case_uid)
    row = await repo.fetch_recon_case_by_uid(uid)
    if row is None:
        raise HTTPException(status_code=404, detail="Case not found")
    result = _fmt_case(row)
    trace = await repo.fetch_last_agent_trace(uid)
    if trace:
        result["last_agent_trace"] = _fmt_trace(trace)
    return result


@app.post("/api/v1/cases/{case_uid}/approve", status_code=200)
async def approve_case(
    case_uid: str,
    body: ApprovalBody,
    repo: LedgerRepo = Depends(get_repo),
    _auth: None = Depends(require_auth),
    _rl: None = Depends(rate_limit),
):
    uid = _parse_uid(case_uid)
    updated = await repo.resolve_recon_case(
        uid, "APPROVED", f"human:{body.reviewer_email}", body.comment
    )
    if not updated:
        row = await repo.fetch_recon_case_by_uid(uid)
        if row is None:
            raise HTTPException(status_code=404, detail="Case not found")
        raise HTTPException(
            status_code=409,
            detail={"code": "STALE_RESOLUTION", "message": f"Current resolution is {row['resolution']!r}; expected PROPOSED"},
        )
    return {"status": "ok"}


@app.post("/api/v1/cases/{case_uid}/reject", status_code=200)
async def reject_case(
    case_uid: str,
    body: ApprovalBody,
    repo: LedgerRepo = Depends(get_repo),
    _auth: None = Depends(require_auth),
    _rl: None = Depends(rate_limit),
):
    uid = _parse_uid(case_uid)
    updated = await repo.resolve_recon_case(
        uid, "REJECTED", f"human:{body.reviewer_email}", body.comment
    )
    if not updated:
        row = await repo.fetch_recon_case_by_uid(uid)
        if row is None:
            raise HTTPException(status_code=404, detail="Case not found")
        raise HTTPException(
            status_code=409,
            detail={"code": "STALE_RESOLUTION", "message": f"Current resolution is {row['resolution']!r}; expected PROPOSED"},
        )
    return {"status": "ok"}


@app.get("/healthz")
async def healthz(request: Request):
    db_status = "error"
    try:
        async with request.app.state.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        db_status = "ok"
    except Exception:
        pass

    return {
        "db": db_status,
        "kafka": "configured" if settings.kafka_brokers else "not_configured",
        "llm": "configured" if settings.anthropic_api_key else "not_configured",
    }


@app.get("/metrics")
async def metrics():
    # Full Prometheus exposition will be wired in obs/metrics.py (Phase 4).
    return Response(content="# Prometheus metrics not yet enabled\n", media_type="text/plain")
